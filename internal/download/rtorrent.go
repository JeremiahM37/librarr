package download

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/netutil"
)

// rtorrentMaxResponse caps an XML-RPC reply. A large session's torrent list is
// megabytes at most, so this only stops a misbehaving endpoint.
const rtorrentMaxResponse = 64 << 20

// RTorrentClient talks XML-RPC to rTorrent and satisfies TorrentClient.
//
// rTorrent serves XML-RPC over SCGI, so RTORRENT_URL points at whatever exposes
// it over HTTP: a web server, or ruTorrent. Librarr scopes its torrents with the
// d.custom1 field, which ruTorrent shows as the label. A finished torrent may
// have been moved by the user's rTorrent config, so paths are always read back
// from the client (d.base_path) and never derived from what was requested.
type RTorrentClient struct {
	cfg            *config.Config
	client         *http.Client
	fetcher        *QBittorrentClient // shares the guarded Prowlarr .torrent fetch
	mu             sync.Mutex
	verifyTimeout  time.Duration
	verifyInterval time.Duration
}

// NewRTorrentClient creates a new rTorrent XML-RPC client.
func NewRTorrentClient(cfg *config.Config) *RTorrentClient {
	return &RTorrentClient{
		cfg:            cfg,
		client:         &http.Client{Timeout: 30 * time.Second},
		fetcher:        NewQBittorrentClient(cfg),
		verifyTimeout:  25 * time.Second,
		verifyInterval: 500 * time.Millisecond,
	}
}

// Name identifies this client in logs and the TorrentClient interface.
func (r *RTorrentClient) Name() string { return "rtorrent" }

// AddTorrent submits a magnet or torrent URL. The requested savePath is not
// used: every caller passes its qBittorrent-oriented default, while rTorrent's
// directory is RTORRENT_DIRECTORY or, when empty, its own directory.default.
//
// load.start reports success before rTorrent has fetched anything, so a bad URL
// only shows up as a torrent that never appears. A Prowlarr URL is therefore
// fetched here and uploaded, and the result is checked by infohash when known.
func (r *RTorrentClient) AddTorrent(torrentURL, title, savePath, category, expectedInfoHash string) error {
	if category == "" {
		category = r.cfg.QBCategory
	}
	expectedInfoHash = firstNonEmptyHash(expectedInfoHash, infoHashFromMagnet(torrentURL))
	commands := r.loadCommands(category)
	logTitle := netutil.SanitizeLogValue(title)

	var err error
	if isHTTPURL(torrentURL) && r.fetcher.isProwlarrTorrentURL(torrentURL) {
		var fetched fetchedTorrent
		if fetched, err = r.fetcher.fetchTorrent(torrentURL); err != nil {
			return err
		}
		if fetched.magnetURL != "" {
			expectedInfoHash = infoHashFromMagnet(fetched.magnetURL)
			_, err = r.call("load.start", append([]interface{}{"", fetched.magnetURL}, commands...)...)
		} else {
			expectedInfoHash = fetched.infoHash
			_, err = r.call("load.raw_start", append([]interface{}{"", rtorrentBase64(fetched.body)}, commands...)...)
		}
	} else {
		_, err = r.call("load.start", append([]interface{}{"", torrentURL}, commands...)...)
	}
	if err != nil {
		return fmt.Errorf("rtorrent add torrent: %w", err)
	}

	if expectedInfoHash != "" {
		if err := r.waitForTorrent(expectedInfoHash); err != nil {
			return &TorrentVerificationWarning{Err: err}
		}
	}
	slog.Info("torrent added to rTorrent", "title", logTitle, "category", netutil.SanitizeLogValue(category))
	return nil
}

// loadCommands are the commands rTorrent runs on a torrent as it loads.
func (r *RTorrentClient) loadCommands(category string) []interface{} {
	var commands []interface{}
	if category != "" {
		// ruTorrent stores labels URL-encoded, and d.custom1 is that same field.
		commands = append(commands, `d.custom1.set=`+rtorrentQuote(url.PathEscape(category)))
	}
	if dir := r.cfg.RTorrentDirectory; dir != "" {
		commands = append(commands, `d.directory.set=`+rtorrentQuote(dir))
	}
	return commands
}

func (r *RTorrentClient) waitForTorrent(hash string) error {
	deadline := time.Now().Add(r.verifyTimeout)
	for {
		rows, err := r.listTorrents()
		if err == nil {
			for _, row := range rows {
				if strings.EqualFold(row.hash, hash) {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for torrent to appear in rTorrent")
		}
		time.Sleep(r.verifyInterval)
	}
}

// rtorrentRow is one d.multicall2 row.
type rtorrentRow struct {
	hash, name, label, basePath, directory string
	size, completed, downRate              int64
	state, active, open, complete          bool
	hashing, multiFile, meta               bool
}

var rtorrentFields = []interface{}{
	"", "main",
	"d.hash=", "d.name=", "d.custom1=", "d.base_path=", "d.directory=",
	"d.size_bytes=", "d.completed_bytes=", "d.down.rate=",
	"d.state=", "d.is_active=", "d.is_open=", "d.complete=",
	"d.hashing=", "d.is_multi_file=", "d.is_meta=",
}

func (r *RTorrentClient) listTorrents() ([]rtorrentRow, error) {
	result, err := r.call("d.multicall2", rtorrentFields...)
	if err != nil {
		return nil, err
	}
	rows, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("rtorrent: unexpected d.multicall2 result")
	}
	out := make([]rtorrentRow, 0, len(rows))
	for _, raw := range rows {
		f, ok := raw.([]interface{})
		if !ok || len(f) != len(rtorrentFields)-2 {
			return nil, errors.New("rtorrent: unexpected d.multicall2 row")
		}
		label := rtorrentString(f[2])
		if decoded, err := url.PathUnescape(label); err == nil {
			label = decoded
		}
		out = append(out, rtorrentRow{
			hash:      rtorrentString(f[0]),
			name:      rtorrentString(f[1]),
			label:     label,
			basePath:  rtorrentString(f[3]),
			directory: rtorrentString(f[4]),
			size:      rtorrentInt(f[5]),
			completed: rtorrentInt(f[6]),
			downRate:  rtorrentInt(f[7]),
			state:     rtorrentInt(f[8]) != 0,
			active:    rtorrentInt(f[9]) != 0,
			open:      rtorrentInt(f[10]) != 0,
			complete:  rtorrentInt(f[11]) != 0,
			hashing:   rtorrentInt(f[12]) != 0,
			multiFile: rtorrentInt(f[13]) != 0,
			meta:      rtorrentInt(f[14]) != 0,
		})
	}
	return out, nil
}

// GetTorrents returns Librarr-scoped torrents, optionally filtered to a single
// category (d.custom1). State is reported using the qBittorrent vocabulary so
// callers can keep using MapTorrentStatus.
func (r *RTorrentClient) GetTorrents(category string) ([]TorrentInfo, error) {
	rows, err := r.listTorrents()
	if err != nil {
		return nil, err
	}
	var out []TorrentInfo
	for _, row := range rows {
		if category != "" && row.label != category {
			continue
		}
		content := row.contentPath()
		var progress float64
		// A magnet's placeholder is "complete" once its metadata arrives. It holds
		// no payload, so it must never read as ready to import.
		if row.complete && !row.meta {
			progress = 1
		} else if row.size > 0 && !row.meta {
			progress = min(float64(row.completed)/float64(row.size), 0.9999)
		}
		out = append(out, TorrentInfo{
			Name:        row.name,
			Hash:        strings.ToLower(row.hash),
			State:       row.qbitState(),
			Progress:    progress,
			TotalSize:   row.size,
			DlSpeed:     row.downRate,
			Category:    row.label,
			SavePath:    path.Dir(content),
			ContentPath: content,
		})
	}
	return out, nil
}

// contentPath is where the torrent's data is now. d.base_path is empty while a
// torrent is closed; d.directory already names a multi-file torrent's own folder.
func (row rtorrentRow) contentPath() string {
	if row.basePath != "" {
		return row.basePath
	}
	if row.multiFile {
		return row.directory
	}
	return path.Join(row.directory, row.name)
}

// qbitState converts rTorrent's flags into the qBittorrent state vocabulary so
// MapTorrentStatus works uniformly across backends. d.message is deliberately
// not consulted: it mostly carries tracker errors, which do not stop a torrent.
func (row rtorrentRow) qbitState() string {
	switch {
	case row.meta:
		return "metaDL"
	case row.hashing && row.complete:
		return "checkingUP"
	case row.hashing:
		return "checkingDL"
	case row.complete && row.state && row.active:
		return "uploading"
	case row.complete:
		return "pausedUP"
	case row.state && row.active:
		return "downloading"
	default:
		return "pausedDL"
	}
}

// GetTorrentFiles lists the files inside a torrent identified by hash.
func (r *RTorrentClient) GetTorrentFiles(hash string) ([]TorrentFile, error) {
	result, err := r.call("f.multicall", strings.ToUpper(hash), "", "f.path=")
	if err != nil {
		return nil, err
	}
	rows, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("rtorrent: unexpected f.multicall result")
	}
	files := make([]TorrentFile, 0, len(rows))
	for _, raw := range rows {
		if f, ok := raw.([]interface{}); ok && len(f) == 1 {
			files = append(files, TorrentFile{Name: rtorrentString(f[0])})
		}
	}
	return files, nil
}

// DeleteTorrent removes a torrent by hash. rTorrent's API cannot delete data, so
// deleteFiles removes the payload from Librarr's own view of the download folder,
// and says so when that folder does not hold it.
func (r *RTorrentClient) DeleteTorrent(hash string, deleteFiles bool) error {
	var payload string
	if deleteFiles {
		rows, err := r.listTorrents()
		if err != nil {
			return err
		}
		for _, row := range rows {
			if strings.EqualFold(row.hash, hash) {
				payload = r.localPayload(row)
				break
			}
		}
	}
	if _, err := r.call("d.erase", strings.ToUpper(hash)); err != nil {
		return fmt.Errorf("rtorrent remove torrent: %w", err)
	}
	if !deleteFiles {
		return nil
	}
	if payload == "" {
		slog.Warn("rTorrent cannot delete data and the payload is not in Librarr's download folder; it was left in place", "hash", hash)
		return nil
	}
	return os.RemoveAll(payload)
}

// localPayload locates a torrent's data under Librarr's own download folder for
// its category. Only the final path element comes from the client, so the result
// is always inside a folder the operator configured for downloads.
func (r *RTorrentClient) localPayload(row rtorrentRow) string {
	name := path.Base(row.contentPath())
	if name == "" || name == "." || name == ".." || name == "/" {
		return ""
	}
	incoming := r.cfg.IncomingDir
	switch {
	case row.label == r.cfg.QBAudiobookCategory && r.cfg.QBAudiobookSavePath != "":
		incoming = r.cfg.QBAudiobookSavePath
	case row.label == r.cfg.QBMangaCategory && r.cfg.MangaIncomingDir != "":
		incoming = r.cfg.MangaIncomingDir
	}
	if incoming == "" {
		return ""
	}
	candidate := filepath.Join(incoming, name)
	if _, err := os.Lstat(candidate); err != nil {
		return ""
	}
	return candidate
}

// Diagnose tests the connection for the settings "Test" button.
func (r *RTorrentClient) Diagnose() map[string]interface{} {
	if !r.cfg.HasRTorrent() {
		return map[string]interface{}{"success": false, "error": "rTorrent not configured"}
	}
	version, err := r.call("system.client_version")
	if err != nil {
		return map[string]interface{}{"success": false, "error": netutil.SanitizeSensitiveText(err.Error())}
	}
	return map[string]interface{}{"success": true, "version": rtorrentString(version)}
}

// endpoint returns the XML-RPC URL. A URL with no path means the conventional /RPC2.
func (r *RTorrentClient) endpoint() string {
	if u, err := url.Parse(r.cfg.RTorrentURL); err == nil && (u.Path == "" || u.Path == "/") {
		return strings.TrimRight(r.cfg.RTorrentURL, "/") + "/RPC2"
	}
	return r.cfg.RTorrentURL
}

// call performs one XML-RPC call and returns its decoded value.
func (r *RTorrentClient) call(method string, params ...interface{}) (interface{}, error) {
	body, err := rtorrentEncodeCall(method, params)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, r.endpoint(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/xml")
	if r.cfg.RTorrentUser != "" {
		req.SetBasicAuth(r.cfg.RTorrentUser, r.cfg.RTorrentPass)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rtorrent request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("rtorrent: authentication failed")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("rtorrent: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, rtorrentMaxResponse))
	if err != nil {
		return nil, err
	}
	return rtorrentDecodeResponse(data)
}

// rtorrentBase64 marks bytes to send as an XML-RPC <base64> value.
type rtorrentBase64 []byte

// rtorrentQuote quotes a command argument, which rTorrent would otherwise split
// on commas and spaces.
func rtorrentQuote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

func rtorrentEncodeCall(method string, params []interface{}) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0"?><methodCall><methodName>`)
	if err := xml.EscapeText(&b, []byte(method)); err != nil {
		return nil, err
	}
	b.WriteString(`</methodName><params>`)
	for _, p := range params {
		b.WriteString(`<param><value>`)
		switch v := p.(type) {
		case string:
			b.WriteString(`<string>`)
			if err := xml.EscapeText(&b, []byte(v)); err != nil {
				return nil, err
			}
			b.WriteString(`</string>`)
		case rtorrentBase64:
			b.WriteString(`<base64>` + base64.StdEncoding.EncodeToString(v) + `</base64>`)
		default:
			return nil, fmt.Errorf("rtorrent: unsupported parameter %T", p)
		}
		b.WriteString(`</value></param>`)
	}
	b.WriteString(`</params></methodCall>`)
	return b.Bytes(), nil
}

// rtorrentValue mirrors an XML-RPC <value>.
type rtorrentValue struct {
	Str    *string          `xml:"string"`
	I4     *string          `xml:"i4"`
	I8     *string          `xml:"i8"`
	Int    *string          `xml:"int"`
	Bool   *string          `xml:"boolean"`
	Array  *[]rtorrentValue `xml:"array>data>value"`
	Struct []struct {
		Name  string        `xml:"name"`
		Value rtorrentValue `xml:"value"`
	} `xml:"struct>member"`
	Text string `xml:",chardata"`
}

func (v rtorrentValue) decode() interface{} {
	for _, n := range []*string{v.I8, v.I4, v.Int, v.Bool} {
		if n != nil {
			i, _ := strconv.ParseInt(strings.TrimSpace(*n), 10, 64)
			return i
		}
	}
	if v.Str != nil {
		return *v.Str
	}
	if v.Array != nil {
		out := make([]interface{}, len(*v.Array))
		for i, item := range *v.Array {
			out[i] = item.decode()
		}
		return out
	}
	if len(v.Struct) > 0 {
		out := make(map[string]interface{}, len(v.Struct))
		for _, m := range v.Struct {
			out[m.Name] = m.Value.decode()
		}
		return out
	}
	// XML-RPC treats a bare <value> as a string.
	return v.Text
}

func rtorrentDecodeResponse(data []byte) (interface{}, error) {
	// Torrent names are arbitrary bytes, and one invalid name must not take the
	// whole list down with it.
	data = bytes.ToValidUTF8(data, []byte("�"))
	var resp struct {
		Params []struct {
			Value rtorrentValue `xml:"value"`
		} `xml:"params>param"`
		Fault *struct {
			Value rtorrentValue `xml:"value"`
		} `xml:"fault"`
	}
	if err := xml.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("rtorrent: invalid XML-RPC response: %w", err)
	}
	if resp.Fault != nil {
		fault, _ := resp.Fault.Value.decode().(map[string]interface{})
		return nil, fmt.Errorf("rtorrent: %s", rtorrentString(fault["faultString"]))
	}
	if len(resp.Params) == 0 {
		return nil, errors.New("rtorrent: empty XML-RPC response")
	}
	return resp.Params[0].Value.decode(), nil
}

func rtorrentString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	}
	return ""
}

func rtorrentInt(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case string:
		i, _ := strconv.ParseInt(t, 10, 64)
		return i
	}
	return 0
}
