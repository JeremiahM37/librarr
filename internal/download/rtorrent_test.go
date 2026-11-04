package download

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

// xmlCall is one decoded XML-RPC request the mock received.
type xmlCall struct {
	method string
	params []string
	base64 []string
}

// mockRTorrent answers XML-RPC from canned d.multicall2 rows.
type mockRTorrent struct {
	mu       sync.Mutex
	calls    []xmlCall
	rows     string
	fault    string
	user     string
	pass     string
	srv      *httptest.Server
	loadRows string // rows after a load.* call
}

func newMockRTorrent(t *testing.T) *mockRTorrent {
	t.Helper()
	m := &mockRTorrent{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.user != "" {
			if u, p, ok := r.BasicAuth(); !ok || u != m.user || p != m.pass {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		var req struct {
			Method string `xml:"methodName"`
			Params []struct {
				Str    *string `xml:"value>string"`
				Base64 *string `xml:"value>base64"`
			} `xml:"params>param"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := xml.Unmarshal(body, &req); err != nil {
			t.Errorf("request is not XML: %v: %s", err, body)
		}
		call := xmlCall{method: req.Method}
		for _, p := range req.Params {
			if p.Str != nil {
				call.params = append(call.params, *p.Str)
			}
			if p.Base64 != nil {
				call.base64 = append(call.base64, *p.Base64)
			}
		}
		m.calls = append(m.calls, call)
		if m.fault != "" {
			fmt.Fprintf(w, `<?xml version="1.0"?><methodResponse><fault><value><struct><member><name>faultCode</name><value><i4>-501</i4></value></member><member><name>faultString</name><value><string>%s</string></value></member></struct></value></fault></methodResponse>`, m.fault)
			return
		}
		value := `<i8>0</i8>`
		switch {
		case req.Method == "d.multicall2":
			value = `<array><data>` + m.rows + `</data></array>`
		case req.Method == "f.multicall":
			value = `<array><data><value><array><data><value><string>Book/a.epub</string></value></data></array></value><value><array><data><value><string>Book/b.epub</string></value></data></array></value></data></array>`
		case req.Method == "system.client_version":
			value = `<string>0.9.8</string>`
		case strings.HasPrefix(req.Method, "load.") && m.loadRows != "":
			m.rows = m.loadRows
		}
		fmt.Fprintf(w, `<?xml version="1.0"?><methodResponse><params><param><value>%s</value></param></params></methodResponse>`, value)
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockRTorrent) client(cfg *config.Config) *RTorrentClient {
	cfg.RTorrentURL = m.srv.URL + "/RPC2"
	r := NewRTorrentClient(cfg)
	r.verifyTimeout = 50 * time.Millisecond
	r.verifyInterval = 5 * time.Millisecond
	return r
}

func (m *mockRTorrent) last(method string) (xmlCall, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.calls) - 1; i >= 0; i-- {
		if m.calls[i].method == method {
			return m.calls[i], true
		}
	}
	return xmlCall{}, false
}

// rtRow renders one d.multicall2 row in rtorrentFields order.
func rtRow(hash, name, label, basePath, directory string, size, completed, rate, state, active, open, complete, hashing, multi, meta int) string {
	s := func(v string) string { return `<value><string>` + v + `</string></value>` }
	i := func(v int) string { return fmt.Sprintf(`<value><i8>%d</i8></value>`, v) }
	return `<value><array><data>` + s(hash) + s(name) + s(label) + s(basePath) + s(directory) +
		i(size) + i(completed) + i(rate) + i(state) + i(active) + i(open) + i(complete) + i(hashing) + i(multi) + i(meta) +
		`</data></array></value>`
}

const rtHash = "DD8255ECDC7CA55FB0BBF81323D87062DB1F6D1C"

func TestRTorrent_AddMagnetSetsLabelOnly(t *testing.T) {
	m := newMockRTorrent(t)
	magnet := "magnet:?xt=urn:btih:" + rtHash
	m.loadRows = rtRow(rtHash, "Book", "my books", "", "/dl", 10, 0, 0, 1, 1, 1, 0, 0, 0, 0)
	r := m.client(&config.Config{})

	if err := r.AddTorrent(magnet, "Book", "/qb/default/path", "my books", ""); err != nil {
		t.Fatalf("AddTorrent: %v", err)
	}
	call, ok := m.last("load.start")
	if !ok {
		t.Fatal("load.start not called")
	}
	want := []string{"", magnet, `d.custom1.set="my%20books"`}
	if strings.Join(call.params, "\n") != strings.Join(want, "\n") {
		t.Fatalf("params = %q, want %q (no directory unless RTORRENT_DIRECTORY is set)", call.params, want)
	}
}

func TestRTorrent_AddUsesConfiguredDirectoryQuoted(t *testing.T) {
	m := newMockRTorrent(t)
	r := m.client(&config.Config{RTorrentDirectory: `/data/my "books", new`})
	if err := r.AddTorrent("http://indexer/a.torrent", "Book", "", "librarr", ""); err != nil {
		t.Fatalf("AddTorrent: %v", err)
	}
	call, _ := m.last("load.start")
	if got := call.params[len(call.params)-1]; got != `d.directory.set="/data/my \"books\", new"` {
		t.Fatalf("directory command = %q", got)
	}
}

func TestRTorrent_AddReportsVerificationWarningWhenNeverSeen(t *testing.T) {
	m := newMockRTorrent(t)
	r := m.client(&config.Config{})
	err := r.AddTorrent("magnet:?xt=urn:btih:"+rtHash, "Book", "", "librarr", "")
	var warning *TorrentVerificationWarning
	if !asWarning(err, &warning) {
		t.Fatalf("err = %v, want a verification warning", err)
	}
}

func asWarning(err error, target **TorrentVerificationWarning) bool {
	w, ok := err.(*TorrentVerificationWarning)
	if ok {
		*target = w
	}
	return ok
}

func TestRTorrent_AddFromProwlarrUploadsBytes(t *testing.T) {
	torrent := []byte("d4:infod4:name4:Book6:lengthi1e12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee")
	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-bittorrent")
		w.Write(torrent)
	}))
	defer prowlarr.Close()

	m := newMockRTorrent(t)
	r := m.client(&config.Config{ProwlarrURL: prowlarr.URL})
	sum, _ := torrentInfoHash(torrent)
	m.loadRows = rtRow(strings.ToUpper(sum), "Book", "librarr", "", "/dl", 1, 0, 0, 1, 1, 1, 0, 0, 0, 0)

	if err := r.AddTorrent(prowlarr.URL+"/1/download", "Book", "", "librarr", ""); err != nil {
		t.Fatalf("AddTorrent: %v", err)
	}
	call, ok := m.last("load.raw_start")
	if !ok {
		t.Fatal("load.raw_start not called")
	}
	got, _ := base64.StdEncoding.DecodeString(call.base64[0])
	if string(got) != string(torrent) {
		t.Fatalf("uploaded %q", got)
	}
	if call.params[1] != `d.custom1.set="librarr"` {
		t.Fatalf("params = %q", call.params)
	}
}

func TestRTorrent_AddFromProwlarrRedirectingToMagnet(t *testing.T) {
	magnet := "magnet:?xt=urn:btih:" + rtHash
	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", magnet)
		w.WriteHeader(http.StatusFound)
	}))
	defer prowlarr.Close()

	m := newMockRTorrent(t)
	m.loadRows = rtRow(rtHash, "Book", "librarr", "", "/dl", 1, 0, 0, 1, 1, 1, 0, 0, 0, 0)
	r := m.client(&config.Config{ProwlarrURL: prowlarr.URL})
	if err := r.AddTorrent(prowlarr.URL+"/1/download", "Book", "", "librarr", ""); err != nil {
		t.Fatalf("AddTorrent: %v", err)
	}
	if call, _ := m.last("load.start"); len(call.params) < 2 || call.params[1] != magnet {
		t.Fatalf("load.start = %+v", call)
	}
}

func TestRTorrent_GetTorrentsFiltersByLabelAndReadsLivePath(t *testing.T) {
	m := newMockRTorrent(t)
	m.rows = rtRow(rtHash, "Book", "my%20books", "/done/my books/Book", "/done/my books/Book", 100, 100, 0, 1, 1, 1, 1, 0, 1, 0) +
		rtRow("AAAA", "Theirs", "other", "/x/Theirs", "/x/Theirs", 100, 100, 0, 1, 1, 1, 1, 0, 1, 0)
	torrents, err := m.client(&config.Config{}).GetTorrents("my books")
	if err != nil {
		t.Fatalf("GetTorrents: %v", err)
	}
	if len(torrents) != 1 {
		t.Fatalf("got %d torrents: %+v", len(torrents), torrents)
	}
	got := torrents[0]
	if got.Hash != strings.ToLower(rtHash) || got.ContentPath != "/done/my books/Book" || got.SavePath != "/done/my books" || got.Progress != 1 || got.Category != "my books" {
		t.Fatalf("torrent = %+v", got)
	}
}

func TestRTorrent_ClosedTorrentPathFallback(t *testing.T) {
	multi := rtorrentRow{name: "Book", directory: "/dl/Book", multiFile: true}
	if got := multi.contentPath(); got != "/dl/Book" {
		t.Fatalf("multi-file = %q", got)
	}
	single := rtorrentRow{name: "b.epub", directory: "/dl"}
	if got := single.contentPath(); got != "/dl/b.epub" {
		t.Fatalf("single-file = %q", got)
	}
}

// A magnet's placeholder is complete as soon as its metadata arrives. Reading it
// as finished would send an empty folder to the importer.
func TestRTorrent_MetaPlaceholderIsNeverReadyToImport(t *testing.T) {
	m := newMockRTorrent(t)
	m.rows = rtRow(rtHash, rtHash+".meta", "librarr", "", "/dl", 5, 5, 0, 1, 1, 1, 1, 0, 0, 1)
	torrents, err := m.client(&config.Config{}).GetTorrents("librarr")
	if err != nil {
		t.Fatalf("GetTorrents: %v", err)
	}
	if torrents[0].Progress != 0 || torrents[0].State != "metaDL" {
		t.Fatalf("torrent = %+v", torrents[0])
	}
}

func TestRTorrent_IncompleteNeverReadsAsDone(t *testing.T) {
	m := newMockRTorrent(t)
	m.rows = rtRow(rtHash, "Book", "librarr", "/dl/Book", "/dl/Book", 100, 100, 0, 1, 1, 1, 0, 1, 1, 0)
	torrents, _ := m.client(&config.Config{}).GetTorrents("librarr")
	if torrents[0].Progress >= 1 {
		t.Fatalf("a torrent rTorrent has not marked complete read as done: %+v", torrents[0])
	}
}

func TestRTorrent_State(t *testing.T) {
	tests := []struct {
		name string
		row  rtorrentRow
		want string
	}{
		{"seeding", rtorrentRow{complete: true, state: true, active: true}, "uploading"},
		{"finished and stopped", rtorrentRow{complete: true}, "pausedUP"},
		{"downloading", rtorrentRow{state: true, active: true}, "downloading"},
		{"paused", rtorrentRow{state: true}, "pausedDL"},
		{"stopped", rtorrentRow{}, "pausedDL"},
		{"checking", rtorrentRow{hashing: true}, "checkingDL"},
		{"rechecking finished", rtorrentRow{hashing: true, complete: true}, "checkingUP"},
		{"metadata", rtorrentRow{meta: true, complete: true}, "metaDL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.row.qbitState(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRTorrent_GetTorrentFiles(t *testing.T) {
	m := newMockRTorrent(t)
	files, err := m.client(&config.Config{}).GetTorrentFiles(strings.ToLower(rtHash))
	if err != nil {
		t.Fatalf("GetTorrentFiles: %v", err)
	}
	if len(files) != 2 || files[1].Name != "Book/b.epub" {
		t.Fatalf("files = %+v", files)
	}
	if call, _ := m.last("f.multicall"); call.params[0] != rtHash {
		t.Fatalf("hash sent as %q; rTorrent matches uppercase only", call.params[0])
	}
}

func TestRTorrent_DeleteKeepsDataUnlessAsked(t *testing.T) {
	incoming := t.TempDir()
	payload := filepath.Join(incoming, "Book")
	if err := os.MkdirAll(payload, 0o755); err != nil {
		t.Fatal(err)
	}
	m := newMockRTorrent(t)
	m.rows = rtRow(rtHash, "Book", "librarr", "/remote/Book", "/remote/Book", 1, 1, 0, 1, 1, 1, 1, 0, 1, 0)
	r := m.client(&config.Config{IncomingDir: incoming, QBCategory: "librarr"})

	if err := r.DeleteTorrent(strings.ToLower(rtHash), false); err != nil {
		t.Fatalf("DeleteTorrent: %v", err)
	}
	if call, _ := m.last("d.erase"); call.params[0] != rtHash {
		t.Fatalf("d.erase = %+v", call)
	}
	if _, err := os.Stat(payload); err != nil {
		t.Fatalf("payload removed without being asked: %v", err)
	}

	if err := r.DeleteTorrent(strings.ToLower(rtHash), true); err != nil {
		t.Fatalf("DeleteTorrent: %v", err)
	}
	if _, err := os.Stat(payload); !os.IsNotExist(err) {
		t.Fatalf("payload should be gone: %v", err)
	}
}

// The name comes from the client, so it must not be able to reach outside the
// download folder.
func TestRTorrent_LocalPayloadStaysInsideDownloadFolder(t *testing.T) {
	incoming := t.TempDir()
	r := NewRTorrentClient(&config.Config{IncomingDir: incoming})
	for _, base := range []string{"/", ".", "..", "/x/.."} {
		row := rtorrentRow{basePath: base}
		if got := r.localPayload(row); got != "" && !strings.HasPrefix(got, incoming+string(filepath.Separator)) {
			t.Fatalf("base %q resolved outside the folder: %q", base, got)
		}
		if got := r.localPayload(row); got == incoming {
			t.Fatalf("base %q resolved to the whole download folder", base)
		}
	}
}

func TestRTorrent_Diagnose(t *testing.T) {
	if res := NewRTorrentClient(&config.Config{}).Diagnose(); res["success"] != false {
		t.Fatalf("unconfigured = %v", res)
	}
	m := newMockRTorrent(t)
	if res := m.client(&config.Config{}).Diagnose(); res["success"] != true || res["version"] != "0.9.8" {
		t.Fatalf("result = %v", res)
	}
}

func TestRTorrent_BasicAuth(t *testing.T) {
	m := newMockRTorrent(t)
	m.user, m.pass = "u", "p"
	if res := m.client(&config.Config{RTorrentUser: "u", RTorrentPass: "p"}).Diagnose(); res["success"] != true {
		t.Fatalf("good credentials = %v", res)
	}
	res := m.client(&config.Config{RTorrentUser: "u", RTorrentPass: "wrong"}).Diagnose()
	if res["success"] != false || !strings.Contains(res["error"].(string), "authentication failed") {
		t.Fatalf("bad credentials = %v", res)
	}
}

func TestRTorrent_FaultIsAnError(t *testing.T) {
	m := newMockRTorrent(t)
	m.fault = "Could not find info-hash."
	_, err := m.client(&config.Config{}).GetTorrents("librarr")
	if err == nil || !strings.Contains(err.Error(), "Could not find info-hash") {
		t.Fatalf("err = %v", err)
	}
}

// One torrent with a non-UTF-8 name must not hide every other torrent.
func TestRTorrent_InvalidUTF8NameDoesNotBreakTheList(t *testing.T) {
	value, err := rtorrentDecodeResponse([]byte("<?xml version=\"1.0\"?><methodResponse><params><param><value><string>caf\xe9</string></value></param></params></methodResponse>"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.HasPrefix(value.(string), "caf") {
		t.Fatalf("value = %q", value)
	}
}

func TestRTorrent_EndpointDefaultsToRPC2(t *testing.T) {
	for in, want := range map[string]string{
		"http://rt":                 "http://rt/RPC2",
		"http://rt/":                "http://rt/RPC2",
		"http://rt/RPC2":            "http://rt/RPC2",
		"http://rt/custom/xmlrpc":   "http://rt/custom/xmlrpc",
		"http://user@rt:8000/plugs": "http://user@rt:8000/plugs",
	} {
		r := NewRTorrentClient(&config.Config{RTorrentURL: in})
		if got := r.endpoint(); got != want {
			t.Errorf("endpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRTorrent_EncodeEscapes(t *testing.T) {
	body, err := rtorrentEncodeCall("load.start", []interface{}{"", "http://x/a?b=1&c=<2>"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "b=1&amp;c=&lt;2&gt;") {
		t.Fatalf("not escaped: %s", body)
	}
}
