package api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/library"
)

const (
	opdsNavMIME  = "application/atom+xml;profile=opds-catalog;kind=navigation"
	opdsAcqMIME  = "application/atom+xml;profile=opds-catalog;kind=acquisition"
	opdsOSMIME   = "application/opensearchdescription+xml"
	opdsPageSize = 50
)

var formatMIMEs = map[string]string{
	"epub": "application/epub+zip",
	"pdf":  "application/pdf",
	"mobi": "application/x-mobipocket-ebook",
	"azw3": "application/x-mobi8-ebook",
	"mp3":  "audio/mpeg",
	"m4b":  "audio/mp4",
	"cbz":  "application/x-cbz",
	"cbr":  "application/x-cbr",
}

type opdsFeed struct {
	XMLName      xml.Name    `xml:"http://www.w3.org/2005/Atom feed"`
	XMLNSOPDS    string      `xml:"xmlns:opds,attr,omitempty"`
	XMLNSDC      string      `xml:"xmlns:dc,attr,omitempty"`
	XMLNSOpen    string      `xml:"xmlns:opensearch,attr,omitempty"`
	ID           string      `xml:"id"`
	Title        string      `xml:"title"`
	Updated      string      `xml:"updated"`
	Author       opdsAuthor  `xml:"author"`
	Links        []opdsLink  `xml:"link"`
	TotalResults int         `xml:"opensearch:totalResults"`
	ItemsPerPage int         `xml:"opensearch:itemsPerPage"`
	StartIndex   int         `xml:"opensearch:startIndex"`
	Entries      []opdsEntry `xml:"entry"`
}

type opdsEntry struct {
	Title     string       `xml:"title"`
	ID        string       `xml:"id"`
	Updated   string       `xml:"updated"`
	Authors   []opdsAuthor `xml:"author,omitempty"`
	Content   *opdsContent `xml:"content,omitempty"`
	Summary   string       `xml:"summary,omitempty"`
	Format    string       `xml:"dc:format,omitempty"`
	Publisher string       `xml:"dc:publisher,omitempty"`
	Issued    string       `xml:"dc:issued,omitempty"`
	Language  string       `xml:"dc:language,omitempty"`
	Links     []opdsLink   `xml:"link"`
}

type opdsAuthor struct {
	Name string `xml:"name"`
}

type opdsContent struct {
	Type string `xml:"type,attr,omitempty"`
	Text string `xml:",chardata"`
}

type opdsLink struct {
	Rel   string `xml:"rel,attr"`
	Href  string `xml:"href,attr"`
	Type  string `xml:"type,attr,omitempty"`
	Title string `xml:"title,attr,omitempty"`
}

type opdsOpenSearchDescription struct {
	XMLName        xml.Name            `xml:"http://a9.com/-/spec/opensearch/1.1/ OpenSearchDescription"`
	ShortName      string              `xml:"ShortName"`
	Description    string              `xml:"Description"`
	InputEncoding  string              `xml:"InputEncoding"`
	OutputEncoding string              `xml:"OutputEncoding"`
	URLs           []opdsOpenSearchURL `xml:"Url"`
}

type opdsOpenSearchURL struct {
	Type     string `xml:"type,attr"`
	Template string `xml:"template,attr"`
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

func opdsNow() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func opdsFeedOpen(feedID, title, kind, selfHref string, total, page int) string {
	mimeType := opdsNavMIME
	if kind == "acquisition" {
		mimeType = opdsAcqMIME
	}
	feed := newOPDSFeed(feedID, title, selfHref, mimeType, total, page)
	out, _ := xml.MarshalIndent(feed, "", "  ")
	return xml.Header + strings.TrimSuffix(string(out), "</feed>") + "\n"
}

func opdsNavEntry(entryID, title, content, href, mimeType string) string {
	entry := opdsEntry{
		Title:   title,
		ID:      "urn:librarr:" + entryID,
		Updated: opdsNow(),
		Content: &opdsContent{Type: "text", Text: content},
		Links:   []opdsLink{{Rel: "subsection", Href: href, Type: mimeType}},
	}
	out, _ := xml.MarshalIndent(entry, "  ", "  ")
	return string(out) + "\n"
}

func newOPDSFeed(feedID, title, selfHref, selfType string, total, page int) opdsFeed {
	if page < 1 {
		page = 1
	}
	return opdsFeed{
		XMLNSOPDS:    "http://opds-spec.org/2010/catalog",
		XMLNSDC:      "http://purl.org/dc/terms/",
		XMLNSOpen:    "http://a9.com/-/spec/opensearch/1.1/",
		ID:           "urn:librarr:" + feedID,
		Title:        title,
		Updated:      opdsNow(),
		Author:       opdsAuthor{Name: "Librarr"},
		Links:        []opdsLink{{Rel: "self", Href: selfHref, Type: selfType}, {Rel: "start", Href: "/opds", Type: opdsNavMIME}, {Rel: "search", Href: "/opds/opensearch.xml", Type: opdsOSMIME}},
		TotalResults: total,
		ItemsPerPage: opdsPageSize,
		StartIndex:   ((page - 1) * opdsPageSize) + 1,
	}
}

func (s *Server) requireOPDSAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authenticateOPDSRequest(w, r) {
			next(w, r)
		}
	}
}

func (s *Server) authenticateOPDSRequest(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg != nil && s.cfg.HasAPIKey() {
		apiKey := r.Header.Get("X-Api-Key")
		if apiKey == "" {
			apiKey = r.URL.Query().Get("apikey")
		}
		if apiKey != "" && apiKey == s.cfg.APIKey {
			return true
		}
	}
	username, password, ok := r.BasicAuth()
	if !ok {
		opdsUnauthorized(w)
		return false
	}
	user, err := s.db.GetUserByUsername(username)
	if err != nil || user == nil || !user.Enabled || !checkPassword(password, user.PasswordHash) {
		opdsUnauthorized(w)
		return false
	}
	return true
}

func opdsUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="Librarr OPDS"`)
	http.Error(w, "Authentication required", http.StatusUnauthorized)
}

func (s *Server) handleOPDSRoot(w http.ResponseWriter, r *http.Request) {
	summary, err := s.library().GetLibrarySummary(r.Context())
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	base := s.publicBaseURL(r)
	feed := newOPDSFeed("root", "Librarr", base+"/opds", opdsNavMIME, summary.TotalBooks, 1)
	absolutizeOPDSLinks(&feed, base)
	feed.Links = append(feed.Links, opdsLink{Rel: "http://opds-spec.org/sort/new", Href: base + "/opds/recent", Type: opdsAcqMIME})
	feed.Entries = []opdsEntry{
		opdsNavigationEntry("all-books", "All Books", fmt.Sprintf("%d books in your Librarr library", summary.TotalBooks), base+"/opds/books", opdsAcqMIME),
		opdsNavigationEntry("recent", "Recently Added", "Newest imported books", base+"/opds/recent", opdsAcqMIME),
		opdsNavigationEntry("authors", "Authors", "Browse by author", base+"/opds/authors", opdsNavMIME),
		opdsNavigationEntry("search", "Search", "Search by title or author", base+"/opds/search?q={searchTerms}", opdsAcqMIME),
	}
	writeOPDSFeed(w, feed)
}

func (s *Server) handleOPDSBooks(w http.ResponseWriter, r *http.Request) {
	s.handleOPDSBookList(w, r, "books", "All Books", library.ListBooksQuery{Sort: "title", Order: "asc"})
}

func (s *Server) handleOPDSRecent(w http.ResponseWriter, r *http.Request) {
	s.handleOPDSBookList(w, r, "recent", "Recently Added", library.ListBooksQuery{Sort: "added", Order: "desc"})
}

func (s *Server) handleOPDSBookList(w http.ResponseWriter, r *http.Request, feedID, title string, query library.ListBooksQuery) {
	page := opdsPage(r)
	query.Limit = opdsPageSize
	query.Offset = (page - 1) * opdsPageSize
	query.MediaType = library.MediaType(strings.TrimSpace(r.URL.Query().Get("type")))
	if query.MediaType != "" && !query.MediaType.Valid() {
		writeOPDSError(w, http.StatusBadRequest)
		return
	}
	total, err := s.library().CountListedBooks(r.Context(), query)
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	items, err := s.library().ListBookReadModels(r.Context(), query)
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	base := s.publicBaseURL(r)
	selfPath := r.URL.RequestURI()
	feed := newOPDSFeed(feedID, title, base+selfPath, opdsAcqMIME, total, page)
	absolutizeOPDSLinks(&feed, base)
	addPaginationLinks(&feed, base, r, page, total)
	for _, item := range items {
		entry, err := s.opdsBookEntry(r.Context(), base, item)
		if err == nil {
			feed.Entries = append(feed.Entries, entry)
		}
	}
	writeOPDSFeed(w, feed)
}

func (s *Server) handleOPDSAuthors(w http.ResponseWriter, r *http.Request) {
	items, err := s.library().ListBookReadModels(r.Context(), library.ListBooksQuery{Sort: "author", Order: "asc", Limit: 10000})
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	counts := map[string]int{}
	names := map[string]string{}
	for _, item := range items {
		name := primaryAuthorName(item)
		if name == "" {
			name = "Unknown Author"
		}
		key := authorKey(name)
		counts[key]++
		names[key] = name
	}
	keys := make([]string, 0, len(names))
	for key := range names {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return strings.ToLower(names[keys[i]]) < strings.ToLower(names[keys[j]]) })
	base := s.publicBaseURL(r)
	feed := newOPDSFeed("authors", "Authors", base+"/opds/authors", opdsNavMIME, len(keys), 1)
	absolutizeOPDSLinks(&feed, base)
	for _, key := range keys {
		feed.Entries = append(feed.Entries, opdsNavigationEntry("author:"+key, names[key], fmt.Sprintf("%d books", counts[key]), base+"/opds/authors/"+key, opdsAcqMIME))
	}
	writeOPDSFeed(w, feed)
}

func (s *Server) handleOPDSAuthorBooks(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	items, err := s.library().ListBookReadModels(r.Context(), library.ListBooksQuery{Sort: "title", Order: "asc", Limit: 10000})
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	base := s.publicBaseURL(r)
	feed := newOPDSFeed("author:"+key, "Author", base+r.URL.RequestURI(), opdsAcqMIME, 0, 1)
	absolutizeOPDSLinks(&feed, base)
	for _, item := range items {
		if authorKey(primaryAuthorName(item)) != key {
			continue
		}
		if feed.Title == "Author" {
			feed.Title = primaryAuthorName(item)
		}
		entry, err := s.opdsBookEntry(r.Context(), base, item)
		if err == nil {
			feed.Entries = append(feed.Entries, entry)
		}
	}
	feed.TotalResults = len(feed.Entries)
	writeOPDSFeed(w, feed)
}

func (s *Server) handleOPDSSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	page := opdsPage(r)
	base := s.publicBaseURL(r)
	if query == "" {
		s.handleOPDSRoot(w, r)
		return
	}
	listQuery := library.ListBooksQuery{Search: query, Sort: "title", Order: "asc", Limit: opdsPageSize, Offset: (page - 1) * opdsPageSize}
	total, err := s.library().CountListedBooks(r.Context(), listQuery)
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	items, err := s.library().ListBookReadModels(r.Context(), listQuery)
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	feed := newOPDSFeed("search:"+query, "Search: "+query, base+r.URL.RequestURI(), opdsAcqMIME, total, page)
	absolutizeOPDSLinks(&feed, base)
	addPaginationLinks(&feed, base, r, page, total)
	for _, item := range items {
		entry, err := s.opdsBookEntry(r.Context(), base, item)
		if err == nil {
			feed.Entries = append(feed.Entries, entry)
		}
	}
	writeOPDSFeed(w, feed)
}

func (s *Server) opdsBookEntry(ctx context.Context, base string, item library.BookReadModel) (opdsEntry, error) {
	files, err := s.library().GetBookFiles(ctx, item.Book.ID)
	if err != nil {
		return opdsEntry{}, err
	}
	sort.SliceStable(files, func(i, j int) bool { return formatRank(files[i].Format) < formatRank(files[j].Format) })
	updated := item.Book.UpdatedAt
	if updated.IsZero() {
		updated = item.Book.CreatedAt
	}
	entry := opdsEntry{
		Title:    item.Book.Title,
		ID:       fmt.Sprintf("urn:librarr:book:%d", item.Book.ID),
		Updated:  updated.UTC().Format(time.RFC3339),
		Summary:  item.Book.Description,
		Issued:   publicationYearString(item.Book.PublicationYear),
		Language: item.Book.Language,
	}
	if author := primaryAuthorName(item); author != "" {
		entry.Authors = []opdsAuthor{{Name: author}}
	}
	if item.LocalCover != nil && strings.TrimSpace(item.LocalCover.LocalPath) != "" {
		coverURL := fmt.Sprintf("%s/opds/cover/%d", base, item.Book.ID)
		entry.Links = append(entry.Links,
			opdsLink{Rel: "http://opds-spec.org/image", Href: coverURL, Type: item.LocalCover.MimeType},
			opdsLink{Rel: "http://opds-spec.org/image/thumbnail", Href: coverURL, Type: item.LocalCover.MimeType},
		)
	}
	for _, file := range files {
		format := normalizedFormat(file.Format, file.Path)
		mimeType := opdsMIME(format)
		entry.Links = append(entry.Links, opdsLink{
			Rel:   "http://opds-spec.org/acquisition",
			Href:  fmt.Sprintf("%s/opds/download/%d", base, file.ID),
			Type:  mimeType,
			Title: strings.ToUpper(format),
		})
	}
	if len(files) == 1 {
		entry.Format = opdsMIME(normalizedFormat(files[0].Format, files[0].Path))
	}
	return entry, nil
}

func (s *Server) handleOPDSDownload(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeOPDSError(w, http.StatusBadRequest)
		return
	}
	file, err := s.library().GetFile(r.Context(), id)
	if errors.Is(err, library.ErrNotFound) || file == nil || file.BookID == 0 {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	realPath, ok := s.safeLibraryFilePath(file.Path)
	if !ok {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	f, err := os.Open(realPath)
	if err != nil {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	format := normalizedFormat(file.Format, realPath)
	w.Header().Set("Content-Type", opdsMIME(format))
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": safeDownloadFilename(realPath)}))
	http.ServeContent(w, r, filepath.Base(realPath), info.ModTime(), f)
}

func (s *Server) handleOPDSCover(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeOPDSError(w, http.StatusBadRequest)
		return
	}
	cover, err := s.library().GetPrimaryCover(r.Context(), id)
	if err != nil || cover == nil || strings.TrimSpace(cover.LocalPath) == "" {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	realPath, err := filepath.EvalSymlinks(cover.LocalPath)
	if err != nil {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	f, err := os.Open(realPath)
	if err != nil {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		writeOPDSError(w, http.StatusNotFound)
		return
	}
	if cover.MimeType != "" {
		w.Header().Set("Content-Type", cover.MimeType)
	}
	http.ServeContent(w, r, filepath.Base(realPath), info.ModTime(), f)
}

func (s *Server) handleOPDSOpenSearch(w http.ResponseWriter, r *http.Request) {
	base := s.publicBaseURL(r)
	desc := opdsOpenSearchDescription{
		ShortName:      "Librarr",
		Description:    "Search your Librarr library",
		InputEncoding:  "UTF-8",
		OutputEncoding: "UTF-8",
		URLs: []opdsOpenSearchURL{{
			Type:     opdsAcqMIME,
			Template: base + "/opds/search?q={searchTerms}",
		}},
	}
	out, err := xml.MarshalIndent(desc, "", "  ")
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", opdsOSMIME+"; charset=utf-8")
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(out)
}

func opdsNavigationEntry(id, title, content, href, mimeType string) opdsEntry {
	return opdsEntry{Title: title, ID: "urn:librarr:" + id, Updated: opdsNow(), Content: &opdsContent{Type: "text", Text: content}, Links: []opdsLink{{Rel: "subsection", Href: href, Type: mimeType}}}
}

func writeOPDSFeed(w http.ResponseWriter, feed opdsFeed) {
	out, err := xml.MarshalIndent(feed, "", "  ")
	if err != nil {
		writeOPDSError(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(out)
}

func writeOPDSError(w http.ResponseWriter, status int) {
	http.Error(w, http.StatusText(status), status)
}

func addPaginationLinks(feed *opdsFeed, base string, r *http.Request, page, total int) {
	if page > 1 {
		feed.Links = append(feed.Links, opdsLink{Rel: "previous", Href: base + opdsPageURL(r, page-1), Type: opdsAcqMIME})
	}
	if page*opdsPageSize < total {
		feed.Links = append(feed.Links, opdsLink{Rel: "next", Href: base + opdsPageURL(r, page+1), Type: opdsAcqMIME})
	}
}

func absolutizeOPDSLinks(feed *opdsFeed, base string) {
	for i := range feed.Links {
		if strings.HasPrefix(feed.Links[i].Href, "/") {
			feed.Links[i].Href = base + feed.Links[i].Href
		}
	}
}

func opdsPage(r *http.Request) int {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		return 1
	}
	return page
}

func opdsPageURL(r *http.Request, page int) string {
	q := r.URL.Query()
	q.Set("page", strconv.Itoa(page))
	return r.URL.Path + "?" + q.Encode()
}

func (s *Server) publicBaseURL(r *http.Request) string {
	scheme := "http"
	host := r.Host
	if r.TLS != nil {
		scheme = "https"
	}
	if remoteFromTrustedProxy(r) {
		if proto := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); proto != "" {
			scheme = proto
		}
		if forwardedHost := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0]); forwardedHost != "" {
			host = forwardedHost
		}
	}
	return scheme + "://" + host
}

func (s *Server) safeLibraryFilePath(path string) (string, bool) {
	if strings.TrimSpace(path) == "" {
		return "", false
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", false
	}
	allowedRoots := []string{s.cfg.EbookDir, s.cfg.AudiobookDir, s.cfg.MangaDir, s.cfg.IncomingDir, s.cfg.MangaIncomingDir}
	for _, root := range allowedRoots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		realRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			realRoot, err = filepath.Abs(root)
			if err != nil {
				continue
			}
		}
		rel, err := filepath.Rel(realRoot, realPath)
		if err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." {
			return realPath, true
		}
		if err == nil && rel == "." {
			return realPath, true
		}
	}
	return "", false
}

func primaryAuthorName(item library.BookReadModel) string {
	if item.PrimaryAuthor != nil {
		return item.PrimaryAuthor.Name
	}
	for _, contributor := range item.Contributors {
		if contributor.Name != "" {
			return contributor.Name
		}
	}
	return ""
}

func authorKey(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		if r == ' ' || r == '-' || r == '_' {
			return '-'
		}
		return -1
	}, name)
	return strings.Trim(name, "-")
}

func normalizedFormat(format, path string) string {
	format = strings.Trim(strings.ToLower(format), ". ")
	if format == "" {
		format = strings.Trim(strings.ToLower(filepath.Ext(path)), ".")
	}
	return format
}

func opdsMIME(format string) string {
	if mimeType := formatMIMEs[strings.ToLower(format)]; mimeType != "" {
		return mimeType
	}
	return "application/octet-stream"
}

func formatRank(format string) int {
	switch strings.ToLower(format) {
	case "epub":
		return 0
	case "pdf":
		return 1
	case "mobi":
		return 2
	case "azw3":
		return 3
	default:
		return 10
	}
}

func publicationYearString(year int) string {
	if year <= 0 {
		return ""
	}
	return strconv.Itoa(year)
}

func safeDownloadFilename(path string) string {
	name := filepath.Base(path)
	name = strings.ReplaceAll(name, "\x00", "")
	if strings.TrimSpace(name) == "" || name == "." || name == string(filepath.Separator) {
		return "download"
	}
	return name
}
