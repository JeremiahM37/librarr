package api

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

func opdsNow() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

func opdsFeedOpen(feedID, title, kind, selfHref string, total, page int) string {
	mime := opdsNavMIME
	if kind == "acquisition" {
		mime = opdsAcqMIME
	}
	startIndex := (page-1)*opdsPageSize + 1
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opds="http://opds-spec.org/2010/catalog"
      xmlns:dc="http://purl.org/dc/terms/"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <id>urn:librarr:%s</id>
  <title>%s</title>
  <updated>%s</updated>
  <author><name>Librarr</name></author>
  <link rel="self" href="%s" type="%s"/>
  <link rel="start" href="/opds/" type="%s"/>
  <link rel="search" href="/opds/opensearch.xml" type="%s"/>
  <opensearch:totalResults>%d</opensearch:totalResults>
  <opensearch:itemsPerPage>%d</opensearch:itemsPerPage>
  <opensearch:startIndex>%d</opensearch:startIndex>
`, xmlEscape(feedID), xmlEscape(title), opdsNow(),
		xmlEscape(selfHref), mime, opdsNavMIME, opdsOSMIME,
		total, opdsPageSize, startIndex)
}

func opdsNavEntry(entryID, title, content, href, mime string) string {
	return fmt.Sprintf(`  <entry>
    <title>%s</title>
    <id>urn:librarr:%s</id>
    <updated>%s</updated>
    <content type="text">%s</content>
    <link rel="subsection" href="%s" type="%s"/>
  </entry>
`, xmlEscape(title), xmlEscape(entryID), opdsNow(),
		xmlEscape(content), xmlEscape(href), mime)
}

func (s *Server) handleOPDSRoot(w http.ResponseWriter, _ *http.Request) {
	totalBooks, _ := s.db.CountItems("ebook")
	totalAudio, _ := s.db.CountItems("audiobook")
	total := totalBooks + totalAudio

	body := opdsFeedOpen("", "Librarr", "navigation", "/opds/", total, 1)
	body += opdsNavEntry("library", fmt.Sprintf("My Library (%d items)", total),
		"Browse your downloaded books and audiobooks", "/opds/books", opdsAcqMIME)
	body += opdsNavEntry("ebooks", fmt.Sprintf("Ebooks (%d)", totalBooks),
		"Browse ebooks", "/opds/books?type=ebook", opdsAcqMIME)
	body += opdsNavEntry("audiobooks", fmt.Sprintf("Audiobooks (%d)", totalAudio),
		"Browse audiobooks", "/opds/books?type=audiobook", opdsAcqMIME)
	body += opdsNavEntry("search", "Search",
		"Search for new books", "/opds/search?q={searchTerms}", opdsAcqMIME)
	body += "</feed>"

	w.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
	w.Write([]byte(body))
}

func (s *Server) handleOPDSBooks(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	mediaType := r.URL.Query().Get("type")
	offset := (page - 1) * opdsPageSize

	items, _ := s.db.GetItems(mediaType, opdsPageSize, offset)
	total, _ := s.db.CountItems(mediaType)

	selfHref := fmt.Sprintf("/opds/books?page=%d", page)
	if mediaType != "" {
		selfHref += "&type=" + mediaType
	}

	body := opdsFeedOpen("library", "My Library", "acquisition", selfHref, total, page)

	// Pagination links.
	if page > 1 {
		prevHref := fmt.Sprintf("/opds/books?page=%d", page-1)
		if mediaType != "" {
			prevHref += "&type=" + mediaType
		}
		body += fmt.Sprintf("  <link rel=\"previous\" href=\"%s\" type=\"%s\"/>\n",
			xmlEscape(prevHref), opdsAcqMIME)
	}
	if offset+opdsPageSize < total {
		nextHref := fmt.Sprintf("/opds/books?page=%d", page+1)
		if mediaType != "" {
			nextHref += "&type=" + mediaType
		}
		body += fmt.Sprintf("  <link rel=\"next\" href=\"%s\" type=\"%s\"/>\n",
			xmlEscape(nextHref), opdsAcqMIME)
	}

	for _, item := range items {
		fmtStr := strings.ToLower(strings.TrimPrefix(item.FileFormat, "."))
		mime := formatMIMEs[fmtStr]
		if mime == "" {
			mime = "application/octet-stream"
		}

		author := item.Author
		if author == "" {
			author = "Unknown Author"
		}

		authorXML := ""
		if item.Author != "" {
			authorXML = fmt.Sprintf("    <author><name>%s</name></author>\n", xmlEscape(author))
		}

		body += fmt.Sprintf(`  <entry>
    <title>%s</title>
    <id>urn:librarr:item:%d</id>
    <updated>%s</updated>
%s    <dc:format>%s</dc:format>
    <link rel="http://opds-spec.org/acquisition"
          href="/opds/download/%d"
          type="%s"/>
  </entry>
`, xmlEscape(item.Title), item.ID,
			item.AddedAt.UTC().Format("2006-01-02T15:04:05Z"),
			authorXML, xmlEscape(mime), item.ID, xmlEscape(mime))
	}

	body += "</feed>"
	w.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
	w.Write([]byte(body))
}

func (s *Server) handleOPDSSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		s.handleOPDSRoot(w, r)
		return
	}

	results, _ := s.searchMgr.Search(r.Context(), "main", query)

	body := opdsFeedOpen("search", fmt.Sprintf("Search: %s", query), "acquisition",
		fmt.Sprintf("/opds/search?q=%s", xmlEscape(query)), len(results), 1)

	for i, r := range results {
		if i >= 50 {
			break
		}
		fmtStr := r.Format
		if fmtStr == "" {
			fmtStr = "epub"
		}
		mime := formatMIMEs[fmtStr]
		if mime == "" {
			mime = "application/epub+zip"
		}

		authorXML := ""
		if r.Author != "" {
			authorXML = fmt.Sprintf("    <author><name>%s</name></author>\n", xmlEscape(r.Author))
		}

		id := r.SourceID
		if id == "" {
			id = r.MD5
		}
		if id == "" {
			id = fmt.Sprintf("search-%d", i)
		}

		body += fmt.Sprintf(`  <entry>
    <title>%s</title>
    <id>urn:librarr:search:%s</id>
    <updated>%s</updated>
%s  </entry>
`, xmlEscape(r.Title), xmlEscape(id), opdsNow(), authorXML)
	}

	body += "</feed>"
	w.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
	w.Write([]byte(body))
}

func (s *Server) handleOPDSDownload(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}
	s.serveLibraryFile(w, r, id)
}

// serveLibraryFile sends a library item's file as an attachment. It backs both
// the OPDS acquisition link and the web UI's download button.
func (s *Server) serveLibraryFile(w http.ResponseWriter, r *http.Request, id int64) {
	item, err := s.db.GetItem(id)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error("library file item lookup failed", "id", id, "error", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		http.Error(w, "Item not found", http.StatusNotFound)
		return
	}
	if item.FilePath == "" {
		http.Error(w, "File not found on disk", http.StatusNotFound)
		return
	}

	// Resolve the real path to prevent path traversal via symlinks or DB tampering.
	realPath, err := filepath.EvalSymlinks(item.FilePath)
	if err != nil {
		http.Error(w, "File not found on disk", http.StatusNotFound)
		return
	}

	// Validate the file is within an allowed directory.
	allowed := false
	for _, dir := range []string{s.cfg.EbookDir, s.cfg.AudiobookDir, s.cfg.MangaDir, s.cfg.IncomingDir, s.cfg.MangaIncomingDir} {
		if dir == "" {
			continue
		}
		absDir, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		// realPath has its symlinks resolved, so the directory must too, or a
		// library directory reached through a symlink never matches.
		if resolved, err := filepath.EvalSymlinks(absDir); err == nil {
			absDir = resolved
		}
		if strings.HasPrefix(realPath, absDir+string(filepath.Separator)) || realPath == absDir {
			allowed = true
			break
		}
	}
	if !allowed {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	// A multi-file audiobook is a directory; ServeFile would list it.
	if info, err := os.Stat(realPath); err != nil || !info.Mode().IsRegular() {
		http.Error(w, "File not found on disk", http.StatusNotFound)
		return
	}

	fmtStr := strings.ToLower(strings.TrimPrefix(filepath.Ext(realPath), "."))
	mime := formatMIMEs[fmtStr]
	if mime == "" {
		mime = "application/octet-stream"
	}

	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%q", filepath.Base(realPath)))
	http.ServeFile(w, r, realPath)
}

func (s *Server) handleOPDSOpenSearch(w http.ResponseWriter, _ *http.Request) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">
  <ShortName>Librarr</ShortName>
  <Description>Search Librarr for books, audiobooks, and web novels</Description>
  <InputEncoding>UTF-8</InputEncoding>
  <OutputEncoding>UTF-8</OutputEncoding>
  <Url type="application/atom+xml;profile=opds-catalog;kind=acquisition"
       template="/opds/search?q={searchTerms}"/>
</OpenSearchDescription>`
	w.Header().Set("Content-Type", "application/opensearchdescription+xml; charset=utf-8")
	w.Write([]byte(xml))
}
