package api

import (
	"database/sql"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

func (s *Server) handleLibrary(w http.ResponseWriter, r *http.Request) {
	// /api/library is the ebooks tab; default to media_type="ebook" so
	// audiobooks/manga imported into the local DB don't bleed into this
	// view. Callers that genuinely want every row can pass ?type=all.
	mediaType := r.URL.Query().Get("type")
	if mediaType == "" {
		mediaType = "ebook"
	} else if mediaType == "all" {
		mediaType = ""
	}
	limit := queryBoundedInt(r, "limit", 50, 1, 500)
	offset := queryBoundedInt(r, "offset", 0, 0, 1_000_000)
	tagFilter := r.URL.Query().Get("tag")

	// If filtering by tag name, look up tag ID and use tag-based query.
	if tagFilter != "" {
		tags, _ := s.db.GetTags()
		var tagID int64
		for _, t := range tags {
			if strings.EqualFold(t.Name, tagFilter) {
				tagID = t.ID
				break
			}
		}
		if tagID > 0 {
			tagItems, err := s.db.GetItemsByTag(tagID, limit, offset)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
				return
			}
			tagJsonItems := s.libraryItemsJSON(tagItems)
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"items":  tagJsonItems,
				"total":  len(tagJsonItems),
				"limit":  limit,
				"offset": offset,
				"tag":    tagFilter,
			})
			return
		}
	}

	// Keep limit/offset clients working; the React UI uses page/pages.
	page := offset/limit + 1
	if r.URL.Query().Has("page") {
		page = queryBoundedInt(r, "page", 1, 1, 1_000_000)
		offset = (page - 1) * limit
	}
	s.serveLocalLibraryPage(w, r, mediaType, page, limit, offset)
}

// handleLibraryItem returns one local library item with its tags, for the
// item detail view.
func (s *Server) handleLibraryItem(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid item ID", nil)
		return
	}
	item, err := s.db.GetItem(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "Item not found", nil)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library item", err)
		return
	}
	tags, err := s.db.GetItemTags(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library item", err)
		return
	}
	if tags == nil {
		tags = []db.Tag{}
	}
	body := s.libraryItemsJSON([]models.LibraryItem{*item})[0]
	body["tags"] = tags
	writeJSON(w, http.StatusOK, body)
}

// handleLibraryFile downloads a local library item's file.
func (s *Server) handleLibraryFile(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}
	s.serveLibraryFile(w, r, id)
}

// serveLocalLibraryByMediaType serves one media type's tab, a page at a time.
func (s *Server) serveLocalLibraryByMediaType(w http.ResponseWriter, r *http.Request, mediaType string) {
	const pageSize = 100
	page := queryBoundedInt(r, "page", 1, 1, 1_000_000)
	s.serveLocalLibraryPage(w, r, mediaType, page, pageSize, (page-1)*pageSize)
}

func (s *Server) serveLocalLibraryPage(w http.ResponseWriter, r *http.Request, mediaType string, page, limit, offset int) {
	items, total, err := s.db.FindItems(mediaType, strings.TrimSpace(r.URL.Query().Get("q")), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items":  s.libraryItemsJSON(items),
		"total":  total,
		"page":   page,
		"pages":  int(math.Ceil(float64(total) / float64(limit))),
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	stats, err := s.db.GetStats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	limit := queryBoundedInt(r, "limit", 50, 1, 500)
	offset := queryBoundedInt(r, "offset", 0, 0, 1_000_000)

	events, err := s.db.GetActivity(limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
		return
	}

	total, _ := s.db.CountActivity()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events": events,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleSources(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.searchMgr.SourceMeta())
}

// queryBoundedInt parses an int query param and clamps it to [min, max].
// Returns fallback when the param is missing, unparseable, or out of bounds.
// Use this for pagination limits to avoid `limit=-1` returning unbounded rows
// or `limit=999999` letting a caller pull the entire table.
func queryBoundedInt(r *http.Request, key string, fallback, minVal, maxVal int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < minVal || n > maxVal {
		return fallback
	}
	return n
}
