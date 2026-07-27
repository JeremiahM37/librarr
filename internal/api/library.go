package api

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/db"
)

func (s *Server) handleLibrary(w http.ResponseWriter, r *http.Request) {
	// If ABS ebook library is configured, pull from there (has covers + series).
	// Otherwise fall back to local DB.
	if s.cfg.HasAudiobookshelf() && s.cfg.ABSEbookLibraryID != "" {
		s.handleLibraryEbooksFromABS(w, r)
		return
	}

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
			var tagJsonItems []map[string]interface{}
			for _, item := range tagItems {
				tagJsonItems = append(tagJsonItems, db.ItemToJSON(item))
			}
			if tagJsonItems == nil {
				tagJsonItems = []map[string]interface{}{}
			}
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

	items, err := s.library().ListLegacyItems(r.Context(), mediaType, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
		return
	}

	total, _ := s.library().CountLegacyItems(r.Context(), mediaType)

	var jsonItems []map[string]interface{}
	for _, item := range items {
		jsonItems = append(jsonItems, db.ItemToJSON(item))
	}
	if jsonItems == nil {
		jsonItems = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items":  jsonItems,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// serveLocalLibraryByMediaType is the fallback used by /api/library/audiobooks
// and /api/library/manga when ABS / Kavita aren't configured. Response shape
// matches the ABS handler (items/total/page/pages) so the UI's existing
// pagination code works against both code paths.
func (s *Server) serveLocalLibraryByMediaType(w http.ResponseWriter, r *http.Request, mediaType string) {
	const pageSize = 100
	page := queryInt(r, "page", 1)
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * pageSize

	items, err := s.library().ListLegacyItems(r.Context(), mediaType, pageSize, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
		return
	}
	total, _ := s.library().CountLegacyItems(r.Context(), mediaType)

	jsonItems := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		jsonItems = append(jsonItems, db.ItemToJSON(item))
	}
	pages := 0
	if total > 0 {
		pages = int(math.Ceil(float64(total) / float64(pageSize)))
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": jsonItems,
		"total": total,
		"page":  page,
		"pages": pages,
	})
}

// handleLibraryEbooksFromABS pulls ebooks from ABS ebook library (with covers + series).
func (s *Server) handleLibraryEbooksFromABS(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	if page < 1 {
		page = 1
	}
	query := r.URL.Query().Get("q")
	limit := queryInt(r, "limit", 100) // default 100 so series group together
	if limit < 1 || limit > 500 {
		limit = 100
	}

	absURL := fmt.Sprintf("%s/api/libraries/%s/items", s.cfg.ABSURL, s.cfg.ABSEbookLibraryID)
	params := url.Values{
		"page":  {strconv.Itoa(page - 1)},
		"limit": {strconv.Itoa(limit)},
		"sort":  {"media.metadata.seriesName"},
	}
	if query != "" {
		params.Set("filter", "search="+url.QueryEscape(query))
	}

	req, err := http.NewRequest("GET", absURL+"?"+params.Encode(), nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library items", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.ABSToken)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{"error": "Failed to reach ABS"})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"error": fmt.Sprintf("ABS returned HTTP %d", resp.StatusCode),
		})
		return
	}

	var absResp absLibraryResponse
	if err := json.NewDecoder(resp.Body).Decode(&absResp); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": "Failed to parse ABS response"})
		return
	}

	publicURL := s.cfg.ABSPublicURL
	if publicURL == "" {
		publicURL = s.cfg.ABSURL
	}

	var items []map[string]interface{}
	for _, item := range absResp.Results {
		author := item.Media.Metadata.AuthorName
		if author == "" && len(item.Media.Metadata.Authors) > 0 {
			author = item.Media.Metadata.Authors[0].Name
		}
		series := item.Media.Metadata.SeriesName
		if series == "" && len(item.Media.Metadata.Series) > 0 {
			series = item.Media.Metadata.Series[0].Name
		}

		coverURL := fmt.Sprintf("%s/api/items/%s/cover", publicURL, item.ID)

		items = append(items, map[string]interface{}{
			"id":        item.ID,
			"title":     item.Media.Metadata.Title,
			"author":    author,
			"series":    series,
			"cover_url": coverURL,
			"abs_url":   fmt.Sprintf("%s/item/%s", publicURL, item.ID),
		})
	}
	if items == nil {
		items = []map[string]interface{}{}
	}

	totalPages := absResp.NumPages
	if totalPages == 0 && absResp.Total > 0 {
		totalPages = int(math.Ceil(float64(absResp.Total) / float64(limit)))
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": items,
		"total": absResp.Total,
		"page":  page,
		"pages": totalPages,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.library().LegacyStats(r.Context())
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

func queryInt(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
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
