package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// --- Audiobooks ---

// handleLibraryAudiobooks lists Librarr's own audiobook records. Items shelved
// in Audiobookshelf carry links out to it.
func (s *Server) handleLibraryAudiobooks(w http.ResponseWriter, r *http.Request) {
	s.serveLocalLibraryByMediaType(w, r, "audiobook")
}

// --- Kavita Library (Manga) ---

func (s *Server) handleLibraryManga(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.HasKavita() {
		// Issue #49: same as audiobooks — fall back to local DB so manga
		// imported via /api/import/files is visible in its own tab
		// instead of leaking into /api/library.
		s.serveLocalLibraryByMediaType(w, r, "manga")
		return
	}

	page := queryInt(r, "page", 1)
	query := r.URL.Query().Get("q")

	// Step 1: Get credentials for Kavita requests.
	authorize, err := s.kavitaAuthorizer()
	if err != nil {
		slog.Error("Kavita login failed", "error", err)
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"error": "Failed to authenticate with Kavita",
		})
		return
	}

	publicURL := s.cfg.KavitaPublicURL
	if publicURL == "" {
		publicURL = s.cfg.KavitaURL
	}

	if query != "" {
		// Use search endpoint.
		s.handleKavitaSearch(w, authorize, query, page, publicURL)
		return
	}

	// Step 2: Get all series via POST /api/Series/all-v2.
	limit := 100
	reqBody := map[string]interface{}{
		"statements":  []interface{}{},
		"combination": 1,
		"limitTo":     0,
		"sortOptions": map[string]interface{}{
			"sortField":   1,
			"isAscending": true,
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	apiURL := fmt.Sprintf("%s/api/Series/all-v2?pageNumber=%d&pageSize=%d", s.cfg.KavitaURL, page-1, limit)
	req, err := http.NewRequest("POST", apiURL, strings.NewReader(string(bodyBytes)))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	authorize(req)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"error": "Failed to reach Kavita",
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		slog.Error("Kavita series non-200", "status", resp.StatusCode, "body", string(body))
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"error": fmt.Sprintf("Kavita returned HTTP %d", resp.StatusCode),
		})
		return
	}

	// Parse pagination header.
	total := 0
	if pagination := resp.Header.Get("Pagination"); pagination != "" {
		var pag struct {
			TotalItems int `json:"totalItems"`
		}
		if err := json.Unmarshal([]byte(pagination), &pag); err == nil {
			total = pag.TotalItems
		}
	}

	var series []kavitaSeries
	if err := json.NewDecoder(resp.Body).Decode(&series); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "Failed to parse Kavita response",
		})
		return
	}

	items := s.kavitaSeriesToItems(series, publicURL)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": items,
		"total": total,
		"page":  page,
		"pages": int(math.Ceil(float64(total) / float64(limit))),
	})
}

func (s *Server) handleKavitaSearch(w http.ResponseWriter, authorize func(*http.Request), query string, page int, publicURL string) {
	apiURL := fmt.Sprintf("%s/api/Search/search?queryString=%s", s.cfg.KavitaURL, url.QueryEscape(query))
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	authorize(req)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"error": "Failed to reach Kavita",
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"error": fmt.Sprintf("Kavita search returned HTTP %d", resp.StatusCode),
		})
		return
	}

	var searchResult struct {
		Series []kavitaSeries `json:"series"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "Failed to parse Kavita search response",
		})
		return
	}

	items := s.kavitaSeriesToItems(searchResult.Series, publicURL)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": items,
		"total": len(items),
		"page":  page,
		"pages": 1,
	})
}

type kavitaSeries struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Pages       int    `json:"pages"`
	LibraryName string `json:"libraryName"`
	LibraryID   int    `json:"libraryId"`
}

func (s *Server) kavitaSeriesToItems(series []kavitaSeries, publicURL string) []map[string]interface{} {
	var items []map[string]interface{}
	for _, sr := range series {
		// Use public URL for covers so the browser can reach them
		coverURL := fmt.Sprintf("%s/api/image/series-cover?seriesId=%d", publicURL, sr.ID)
		kavitaURL := fmt.Sprintf("%s/library/%d/series/%d", publicURL, sr.LibraryID, sr.ID)

		items = append(items, map[string]interface{}{
			"id":         sr.ID,
			"name":       sr.Name,
			"pages":      sr.Pages,
			"library":    sr.LibraryName,
			"cover_url":  coverURL,
			"kavita_url": kavitaURL,
		})
	}
	if items == nil {
		items = []map[string]interface{}{}
	}
	s.linkLocalItems(items, "manga")
	return items
}

// kavitaAuthorizer returns a function that adds credentials to a Kavita request.
// A configured auth key is sent as-is; otherwise it logs in with the username
// and password to get a JWT.
func (s *Server) kavitaAuthorizer() (func(*http.Request), error) {
	if key := s.cfg.KavitaAPIKey; key != "" {
		return func(req *http.Request) { req.Header.Set("x-api-key", key) }, nil
	}
	token, err := s.kavitaLogin()
	if err != nil {
		return nil, err
	}
	return func(req *http.Request) { req.Header.Set("Authorization", "Bearer "+token) }, nil
}

func (s *Server) kavitaLogin() (string, error) {
	payload, _ := json.Marshal(map[string]string{
		"username": s.cfg.KavitaUser,
		"password": s.cfg.KavitaPass,
	})

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(
		s.cfg.KavitaURL+"/api/Account/login",
		"application/json",
		strings.NewReader(string(payload)),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("kavita login HTTP %d", resp.StatusCode)
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.Token, nil
}

// --- Delete library items ---

func (s *Server) handleDeleteBook(w http.ResponseWriter, r *http.Request) {
	s.deleteLibraryItem(w, r, "ebook")
}

// deleteLibraryItem removes a Librarr library record. Audiobookshelf is left
// alone: what it shelves is managed there.
func (s *Server) deleteLibraryItem(w http.ResponseWriter, r *http.Request, mediaType string) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "Invalid library item ID"})
		return
	}
	item, err := s.db.GetItem(id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && item.MediaType != mediaType) {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"error": "Library item not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library item", err)
		return
	}
	if err := s.db.DeleteItem(id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	username, _ := r.Context().Value(ctxUsername).(string)
	s.db.LogActivity(username, "library_remove", idStr, fmt.Sprintf("Removed library item %s", idStr))
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (s *Server) handleDeleteAudiobook(w http.ResponseWriter, r *http.Request) {
	s.deleteLibraryItem(w, r, "audiobook")
}

func (s *Server) handleDeleteManga(w http.ResponseWriter, r *http.Request) {
	if s.cfg.HasKavita() && r.URL.Query().Get("source") != "local" {
		writeJSON(w, http.StatusConflict, map[string]interface{}{"error": "Manage Kavita series in Kavita; use source=local for a Librarr library item ID"})
		return
	}
	s.deleteLibraryItem(w, r, "manga")
}
