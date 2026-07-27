package api

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

// ExportEnvelope wraps exported data with metadata.
type ExportEnvelope struct {
	Version    string      `json:"version"`
	ExportedAt string      `json:"exported_at"`
	ItemCount  int         `json:"item_count"`
	Items      interface{} `json:"items"`
}

// handleExportLibrary exports the library as JSON.
func (s *Server) handleExportLibrary(w http.ResponseWriter, r *http.Request) {
	items, err := s.library().ListLegacyItems(r.Context(), "", 100000, 0)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to export library",
		})
		return
	}
	if items == nil {
		items = []models.LibraryItem{}
	}

	w.Header().Set("Content-Disposition", "attachment; filename=librarr-library.json")
	writeJSON(w, http.StatusOK, ExportEnvelope{
		Version:    "1.0.0",
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
		ItemCount:  len(items),
		Items:      items,
	})
}

// handleExportWishlist exports the wishlist as JSON.
func (s *Server) handleExportWishlist(w http.ResponseWriter, r *http.Request) {
	items, err := s.db.GetWishlist()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to export wishlist",
		})
		return
	}
	if items == nil {
		items = []models.WishlistItem{}
	}

	w.Header().Set("Content-Disposition", "attachment; filename=librarr-wishlist.json")
	writeJSON(w, http.StatusOK, ExportEnvelope{
		Version:    "1.0.0",
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
		ItemCount:  len(items),
		Items:      items,
	})
}

// handleExportRequests exports book requests as JSON.
func (s *Server) handleExportRequests(w http.ResponseWriter, r *http.Request) {
	requests, err := s.db.ListRequests(0, "", 100000, 0)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to export requests",
		})
		return
	}
	if requests == nil {
		requests = []models.Request{}
	}

	w.Header().Set("Content-Disposition", "attachment; filename=librarr-requests.json")
	writeJSON(w, http.StatusOK, ExportEnvelope{
		Version:    "1.0.0",
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
		ItemCount:  len(requests),
		Items:      requests,
	})
}

// handleImportLibrary imports library items from JSON, skipping duplicates.
func (s *Server) handleImportLibrary(w http.ResponseWriter, r *http.Request) {
	var envelope struct {
		Items []models.LibraryItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}

	imported := 0
	skipped := 0
	existingItems, existingErr := s.library().ListLegacyItems(r.Context(), "", 100000, 0)
	existingTitles := make(map[string]bool, len(existingItems))
	for _, item := range existingItems {
		existingTitles[strings.ToLower(item.Title)] = true
	}
	for _, item := range envelope.Items {
		// Preserve the existing import behavior: skip duplicates by source_id
		// or by an existing title.
		if item.SourceID != "" {
			exists, err := s.library().HasLegacySourceID(r.Context(), item.SourceID)
			if err != nil || exists {
				skipped++
				continue
			}
		}
		if existingErr != nil {
			skipped++
			continue
		}
		if existingTitles[strings.ToLower(item.Title)] {
			skipped++
			continue
		}

		item.ID = 0 // reset ID for insert
		if _, err := s.library().ImportLegacyItem(r.Context(), item); err != nil {
			skipped++
			continue
		}
		existingTitles[strings.ToLower(item.Title)] = true
		imported++
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"imported": imported,
		"skipped":  skipped,
		"total":    len(envelope.Items),
	})
}

// handleImportWishlist imports wishlist items from JSON.
func (s *Server) handleImportWishlist(w http.ResponseWriter, r *http.Request) {
	var envelope struct {
		Items []models.WishlistItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}

	imported := 0
	skipped := 0
	// Get existing wishlist to deduplicate.
	existing, _ := s.db.GetWishlist()
	existingTitles := make(map[string]bool)
	for _, e := range existing {
		existingTitles[strings.ToLower(e.Title)] = true
	}

	for _, item := range envelope.Items {
		if existingTitles[strings.ToLower(item.Title)] {
			skipped++
			continue
		}
		mediaType := item.MediaType
		if mediaType == "" {
			mediaType = "ebook"
		}
		if _, err := s.db.AddWishlistItem(item.Title, item.Author, mediaType); err != nil {
			skipped++
			continue
		}
		imported++
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"imported": imported,
		"skipped":  skipped,
		"total":    len(envelope.Items),
	})
}

// handleImportCSVData imports from CSV with title, author, format columns (adds to wishlist).
func (s *Server) handleImportCSVData(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Failed to parse form: " + err.Error(),
		})
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "No file uploaded",
		})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Failed to read CSV header",
		})
		return
	}

	colMap := make(map[string]int)
	for i, h := range header {
		colMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	titleIdx, hasTitleCol := colMap["title"]
	if !hasTitleCol {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "CSV must have a 'title' column",
		})
		return
	}

	imported := 0
	var errors []string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors = append(errors, "CSV read error: "+err.Error())
			continue
		}

		if titleIdx >= len(record) {
			continue
		}
		title := strings.TrimSpace(record[titleIdx])
		if title == "" {
			continue
		}

		author := ""
		if idx, ok := colMap["author"]; ok && idx < len(record) {
			author = strings.TrimSpace(record[idx])
		}

		mediaType := "ebook"
		if idx, ok := colMap["format"]; ok && idx < len(record) {
			mt := strings.TrimSpace(strings.ToLower(record[idx]))
			if mt == "audiobook" || mt == "manga" {
				mediaType = mt
			}
		}
		if idx, ok := colMap["media_type"]; ok && idx < len(record) {
			mt := strings.TrimSpace(strings.ToLower(record[idx]))
			if mt == "audiobook" || mt == "manga" {
				mediaType = mt
			}
		}

		if _, err := s.db.AddWishlistItem(title, author, mediaType); err != nil {
			errors = append(errors, "Failed to add: "+title)
			continue
		}
		imported++
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"imported": imported,
		"errors":   errors,
	})
}
