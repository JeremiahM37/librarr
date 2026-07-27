package api

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"log/slog"
	"net/http"
	"os"
	"strings"

	libraryscanner "github.com/jamie75/librarr/internal/library/scanner"
)

func (s *Server) handleV1LibraryScanStart(w http.ResponseWriter, r *http.Request) {
	if s.libraryScanner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Library scanner is not configured"})
		return
	}
	job, err := s.libraryScanner.Start(r.Context(), s.currentLibraryScanRoots())
	if err != nil {
		var active *libraryscanner.ActiveJobError
		if errors.As(err, &active) {
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"success": false,
				"error":   "Library scan already running",
				"job_id":  active.JobID,
			})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"success": false, "error": "Failed to start library scan"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"success": true,
		"job_id":  job.ID,
		"job":     job,
	})
}

func (s *Server) handleV1LibraryScanJob(w http.ResponseWriter, r *http.Request) {
	if s.libraryScanner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Library scanner is not configured"})
		return
	}
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	job, ok := s.libraryScanner.Get(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Scan job not found"})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleV1LibraryScanResults(w http.ResponseWriter, r *http.Request) {
	if s.libraryScanner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Library scanner is not configured"})
		return
	}
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	job, ok := s.libraryScanner.Get(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Scan job not found"})
		return
	}
	if job.Status != libraryscanner.StatusCompleted {
		writeJSON(w, http.StatusConflict, map[string]interface{}{"success": false, "error": "Scan results are not ready", "status": job.Status})
		return
	}
	result, ok := s.libraryScanner.Result(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Scan results not found"})
		return
	}
	if r.URL.Query().Get("pretty") == "1" {
		writeLibraryScanDebugHTML(w, result)
		return
	}
	s.backfillScanCandidateCovers(r.Context(), result.Candidates, false)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleV1LibraryScanCover(w http.ResponseWriter, r *http.Request) {
	if s.libraryScanner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Library scanner is not configured"})
		return
	}
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	candidateID := strings.TrimSpace(r.PathValue("candidate_id"))
	coverPath, ok := s.libraryScanner.CoverPath(jobID, candidateID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Cover not found"})
		return
	}
	data, err := os.ReadFile(coverPath)
	if err != nil || len(data) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Cover not found"})
		return
	}
	w.Header().Set("Content-Type", http.DetectContentType(data))
	w.Header().Set("Cache-Control", "private, max-age=3600")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleV1LibraryScanResolve(w http.ResponseWriter, r *http.Request) {
	if s.libraryScanner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Library scanner is not configured"})
		return
	}
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	var req libraryscanner.CandidateResolution
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid JSON: " + err.Error()})
		return
	}
	result, ok, err := s.libraryScanner.ResolveCandidate(jobID, req)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Scan results not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	if result != nil {
		s.backfillScanCandidateCovers(r.Context(), result.Candidates, true)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) backfillScanCandidateCovers(ctx context.Context, candidates []libraryscanner.Candidate, includeReady bool) {
	for _, candidate := range candidates {
		if candidate.CoverURL == "" && candidate.CoverPath == "" {
			continue
		}
		switch candidate.Classification {
		case libraryscanner.ClassificationAlreadyImported, libraryscanner.ClassificationDuplicate:
		case libraryscanner.ClassificationNew:
			if !includeReady {
				continue
			}
		default:
			continue
		}
		s.attachExistingScanCandidateCover(ctx, candidate)
	}
}

func (s *Server) attachExistingScanCandidateCover(ctx context.Context, candidate libraryscanner.Candidate) {
	if s == nil || s.coverCache == nil || s.libraryService == nil {
		return
	}
	bookID := candidate.ExistingBookID
	if bookID == 0 && strings.TrimSpace(candidate.ExistingPath) != "" {
		if file, err := s.libraryService.FindFileByPath(ctx, candidate.ExistingPath); err == nil && file != nil {
			bookID = file.BookID
		}
	}
	if bookID == 0 {
		return
	}
	if _, err := s.coverCache.AttachBookCover(ctx, s.libraryService, bookID, candidate.Path); err != nil {
		slog.Debug("failed to backfill scan candidate cover", "book_id", bookID, "path", candidate.Path, "error", err)
	}
}

func writeLibraryScanDebugHTML(w http.ResponseWriter, result *libraryscanner.Result) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<!doctype html><html><head><meta charset="utf-8"><title>Librarr Library Scan</title><style>
body{font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#111827;color:#e5e7eb;margin:24px}
table{width:100%;border-collapse:collapse;background:#0f172a}
th,td{border:1px solid #334155;padding:8px;text-align:left;vertical-align:top;font-size:13px}
th{background:#1e293b;color:#f8fafc}
code{color:#fbbf24;word-break:break-all}
</style></head><body>`))
	_, _ = w.Write([]byte(`<h1>Librarr Library Scan Results</h1><table><thead><tr><th>Classification</th><th>Title</th><th>Author</th><th>Media Type</th><th>Metadata Source</th><th>Reason</th><th>Path</th></tr></thead><tbody>`))
	for _, candidate := range result.Candidates {
		_, _ = w.Write([]byte(`<tr><td>` + html.EscapeString(string(candidate.Classification)) + `</td><td>` +
			html.EscapeString(candidate.Title) + `</td><td>` +
			html.EscapeString(candidate.Author) + `</td><td>` +
			html.EscapeString(string(candidate.MediaType)) + `</td><td>` +
			html.EscapeString(candidate.Metadata.Source) + `</td><td>` +
			html.EscapeString(candidate.ClassificationReason) + `</td><td><code>` +
			html.EscapeString(candidate.Path) + `</code></td></tr>`))
	}
	_, _ = w.Write([]byte(`</tbody></table></body></html>`))
}

func (s *Server) currentLibraryScanRoots() libraryscanner.Roots {
	roots := libraryscanner.Roots{
		EbookDir:     s.cfg.EbookDir,
		AudiobookDir: s.cfg.AudiobookDir,
		MangaDir:     s.cfg.MangaDir,
	}
	settings := s.loadSettings()
	if value, ok := settings["ebook_dir"].(string); ok && strings.TrimSpace(value) != "" {
		roots.EbookDir = strings.TrimSpace(value)
	}
	if value, ok := settings["audiobook_dir"].(string); ok && strings.TrimSpace(value) != "" {
		roots.AudiobookDir = strings.TrimSpace(value)
	}
	if value, ok := settings["manga_dir"].(string); ok && strings.TrimSpace(value) != "" {
		roots.MangaDir = strings.TrimSpace(value)
	}
	return roots
}
