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

	"github.com/jamie75/librarr/internal/library"
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
	if strings.TrimSpace(req.Action) == "merge_matching_books" {
		result, ok, err := s.mergeMatchingScanBooks(r.Context(), jobID, req.ID)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Scan results not found"})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, result)
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

func (s *Server) mergeMatchingScanBooks(ctx context.Context, jobID, candidateID string) (*libraryscanner.Result, bool, error) {
	result, ok := s.libraryScanner.Result(jobID)
	if !ok {
		return nil, false, nil
	}
	var candidate *libraryscanner.Candidate
	for i := range result.Candidates {
		if result.Candidates[i].ID == strings.TrimSpace(candidateID) {
			candidate = &result.Candidates[i]
			break
		}
	}
	if candidate == nil {
		return nil, true, errors.New("candidate not found")
	}
	if candidate.Classification != libraryscanner.ClassificationManualReview || !strings.Contains(strings.ToLower(candidate.ClassificationReason), "multiple existing books") {
		return nil, true, errors.New("candidate is not an ambiguous existing-book match")
	}
	matches, err := s.findMatchingScanBooks(ctx, *candidate)
	if err != nil {
		return nil, true, err
	}
	if len(matches) < 2 {
		return nil, true, errors.New("fewer than two matching books were found")
	}
	target := selectMergeTarget(matches)
	for _, source := range matches {
		if source.Book.ID == target.Book.ID {
			continue
		}
		if _, err := s.library().MergeBooks(ctx, source.Book.ID, target.Book.ID); err != nil {
			return nil, true, err
		}
	}
	update := libraryscanner.CandidateUpdate{
		ID:                   candidate.ID,
		Classification:       libraryscanner.ClassificationNew,
		ClassificationReason: "Ready to import after merging matching books",
		ExistingBookID:       target.Book.ID,
	}
	if file, err := s.library().FindFileByPath(ctx, candidate.Path); err == nil && file != nil {
		update.Classification = libraryscanner.ClassificationAlreadyImported
		update.ClassificationReason = "Same destination already exists"
		update.ExistingBookID = file.BookID
		update.ExistingFileID = file.ID
		update.ExistingPath = file.Path
	} else if err != nil && !errors.Is(err, library.ErrNotFound) {
		return nil, true, err
	}
	updated, ok := s.libraryScanner.UpdateCandidates(jobID, []libraryscanner.CandidateUpdate{update})
	if !ok {
		return nil, false, nil
	}
	if updated != nil {
		s.backfillScanCandidateCovers(ctx, updated.Candidates, true)
	}
	return updated, true, nil
}

func (s *Server) findMatchingScanBooks(ctx context.Context, candidate libraryscanner.Candidate) ([]library.BookReadModel, error) {
	titleKey := library.TitleMatchKey(firstNonBlank(candidate.Title, candidate.Metadata.Title, candidate.Filename))
	authorKey := library.ContributorMatchKey(firstNonBlank(candidate.Author, candidate.Metadata.Author))
	if titleKey == "" || authorKey == "" {
		return nil, errors.New("candidate title and author are required to merge matching books")
	}
	seen := map[int64]struct{}{}
	var matches []library.BookReadModel
	for _, search := range scanBookMatchSearchTerms(candidate) {
		search = strings.TrimSpace(search)
		if search == "" {
			continue
		}
		items, err := s.library().ListBookReadModels(ctx, library.ListBooksQuery{MediaType: candidate.MediaType, Search: search, Limit: 500})
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if _, ok := seen[item.Book.ID]; ok {
				continue
			}
			if library.TitleMatchKey(item.Book.Title) != titleKey {
				continue
			}
			if library.ContributorMatchKey(primaryReadModelAuthor(item)) != authorKey {
				continue
			}
			seen[item.Book.ID] = struct{}{}
			matches = append(matches, item)
		}
	}
	return matches, nil
}

func scanBookMatchSearchTerms(candidate libraryscanner.Candidate) []string {
	return bookMatchSearchTerms(
		firstNonBlank(candidate.Title, candidate.Metadata.Title),
		firstNonBlank(candidate.Author, candidate.Metadata.Author),
	)
}

func bookMatchSearchTerms(title, author string) []string {
	terms := []string{title, author}
	terms = append(terms, leadingWords(library.TitleMatchKey(title), 3))
	terms = append(terms, leadingWords(library.ContributorMatchKey(author), 2))
	return uniqueNonBlankTerms(terms)
}

func uniqueNonBlankTerms(terms []string) []string {
	seen := map[string]struct{}{}
	var unique []string
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		key := library.NormalizeKey(term)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, term)
	}
	return unique
}

func leadingWords(value string, count int) string {
	fields := strings.Fields(value)
	if len(fields) <= count {
		return strings.Join(fields, " ")
	}
	return strings.Join(fields[:count], " ")
}

func selectMergeTarget(matches []library.BookReadModel) library.BookReadModel {
	target := matches[0]
	for _, item := range matches[1:] {
		switch {
		case target.LocalCover == nil && item.LocalCover != nil:
			target = item
		case (target.LocalCover == nil) == (item.LocalCover == nil) && item.FileCount > target.FileCount:
			target = item
		case (target.LocalCover == nil) == (item.LocalCover == nil) && item.FileCount == target.FileCount && item.Book.ID < target.Book.ID:
			target = item
		}
	}
	return target
}

func primaryReadModelAuthor(item library.BookReadModel) string {
	if item.PrimaryAuthor != nil {
		return item.PrimaryAuthor.Name
	}
	for _, contributor := range item.Contributors {
		if strings.TrimSpace(contributor.Name) != "" {
			return contributor.Name
		}
	}
	return ""
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
