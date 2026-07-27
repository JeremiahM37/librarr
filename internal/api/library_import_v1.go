package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	libraryscanner "github.com/jamie75/librarr/internal/library/scanner"
)

type libraryImportStatus string

const (
	libraryImportStatusPending   libraryImportStatus = "pending"
	libraryImportStatusImporting libraryImportStatus = "importing"
	libraryImportStatusCompleted libraryImportStatus = "completed"
	libraryImportStatusFailed    libraryImportStatus = "failed"
)

type libraryImportProgress struct {
	Status       libraryImportStatus `json:"status"`
	Imported     int                 `json:"imported"`
	Failed       int                 `json:"failed"`
	Duplicates   int                 `json:"duplicates"`
	Skipped      int                 `json:"skipped"`
	Total        int                 `json:"total"`
	CurrentTitle string              `json:"current_title,omitempty"`
	CurrentPath  string              `json:"current_path,omitempty"`
	StartedAt    time.Time           `json:"started_at"`
	CompletedAt  *time.Time          `json:"completed_at,omitempty"`
}

type libraryImportItemResult struct {
	CandidateID string `json:"candidate_id"`
	Title       string `json:"title,omitempty"`
	Author      string `json:"author,omitempty"`
	Path        string `json:"path,omitempty"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
	Error       string `json:"error,omitempty"`
}

type libraryImportResult struct {
	JobID       string                    `json:"job_id"`
	ScanJobID   string                    `json:"scan_job_id"`
	Status      libraryImportStatus       `json:"status"`
	Summary     map[string]int            `json:"summary"`
	Items       []libraryImportItemResult `json:"items"`
	StartedAt   time.Time                 `json:"started_at"`
	CompletedAt *time.Time                `json:"completed_at,omitempty"`
}

type libraryImportJob struct {
	ID          string                `json:"id"`
	ScanJobID   string                `json:"scan_job_id"`
	Status      libraryImportStatus   `json:"status"`
	Progress    libraryImportProgress `json:"progress"`
	Error       string                `json:"error,omitempty"`
	Result      *libraryImportResult  `json:"result,omitempty"`
	StartedAt   time.Time             `json:"started_at"`
	CompletedAt *time.Time            `json:"completed_at,omitempty"`
}

type libraryImportJobManager struct {
	engine        libraryimport.ImportEngine
	updateScanner func(string, []libraryscanner.CandidateUpdate) (*libraryscanner.Result, bool)
	attachCover   func(context.Context, libraryscanner.Candidate, *libraryimport.EngineResult)
	now           func() time.Time

	mu       sync.Mutex
	jobs     map[string]*libraryImportJob
	order    []string
	activeID string
}

type activeLibraryImportError struct {
	JobID string
}

func (e *activeLibraryImportError) Error() string {
	return "library import already running"
}

func newLibraryImportJobManager(engine libraryimport.ImportEngine, updateScanner func(string, []libraryscanner.CandidateUpdate) (*libraryscanner.Result, bool), attachCover func(context.Context, libraryscanner.Candidate, *libraryimport.EngineResult)) *libraryImportJobManager {
	return &libraryImportJobManager{
		engine:        engine,
		updateScanner: updateScanner,
		attachCover:   attachCover,
		now:           time.Now,
		jobs:          map[string]*libraryImportJob{},
	}
}

func (m *libraryImportJobManager) start(ctx context.Context, scanJobID string, candidates []libraryscanner.Candidate) (*libraryImportJob, error) {
	if m == nil || m.engine == nil {
		return nil, fmt.Errorf("library import engine is not configured")
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no ready candidates selected")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := m.now()
	job := &libraryImportJob{
		ID:        "libimp_" + newLibraryImportJobID(),
		ScanJobID: scanJobID,
		Status:    libraryImportStatusPending,
		StartedAt: now,
		Progress: libraryImportProgress{
			Status:    libraryImportStatusPending,
			Total:     len(candidates),
			StartedAt: now,
		},
	}

	m.mu.Lock()
	if m.activeID != "" {
		active := m.cloneJobLocked(m.activeID)
		m.mu.Unlock()
		if active != nil {
			return nil, &activeLibraryImportError{JobID: active.ID}
		}
		return nil, &activeLibraryImportError{}
	}
	m.jobs[job.ID] = job
	m.order = append(m.order, job.ID)
	m.activeID = job.ID
	m.pruneLocked()
	started := cloneLibraryImportJob(job)
	m.mu.Unlock()

	go m.run(context.Background(), job.ID, scanJobID, append([]libraryscanner.Candidate(nil), candidates...))
	return started, nil
}

func (m *libraryImportJobManager) get(jobID string) (*libraryImportJob, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.cloneJobLocked(jobID)
	return job, job != nil
}

func (m *libraryImportJobManager) result(jobID string) (*libraryImportResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil || job.Result == nil {
		return nil, false
	}
	result := *job.Result
	result.Items = append([]libraryImportItemResult(nil), job.Result.Items...)
	result.Summary = cloneIntMap(job.Result.Summary)
	return &result, true
}

func (m *libraryImportJobManager) run(ctx context.Context, jobID, scanJobID string, candidates []libraryscanner.Candidate) {
	m.update(jobID, func(job *libraryImportJob) {
		job.Status = libraryImportStatusImporting
		job.Progress.Status = libraryImportStatusImporting
	})

	items := make([]libraryImportItemResult, 0, len(candidates))
	updates := make([]libraryscanner.CandidateUpdate, 0, len(candidates))
	for _, candidate := range candidates {
		title := candidateDisplayTitle(candidate)
		m.update(jobID, func(job *libraryImportJob) {
			job.Progress.CurrentTitle = title
			job.Progress.CurrentPath = candidate.Path
		})

		item := libraryImportItemResult{
			CandidateID: candidate.ID,
			Title:       title,
			Author:      candidateDisplayAuthor(candidate),
			Path:        candidate.Path,
		}
		result, err := m.engine.Import(ctx, libraryimport.ImportRequest{
			Source: library.ImportSource{
				Name:      "library_scan",
				SourceID:  candidate.ID,
				MediaType: candidate.MediaType,
			},
			RootPath:         candidate.Path,
			OriginalPath:     candidate.Path,
			TitleHint:        title,
			AuthorHint:       item.Author,
			MetadataOverride: importMetadataOverride(candidate),
		})
		switch {
		case err != nil:
			item.Status = "failed"
			item.Error = err.Error()
			updates = append(updates, libraryscanner.CandidateUpdate{
				ID:                   candidate.ID,
				Classification:       libraryscanner.ClassificationNew,
				ClassificationReason: "Import failed",
				Error:                err.Error(),
			})
		case result != nil && result.DuplicateCount > 0 && result.InsertedCount == 0:
			item.Status = "duplicate"
			item.Reason = "Duplicate detected during import"
			updates = append(updates, libraryscanner.CandidateUpdate{
				ID:                   candidate.ID,
				Classification:       libraryscanner.ClassificationDuplicate,
				ClassificationReason: "Duplicate detected during import",
				ExistingPath:         candidate.ExistingPath,
			})
		default:
			item.Status = "imported"
			item.Reason = "Imported into library"
			if m.attachCover != nil {
				m.attachCover(ctx, candidate, result)
			}
			updates = append(updates, libraryscanner.CandidateUpdate{
				ID:                   candidate.ID,
				Classification:       libraryscanner.ClassificationAlreadyImported,
				ClassificationReason: "Imported into library",
				ExistingPath:         candidate.Path,
			})
		}
		items = append(items, item)
		m.update(jobID, func(job *libraryImportJob) {
			switch item.Status {
			case "imported":
				job.Progress.Imported++
			case "duplicate":
				job.Progress.Duplicates++
			case "failed":
				job.Progress.Failed++
			default:
				job.Progress.Skipped++
			}
		})
	}

	if m.updateScanner != nil && len(updates) > 0 {
		m.updateScanner(scanJobID, updates)
	}
	completed := m.now()
	summary := summarizeLibraryImportItems(items)
	result := &libraryImportResult{
		JobID:       jobID,
		ScanJobID:   scanJobID,
		Status:      libraryImportStatusCompleted,
		Summary:     summary,
		Items:       items,
		StartedAt:   m.startedAt(jobID),
		CompletedAt: &completed,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil {
		return
	}
	job.Status = libraryImportStatusCompleted
	job.Progress.Status = libraryImportStatusCompleted
	job.Progress.CurrentTitle = ""
	job.Progress.CurrentPath = ""
	job.Progress.CompletedAt = &completed
	job.CompletedAt = &completed
	job.Result = result
	m.activeID = ""
}

func (m *libraryImportJobManager) update(jobID string, fn func(*libraryImportJob)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job != nil {
		fn(job)
	}
}

func (m *libraryImportJobManager) startedAt(jobID string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job := m.jobs[jobID]; job != nil {
		return job.StartedAt
	}
	return time.Time{}
}

func (m *libraryImportJobManager) cloneJobLocked(jobID string) *libraryImportJob {
	job := m.jobs[jobID]
	if job == nil {
		return nil
	}
	return cloneLibraryImportJob(job)
}

func (m *libraryImportJobManager) pruneLocked() {
	for len(m.order) > maxCompletedLibraryImportJobs {
		id := m.order[0]
		m.order = m.order[1:]
		if id == m.activeID {
			continue
		}
		delete(m.jobs, id)
	}
}

const maxCompletedLibraryImportJobs = 8

func cloneLibraryImportJob(job *libraryImportJob) *libraryImportJob {
	if job == nil {
		return nil
	}
	clone := *job
	if job.Result != nil {
		result := *job.Result
		result.Items = append([]libraryImportItemResult(nil), job.Result.Items...)
		result.Summary = cloneIntMap(job.Result.Summary)
		clone.Result = &result
	}
	return &clone
}

func cloneIntMap(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func summarizeLibraryImportItems(items []libraryImportItemResult) map[string]int {
	summary := map[string]int{
		"imported":   0,
		"duplicates": 0,
		"failed":     0,
		"skipped":    0,
		"total":      len(items),
	}
	for _, item := range items {
		switch item.Status {
		case "imported":
			summary["imported"]++
		case "duplicate":
			summary["duplicates"]++
		case "failed":
			summary["failed"]++
		default:
			summary["skipped"]++
		}
	}
	return summary
}

func candidateDisplayTitle(candidate libraryscanner.Candidate) string {
	return firstNonBlank(candidate.Title, candidate.Metadata.Title, candidate.Filename, candidate.Path)
}

func candidateDisplayAuthor(candidate libraryscanner.Candidate) string {
	return firstNonBlank(candidate.Author, candidate.Metadata.Author)
}

func importMetadataOverride(candidate libraryscanner.Candidate) libraryimport.CandidateMetadata {
	return libraryimport.CandidateMetadata{
		SelectedTitle:   candidateDisplayTitle(candidate),
		SelectedAuthor:  candidateDisplayAuthor(candidate),
		Subtitle:        candidate.Metadata.Subtitle,
		Series:          candidate.Metadata.Series,
		SeriesNumber:    firstNonBlank(candidate.Metadata.SeriesNumber, candidate.Metadata.Volume),
		Publisher:       candidate.Metadata.Publisher,
		PublicationYear: candidate.Metadata.PublicationYear,
		ISBN:            candidate.Metadata.ISBN,
		Language:        candidate.Metadata.Language,
		Description:     candidate.Metadata.Description,
		Tags:            append([]string(nil), candidate.Metadata.Tags...),
		Library:         firstNonBlank(candidate.Metadata.Library, string(candidate.MediaType)),
	}
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func newLibraryImportJobID() string {
	return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
}

func (s *Server) handleV1LibraryImportStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ScanJobID    string   `json:"scan_job_id"`
		CandidateIDs []string `json:"candidate_ids"`
		AllReady     bool     `json:"all_ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid JSON: " + err.Error()})
		return
	}
	scanJobID := strings.TrimSpace(req.ScanJobID)
	if scanJobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "scan_job_id is required"})
		return
	}
	if s.libraryScanner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Library scanner is not configured"})
		return
	}
	scanResult, ok := s.libraryScanner.Result(scanJobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Scan results not found"})
		return
	}
	candidates := selectReadyImportCandidates(scanResult.Candidates, req.CandidateIDs, req.AllReady)
	if len(candidates) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "No ready-to-import candidates selected"})
		return
	}
	manager := s.libraryImportJobs()
	job, err := manager.start(r.Context(), scanJobID, candidates)
	if err != nil {
		var active *activeLibraryImportError
		if errors.As(err, &active) {
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"success": false,
				"error":   "Library import already running",
				"job_id":  active.JobID,
			})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"success": true,
		"job_id":  job.ID,
		"job":     job,
	})
}

func (s *Server) handleV1LibraryImportJob(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	job, ok := s.libraryImportJobs().get(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Import job not found"})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleV1LibraryImportResults(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	job, ok := s.libraryImportJobs().get(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Import job not found"})
		return
	}
	if job.Status != libraryImportStatusCompleted {
		writeJSON(w, http.StatusConflict, map[string]interface{}{"success": false, "error": "Import results are not ready", "status": job.Status})
		return
	}
	result, ok := s.libraryImportJobs().result(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "Import results not found"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) libraryImportJobs() *libraryImportJobManager {
	if s.libraryScanImporter == nil {
		s.libraryScanImporter = newLibraryImportJobManager(s.importer(), func(jobID string, updates []libraryscanner.CandidateUpdate) (*libraryscanner.Result, bool) {
			if s.libraryScanner == nil {
				return nil, false
			}
			return s.libraryScanner.UpdateCandidates(jobID, updates)
		}, func(ctx context.Context, candidate libraryscanner.Candidate, result *libraryimport.EngineResult) {
			s.attachImportedCandidateCover(ctx, candidate, result)
		})
	}
	return s.libraryScanImporter
}

func (s *Server) attachImportedCandidateCover(ctx context.Context, candidate libraryscanner.Candidate, result *libraryimport.EngineResult) {
	if s == nil || s.coverCache == nil || s.libraryService == nil || result == nil || result.Execution == nil {
		return
	}
	for _, execution := range result.Execution.Results {
		if execution.Status != libraryimport.ExecutionStatusSuccess || execution.BookID == 0 {
			continue
		}
		if _, err := s.coverCache.AttachBookCover(ctx, s.libraryService, execution.BookID, candidate.Path); err != nil {
			slog.Debug("failed to attach imported book cover", "book_id", execution.BookID, "path", candidate.Path, "error", err)
		}
		return
	}
}

func selectReadyImportCandidates(candidates []libraryscanner.Candidate, ids []string, allReady bool) []libraryscanner.Candidate {
	selected := map[string]struct{}{}
	if !allReady {
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id != "" {
				selected[id] = struct{}{}
			}
		}
	}
	out := make([]libraryscanner.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Classification != libraryscanner.ClassificationNew {
			continue
		}
		if !allReady {
			if _, ok := selected[candidate.ID]; !ok {
				continue
			}
		}
		out = append(out, candidate)
	}
	return out
}
