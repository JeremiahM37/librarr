package scanner

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
)

const maxCompletedJobs = 8

type Manager struct {
	catalog libraryimport.Catalog
	now     func() time.Time
	covers  *library.CoverCache

	mu       sync.Mutex
	jobs     map[string]*Job
	order    []string
	activeID string
}

type Option func(*Manager)

func WithCoverCache(covers *library.CoverCache) Option {
	return func(m *Manager) {
		m.covers = covers
	}
}

func NewManager(catalog libraryimport.Catalog, opts ...Option) *Manager {
	m := &Manager{
		catalog: catalog,
		now:     time.Now,
		jobs:    map[string]*Job{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type ActiveJobError struct {
	JobID string
}

func (e *ActiveJobError) Error() string {
	return "library scan already running"
}

func (m *Manager) Start(ctx context.Context, roots Roots) (*Job, error) {
	if m == nil || m.catalog == nil {
		return nil, fmt.Errorf("library scanner is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := m.now()
	job := &Job{
		ID:        newJobID(),
		Status:    StatusPending,
		StartedAt: now,
		Progress: Progress{
			Status:    StatusPending,
			StartedAt: now,
		},
	}

	m.mu.Lock()
	if m.activeID != "" {
		active := m.cloneJobLocked(m.activeID)
		m.mu.Unlock()
		if active != nil {
			return nil, &ActiveJobError{JobID: active.ID}
		}
		return nil, &ActiveJobError{}
	}
	m.jobs[job.ID] = job
	m.order = append(m.order, job.ID)
	m.activeID = job.ID
	m.pruneLocked()
	m.mu.Unlock()

	started := cloneJob(job)
	go m.run(context.Background(), job.ID, roots)
	return started, nil
}

func (m *Manager) Get(jobID string) (*Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.cloneJobLocked(jobID)
	return job, job != nil
}

func (m *Manager) Result(jobID string) (*Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil || job.Result == nil {
		return nil, false
	}
	result := *job.Result
	result.Candidates = append([]Candidate(nil), job.Result.Candidates...)
	result.Warnings = append([]Warning(nil), job.Result.Warnings...)
	return &result, true
}

func (m *Manager) CoverPath(jobID, candidateID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil || job.Result == nil {
		return "", false
	}
	for _, candidate := range job.Result.Candidates {
		if candidate.ID == candidateID && strings.TrimSpace(candidate.CoverPath) != "" {
			return candidate.CoverPath, true
		}
	}
	return "", false
}

func (m *Manager) UpdateCandidates(jobID string, updates []CandidateUpdate) (*Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil || job.Result == nil {
		return nil, false
	}
	byID := make(map[string]CandidateUpdate, len(updates))
	for _, update := range updates {
		if strings.TrimSpace(update.ID) != "" {
			byID[update.ID] = update
		}
	}
	for i := range job.Result.Candidates {
		update, ok := byID[job.Result.Candidates[i].ID]
		if !ok {
			continue
		}
		if update.Classification != "" {
			job.Result.Candidates[i].Classification = update.Classification
		}
		job.Result.Candidates[i].ClassificationReason = update.ClassificationReason
		if update.ExistingPath != "" {
			job.Result.Candidates[i].ExistingPath = update.ExistingPath
		}
		if update.ExistingBookID != 0 {
			job.Result.Candidates[i].ExistingBookID = update.ExistingBookID
		}
		if update.ExistingFileID != 0 {
			job.Result.Candidates[i].ExistingFileID = update.ExistingFileID
		}
		job.Result.Candidates[i].Error = update.Error
	}
	job.Result.Totals = countTotals(job.Result.Candidates)
	result := *job.Result
	result.Candidates = append([]Candidate(nil), job.Result.Candidates...)
	result.Warnings = append([]Warning(nil), job.Result.Warnings...)
	return &result, true
}

func (m *Manager) ResolveCandidate(jobID string, resolution CandidateResolution) (*Result, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil || job.Result == nil {
		return nil, false, nil
	}
	id := strings.TrimSpace(resolution.ID)
	action := strings.TrimSpace(resolution.Action)
	if id == "" {
		return nil, true, fmt.Errorf("candidate_id is required")
	}
	if action != "use_suggested" && action != "edit_metadata" {
		return nil, true, fmt.Errorf("unsupported resolution action")
	}
	for i := range job.Result.Candidates {
		if job.Result.Candidates[i].ID != id {
			continue
		}
		candidate := &job.Result.Candidates[i]
		if action == "use_suggested" && candidate.Classification != ClassificationManualReview {
			return nil, true, fmt.Errorf("candidate does not require manual review")
		}
		if action == "edit_metadata" && candidate.Classification != ClassificationManualReview && candidate.Classification != ClassificationNew {
			return nil, true, fmt.Errorf("candidate cannot be edited")
		}
		if action == "edit_metadata" {
			if err := validateResolution(resolution, job.Result.Candidates, id); err != nil {
				return nil, true, err
			}
		}
		title := firstNonBlank(resolution.Title, candidate.Title, candidate.Metadata.Title, candidate.Filename)
		author := firstNonBlank(resolution.Author, candidate.Author, candidate.Metadata.Author)
		candidate.Title = title
		candidate.Author = author
		candidate.Metadata.Title = title
		candidate.Metadata.Author = author
		if action == "edit_metadata" {
			candidate.Metadata.Subtitle = strings.TrimSpace(resolution.Subtitle)
			candidate.Metadata.Series = strings.TrimSpace(resolution.Series)
			candidate.Metadata.Volume = strings.TrimSpace(resolution.SeriesNumber)
			candidate.Metadata.SeriesNumber = strings.TrimSpace(resolution.SeriesNumber)
			candidate.Metadata.Publisher = strings.TrimSpace(resolution.Publisher)
			candidate.Metadata.PublicationYear = strings.TrimSpace(resolution.PublicationYear)
			candidate.Metadata.ISBN = normalizeISBN(resolution.ISBN)
			candidate.Metadata.Language = strings.TrimSpace(resolution.Language)
			candidate.Metadata.Description = strings.TrimSpace(resolution.Description)
			candidate.Metadata.Tags = cleanResolutionTags(resolution.Tags)
			candidate.Metadata.Library = firstNonBlank(resolution.Library, string(candidate.MediaType))
			candidate.Metadata.Source = "manual_edit"
			candidate.Metadata.Confidence = library.ConfidenceHigh
		}
		candidate.DestinationPath = previewDestination(*candidate)
		candidate.Classification = ClassificationNew
		candidate.ClassificationReason = "Ready to import after manual review"
		candidate.ManualReview = nil
		candidate.Error = ""
		job.Result.Totals = countTotals(job.Result.Candidates)
		result := *job.Result
		result.Candidates = append([]Candidate(nil), job.Result.Candidates...)
		result.Warnings = append([]Warning(nil), job.Result.Warnings...)
		return &result, true, nil
	}
	return nil, true, fmt.Errorf("candidate not found")
}

var isbnCharsRe = regexp.MustCompile(`^[0-9Xx -]+$`)

func validateResolution(resolution CandidateResolution, candidates []Candidate, candidateID string) error {
	title := strings.TrimSpace(resolution.Title)
	author := strings.TrimSpace(resolution.Author)
	if title == "" {
		return fmt.Errorf("title is required")
	}
	if author == "" {
		return fmt.Errorf("author is required")
	}
	if year := strings.TrimSpace(resolution.PublicationYear); year != "" {
		if len(year) != 4 || year < "1000" || year > "2999" {
			return fmt.Errorf("publication year must be a four-digit year")
		}
	}
	if isbn := strings.TrimSpace(resolution.ISBN); isbn != "" {
		normalized := normalizeISBN(isbn)
		if !isbnCharsRe.MatchString(isbn) || (len(normalized) != 10 && len(normalized) != 13) {
			return fmt.Errorf("ISBN must be ISBN-10 or ISBN-13")
		}
	}
	preview := previewDestination(Candidate{
		ID:              candidateID,
		Title:           title,
		Author:          author,
		Format:          firstNonBlank(candidateFormat(candidates, candidateID), "epub"),
		Path:            candidatePath(candidates, candidateID),
		DestinationPath: candidateDestination(candidates, candidateID),
	})
	if strings.TrimSpace(preview) == "" {
		return fmt.Errorf("destination preview is empty")
	}
	for _, candidate := range candidates {
		if candidate.ID == candidateID || candidate.Classification != ClassificationNew {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(candidate.DestinationPath), preview) {
			return fmt.Errorf("another ready candidate already uses this destination filename")
		}
	}
	return nil
}

func candidateFormat(candidates []Candidate, id string) string {
	for _, candidate := range candidates {
		if candidate.ID == id {
			return candidate.Format
		}
	}
	return ""
}

func candidatePath(candidates []Candidate, id string) string {
	for _, candidate := range candidates {
		if candidate.ID == id {
			return candidate.Path
		}
	}
	return ""
}

func candidateDestination(candidates []Candidate, id string) string {
	for _, candidate := range candidates {
		if candidate.ID == id {
			return candidate.DestinationPath
		}
	}
	return ""
}

func previewDestination(candidate Candidate) string {
	base := firstNonBlank(candidate.DestinationPath, candidate.Path)
	dir := filepath.Dir(base)
	if strings.TrimSpace(dir) == "." || strings.TrimSpace(dir) == "" {
		dir = filepath.Dir(candidate.Path)
	}
	dir = collapseRepeatedLibrarySegment(dir)
	title := safePathSegment(firstNonBlank(candidate.Title, candidate.Metadata.Title, candidate.Filename, "Untitled"))
	author := safePathSegment(firstNonBlank(candidate.Author, candidate.Metadata.Author))
	format := strings.Trim(strings.ToLower(firstNonBlank(candidate.Format, strings.TrimPrefix(filepath.Ext(candidate.Path), "."))), ".")
	if format == "" {
		format = "book"
	}
	name := title + "." + format
	if author != "" {
		name = author + " - " + name
	}
	if dir == "." || dir == "" {
		return name
	}
	return filepath.Join(dir, name)
}

func collapseRepeatedLibrarySegment(dir string) string {
	cleaned := filepath.Clean(strings.TrimSpace(dir))
	if cleaned == "." || cleaned == "" {
		return dir
	}
	separator := string(filepath.Separator)
	parts := strings.Split(cleaned, separator)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(out) > 0 && repeatedLibrarySegment(out[len(out)-1], part) {
			continue
		}
		out = append(out, part)
	}
	return strings.Join(out, separator)
}

func repeatedLibrarySegment(previous, current string) bool {
	if !strings.EqualFold(previous, current) {
		return false
	}
	switch strings.ToLower(current) {
	case "ebooks", "audiobooks", "manga":
		return true
	default:
		return false
	}
}

func safePathSegment(value string) string {
	value = strings.TrimSpace(value)
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", " -", "\x00", "")
	value = replacer.Replace(value)
	value = strings.Join(strings.Fields(value), " ")
	return strings.Trim(value, ". ")
}

func normalizeISBN(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, " ", "")
	return strings.ToUpper(value)
}

func cleanResolutionTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	seen := map[string]struct{}{}
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		key := strings.ToLower(tag)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func (m *Manager) run(ctx context.Context, jobID string, roots Roots) {
	result, err := m.scan(ctx, jobID, roots)
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job == nil {
		return
	}
	completed := m.now()
	job.CompletedAt = &completed
	job.Progress.CompletedAt = &completed
	m.activeID = ""
	if err != nil {
		if errors.Is(err, context.Canceled) {
			job.Status = StatusCancelled
			job.Progress.Status = StatusCancelled
		} else {
			job.Status = StatusFailed
			job.Progress.Status = StatusFailed
			job.Error = err.Error()
		}
		return
	}
	job.Status = StatusCompleted
	job.Progress.Status = StatusCompleted
	job.Progress.CandidatesReady = len(result.Candidates)
	result.Status = StatusCompleted
	result.CompletedAt = &completed
	job.Result = result
}

func (m *Manager) scan(ctx context.Context, jobID string, roots Roots) (*Result, error) {
	m.update(jobID, func(p *Progress) {
		p.Status = StatusScanning
		p.CurrentPhase = "scanning"
	})

	items := make([]discoveredItem, 0)
	warnings := make([]Warning, 0)
	for _, root := range configuredRoots(roots) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		found, rootWarnings := m.scanRoot(ctx, jobID, root)
		items = append(items, found...)
		warnings = append(warnings, rootWarnings...)
	}

	m.update(jobID, func(p *Progress) {
		p.Status = StatusProcessingMetadata
		p.CurrentPhase = "processing_metadata"
		p.Warnings = append([]Warning(nil), warnings...)
	})

	supported := make([]libraryimport.ImportCandidate, 0)
	candidates := make([]Candidate, 0, len(items))
	indexByPath := map[string]int{}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		candidate := item.Candidate
		switch item.State {
		case ClassificationUnsupported, ClassificationUnreadable:
			candidates = append(candidates, candidate)
		default:
			indexByPath[candidate.Path] = len(candidates)
			candidates = append(candidates, candidate)
			supported = append(supported, item.ImportCandidate)
		}
		m.update(jobID, func(p *Progress) {
			p.FilesProcessed++
			p.CandidatesReady = len(candidates)
			p.CurrentPath = candidate.Path
		})
	}

	m.update(jobID, func(p *Progress) {
		p.Status = StatusClassifying
		p.CurrentPhase = "classifying"
	})

	if len(supported) > 0 {
		planner := libraryimport.NewImportPlanner(m.catalog)
		planned, err := planner.PlanCandidates(ctx, libraryimport.PlanningContext{
			Source: library.ImportSource{Name: "library_scan"},
			Now:    m.now(),
		}, supported)
		if err != nil {
			return nil, err
		}
		for _, plan := range planned.Plans {
			idx, ok := indexByPath[plan.Candidate.Path]
			if !ok {
				continue
			}
			applyPlan(&candidates[idx], plan)
		}
	}

	totals := countTotals(candidates)
	return &Result{
		JobID:      jobID,
		Status:     StatusCompleted,
		Totals:     totals,
		Candidates: candidates,
		Warnings:   warnings,
		StartedAt:  m.jobsStartedAt(jobID),
	}, nil
}

type rootConfig struct {
	MediaType library.MediaType
	Path      string
	Formats   map[string]struct{}
}

type discoveredState = Classification

type discoveredItem struct {
	State           discoveredState
	Candidate       Candidate
	ImportCandidate libraryimport.ImportCandidate
}

func configuredRoots(roots Roots) []rootConfig {
	return []rootConfig{
		{MediaType: library.MediaTypeEbook, Path: roots.EbookDir, Formats: extensions(".epub", ".pdf", ".mobi", ".azw3")},
		{MediaType: library.MediaTypeAudiobook, Path: roots.AudiobookDir, Formats: extensions(".m4b", ".mp3")},
		{MediaType: library.MediaTypeManga, Path: roots.MangaDir, Formats: extensions(".cbz", ".cbr", ".pdf")},
	}
}

func extensions(values ...string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func (m *Manager) scanRoot(ctx context.Context, jobID string, root rootConfig) ([]discoveredItem, []Warning) {
	cleanRoot := filepath.Clean(root.Path)
	if strings.TrimSpace(root.Path) == "" || cleanRoot == "." {
		return nil, []Warning{{MediaType: root.MediaType, Message: "Library folder is not configured"}}
	}
	info, err := os.Stat(cleanRoot)
	if err != nil {
		return nil, []Warning{{MediaType: root.MediaType, Path: cleanRoot, Message: "Folder not found"}}
	}
	if !info.IsDir() {
		return nil, []Warning{{MediaType: root.MediaType, Path: cleanRoot, Message: "Configured library folder is not a directory"}}
	}

	var items []discoveredItem
	var warnings []Warning
	err = filepath.WalkDir(cleanRoot, func(path string, d os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			warnings = append(warnings, Warning{MediaType: root.MediaType, Path: path, Message: "Folder entry could not be read"})
			return nil
		}
		if d.IsDir() {
			m.update(jobID, func(p *Progress) {
				p.CurrentMediaType = root.MediaType
				p.CurrentPath = path
				p.DirectoriesScanned++
			})
			return nil
		}
		m.update(jobID, func(p *Progress) {
			p.CurrentMediaType = root.MediaType
			p.CurrentPath = path
			p.FilesDiscovered++
		})
		items = append(items, m.discoverFile(jobID, cleanRoot, root, path))
		return nil
	})
	if err != nil {
		return items, append(warnings, Warning{MediaType: root.MediaType, Path: cleanRoot, Message: err.Error()})
	}
	return items, warnings
}

func (m *Manager) discoverFile(jobID, root string, cfg rootConfig, path string) discoveredItem {
	rel, err := filepath.Rel(root, path)
	if err != nil || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return unreadableCandidate(cfg.MediaType, root, path, "File is outside configured root")
	}
	ext := strings.ToLower(filepath.Ext(path))
	format := strings.TrimPrefix(ext, ".")
	info, statErr := os.Stat(path)
	filename := filepath.Base(path)
	base := Candidate{
		MediaType:      cfg.MediaType,
		Format:         format,
		Path:           path,
		RelativePath:   rel,
		Filename:       filename,
		Classification: ClassificationNew,
	}
	if statErr == nil {
		base.Size = info.Size()
		base.ModifiedAt = info.ModTime()
	}
	base.ID = candidateID(base)

	if _, ok := cfg.Formats[ext]; !ok {
		base.Classification = ClassificationUnsupported
		base.ClassificationReason = "Unsupported file extension for configured media root"
		return discoveredItem{State: ClassificationUnsupported, Candidate: base}
	}
	if statErr != nil {
		base.Classification = ClassificationUnreadable
		base.ClassificationReason = "File could not be read"
		base.Error = statErr.Error()
		return discoveredItem{State: ClassificationUnreadable, Candidate: base}
	}
	hash, err := fileSHA256(path)
	if err != nil {
		base.Classification = ClassificationUnreadable
		base.ClassificationReason = "File could not be read"
		base.Error = err.Error()
		return discoveredItem{State: ClassificationUnreadable, Candidate: base}
	}
	if coverPath, err := m.extractScanCover(jobID, base.ID, path); err == nil && coverPath != "" {
		base.CoverPath = coverPath
		base.CoverURL = fmt.Sprintf("/api/v1/library/scan/%s/cover/%s", jobID, base.ID)
	}
	importCandidate := libraryimport.ImportCandidate{
		Path:         path,
		RelativePath: rel,
		OriginalPath: path,
		MediaType:    cfg.MediaType,
		Format:       format,
		Size:         info.Size(),
		ContentHash:  hash,
		Evidence: []libraryimport.PlanningEvidence{{
			Signal:      "library_scan_file",
			Value:       rel,
			Source:      "filesystem",
			Confidence:  library.ConfidenceExact,
			Explanation: "Library scanner discovered a supported file",
		}},
	}
	return discoveredItem{State: ClassificationNew, Candidate: base, ImportCandidate: importCandidate}
}

func (m *Manager) extractScanCover(jobID, candidateID, sourcePath string) (string, error) {
	if m == nil || m.covers == nil {
		return "", nil
	}
	coverPath, err := m.covers.ExtractForScan(jobID, candidateID, sourcePath)
	if err != nil {
		slog.Debug("local cover extraction failed", "path", sourcePath, "error", err)
		return "", nil
	}
	return coverPath, nil
}

func unreadableCandidate(mediaType library.MediaType, root, path, msg string) discoveredItem {
	rel, _ := filepath.Rel(root, path)
	c := Candidate{
		ID:                   candidateID(Candidate{MediaType: mediaType, Path: path, RelativePath: rel}),
		MediaType:            mediaType,
		Format:               strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), "."),
		Path:                 path,
		RelativePath:         rel,
		Filename:             filepath.Base(path),
		Classification:       ClassificationUnreadable,
		ClassificationReason: msg,
		Error:                msg,
	}
	return discoveredItem{State: ClassificationUnreadable, Candidate: c}
}

func applyPlan(candidate *Candidate, plan libraryimport.ImportPlan) {
	meta := metadataFromPlan(plan.Candidate)
	candidate.Metadata = meta
	candidate.Title = meta.Title
	candidate.Author = meta.Author
	candidate.DestinationPath = proposedDestination(plan)
	if plan.Book.Existing != nil {
		candidate.ExistingBookID = plan.Book.Existing.ID
	}
	if plan.File.Existing != nil {
		candidate.ExistingFileID = plan.File.Existing.ID
		if plan.File.Existing.BookID != 0 {
			candidate.ExistingBookID = plan.File.Existing.BookID
		}
	}
	candidate.Classification = ClassificationNew
	candidate.ClassificationReason = "Ready to import"

	switch plan.Disposition {
	case libraryimport.DispositionIgnoreDuplicate:
		if plan.File.Existing != nil {
			candidate.ExistingPath = plan.File.Existing.Path
		}
		signal, reason := duplicateReason(plan)
		candidate.Duplicate = duplicateDetails(plan, signal, reason, candidate)
		if signal == "duplicate_path" {
			candidate.Classification = ClassificationAlreadyImported
			candidate.ClassificationReason = reason
		} else {
			candidate.Classification = ClassificationDuplicate
			candidate.ClassificationReason = reason
		}
	case libraryimport.DispositionConflict:
		if plan.File.Existing != nil {
			candidate.ExistingPath = plan.File.Existing.Path
		}
		signal, reason := duplicateReason(plan)
		candidate.Classification = ClassificationDuplicate
		candidate.ClassificationReason = firstNonBlank(reason, firstEvidenceExplanation(plan, "Import planner found a duplicate or conflicting file"))
		candidate.Duplicate = duplicateDetails(plan, signal, candidate.ClassificationReason, candidate)
	case libraryimport.DispositionNeedsManualReview:
		reason := firstEvidenceExplanation(plan, "Needs manual review before import")
		candidate.Classification = ClassificationManualReview
		candidate.ClassificationReason = reason
		candidate.ManualReview = &ManualReviewDetails{
			Reason:               reason,
			PlannerDisposition:   string(plan.Disposition),
			SuggestedDestination: candidate.DestinationPath,
			MetadataSource:       meta.Source,
			Confidence:           meta.Confidence,
		}
	default:
		candidate.Classification = ClassificationNew
		candidate.ClassificationReason = "Ready to import"
	}
}

func proposedDestination(plan libraryimport.ImportPlan) string {
	if plan.File.Proposed != nil {
		return strings.TrimSpace(plan.File.Proposed.Path)
	}
	if plan.File.Existing != nil {
		return strings.TrimSpace(plan.File.Existing.Path)
	}
	return strings.TrimSpace(plan.Candidate.Path)
}

func metadataFromPlan(candidate libraryimport.ImportCandidate) Metadata {
	meta := Metadata{
		Title:           strings.TrimSpace(candidate.Metadata.SelectedTitle),
		Author:          strings.TrimSpace(candidate.Metadata.SelectedAuthor),
		Subtitle:        strings.TrimSpace(candidate.Metadata.Subtitle),
		Series:          strings.TrimSpace(candidate.Metadata.Series),
		Volume:          strings.TrimSpace(candidate.Metadata.SeriesNumber),
		SeriesNumber:    strings.TrimSpace(candidate.Metadata.SeriesNumber),
		Publisher:       strings.TrimSpace(candidate.Metadata.Publisher),
		PublicationYear: strings.TrimSpace(candidate.Metadata.PublicationYear),
		ISBN:            strings.TrimSpace(candidate.Metadata.ISBN),
		Language:        strings.TrimSpace(candidate.Metadata.Language),
		Description:     strings.TrimSpace(candidate.Metadata.Description),
		Tags:            append([]string(nil), candidate.Metadata.Tags...),
		Library:         strings.TrimSpace(candidate.Metadata.Library),
		Source:          "filename_fallback",
		Confidence:      library.ConfidenceMedium,
	}
	for _, identifier := range candidate.Metadata.Identifiers {
		if strings.EqualFold(identifier.Provider, "isbn") && meta.ISBN == "" {
			meta.ISBN = identifier.Value
		}
	}
	if candidate.Metadata.EmbeddedTitle != "" || candidate.Metadata.EmbeddedAuthor != "" {
		meta.Source = "embedded_metadata"
		meta.Confidence = library.ConfidenceHigh
	}
	if candidate.MetadataOverride.SelectedTitle != "" || candidate.MetadataOverride.SelectedAuthor != "" {
		meta.Source = "manual_edit"
		meta.Confidence = library.ConfidenceHigh
	}
	if meta.Title == "" && candidate.Metadata.FilenameTitle != "" {
		meta.Title = candidate.Metadata.FilenameTitle
	}
	if meta.Author == "" && candidate.Metadata.FilenameAuthor != "" {
		meta.Author = candidate.Metadata.FilenameAuthor
	}
	if meta.Title == "" {
		meta.Title = strings.TrimSuffix(filepath.Base(candidate.Path), filepath.Ext(candidate.Path))
		meta.Source = "filename_fallback"
		meta.Confidence = library.ConfidenceLow
	}
	return meta
}

func duplicateReason(plan libraryimport.ImportPlan) (string, string) {
	for _, evidence := range plan.File.Evidence {
		switch evidence.Signal {
		case "duplicate_path":
			return evidence.Signal, "Same destination already exists"
		case "duplicate_content_hash":
			return evidence.Signal, "Identical hash"
		case "duplicate_format", "planned_duplicate_format":
			return evidence.Signal, "A file with the same format is already planned or imported"
		}
	}
	for _, evidence := range plan.Evidence {
		if strings.TrimSpace(evidence.Signal) != "" && strings.Contains(evidence.Signal, "duplicate") {
			return evidence.Signal, firstNonBlank(evidence.Explanation, "Duplicate metadata or destination")
		}
	}
	return "", firstEvidenceExplanation(plan, "Duplicate")
}

func duplicateDetails(plan libraryimport.ImportPlan, signal, reason string, candidate *Candidate) *DuplicateDetails {
	details := &DuplicateDetails{
		Reason:         reason,
		Signal:         signal,
		ExistingTitle:  candidate.Title,
		ExistingAuthor: candidate.Author,
	}
	if plan.File.Existing != nil {
		details.ExistingFormat = plan.File.Existing.Format
		details.ExistingPath = plan.File.Existing.Path
		details.ExistingTitle = firstNonBlank(plan.File.Existing.EmbeddedMetadata["title"], details.ExistingTitle)
		details.ExistingAuthor = firstNonBlank(plan.File.Existing.EmbeddedMetadata["author"], details.ExistingAuthor)
	}
	if details.ExistingPath == "" {
		details.ExistingPath = candidate.ExistingPath
	}
	return details
}

func firstEvidenceExplanation(plan libraryimport.ImportPlan, fallback string) string {
	for _, evidence := range plan.File.Evidence {
		if strings.TrimSpace(evidence.Explanation) != "" {
			return evidence.Explanation
		}
	}
	for _, evidence := range plan.Evidence {
		if strings.TrimSpace(evidence.Explanation) != "" {
			return evidence.Explanation
		}
	}
	return fallback
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func countTotals(candidates []Candidate) Totals {
	totals := Totals{Found: len(candidates)}
	for _, c := range candidates {
		switch c.Classification {
		case ClassificationNew:
			totals.ReadyToImport++
		case ClassificationManualReview:
			totals.ManualReview++
		case ClassificationAlreadyImported:
			totals.AlreadyImported++
		case ClassificationDuplicate:
			totals.Duplicates++
		case ClassificationUnsupported:
			totals.Unsupported++
		case ClassificationUnreadable:
			totals.Unreadable++
		}
	}
	return totals
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func candidateID(c Candidate) string {
	h := sha256.New()
	_, _ = io.WriteString(h, string(c.MediaType))
	_, _ = io.WriteString(h, "\x00")
	_, _ = io.WriteString(h, c.Path)
	_, _ = io.WriteString(h, "\x00")
	_, _ = io.WriteString(h, c.RelativePath)
	_, _ = io.WriteString(h, "\x00")
	_, _ = io.WriteString(h, fmt.Sprint(c.Size))
	_, _ = io.WriteString(h, "\x00")
	_, _ = io.WriteString(h, fmt.Sprint(c.ModifiedAt.UnixNano()))
	return hex.EncodeToString(h.Sum(nil))[:24]
}

func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	}
	return hex.EncodeToString(b[:])
}

func (m *Manager) update(jobID string, fn func(*Progress)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job := m.jobs[jobID]; job != nil {
		fn(&job.Progress)
		job.Status = job.Progress.Status
	}
}

func (m *Manager) jobsStartedAt(jobID string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job := m.jobs[jobID]; job != nil {
		return job.StartedAt
	}
	return time.Time{}
}

func (m *Manager) pruneLocked() {
	if len(m.order) <= maxCompletedJobs {
		return
	}
	kept := m.order[:0]
	for _, id := range m.order {
		job := m.jobs[id]
		if id == m.activeID || job == nil || job.CompletedAt == nil {
			kept = append(kept, id)
			continue
		}
		if len(m.order)-len(kept) <= maxCompletedJobs {
			kept = append(kept, id)
			continue
		}
		delete(m.jobs, id)
	}
	m.order = kept
}

func (m *Manager) cloneJobLocked(jobID string) *Job {
	return cloneJob(m.jobs[jobID])
}

func cloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	copy := *job
	copy.Progress.Warnings = append([]Warning(nil), job.Progress.Warnings...)
	if job.Result != nil {
		result := *job.Result
		result.Candidates = append([]Candidate(nil), job.Result.Candidates...)
		result.Warnings = append([]Warning(nil), job.Result.Warnings...)
		copy.Result = &result
	}
	return &copy
}
