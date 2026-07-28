package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jamie75/librarr/internal/library"
)

const (
	nestedEbookRepairReady           = "ready"
	nestedEbookRepairCollision       = "collision"
	nestedEbookRepairMissing         = "missing"
	nestedEbookRepairUnsafe          = "unsafe"
	nestedEbookRepairAlreadyRepaired = "already_repaired"
	nestedEbookRepairMoved           = "moved"
	nestedEbookRepairFailed          = "failed"

	nestedEbookClassCatalogedNormalized = "cataloged_normalized"
	nestedEbookClassCatalogedLegacyOnly = "cataloged_legacy_only"
	nestedEbookClassCatalogedUnmanaged  = "cataloged_unmanaged"
	nestedEbookClassDuplicatePhysical   = "duplicate_physical_copy"
	nestedEbookClassUncataloged         = "uncataloged"
	nestedEbookClassMissingDBRecord     = "missing_db_record"
)

type nestedEbookRepairResponse struct {
	Success             bool                         `json:"success"`
	Executed            bool                         `json:"executed"`
	EbookRoot           string                       `json:"ebook_root"`
	LegacyRoot          string                       `json:"legacy_root"`
	TotalAffectedFiles  int                          `json:"total_affected_files"`
	TotalAffectedBooks  int                          `json:"total_affected_books"`
	FilesFoundOnDisk    int                          `json:"files_found_on_disk"`
	CorrectRootFiles    int                          `json:"correct_root_files"`
	Reconciliation      map[string]int               `json:"reconciliation"`
	Summary             map[string]int               `json:"summary"`
	Entries             []nestedEbookRepairPlanEntry `json:"entries"`
	LegacyRootRemoved   bool                         `json:"legacy_root_removed,omitempty"`
	RemovedEmptyFolders []string                     `json:"removed_empty_folders,omitempty"`
	Error               string                       `json:"error,omitempty"`
}

type nestedEbookRepairPlanEntry struct {
	BookID          int64  `json:"book_id"`
	BookTitle       string `json:"book_title,omitempty"`
	FileID          int64  `json:"file_id"`
	Format          string `json:"format,omitempty"`
	SourcePath      string `json:"source_path"`
	DestinationPath string `json:"destination_path"`
	Status          string `json:"status"`
	Message         string `json:"message,omitempty"`
	Class           string `json:"class,omitempty"`
	MatchedPath     string `json:"matched_path,omitempty"`
	MatchedRecord   string `json:"matched_record,omitempty"`
	Hash            string `json:"hash,omitempty"`
	Size            int64  `json:"size,omitempty"`
}

type nestedEbookRepairCatalogRecord struct {
	Kind         string
	ID           int64
	BookID       int64
	BookTitle    string
	Format       string
	Path         string
	OriginalPath string
	Hash         string
	Size         int64
	Managed      bool
	Incoming     bool
}

type nestedEbookDiskFile struct {
	Path string
	Rel  string
	Hash string
	Size int64
}

func (s *Server) handleV1NestedEbookPathRepairPreview(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	plan, err := s.buildNestedEbookRepairPlan(r.Context())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, nestedEbookRepairResponse{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) handleV1NestedEbookPathRepairRun(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	plan, err := s.buildNestedEbookRepairPlan(r.Context())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, nestedEbookRepairResponse{Success: false, Error: err.Error()})
		return
	}
	executed := s.executeNestedEbookRepair(r.Context(), plan)
	writeJSON(w, http.StatusOK, executed)
}

func (s *Server) buildNestedEbookRepairPlan(ctx context.Context) (nestedEbookRepairResponse, error) {
	ebookRoot, err := configuredEbookRoot(s.cfg.EbookDir)
	if err != nil {
		return nestedEbookRepairResponse{}, err
	}
	legacyRoot := filepath.Join(ebookRoot, filepath.Base(ebookRoot))
	if sameCleanPath(ebookRoot, legacyRoot) {
		return nestedEbookRepairResponse{}, fmt.Errorf("configured ebook root cannot produce a nested repair root")
	}
	resolvedRoot, err := existingResolvedPath(ebookRoot)
	if err != nil {
		return nestedEbookRepairResponse{}, fmt.Errorf("ebook root is not available: %w", err)
	}
	resolvedLegacyRoot, err := existingResolvedPath(legacyRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nestedEbookRepairResponse{}, fmt.Errorf("legacy nested ebook root is unsafe: %w", err)
	}

	books, err := s.listAllBookReadModels(ctx, library.MediaTypeEbook)
	if err != nil {
		return nestedEbookRepairResponse{}, err
	}
	normalizedRecords := map[string]nestedEbookRepairCatalogRecord{}
	normalizedByHash := map[string][]nestedEbookRepairCatalogRecord{}
	affectedBooks := map[int64]struct{}{}
	seenSourcePaths := map[string]int64{}
	entries := []nestedEbookRepairPlanEntry{}
	normalizedInspected := 0
	matchingNestedPaths := 0
	for _, book := range books {
		files, err := s.library().GetBookFiles(ctx, book.Book.ID)
		if err != nil {
			return nestedEbookRepairResponse{}, err
		}
		for _, file := range files {
			normalizedInspected++
			record := nestedRepairRecordFromNormalized(book, file, s.cfg.IncomingDir)
			indexNestedRepairRecord(normalizedRecords, normalizedByHash, record)
			entry, ok := buildNestedEbookRepairEntry(book, file, ebookRoot, legacyRoot, resolvedRoot, resolvedLegacyRoot)
			if !ok {
				continue
			}
			matchingNestedPaths++
			if previousID, ok := seenSourcePaths[entry.SourcePath]; ok {
				entry.Status = nestedEbookRepairUnsafe
				entry.Message = fmt.Sprintf("duplicate catalog references share this physical path with file %d", previousID)
			} else {
				seenSourcePaths[entry.SourcePath] = entry.FileID
			}
			affectedBooks[book.Book.ID] = struct{}{}
			entries = append(entries, entry)
		}
	}
	legacyRecords, legacyByHash, legacyOnlyRecords, err := s.nestedEbookLegacyRecords(ctx, normalizedRecords, legacyRoot, s.cfg.IncomingDir)
	if err != nil {
		return nestedEbookRepairResponse{}, err
	}
	nestedFiles, err := collectNestedRepairDiskFiles(legacyRoot)
	if err != nil {
		return nestedEbookRepairResponse{}, err
	}
	correctFiles, err := collectNestedRepairDiskFiles(ebookRoot)
	if err != nil {
		return nestedEbookRepairResponse{}, err
	}
	reconciliationEntries := reconcileNestedEbookDiskFiles(nestedFiles, correctFiles, ebookRoot, legacyRoot, normalizedRecords, normalizedByHash, legacyRecords, legacyByHash)
	entries = append(entries, reconciliationEntries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].SourcePath == entries[j].SourcePath {
			return entries[i].FileID < entries[j].FileID
		}
		return entries[i].SourcePath < entries[j].SourcePath
	})
	plan := nestedEbookRepairResponse{
		Success:            true,
		Executed:           false,
		EbookRoot:          ebookRoot,
		LegacyRoot:         legacyRoot,
		TotalAffectedFiles: len(entries),
		TotalAffectedBooks: len(affectedBooks),
		FilesFoundOnDisk:   len(nestedFiles),
		CorrectRootFiles:   countCorrectRootFilesOutsideLegacy(correctFiles, legacyRoot),
		Reconciliation:     nestedRepairReconciliationSummary(entries),
		Summary:            nestedRepairSummary(entries),
		Entries:            entries,
	}
	slog.Info("nested ebook path repair preview built",
		"normalized_records_inspected", normalizedInspected,
		"matching_nested_paths", matchingNestedPaths,
		"ready", plan.Summary[nestedEbookRepairReady],
		"collision", plan.Summary[nestedEbookRepairCollision],
		"missing", plan.Summary[nestedEbookRepairMissing],
		"unsafe", plan.Summary[nestedEbookRepairUnsafe],
		"uncataloged_filesystem_files", plan.Reconciliation[nestedEbookClassUncataloged],
		"legacy_only_records", legacyOnlyRecords,
		"unmanaged_records", plan.Reconciliation[nestedEbookClassCatalogedUnmanaged],
		"duplicate_physical_copies", plan.Reconciliation[nestedEbookClassDuplicatePhysical],
		"files_found_on_disk", plan.FilesFoundOnDisk,
	)
	return plan, nil
}

func (s *Server) listAllBookReadModels(ctx context.Context, mediaType library.MediaType) ([]library.BookReadModel, error) {
	const pageSize = 500
	var all []library.BookReadModel
	for offset := 0; ; offset += pageSize {
		items, err := s.library().ListBookReadModels(ctx, library.ListBooksQuery{
			MediaType: mediaType,
			Sort:      "title",
			Order:     "asc",
			Limit:     pageSize,
			Offset:    offset,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
		if len(items) < pageSize {
			break
		}
	}
	return all, nil
}

func nestedRepairRecordFromNormalized(book library.BookReadModel, file library.BookFile, incomingRoot string) nestedEbookRepairCatalogRecord {
	path := filepath.Clean(strings.TrimSpace(file.Path))
	originalPath := filepath.Clean(strings.TrimSpace(file.OriginalPath))
	return nestedEbookRepairCatalogRecord{
		Kind:         "normalized",
		ID:           file.ID,
		BookID:       book.Book.ID,
		BookTitle:    book.Book.Title,
		Format:       file.Format,
		Path:         path,
		OriginalPath: originalPath,
		Hash:         strings.ToLower(strings.TrimSpace(file.ContentHash)),
		Size:         file.Size,
		Managed:      file.Managed,
		Incoming:     pathUnderOptionalRoot(path, incomingRoot) || pathUnderOptionalRoot(originalPath, incomingRoot),
	}
}

func indexNestedRepairRecord(byPath map[string]nestedEbookRepairCatalogRecord, byHash map[string][]nestedEbookRepairCatalogRecord, record nestedEbookRepairCatalogRecord) {
	if record.Path != "" && record.Path != "." && filepath.IsAbs(record.Path) {
		byPath[record.Path] = record
	}
	if record.OriginalPath != "" && record.OriginalPath != "." && filepath.IsAbs(record.OriginalPath) {
		if _, ok := byPath[record.OriginalPath]; !ok {
			byPath[record.OriginalPath] = record
		}
	}
	if record.Hash != "" {
		byHash[record.Hash] = append(byHash[record.Hash], record)
	}
}

func (s *Server) nestedEbookLegacyRecords(ctx context.Context, normalizedByPath map[string]nestedEbookRepairCatalogRecord, legacyRoot, incomingRoot string) (map[string]nestedEbookRepairCatalogRecord, map[string][]nestedEbookRepairCatalogRecord, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}
	items, err := s.db.GetItems("ebook", 100000, 0)
	if err != nil {
		return nil, nil, 0, err
	}
	byPath := map[string]nestedEbookRepairCatalogRecord{}
	byHash := map[string][]nestedEbookRepairCatalogRecord{}
	legacyOnly := 0
	for _, item := range items {
		record := nestedEbookRepairCatalogRecord{
			Kind:         "legacy",
			ID:           item.ID,
			BookTitle:    item.Title,
			Format:       item.FileFormat,
			Path:         filepath.Clean(strings.TrimSpace(item.FilePath)),
			OriginalPath: filepath.Clean(strings.TrimSpace(item.OriginalPath)),
			Hash:         strings.ToLower(strings.TrimSpace(item.ContentHash)),
			Size:         item.FileSize,
			Managed:      true,
			Incoming:     pathUnderOptionalRoot(item.FilePath, incomingRoot) || pathUnderOptionalRoot(item.OriginalPath, incomingRoot),
		}
		if record.Path != "" && record.Path != "." && pathWithin(record.Path, legacyRoot) {
			if _, ok := normalizedByPath[record.Path]; !ok {
				legacyOnly++
			}
		}
		indexNestedRepairRecord(byPath, byHash, record)
	}
	return byPath, byHash, legacyOnly, nil
}

func collectNestedRepairDiskFiles(root string) ([]nestedEbookDiskFile, error) {
	if strings.TrimSpace(root) == "" {
		return nil, nil
	}
	if _, err := os.Lstat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var files []nestedEbookDiskFile
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			files = append(files, nestedEbookDiskFile{Path: filepath.Clean(path)})
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil {
			files = append(files, nestedEbookDiskFile{Path: filepath.Clean(path)})
			return nil
		}
		if !info.Mode().IsRegular() {
			files = append(files, nestedEbookDiskFile{Path: filepath.Clean(path), Size: info.Size()})
			return nil
		}
		hash, hashErr := fileSHA256(path)
		if hashErr != nil {
			files = append(files, nestedEbookDiskFile{Path: filepath.Clean(path), Size: info.Size()})
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = filepath.Base(path)
		}
		files = append(files, nestedEbookDiskFile{
			Path: filepath.Clean(path),
			Rel:  rel,
			Hash: hash,
			Size: info.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func reconcileNestedEbookDiskFiles(nestedFiles, correctFiles []nestedEbookDiskFile, ebookRoot, legacyRoot string, normalizedByPath map[string]nestedEbookRepairCatalogRecord, normalizedByHash map[string][]nestedEbookRepairCatalogRecord, legacyByPath map[string]nestedEbookRepairCatalogRecord, legacyByHash map[string][]nestedEbookRepairCatalogRecord) []nestedEbookRepairPlanEntry {
	correctByHash := map[string][]nestedEbookDiskFile{}
	for _, file := range correctFiles {
		if pathWithin(file.Path, legacyRoot) {
			continue
		}
		if file.Hash != "" {
			correctByHash[file.Hash] = append(correctByHash[file.Hash], file)
		}
	}
	var entries []nestedEbookRepairPlanEntry
	for _, disk := range nestedFiles {
		if _, ok := normalizedByPath[disk.Path]; ok {
			continue
		}
		entry := nestedEbookRepairPlanEntry{
			SourcePath: disk.Path,
			Size:       disk.Size,
			Hash:       disk.Hash,
			Status:     nestedEbookRepairAlreadyRepaired,
		}
		rel, relErr := filepath.Rel(legacyRoot, disk.Path)
		if relErr == nil {
			entry.DestinationPath = filepath.Join(ebookRoot, rel)
		}
		if record, ok := legacyByPath[disk.Path]; ok {
			entry.Class = nestedEbookClassCatalogedLegacyOnly
			entry.Status = nestedEbookRepairAlreadyRepaired
			entry.BookTitle = record.BookTitle
			entry.FileID = record.ID
			entry.Format = record.Format
			entry.MatchedRecord = fmt.Sprintf("legacy:library_item:%d", record.ID)
			entry.Message = "legacy compatibility row references this nested file, but no normalized file record does; left unchanged for a future adopt-and-organize workflow"
		} else if record, ok := firstIncomingRecordByHash(disk.Hash, normalizedByHash); ok {
			entry.Class = nestedEbookClassCatalogedUnmanaged
			entry.Status = nestedEbookRepairAlreadyRepaired
			entry.BookID = record.BookID
			entry.BookTitle = record.BookTitle
			entry.FileID = record.ID
			entry.Format = record.Format
			entry.MatchedPath = record.Path
			entry.MatchedRecord = fmt.Sprintf("normalized:file:%d", record.ID)
			entry.Message = "a normalized record with the same checksum is cataloged from the import/incoming area; left unchanged for a future adopt-and-organize workflow"
		} else if record, ok := firstIncomingRecordByHash(disk.Hash, legacyByHash); ok {
			entry.Class = nestedEbookClassCatalogedUnmanaged
			entry.Status = nestedEbookRepairAlreadyRepaired
			entry.BookTitle = record.BookTitle
			entry.FileID = record.ID
			entry.Format = record.Format
			entry.MatchedPath = record.Path
			entry.MatchedRecord = fmt.Sprintf("legacy:library_item:%d", record.ID)
			entry.Message = "a legacy row with the same checksum is cataloged from the import/incoming area; left unchanged for a future adopt-and-organize workflow"
		} else if matches := correctByHash[disk.Hash]; disk.Hash != "" && len(matches) > 0 {
			entry.Class = nestedEbookClassDuplicatePhysical
			entry.Status = nestedEbookRepairAlreadyRepaired
			entry.MatchedPath = matches[0].Path
			entry.Message = "an identical physical copy already exists under the configured ebook root; skipped to avoid deleting either copy"
		} else if entry.DestinationPath != "" {
			if _, err := os.Lstat(entry.DestinationPath); err == nil {
				entry.Class = nestedEbookRepairCollision
				entry.Status = nestedEbookRepairCollision
				entry.Message = "a different file already exists at the repaired destination; skipped to avoid overwriting"
			}
		}
		if entry.Class == "" {
			if disk.Hash == "" {
				entry.Class = nestedEbookRepairUnsafe
				entry.Status = nestedEbookRepairUnsafe
				entry.Message = "file could not be safely fingerprinted; left unchanged"
			} else {
				entry.Class = nestedEbookClassUncataloged
				entry.Status = nestedEbookRepairAlreadyRepaired
				entry.Message = "file exists under the legacy nested root but is not referenced by normalized or legacy catalog data; left unchanged"
			}
		}
		entries = append(entries, entry)
	}
	return entries
}

func firstIncomingRecordByHash(hash string, byHash map[string][]nestedEbookRepairCatalogRecord) (nestedEbookRepairCatalogRecord, bool) {
	if hash == "" {
		return nestedEbookRepairCatalogRecord{}, false
	}
	for _, record := range byHash[hash] {
		if record.Incoming {
			return record, true
		}
	}
	return nestedEbookRepairCatalogRecord{}, false
}

func countCorrectRootFilesOutsideLegacy(files []nestedEbookDiskFile, legacyRoot string) int {
	count := 0
	for _, file := range files {
		if !pathWithin(file.Path, legacyRoot) {
			count++
		}
	}
	return count
}

func buildNestedEbookRepairEntry(book library.BookReadModel, file library.BookFile, ebookRoot, legacyRoot, resolvedRoot, resolvedLegacyRoot string) (nestedEbookRepairPlanEntry, bool) {
	sourcePath := filepath.Clean(strings.TrimSpace(file.Path))
	if sourcePath == "." || !filepath.IsAbs(sourcePath) {
		return nestedEbookRepairPlanEntry{}, false
	}
	if !pathWithin(sourcePath, legacyRoot) {
		return nestedEbookRepairPlanEntry{}, false
	}
	rel, err := filepath.Rel(legacyRoot, sourcePath)
	entry := nestedEbookRepairPlanEntry{
		BookID:          book.Book.ID,
		BookTitle:       book.Book.Title,
		FileID:          file.ID,
		Format:          file.Format,
		SourcePath:      sourcePath,
		DestinationPath: filepath.Join(ebookRoot, rel),
		Status:          nestedEbookRepairReady,
		Message:         "ready to move",
		Class:           nestedEbookClassCatalogedNormalized,
		MatchedRecord:   fmt.Sprintf("normalized:file:%d", file.ID),
		Hash:            file.ContentHash,
		Size:            file.Size,
	}
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		entry.Status = nestedEbookRepairUnsafe
		entry.Message = "source path is not safely contained in the legacy nested ebook root"
		return entry, true
	}
	if !pathWithin(entry.DestinationPath, ebookRoot) || pathWithin(entry.DestinationPath, legacyRoot) {
		entry.Status = nestedEbookRepairUnsafe
		entry.Message = "proposed destination is not safely contained in the configured ebook root"
		return entry, true
	}
	if err := validateNestedRepairSource(sourcePath, resolvedLegacyRoot); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			entry.Status = nestedEbookRepairMissing
			entry.Class = nestedEbookClassMissingDBRecord
			entry.Message = "source file is missing; database path was not changed"
		} else {
			entry.Status = nestedEbookRepairUnsafe
			entry.Message = err.Error()
		}
		return entry, true
	}
	if err := validateNestedRepairDestination(entry.DestinationPath, resolvedRoot); err != nil {
		entry.Status = nestedEbookRepairUnsafe
		entry.Message = err.Error()
		return entry, true
	}
	if _, err := os.Lstat(entry.DestinationPath); err == nil {
		if same, compareErr := sameFileContent(sourcePath, entry.DestinationPath, file.ContentHash); compareErr == nil && same {
			entry.Status = nestedEbookRepairAlreadyRepaired
			entry.Message = "destination already contains identical content; skipped to avoid deleting either copy"
			return entry, true
		}
		entry.Status = nestedEbookRepairCollision
		entry.Message = "destination already exists; skipped to avoid overwriting a different file"
		return entry, true
	} else if !errors.Is(err, os.ErrNotExist) {
		entry.Status = nestedEbookRepairUnsafe
		entry.Message = "destination could not be checked: " + err.Error()
		return entry, true
	}
	return entry, true
}

func configuredEbookRoot(raw string) (string, error) {
	root := filepath.Clean(strings.TrimSpace(raw))
	if root == "" || root == "." {
		return "", fmt.Errorf("ebook library root is not configured")
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("ebook library root must be an absolute path")
	}
	return root, nil
}

func validateNestedRepairSource(path, resolvedLegacyRoot string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("source path is a directory, not a file")
	}
	resolvedSource, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if resolvedLegacyRoot != "" && !pathWithin(resolvedSource, resolvedLegacyRoot) {
		return fmt.Errorf("source symlink resolves outside the legacy nested ebook root")
	}
	return nil
}

func validateNestedRepairDestination(path, resolvedRoot string) error {
	parent := filepath.Dir(path)
	existingParent := parent
	for {
		if info, err := os.Lstat(existingParent); err == nil {
			if !info.IsDir() {
				return fmt.Errorf("destination parent is not a directory")
			}
			break
		} else if errors.Is(err, os.ErrNotExist) {
			next := filepath.Dir(existingParent)
			if next == existingParent {
				return fmt.Errorf("destination parent is not available")
			}
			existingParent = next
			continue
		} else {
			return err
		}
	}
	resolvedParent, err := filepath.EvalSymlinks(existingParent)
	if err != nil {
		return err
	}
	if !pathWithin(resolvedParent, resolvedRoot) {
		return fmt.Errorf("destination parent resolves outside the configured ebook root")
	}
	return nil
}

func existingResolvedPath(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return "", os.ErrNotExist
	}
	return "", err
}

func sameFileContent(source, destination, expectedHash string) (bool, error) {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return false, err
	}
	destInfo, err := os.Stat(destination)
	if err != nil {
		return false, err
	}
	if sourceInfo.Size() != destInfo.Size() {
		return false, nil
	}
	sourceHash, err := fileSHA256(source)
	if err != nil {
		return false, err
	}
	destHash, err := fileSHA256(destination)
	if err != nil {
		return false, err
	}
	if expectedHash != "" && !strings.EqualFold(sourceHash, expectedHash) {
		return false, nil
	}
	return sourceHash == destHash, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (s *Server) executeNestedEbookRepair(ctx context.Context, plan nestedEbookRepairResponse) nestedEbookRepairResponse {
	plan.Executed = true
	ready := plan.Summary[nestedEbookRepairReady]
	var cleanupRoots []string
	for i := range plan.Entries {
		entry := &plan.Entries[i]
		if entry.Status != nestedEbookRepairReady {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(entry.DestinationPath), 0755); err != nil {
			entry.Status = nestedEbookRepairFailed
			entry.Message = "failed to create destination directory: " + err.Error()
			continue
		}
		if err := os.Rename(entry.SourcePath, entry.DestinationPath); err != nil {
			entry.Status = nestedEbookRepairFailed
			entry.Message = "failed to move file: " + err.Error()
			continue
		}
		if _, err := s.library().MoveFile(ctx, entry.FileID, entry.DestinationPath); err != nil {
			if rollbackErr := os.Rename(entry.DestinationPath, entry.SourcePath); rollbackErr != nil {
				entry.Status = nestedEbookRepairFailed
				entry.Message = fmt.Sprintf("database update failed after move (%v); rollback also failed (%v)", err, rollbackErr)
			} else {
				entry.Status = nestedEbookRepairFailed
				entry.Message = "database update failed after move; file was moved back: " + err.Error()
			}
			continue
		}
		entry.Status = nestedEbookRepairMoved
		entry.Message = "file moved and catalog path updated"
		cleanupRoots = append(cleanupRoots, filepath.Dir(entry.SourcePath))
	}
	removed := removeEmptyNestedRepairDirs(cleanupRoots, plan.LegacyRoot)
	plan.RemovedEmptyFolders = removed
	for _, removedPath := range removed {
		if sameCleanPath(removedPath, plan.LegacyRoot) {
			plan.LegacyRootRemoved = true
			break
		}
	}
	plan.Summary = nestedRepairSummary(plan.Entries)
	plan.Reconciliation = nestedRepairReconciliationSummary(plan.Entries)
	if ready == 0 {
		slog.Info("nested ebook path repair executed as no-op",
			"files_found_on_disk", plan.FilesFoundOnDisk,
			"uncataloged_filesystem_files", plan.Reconciliation[nestedEbookClassUncataloged],
			"legacy_only", plan.Reconciliation[nestedEbookClassCatalogedLegacyOnly],
			"unmanaged", plan.Reconciliation[nestedEbookClassCatalogedUnmanaged],
			"duplicates", plan.Reconciliation[nestedEbookClassDuplicatePhysical],
		)
	} else {
		slog.Info("nested ebook path repair executed",
			"ready_before_execution", ready,
			"moved", plan.Summary[nestedEbookRepairMoved],
			"failed", plan.Summary[nestedEbookRepairFailed],
			"collision", plan.Summary[nestedEbookRepairCollision],
			"missing", plan.Summary[nestedEbookRepairMissing],
			"unsafe", plan.Summary[nestedEbookRepairUnsafe],
		)
	}
	return plan
}

func nestedRepairSummary(entries []nestedEbookRepairPlanEntry) map[string]int {
	summary := map[string]int{
		nestedEbookRepairReady:           0,
		nestedEbookRepairMoved:           0,
		nestedEbookRepairCollision:       0,
		nestedEbookRepairMissing:         0,
		nestedEbookRepairUnsafe:          0,
		nestedEbookRepairAlreadyRepaired: 0,
		nestedEbookRepairFailed:          0,
	}
	for _, entry := range entries {
		summary[entry.Status]++
	}
	return summary
}

func nestedRepairReconciliationSummary(entries []nestedEbookRepairPlanEntry) map[string]int {
	summary := map[string]int{
		nestedEbookClassCatalogedNormalized: 0,
		nestedEbookClassCatalogedLegacyOnly: 0,
		nestedEbookClassCatalogedUnmanaged:  0,
		nestedEbookClassDuplicatePhysical:   0,
		nestedEbookClassUncataloged:         0,
		nestedEbookRepairCollision:          0,
		nestedEbookClassMissingDBRecord:     0,
		nestedEbookRepairUnsafe:             0,
	}
	for _, entry := range entries {
		class := strings.TrimSpace(entry.Class)
		if class == "" {
			switch entry.Status {
			case nestedEbookRepairCollision:
				class = nestedEbookRepairCollision
			case nestedEbookRepairMissing:
				class = nestedEbookClassMissingDBRecord
			case nestedEbookRepairUnsafe:
				class = nestedEbookRepairUnsafe
			default:
				continue
			}
		}
		summary[class]++
	}
	return summary
}

func removeEmptyNestedRepairDirs(paths []string, stopRoot string) []string {
	seen := map[string]struct{}{}
	var removed []string
	for _, path := range paths {
		path = filepath.Clean(path)
		for pathWithin(path, stopRoot) {
			if _, ok := seen[path]; ok {
				break
			}
			if err := os.Remove(path); err != nil {
				break
			}
			seen[path] = struct{}{}
			removed = append(removed, path)
			if sameCleanPath(path, stopRoot) {
				break
			}
			path = filepath.Dir(path)
		}
	}
	sort.Strings(removed)
	return removed
}

func pathWithin(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	if sameCleanPath(path, root) {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return false
	}
	return true
}

func pathUnderOptionalRoot(path, root string) bool {
	path = strings.TrimSpace(path)
	root = strings.TrimSpace(root)
	if path == "" || root == "" {
		return false
	}
	cleanPath := filepath.Clean(path)
	cleanRoot := filepath.Clean(root)
	return filepath.IsAbs(cleanPath) && filepath.IsAbs(cleanRoot) && pathWithin(cleanPath, cleanRoot)
}

func sameCleanPath(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}
