package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
)

type nestedEbookRepairResponse struct {
	Success             bool                         `json:"success"`
	Executed            bool                         `json:"executed"`
	EbookRoot           string                       `json:"ebook_root"`
	LegacyRoot          string                       `json:"legacy_root"`
	TotalAffectedFiles  int                          `json:"total_affected_files"`
	TotalAffectedBooks  int                          `json:"total_affected_books"`
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
	affectedBooks := map[int64]struct{}{}
	seenSourcePaths := map[string]int64{}
	entries := []nestedEbookRepairPlanEntry{}
	for _, book := range books {
		files, err := s.library().GetBookFiles(ctx, book.Book.ID)
		if err != nil {
			return nestedEbookRepairResponse{}, err
		}
		for _, file := range files {
			entry, ok := buildNestedEbookRepairEntry(book, file, ebookRoot, legacyRoot, resolvedRoot, resolvedLegacyRoot)
			if !ok {
				continue
			}
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
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].SourcePath == entries[j].SourcePath {
			return entries[i].FileID < entries[j].FileID
		}
		return entries[i].SourcePath < entries[j].SourcePath
	})
	return nestedEbookRepairResponse{
		Success:            true,
		Executed:           false,
		EbookRoot:          ebookRoot,
		LegacyRoot:         legacyRoot,
		TotalAffectedFiles: len(entries),
		TotalAffectedBooks: len(affectedBooks),
		Summary:            nestedRepairSummary(entries),
		Entries:            entries,
	}, nil
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

func sameCleanPath(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}
