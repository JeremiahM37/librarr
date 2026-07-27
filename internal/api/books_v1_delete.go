package api

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/library"
)

type v1BookDeleteResponse struct {
	Success      bool                     `json:"success"`
	BookID       int64                    `json:"book_id"`
	Title        string                   `json:"title"`
	DeleteFiles  bool                     `json:"delete_files"`
	DeletedFiles int                      `json:"deleted_files"`
	MissingFiles int                      `json:"missing_files"`
	FailedFiles  int                      `json:"failed_files"`
	Files        []v1BookDeleteFileResult `json:"files,omitempty"`
	Error        string                   `json:"error,omitempty"`
}

type v1BookDeleteFileResult struct {
	ID       int64  `json:"id"`
	Filename string `json:"filename"`
	Format   string `json:"format"`
	Deleted  bool   `json:"deleted"`
	Missing  bool   `json:"missing"`
	Error    string `json:"error,omitempty"`
}

type resolvedBookDeleteFile struct {
	file     library.BookFile
	path     string
	root     string
	missing  bool
	filename string
}

func (s *Server) handleV1BookDelete(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	deleteFiles := queryBool(r, "delete_files", false)

	book, err := s.library().GetBook(r.Context(), bookID)
	if errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book", err)
		return
	}

	files, err := s.library().GetBookFiles(r.Context(), bookID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book files", err)
		return
	}
	var coverPath string
	if cover, err := s.library().GetPrimaryCover(r.Context(), bookID); err == nil && cover != nil {
		coverPath = strings.TrimSpace(cover.LocalPath)
	}

	response := v1BookDeleteResponse{
		Success:     false,
		BookID:      book.ID,
		Title:       book.Title,
		DeleteFiles: deleteFiles,
	}

	if deleteFiles {
		resolved, results, err := resolveBookFilesForDeletion(files, s.cfg)
		response.Files = results
		if err != nil {
			response.Error = err.Error()
			response.FailedFiles = countDeleteFailures(results)
			writeJSON(w, http.StatusBadRequest, response)
			return
		}
		results, hardFailure := deleteResolvedBookFiles(resolved)
		response.Files = results
		response.DeletedFiles = countDeletedFiles(results)
		response.MissingFiles = countMissingFiles(results)
		response.FailedFiles = countDeleteFailures(results)
		if hardFailure {
			response.Error = "One or more files could not be deleted. The book remains in the catalog so deletion can be retried."
			writeJSON(w, http.StatusConflict, response)
			return
		}
	}

	if err := s.library().DeleteBook(r.Context(), bookID); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, library.ErrBookNotFound) {
			status = http.StatusNotFound
		}
		response.Error = "Failed to remove book from catalog"
		writeJSON(w, status, response)
		return
	}

	if s.db != nil {
		username, _ := r.Context().Value(ctxUsername).(string)
		action := "library_remove"
		detail := fmt.Sprintf("Removed book %s from Librarr", book.Title)
		if deleteFiles {
			action = "library_delete_files"
			detail = fmt.Sprintf("Deleted book %s and %d files", book.Title, response.DeletedFiles)
		}
		s.db.LogActivity(username, action, strconv.FormatInt(bookID, 10), detail)
	}
	s.cleanupUnreferencedCoverCache(coverPath)

	response.Success = true
	if !deleteFiles {
		response.Files = nil
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) cleanupUnreferencedCoverCache(path string) {
	path = strings.TrimSpace(path)
	if path == "" || s == nil || s.db == nil || s.coverCache == nil || strings.TrimSpace(s.coverCache.Dir()) == "" {
		return
	}
	roots := []string{}
	root, err := filepath.Abs(filepath.Clean(s.coverCache.Dir()))
	if err == nil {
		roots = append(roots, root)
		if real := evalRootOrEmpty(root); real != "" {
			roots = append(roots, real)
		}
	}
	cleanPath, root, missing, err := resolveManagedDeletePath(path, roots)
	if err != nil || missing {
		return
	}
	var references int
	if err := s.db.SQLDB().QueryRow(`SELECT COUNT(*) FROM covers WHERE local_path = ?`, path).Scan(&references); err != nil || references > 0 {
		return
	}
	if err := os.Remove(cleanPath); err == nil {
		removeEmptyLibraryParents(cleanPath, root)
	}
}

func resolveBookFilesForDeletion(files []library.BookFile, cfg *config.Config) ([]resolvedBookDeleteFile, []v1BookDeleteFileResult, error) {
	roots := normalizedDeleteRoots(cfg)
	if len(roots) == 0 {
		return nil, nil, fmt.Errorf("No configured library roots are available for file deletion")
	}

	resolved := make([]resolvedBookDeleteFile, 0, len(files))
	results := make([]v1BookDeleteFileResult, 0, len(files))
	seen := map[string]struct{}{}
	var problems []string
	for _, file := range files {
		result := v1BookDeleteFileResult{
			ID:       file.ID,
			Filename: filepath.Base(strings.TrimSpace(file.Path)),
			Format:   strings.ToUpper(strings.TrimSpace(file.Format)),
		}
		path, root, missing, err := resolveManagedDeletePath(file.Path, roots)
		if err != nil {
			result.Error = err.Error()
			problems = append(problems, fmt.Sprintf("%s: %s", result.Filename, err.Error()))
			results = append(results, result)
			continue
		}
		if _, ok := seen[path]; ok {
			result.Missing = missing
			results = append(results, result)
			continue
		}
		seen[path] = struct{}{}
		if result.Filename == "." || result.Filename == string(filepath.Separator) || result.Filename == "" {
			result.Filename = filepath.Base(path)
		}
		resolved = append(resolved, resolvedBookDeleteFile{file: file, path: path, root: root, missing: missing, filename: result.Filename})
		results = append(results, result)
	}
	if len(problems) > 0 {
		return nil, results, fmt.Errorf("Unsafe file deletion request: %s", strings.Join(problems, "; "))
	}
	return resolved, results, nil
}

func normalizedDeleteRoots(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}
	rawRoots := []string{cfg.EbookDir, cfg.AudiobookDir, cfg.MangaDir}
	roots := make([]string, 0, len(rawRoots))
	seen := map[string]struct{}{}
	for _, root := range rawRoots {
		root = strings.TrimSpace(root)
		if root == "" || strings.ContainsRune(root, 0) {
			continue
		}
		abs, err := filepath.Abs(filepath.Clean(root))
		if err != nil {
			continue
		}
		for _, candidate := range []string{abs, evalRootOrEmpty(abs)} {
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			roots = append(roots, candidate)
		}
	}
	return roots
}

func evalRootOrEmpty(root string) string {
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		return ""
	}
	abs, err := filepath.Abs(real)
	if err != nil {
		return ""
	}
	return abs
}

func resolveManagedDeletePath(rawPath string, roots []string) (string, string, bool, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", "", false, fmt.Errorf("file path is missing")
	}
	if strings.ContainsRune(rawPath, 0) {
		return "", "", false, fmt.Errorf("file path is invalid")
	}
	if !filepath.IsAbs(rawPath) {
		return "", "", false, fmt.Errorf("file path is not absolute")
	}
	cleanPath, err := filepath.Abs(filepath.Clean(rawPath))
	if err != nil {
		return "", "", false, fmt.Errorf("file path is invalid")
	}
	root, ok := containingRoot(cleanPath, roots)
	if !ok {
		return "", "", false, fmt.Errorf("file path is outside configured library roots")
	}

	info, err := os.Lstat(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return cleanPath, root, true, nil
		}
		return "", "", false, fmt.Errorf("file path cannot be inspected")
	}
	if info.Mode()&os.ModeSymlink != 0 {
		realPath, err := filepath.EvalSymlinks(cleanPath)
		if err != nil {
			return "", "", false, fmt.Errorf("symlink target cannot be verified")
		}
		realPath, err = filepath.Abs(realPath)
		if err != nil {
			return "", "", false, fmt.Errorf("symlink target is invalid")
		}
		if _, ok := containingRoot(realPath, roots); !ok {
			return "", "", false, fmt.Errorf("symlink target escapes configured library roots")
		}
	}
	return cleanPath, root, false, nil
}

func containingRoot(path string, roots []string) (string, bool) {
	for _, root := range roots {
		if pathUnderRoot(path, root) {
			return root, true
		}
	}
	return "", false
}

func deleteResolvedBookFiles(files []resolvedBookDeleteFile) ([]v1BookDeleteFileResult, bool) {
	results := make([]v1BookDeleteFileResult, 0, len(files))
	hardFailure := false
	for _, item := range files {
		result := v1BookDeleteFileResult{
			ID:       item.file.ID,
			Filename: item.filename,
			Format:   strings.ToUpper(strings.TrimSpace(item.file.Format)),
		}
		if item.missing {
			result.Missing = true
			results = append(results, result)
			continue
		}
		if err := os.Remove(item.path); err != nil {
			if os.IsNotExist(err) {
				result.Missing = true
			} else {
				result.Error = sanitizedDeleteError(err)
				hardFailure = true
			}
			results = append(results, result)
			continue
		}
		result.Deleted = true
		removeEmptyLibraryParents(item.path, item.root)
		results = append(results, result)
	}
	return results, hardFailure
}

func sanitizedDeleteError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, os.ErrPermission) {
		return "permission denied"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "directory not empty"):
		return "directory is not empty"
	case strings.Contains(msg, "is a directory"):
		return "path is a directory"
	case strings.Contains(msg, "read-only file system"):
		return "read-only file system"
	default:
		return "delete failed"
	}
}

func removeEmptyLibraryParents(path, root string) {
	parent := filepath.Dir(path)
	for {
		if parent == "." || parent == string(filepath.Separator) || parent == "" || parent == root || !pathUnderRoot(parent, root) {
			return
		}
		if err := os.Remove(parent); err != nil {
			return
		}
		parent = filepath.Dir(parent)
	}
}

func countDeletedFiles(results []v1BookDeleteFileResult) int {
	count := 0
	for _, result := range results {
		if result.Deleted {
			count++
		}
	}
	return count
}

func countMissingFiles(results []v1BookDeleteFileResult) int {
	count := 0
	for _, result := range results {
		if result.Missing {
			count++
		}
	}
	return count
}

func countDeleteFailures(results []v1BookDeleteFileResult) int {
	count := 0
	for _, result := range results {
		if result.Error != "" {
			count++
		}
	}
	return count
}

func queryBool(r *http.Request, key string, fallback bool) bool {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
