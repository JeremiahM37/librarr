package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	"github.com/jamie75/librarr/internal/organize"
)

// importableExtensions are file extensions that can be imported.
var importableExtensions = map[string]bool{
	".epub": true,
	".pdf":  true,
	".mobi": true,
	".azw3": true,
	".mp3":  true,
	".m4b":  true,
	".cbz":  true,
	".cbr":  true,
	".fb2":  true,
}

// ScannedFile represents a file found during directory scan.
type ScannedFile struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Format string `json:"format"`
	Title  string `json:"title"`
	Author string `json:"author"`
}

func (s *Server) handleScanImport(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	if req.Path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Path is required",
		})
		return
	}

	if err := validateAllowedPath(req.Path, s.cfg); err != nil {
		writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	info, err := os.Stat(req.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Path not found: " + err.Error(),
		})
		return
	}
	if !info.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Path must be a directory",
		})
		return
	}

	var files []ScannedFile
	filepath.Walk(req.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if !importableExtensions[ext] {
			return nil
		}

		sf := ScannedFile{
			Path:   path,
			Name:   info.Name(),
			Size:   info.Size(),
			Format: strings.TrimPrefix(ext, "."),
		}

		// Try to extract metadata from EPUB files.
		if ext == ".epub" {
			if meta, err := organize.ExtractEPUBMeta(path); err == nil {
				if meta.Title != "" {
					sf.Title = meta.Title
				}
				if meta.Author != "" {
					sf.Author = meta.Author
				}
			}
		}

		// Fallback: guess title from filename.
		if sf.Title == "" {
			name := strings.TrimSuffix(info.Name(), filepath.Ext(info.Name()))
			// Try to parse "Author - Title" format.
			parts := strings.SplitN(name, " - ", 2)
			if len(parts) == 2 {
				sf.Author = strings.TrimSpace(parts[0])
				sf.Title = strings.TrimSpace(parts[1])
			} else {
				sf.Title = name
			}
		}

		files = append(files, sf)
		return nil
	})

	if files == nil {
		files = []ScannedFile{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"files":   files,
		"count":   len(files),
	})
}

func (s *Server) handleImportFiles(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Files []struct {
			Path      string `json:"path"`
			Title     string `json:"title"`
			Author    string `json:"author"`
			MediaType string `json:"media_type"`
			Copy      bool   `json:"copy"` // true = copy, false = move
		} `json:"files"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}

	imported := 0
	var errors []string

	for _, f := range req.Files {
		if err := validateAllowedPath(f.Path, s.cfg); err != nil {
			errors = append(errors, fmt.Sprintf("Access denied: %s", f.Path))
			continue
		}

		if _, err := os.Stat(f.Path); err != nil {
			errors = append(errors, fmt.Sprintf("File not found: %s", f.Path))
			continue
		}

		mediaType := f.MediaType
		if mediaType == "" {
			ext := strings.ToLower(filepath.Ext(f.Path))
			switch ext {
			case ".mp3", ".m4b":
				mediaType = "audiobook"
			case ".cbz", ".cbr":
				mediaType = "manga"
			default:
				mediaType = "ebook"
			}
		}

		title := f.Title
		if title == "" {
			title = strings.TrimSuffix(filepath.Base(f.Path), filepath.Ext(f.Path))
		}

		var destPath string
		var orgErr error

		switch mediaType {
		case "audiobook":
			destPath, orgErr = s.organizer.OrganizeAudiobook(f.Path, title, f.Author)
		case "manga":
			destPath, orgErr = s.organizer.OrganizeManga(f.Path, title)
		default:
			destPath, orgErr = s.organizer.OrganizeEbook(f.Path, title, f.Author)
		}

		if orgErr != nil {
			errors = append(errors, fmt.Sprintf("Failed to organize %s: %v", f.Path, orgErr))
			continue
		}

		result, err := s.importer().Import(context.Background(), libraryimport.ImportRequest{
			Source: library.ImportSource{
				Name:      "manual_import",
				MediaType: manualImportMediaType(mediaType),
			},
			RootPath:     destPath,
			OriginalPath: f.Path,
			TitleHint:    title,
			AuthorHint:   f.Author,
		})
		if err != nil {
			errors = append(errors, fmt.Sprintf("Failed to save %s: %v", title, err))
			continue
		}
		if result == nil || result.InsertedCount >= 0 {
			imported++
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"imported": imported,
		"errors":   errors,
	})
}

func manualImportMediaType(value string) library.MediaType {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "audiobook":
		return library.MediaTypeAudiobook
	case "manga":
		return library.MediaTypeManga
	case "comic":
		return library.MediaTypeComic
	default:
		return library.MediaTypeEbook
	}
}
