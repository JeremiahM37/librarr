package api

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jamie75/librarr/internal/config"
)

// validateAllowedPath ensures path resolves under one of the configured library roots.
func validateAllowedPath(path string, cfg *config.Config) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}

	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("path not found")
	}
	absPath, err := filepath.Abs(realPath)
	if err != nil {
		return fmt.Errorf("invalid path")
	}

	roots := []string{
		cfg.EbookDir,
		cfg.AudiobookDir,
		cfg.MangaDir,
		cfg.IncomingDir,
		cfg.MangaIncomingDir,
		cfg.CalibreLibraryPath,
		cfg.KavitaLibraryPath,
		cfg.KavitaMangaLibraryPath,
		cfg.KomgaLibraryPath,
	}

	for _, root := range roots {
		if root == "" {
			continue
		}
		absRoot, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		realRoot, err := filepath.EvalSymlinks(absRoot)
		if err != nil {
			realRoot = absRoot
		}
		if pathUnderRoot(absPath, realRoot) {
			return nil
		}
	}
	return fmt.Errorf("path is outside allowed directories")
}

func pathUnderRoot(absPath, absRoot string) bool {
	if absPath == absRoot {
		return true
	}
	sep := string(filepath.Separator)
	return strings.HasPrefix(absPath, absRoot+sep)
}
