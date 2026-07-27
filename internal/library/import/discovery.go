package libraryimport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamie75/librarr/internal/library"
)

var supportedFormats = map[string]library.MediaType{
	".epub": library.MediaTypeEbook,
	".mobi": library.MediaTypeEbook,
	".azw3": library.MediaTypeEbook,
	".pdf":  library.MediaTypeEbook,
	".cbz":  library.MediaTypeComic,
	".cbr":  library.MediaTypeComic,
	".mp3":  library.MediaTypeAudiobook,
	".m4b":  library.MediaTypeAudiobook,
}

func discoverCandidates(ctx context.Context, pc PlanningContext) ([]ImportCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Clean(pc.RootPath)
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}

	if info.IsDir() && mediaTypeForContext(pc) == library.MediaTypeAudiobook {
		candidate, err := newDirectoryCandidate(root, filepath.Base(root), pc.Source.MediaType, firstNonEmptyString(pc.OriginalPath, root))
		if err != nil {
			return nil, err
		}
		candidate.TitleHint = pc.TitleHint
		candidate.AuthorHint = pc.AuthorHint
		candidate.MetadataOverride = pc.MetadataOverride
		return []ImportCandidate{candidate}, nil
	}

	if !info.IsDir() {
		candidate, ok, err := newFileCandidate(root, filepath.Base(root), pc.Source.MediaType, firstNonEmptyString(pc.OriginalPath, root))
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: %s", library.ErrUnsupportedFormat, root)
		}
		candidate.TitleHint = pc.TitleHint
		candidate.AuthorHint = pc.AuthorHint
		candidate.MetadataOverride = pc.MetadataOverride
		return []ImportCandidate{candidate}, nil
	}

	var candidates []ImportCandidate
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		candidate, ok, err := newFileCandidate(path, rel, pc.Source.MediaType, firstNonEmptyString(pc.OriginalPath, path))
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		candidate.TitleHint = pc.TitleHint
		candidate.AuthorHint = pc.AuthorHint
		candidate.MetadataOverride = pc.MetadataOverride
		candidates = append(candidates, candidate)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return candidates, nil
}

func newFileCandidate(path, relative string, hinted library.MediaType, originalPath string) (ImportCandidate, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return ImportCandidate{}, false, err
	}
	ext := strings.ToLower(filepath.Ext(path))
	mediaType, ok := supportedFormats[ext]
	if !ok {
		return ImportCandidate{}, false, nil
	}
	if hinted.Valid() {
		mediaType = hinted
	}
	hash, err := fileSHA256(path)
	if err != nil {
		return ImportCandidate{}, false, err
	}
	return ImportCandidate{
		Path:         path,
		RelativePath: relative,
		OriginalPath: originalPath,
		MediaType:    normalizeMediaType(mediaType, ext),
		Format:       strings.TrimPrefix(ext, "."),
		Size:         info.Size(),
		ContentHash:  hash,
		Evidence: []PlanningEvidence{{
			Signal:      "discovered_file",
			Value:       relative,
			Source:      "filesystem",
			Confidence:  library.ConfidenceExact,
			Explanation: "Planner discovered an importable file",
		}},
	}, true, nil
}

func newDirectoryCandidate(path, relative string, hinted library.MediaType, originalPath string) (ImportCandidate, error) {
	size, err := directorySize(path)
	if err != nil {
		return ImportCandidate{}, err
	}
	mediaType := hinted
	if !mediaType.Valid() {
		mediaType = library.MediaTypeAudiobook
	}
	return ImportCandidate{
		Path:         path,
		RelativePath: relative,
		OriginalPath: originalPath,
		MediaType:    mediaType,
		Format:       "directory",
		Size:         size,
		IsDirectory:  true,
		Evidence: []PlanningEvidence{{
			Signal:      "discovered_directory",
			Value:       relative,
			Source:      "filesystem",
			Confidence:  library.ConfidenceHigh,
			Explanation: "Planner discovered an audiobook directory candidate",
		}},
	}, nil
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

func directorySize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func mediaTypeForContext(pc PlanningContext) library.MediaType {
	if pc.Source.MediaType.Valid() {
		return pc.Source.MediaType
	}
	return library.MediaTypeEbook
}

func normalizeMediaType(mediaType library.MediaType, ext string) library.MediaType {
	switch ext {
	case ".cbz", ".cbr":
		if mediaType == library.MediaTypeManga {
			return library.MediaTypeManga
		}
		return library.MediaTypeComic
	default:
		return mediaType
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
