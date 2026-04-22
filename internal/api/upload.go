package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// allowedUploadExts defines accepted file extensions for upload.
var allowedUploadExts = map[string]string{
	".epub": "ebook",
	".pdf":  "ebook",
	".mobi": "ebook",
	".azw3": "ebook",
	".m4b":  "audiobook",
	".mp3":  "audiobook",
	".zip":  "archive",
	".rar":  "archive",
}

// maxUploadSize is 500MB.
const maxUploadSize = 500 << 20

const (
	maxUploadTitleLength  = 200
	maxUploadAuthorLength = 120
	maxUploadListLimit    = 200
	maxUploadListOffset   = 10000
)

var uploadMetadataPattern = regexp.MustCompile(`^[\p{L}\p{N}\s\-._,'&():]+$`)

func sanitizeUploadFilename(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	if base == "." || base == "" {
		return "upload"
	}
	return strings.ReplaceAll(base, string(filepath.Separator), "_")
}

func hasPrefixBytes(content []byte, prefix []byte) bool {
	if len(content) < len(prefix) {
		return false
	}
	for i := range prefix {
		if content[i] != prefix[i] {
			return false
		}
	}
	return true
}

// isUploadSignatureAllowed does a lightweight signature check to stop obvious
// extension spoofing while keeping uploads fast.
func isUploadSignatureAllowed(ext string, header []byte) bool {
	header = bytes.TrimSpace(header)
	if len(header) == 0 {
		return false
	}

	switch ext {
	case ".pdf":
		return hasPrefixBytes(header, []byte("%PDF-"))
	case ".epub", ".zip":
		return len(header) >= 4 && header[0] == 0x50 && header[1] == 0x4B
	case ".rar":
		return hasPrefixBytes(header, []byte{0x52, 0x61, 0x72, 0x21})
	case ".mobi", ".azw3":
		return hasPrefixBytes(header, []byte("BOOK")) || (len(header) >= 3 && header[0] == 0xEB && header[2] == 0x48)
	case ".mp3":
		return hasPrefixBytes(header, []byte("ID3")) || (len(header) >= 2 && header[0] == 0xFF && (header[1]&0xE0) == 0xE0)
	case ".m4b":
		return len(header) >= 12 && string(header[4:8]) == "ftyp"
	default:
		return false
	}
}

func validateUploadMetadata(title, author string) error {
	if len(title) > maxUploadTitleLength {
		return fmt.Errorf("Title too long (max %d characters)", maxUploadTitleLength)
	}
	if len(author) > maxUploadAuthorLength {
		return fmt.Errorf("Author too long (max %d characters)", maxUploadAuthorLength)
	}
	if title != "" && !uploadMetadataPattern.MatchString(title) {
		return errors.New("Title contains unsupported characters")
	}
	if author != "" && !uploadMetadataPattern.MatchString(author) {
		return errors.New("Author contains unsupported characters")
	}
	return nil
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Enforce size limit.
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "File too large or invalid multipart form (max 500MB)",
		})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "No file provided (use form field 'file')",
		})
		return
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(header.Filename))
	fileType, ok := allowedUploadExts[ext]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Unsupported file type: %s (accepted: .epub, .pdf, .mobi, .azw3, .m4b, .mp3, .zip, .rar)", ext),
		})
		return
	}

	// Read a small prefix first so we can validate signature before persisting.
	prefix := make([]byte, 512)
	n, readErr := io.ReadFull(file, prefix)
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Failed to inspect uploaded file",
		})
		return
	}
	prefix = prefix[:n]
	if !isUploadSignatureAllowed(ext, prefix) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Uploaded file content does not match file extension",
		})
		return
	}

	// Determine media type from form field or extension.
	mediaType := r.FormValue("media_type")
	if mediaType == "" {
		if fileType == "archive" {
			mediaType = "ebook" // default for archives
		} else {
			mediaType = fileType
		}
	}

	// Save to temp file.
	tmpDir := os.TempDir()
	tmpFile, err := os.CreateTemp(tmpDir, "librarr-upload-*"+ext)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to create temp file",
		})
		return
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
	}()

	reader := io.MultiReader(bytes.NewReader(prefix), file)
	written, err := io.Copy(tmpFile, reader)
	if err != nil {
		os.Remove(tmpPath)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to save uploaded file",
		})
		return
	}

	username, _ := r.Context().Value(ctxUsername).(string)
	title := strings.TrimSpace(r.FormValue("title"))
	author := strings.TrimSpace(r.FormValue("author"))
	if title == "" {
		// Use a sanitized version of the original filename as fallback title.
		title = strings.TrimSpace(strings.TrimSuffix(sanitizeUploadFilename(header.Filename), ext))
	}
	if err := validateUploadMetadata(title, author); err != nil {
		os.Remove(tmpPath)
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Organize the file.
	var organizedPath string
	var orgErr error

	switch mediaType {
	case "ebook":
		if s.organizer != nil {
			organizedPath, orgErr = s.organizer.OrganizeEbook(tmpPath, title, author)
		} else {
			organizedPath = tmpPath
		}
		if orgErr == nil && s.targets != nil {
			s.targets.ImportEbook(organizedPath, title, author)
		}
	case "audiobook":
		if s.organizer != nil {
			organizedPath, orgErr = s.organizer.OrganizeAudiobook(tmpPath, title, author)
		} else {
			organizedPath = tmpPath
		}
		if orgErr == nil && s.targets != nil {
			s.targets.ImportAudiobook()
		}
	default:
		organizedPath = tmpPath
	}

	status := "completed"
	errMsg := ""
	if orgErr != nil {
		slog.Warn("upload organize failed", "error", orgErr)
		status = "error"
		errMsg = orgErr.Error()
		organizedPath = tmpPath
	}

	// Record in database.
	s.db.SaveUpload(username, filepath.Base(organizedPath), sanitizeUploadFilename(header.Filename), mediaType, written, organizedPath, status, errMsg)

	// Log activity.
	s.db.LogActivity(username, "upload", sanitizeUploadFilename(header.Filename), fmt.Sprintf("Uploaded %s (%s, %d bytes)", sanitizeUploadFilename(header.Filename), mediaType, written))

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  orgErr == nil,
		"filename": sanitizeUploadFilename(header.Filename),
		"type":     mediaType,
		"size":     written,
		"error":    errMsg,
	})
}

func (s *Server) handleListUploads(w http.ResponseWriter, r *http.Request) {
	limit := QueryIntBounded(r, "limit", 50, 1, maxUploadListLimit)
	offset := QueryIntBounded(r, "offset", 0, 0, maxUploadListOffset)

	uploads, err := s.db.GetUploads(limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if uploads == nil {
		uploads = nil
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"uploads": uploads,
		"limit":   limit,
		"offset":  offset,
	})
}
