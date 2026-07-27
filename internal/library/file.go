package library

import (
	"fmt"
	"io"
	"strings"
	"time"
)

type BookFile struct {
	ID                   int64
	BookID               int64
	EditionID            int64
	MediaType            MediaType
	Format               string
	Path                 string
	OriginalPath         string
	Size                 int64
	ContentHash          string
	SourceID             string
	SourceType           string
	Quality              string
	Managed              bool
	EmbeddedMetadata     map[string]string
	EmbeddedMetadataJSON string
	ImportedAt           time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func (f BookFile) Validate() error {
	if f.EditionID == 0 {
		return fmt.Errorf("%w: file edition is required", ErrInvalidDomainObject)
	}
	if strings.TrimSpace(f.Format) == "" {
		return fmt.Errorf("%w: file format is required", ErrInvalidDomainObject)
	}
	if f.MediaType != "" && !f.MediaType.Valid() {
		return fmt.Errorf("%w: media type %q is invalid", ErrInvalidDomainObject, f.MediaType)
	}
	return nil
}

type DownloadFile struct {
	File     BookFile
	Name     string
	MimeType string
	Body     io.ReadCloser
}
