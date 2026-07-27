package library

import (
	"fmt"
	"strings"
	"time"
)

type Cover struct {
	ID        int64
	BookID    int64
	EditionID int64
	Source    string
	SourceURL string
	LocalPath string
	MimeType  string
	Width     int
	Height    int
	IsPrimary bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (c Cover) Validate() error {
	if c.BookID == 0 && c.EditionID == 0 {
		return fmt.Errorf("%w: cover owner is required", ErrInvalidDomainObject)
	}
	if c.BookID != 0 && c.EditionID != 0 {
		return fmt.Errorf("%w: cover can have only one owner", ErrInvalidDomainObject)
	}
	if strings.TrimSpace(c.LocalPath) == "" && strings.TrimSpace(c.SourceURL) == "" {
		return fmt.Errorf("%w: cover local path or source URL is required", ErrInvalidDomainObject)
	}
	return nil
}
