package library

import (
	"fmt"
	"strings"
	"time"
)

type Edition struct {
	ID              int64
	BookID          int64
	Title           string
	Subtitle        string
	Description     string
	Publisher       string
	PublicationDate string
	Language        string
	PageCount       int
	EditionName     string
	Contributors    []Contributor
	Identifiers     []Identifier
	Covers          []Cover
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (e Edition) Validate() error {
	if strings.TrimSpace(e.Title) == "" {
		return fmt.Errorf("%w: edition title is required", ErrInvalidDomainObject)
	}
	return nil
}
