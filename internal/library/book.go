package library

import (
	"fmt"
	"strings"
	"time"
)

type Book struct {
	ID               int64
	Title            string
	SortTitle        string
	Description      string
	PublicationYear  int
	Language         string
	MediaType        MediaType
	Monitored        bool
	Status           BookStatus
	PreferredEdition *Edition
	Contributors     []Contributor
	Series           []BookSeries
	Identifiers      []Identifier
	Covers           []Cover
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (b Book) Validate() error {
	if strings.TrimSpace(b.Title) == "" {
		return fmt.Errorf("%w: book title is required", ErrInvalidDomainObject)
	}
	if b.MediaType != "" && !b.MediaType.Valid() {
		return fmt.Errorf("%w: media type %q is invalid", ErrInvalidDomainObject, b.MediaType)
	}
	return nil
}

type BookQuery struct {
	Title     string
	Author    string
	MediaType MediaType
}
