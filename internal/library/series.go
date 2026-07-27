package library

import (
	"fmt"
	"strings"
	"time"
)

type Series struct {
	ID          int64
	Title       string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (s Series) Validate() error {
	if strings.TrimSpace(s.Title) == "" {
		return fmt.Errorf("%w: series title is required", ErrInvalidDomainObject)
	}
	return nil
}

type BookSeries struct {
	Series          Series
	Position        float64
	DisplayPosition string
}
