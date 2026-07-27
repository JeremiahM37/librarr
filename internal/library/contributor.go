package library

import (
	"fmt"
	"strings"
	"time"
)

type Contributor struct {
	ID        int64
	Name      string
	SortName  string
	Roles     []ContributorRole
	Position  int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (c Contributor) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("%w: contributor name is required", ErrInvalidDomainObject)
	}
	return nil
}
