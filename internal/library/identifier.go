package library

import (
	"fmt"
	"strings"
	"time"
)

type Identifier struct {
	ID         int64
	Scope      IdentifierScope
	Provider   string
	Value      string
	Source     string
	Confidence Confidence
	CreatedAt  time.Time
}

func (i Identifier) Validate() error {
	if i.Scope != IdentifierScopeBook && i.Scope != IdentifierScopeEdition {
		return fmt.Errorf("%w: identifier scope is required", ErrInvalidDomainObject)
	}
	if strings.TrimSpace(i.Provider) == "" || strings.TrimSpace(i.Value) == "" {
		return fmt.Errorf("%w: identifier provider and value are required", ErrInvalidDomainObject)
	}
	return nil
}
