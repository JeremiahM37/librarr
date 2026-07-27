package library

import (
	"errors"
	"testing"
)

func TestDomainValidation(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "book requires title", err: Book{MediaType: MediaTypeEbook}.Validate()},
		{name: "book rejects invalid media type", err: Book{Title: "Dune", MediaType: MediaType("movie")}.Validate()},
		{name: "edition requires title", err: Edition{}.Validate()},
		{name: "contributor requires name", err: Contributor{}.Validate()},
		{name: "series requires title", err: Series{}.Validate()},
		{name: "identifier requires owner scope", err: Identifier{Provider: "isbn13", Value: "1"}.Validate()},
		{name: "identifier requires value", err: Identifier{Scope: IdentifierScopeBook, Provider: "isbn13"}.Validate()},
		{name: "cover requires owner", err: Cover{LocalPath: "/covers/a.jpg"}.Validate()},
		{name: "cover rejects two owners", err: Cover{BookID: 1, EditionID: 2, LocalPath: "/covers/a.jpg"}.Validate()},
		{name: "file requires edition", err: BookFile{Format: "epub"}.Validate()},
		{name: "file requires format", err: BookFile{EditionID: 1}.Validate()},
	}

	for _, tt := range tests {
		if !errors.Is(tt.err, ErrInvalidDomainObject) {
			t.Fatalf("%s: error = %v, want ErrInvalidDomainObject", tt.name, tt.err)
		}
	}

	if err := (Book{Title: "Dune", MediaType: MediaTypeEbook}).Validate(); err != nil {
		t.Fatalf("valid book rejected: %v", err)
	}
	if err := (Identifier{Scope: IdentifierScopeBook, Provider: "isbn13", Value: "9780441172719"}).Validate(); err != nil {
		t.Fatalf("valid identifier rejected: %v", err)
	}
	if err := (Cover{EditionID: 1, SourceURL: "https://example.invalid/cover.jpg"}).Validate(); err != nil {
		t.Fatalf("valid cover rejected: %v", err)
	}
	if err := (BookFile{EditionID: 1, Format: "epub", MediaType: MediaTypeEbook}).Validate(); err != nil {
		t.Fatalf("valid file rejected: %v", err)
	}
}

func TestNormalizeKey(t *testing.T) {
	got := NormalizeKey("  The   Guardian's PATH  ")
	if got != "the guardian's path" {
		t.Fatalf("NormalizeKey = %q", got)
	}
}
