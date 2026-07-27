package library

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MetadataScope string

const (
	MetadataScopeBook    MetadataScope = "book"
	MetadataScopeEdition MetadataScope = "edition"
)

type MetadataField string

const (
	MetadataFieldTitle           MetadataField = "title"
	MetadataFieldEditionTitle    MetadataField = "edition_title"
	MetadataFieldSubtitle        MetadataField = "subtitle"
	MetadataFieldDescription     MetadataField = "description"
	MetadataFieldGenres          MetadataField = "genres"
	MetadataFieldLanguage        MetadataField = "language"
	MetadataFieldPublicationDate MetadataField = "publication_date"
	MetadataFieldPublisher       MetadataField = "publisher"
)

type MetadataEntry struct {
	Field          MetadataField
	Value          string
	Source         string
	Confidence     Confidence
	UpdatedAt      time.Time
	ManualOverride bool
}

type MetadataEvidence struct {
	Field          MetadataField
	Value          string
	Source         string
	Confidence     Confidence
	UpdatedAt      time.Time
	ManualOverride bool
	Selected       bool
}

type MetadataContributor struct {
	Name           string
	Role           ContributorRole
	Source         string
	Confidence     Confidence
	UpdatedAt      time.Time
	ManualOverride bool
}

type MetadataIdentifier struct {
	Type           string
	Value          string
	Source         string
	Confidence     Confidence
	UpdatedAt      time.Time
	ManualOverride bool
}

type BookMetadata struct {
	BookID             int64
	EffectiveEditionID int64
	Fields             map[MetadataField]MetadataEntry
	Contributors       []MetadataContributor
	Identifiers        []MetadataIdentifier
}

type BookMetadataProvenance struct {
	BookID             int64
	EffectiveEditionID int64
	Fields             map[MetadataField][]MetadataEvidence
	Contributors       []MetadataContributor
	Identifiers        []MetadataIdentifier
}

type BookMetadataPatch struct {
	Fields map[MetadataField]string
}

type MetadataUpdate struct {
	BookID       int64
	Source       string
	Confidence   Confidence
	Fields       map[MetadataField]string
	UpdatedAt    time.Time
	ScopeByField map[MetadataField]MetadataScope
}

type MetadataEngine struct {
	repo MetadataRepository
}

func NewMetadataEngine(repo MetadataRepository) (*MetadataEngine, error) {
	if repo == nil {
		return nil, fmt.Errorf("metadata repository is required")
	}
	return &MetadataEngine{repo: repo}, nil
}

func (e *MetadataEngine) GetBookMetadata(ctx context.Context, bookID int64) (*BookMetadata, error) {
	return e.repo.GetBookMetadata(ctx, bookID)
}

func (e *MetadataEngine) GetBookProvenance(ctx context.Context, bookID int64) (*BookMetadataProvenance, error) {
	return e.repo.GetBookProvenance(ctx, bookID)
}

func (e *MetadataEngine) PatchBookMetadata(ctx context.Context, bookID int64, patch BookMetadataPatch) (*BookMetadata, error) {
	cleaned := make(map[MetadataField]string, len(patch.Fields))
	for field, value := range patch.Fields {
		if !field.Valid() {
			return nil, fmt.Errorf("%w: unsupported metadata field %q", ErrInvalidDomainObject, field)
		}
		trimmed := sanitizeMetadataValue(field, value)
		if trimmed == "" {
			continue
		}
		cleaned[field] = trimmed
	}
	if len(cleaned) == 0 {
		return e.repo.GetBookMetadata(ctx, bookID)
	}
	return e.repo.PatchBookMetadata(ctx, bookID, BookMetadataPatch{Fields: cleaned})
}

func (e *MetadataEngine) ApplyBookMetadataSource(ctx context.Context, update MetadataUpdate) (*BookMetadata, error) {
	if update.BookID == 0 {
		return nil, fmt.Errorf("%w: book id is required", ErrInvalidDomainObject)
	}
	if strings.TrimSpace(update.Source) == "" {
		return nil, fmt.Errorf("%w: metadata source is required", ErrInvalidDomainObject)
	}
	if update.Confidence == "" {
		update.Confidence = ConfidenceMedium
	}
	cleaned := make(map[MetadataField]string, len(update.Fields))
	for field, value := range update.Fields {
		if !field.Valid() {
			return nil, fmt.Errorf("%w: unsupported metadata field %q", ErrInvalidDomainObject, field)
		}
		trimmed := sanitizeMetadataValue(field, value)
		if trimmed == "" {
			continue
		}
		cleaned[field] = trimmed
	}
	if len(cleaned) == 0 {
		return e.repo.GetBookMetadata(ctx, update.BookID)
	}
	update.Fields = cleaned
	return e.repo.ApplyBookMetadataSource(ctx, update)
}

func (f MetadataField) Valid() bool {
	switch f {
	case MetadataFieldTitle,
		MetadataFieldEditionTitle,
		MetadataFieldSubtitle,
		MetadataFieldDescription,
		MetadataFieldGenres,
		MetadataFieldLanguage,
		MetadataFieldPublicationDate,
		MetadataFieldPublisher:
		return true
	default:
		return false
	}
}

func BookMetadataFieldOrder() []MetadataField {
	return []MetadataField{
		MetadataFieldTitle,
		MetadataFieldEditionTitle,
		MetadataFieldSubtitle,
		MetadataFieldDescription,
		MetadataFieldGenres,
		MetadataFieldLanguage,
		MetadataFieldPublicationDate,
		MetadataFieldPublisher,
	}
}

func MetadataFieldScope(field MetadataField) MetadataScope {
	switch field {
	case MetadataFieldEditionTitle, MetadataFieldSubtitle, MetadataFieldPublicationDate, MetadataFieldPublisher:
		return MetadataScopeEdition
	default:
		return MetadataScopeBook
	}
}

func sanitizeMetadataValue(field MetadataField, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if field == MetadataFieldGenres {
		parts := strings.Split(value, ",")
		clean := make([]string, 0, len(parts))
		seen := map[string]struct{}{}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			key := strings.ToLower(part)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			clean = append(clean, part)
		}
		return strings.Join(clean, ", ")
	}
	return value
}

func MetadataConfidenceScore(confidence Confidence) int {
	switch confidence {
	case ConfidenceExact:
		return 100
	case ConfidenceHigh:
		return 90
	case ConfidenceMedium:
		return 70
	case ConfidenceLow:
		return 40
	default:
		return 0
	}
}

func sortMetadataFields[T any](m map[MetadataField]T) []MetadataField {
	fields := make([]MetadataField, 0, len(m))
	for field := range m {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i] < fields[j]
	})
	return fields
}
