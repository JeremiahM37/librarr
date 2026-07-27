package library

import "strings"

type MediaType string

const (
	MediaTypeEbook     MediaType = "ebook"
	MediaTypeAudiobook MediaType = "audiobook"
	MediaTypeManga     MediaType = "manga"
	MediaTypeComic     MediaType = "comic"
)

func (m MediaType) Valid() bool {
	switch m {
	case MediaTypeEbook, MediaTypeAudiobook, MediaTypeManga, MediaTypeComic:
		return true
	default:
		return false
	}
}

type BookStatus string

const (
	BookStatusUnknown   BookStatus = ""
	BookStatusWanted    BookStatus = "wanted"
	BookStatusOwned     BookStatus = "owned"
	BookStatusMonitored BookStatus = "monitored"
	BookStatusSkipped   BookStatus = "skipped"
)

type ContributorRole string

const (
	RoleAuthor      ContributorRole = "author"
	RoleEditor      ContributorRole = "editor"
	RoleTranslator  ContributorRole = "translator"
	RoleIllustrator ContributorRole = "illustrator"
	RoleNarrator    ContributorRole = "narrator"
	RoleContributor ContributorRole = "contributor"
)

type IdentifierScope string

const (
	IdentifierScopeBook    IdentifierScope = "book"
	IdentifierScopeEdition IdentifierScope = "edition"
)

type Confidence string

const (
	ConfidenceNone   Confidence = "none"
	ConfidenceLow    Confidence = "low"
	ConfidenceMedium Confidence = "medium"
	ConfidenceHigh   Confidence = "high"
	ConfidenceExact  Confidence = "exact"
)

type MatchDecision string

const (
	MatchNone      MatchDecision = "none"
	MatchExisting  MatchDecision = "existing"
	MatchCreateNew MatchDecision = "create_new"
	MatchAmbiguous MatchDecision = "ambiguous"
)

func NormalizeKey(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}
