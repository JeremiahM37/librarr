package library

import "context"

type Evidence struct {
	Signal      string
	Value       string
	Source      string
	Confidence  Confidence
	Explanation string
}

type MatchResult[T any] struct {
	Decision   MatchDecision
	Candidate  *T
	Candidates []T
	Confidence Confidence
	Evidence   []Evidence
	Ambiguous  bool
	Reason     string
}

type BookMatcher interface {
	MatchBook(context.Context, BookCandidate) (MatchResult[Book], error)
}

type EditionMatcher interface {
	MatchEdition(context.Context, EditionCandidate) (MatchResult[Edition], error)
}

type ContributorResolver interface {
	ResolveContributor(context.Context, ContributorCandidate) (MatchResult[Contributor], error)
}

type IdentifierResolver interface {
	ResolveIdentifier(context.Context, IdentifierCandidate) (MatchResult[Identifier], error)
}

type MatchingService interface {
	BookMatcher
	EditionMatcher
	ContributorResolver
	IdentifierResolver
}

type BookCandidate struct {
	Title       string
	Author      string
	MediaType   MediaType
	Series      string
	Identifiers []Identifier
	Evidence    []Evidence
}

type EditionCandidate struct {
	BookID      int64
	Title       string
	Subtitle    string
	Publisher   string
	Language    string
	Identifiers []Identifier
	Evidence    []Evidence
}

type ContributorCandidate struct {
	Name string
	Role ContributorRole
}

type IdentifierCandidate struct {
	Provider string
	Value    string
	Scope    IdentifierScope
}
