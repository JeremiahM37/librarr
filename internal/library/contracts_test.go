package library

import (
	"context"
	"testing"
)

type noOpMatcher struct{}

func (noOpMatcher) MatchBook(context.Context, BookCandidate) (MatchResult[Book], error) {
	return MatchResult[Book]{Decision: MatchCreateNew, Confidence: ConfidenceLow}, nil
}

func (noOpMatcher) MatchEdition(context.Context, EditionCandidate) (MatchResult[Edition], error) {
	return MatchResult[Edition]{Decision: MatchCreateNew, Confidence: ConfidenceLow}, nil
}

func (noOpMatcher) ResolveContributor(context.Context, ContributorCandidate) (MatchResult[Contributor], error) {
	return MatchResult[Contributor]{Decision: MatchCreateNew, Confidence: ConfidenceLow}, nil
}

func (noOpMatcher) ResolveIdentifier(context.Context, IdentifierCandidate) (MatchResult[Identifier], error) {
	return MatchResult[Identifier]{Decision: MatchCreateNew, Confidence: ConfidenceLow}, nil
}

func TestMatchingContracts(t *testing.T) {
	var matcher MatchingService = noOpMatcher{}
	result, err := matcher.MatchBook(context.Background(), BookCandidate{
		Title: "The Guardian's Path",
		Evidence: []Evidence{{
			Signal: "filename_title", Value: "The Guardian's Path", Confidence: ConfidenceLow,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != MatchCreateNew || result.Confidence != ConfidenceLow {
		t.Fatalf("match result = %+v", result)
	}
}
