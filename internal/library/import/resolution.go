package libraryimport

import (
	"context"
	"errors"
	"strings"

	"github.com/jamie75/librarr/internal/library"
)

type BookResolver struct {
	catalog Catalog
}

func (r *BookResolver) Resolve(ctx context.Context, _ PlanningContext, candidate ImportCandidate, state *batchState) (ResolvedBook, error) {
	title := strings.TrimSpace(candidate.Metadata.SelectedTitle)
	author := strings.TrimSpace(candidate.Metadata.SelectedAuthor)
	if title == "" {
		return ResolvedBook{
			Action: BookActionNeedsManualReview,
			Evidence: []PlanningEvidence{{
				Signal:      "missing_title",
				Source:      "metadata",
				Confidence:  library.ConfidenceLow,
				Explanation: "Planner could not determine a book title",
			}},
		}, nil
	}
	if state != nil {
		if planned := state.findPlannedBook(candidate); planned != nil {
			var existing *library.Book
			if planned.Book.Existing != nil {
				copy := *planned.Book.Existing
				existing = &copy
			}
			var proposed *library.Book
			if planned.Book.Proposed != nil {
				copy := *planned.Book.Proposed
				proposed = &copy
			}
			return ResolvedBook{
				Action:     BookActionReuse,
				Existing:   existing,
				Proposed:   proposed,
				Confidence: library.ConfidenceHigh,
				Evidence: []PlanningEvidence{{
					Signal:      "planned_batch_match",
					Value:       title,
					Source:      "planner_batch",
					Confidence:  library.ConfidenceHigh,
					Explanation: "Candidate matched a book already planned in this import batch",
				}},
			}, nil
		}
	}

	var identifierMatch *library.Book
	identifierIDs := map[int64]struct{}{}
	for _, identifier := range candidate.Metadata.Identifiers {
		book, err := r.catalog.FindBookByIdentifier(ctx, identifier)
		if err != nil {
			if errors.Is(err, library.ErrNotFound) {
				continue
			}
			return ResolvedBook{}, err
		}
		identifierIDs[book.ID] = struct{}{}
		identifierMatch = book
	}
	switch len(identifierIDs) {
	case 1:
		return ResolvedBook{
			Action:     BookActionReuse,
			Existing:   identifierMatch,
			Confidence: library.ConfidenceExact,
			Evidence: []PlanningEvidence{{
				Signal:      "identifier_match",
				Value:       title,
				Source:      "identifier",
				Confidence:  library.ConfidenceExact,
				Explanation: "Existing book matched by trusted identifier",
			}},
		}, nil
	case 2, 3, 4, 5:
		return ResolvedBook{
			Action: BookActionNeedsManualReview,
			Evidence: []PlanningEvidence{{
				Signal:      "ambiguous_identifier_match",
				Value:       title,
				Source:      "identifier",
				Confidence:  library.ConfidenceLow,
				Explanation: "Identifiers resolved to multiple books",
			}},
		}, nil
	}

	books, err := searchBooksByTerms(ctx, r.catalog, candidate.MediaType, bookSearchTerms(title, author))
	if err != nil {
		return ResolvedBook{}, err
	}

	var exactTitleMatches []library.Book
	var exactAuthorMatches []library.Book
	titleKey := importTitleMatchKey(title)
	authorKey := library.ContributorMatchKey(author)
	for _, book := range books {
		fullBook := book
		if fullBook.ID != 0 {
			if loaded, err := r.catalog.GetBook(ctx, fullBook.ID); err == nil && loaded != nil {
				fullBook = *loaded
			}
		}
		if library.TitleMatchKey(fullBook.Title) != titleKey {
			continue
		}
		exactTitleMatches = append(exactTitleMatches, fullBook)
		bookAuthor := library.ContributorMatchKey(primaryContributorName(&fullBook))
		if authorKey != "" && bookAuthor == authorKey {
			exactAuthorMatches = append(exactAuthorMatches, fullBook)
		}
	}

	if len(exactAuthorMatches) == 1 {
		book := exactAuthorMatches[0]
		return ResolvedBook{
			Action:     BookActionReuse,
			Existing:   &book,
			Confidence: library.ConfidenceHigh,
			Evidence: []PlanningEvidence{{
				Signal:      "title_author_match",
				Value:       title,
				Source:      "catalog",
				Confidence:  library.ConfidenceHigh,
				Explanation: "Existing book matched by exact title and primary author",
			}},
		}, nil
	}
	if len(exactAuthorMatches) > 1 {
		return ResolvedBook{
			Action: BookActionNeedsManualReview,
			Evidence: []PlanningEvidence{{
				Signal:      "ambiguous_title_author_match",
				Value:       title,
				Source:      "catalog",
				Confidence:  library.ConfidenceLow,
				Explanation: "Multiple existing books share the same title and author",
			}},
		}, nil
	}
	if author == "" && len(exactTitleMatches) == 1 {
		book := exactTitleMatches[0]
		return ResolvedBook{
			Action:     BookActionReuse,
			Existing:   &book,
			Confidence: library.ConfidenceMedium,
			Evidence: []PlanningEvidence{{
				Signal:      "title_only_match",
				Value:       title,
				Source:      "catalog",
				Confidence:  library.ConfidenceMedium,
				Explanation: "Existing book matched by exact title with no incoming author",
			}},
		}, nil
	}
	if len(exactTitleMatches) > 0 {
		var existing *library.Book
		if len(exactTitleMatches) == 1 {
			book := exactTitleMatches[0]
			existing = &book
		}
		return ResolvedBook{
			Action:   BookActionNeedsManualReview,
			Existing: existing,
			Evidence: []PlanningEvidence{{
				Signal:      "title_conflict",
				Value:       title,
				Source:      "catalog",
				Confidence:  library.ConfidenceLow,
				Explanation: "Existing title match did not cleanly agree on author",
			}},
		}, nil
	}

	book := &library.Book{
		Title:     title,
		SortTitle: library.NormalizeKey(title),
		MediaType: candidate.MediaType,
	}
	return ResolvedBook{
		Action:     BookActionCreate,
		Proposed:   book,
		Confidence: library.ConfidenceHigh,
		Evidence: []PlanningEvidence{{
			Signal:      "new_book",
			Value:       title,
			Source:      "planner",
			Confidence:  library.ConfidenceHigh,
			Explanation: "No existing logical book matched the candidate",
		}},
	}, nil
}

func importTitleMatchKey(value string) string {
	return library.TitleMatchKey(value)
}

type bookSearcher interface {
	SearchBooks(context.Context, library.BookQuery) ([]library.Book, error)
}

func searchBooksByTerms(ctx context.Context, searcher bookSearcher, mediaType library.MediaType, terms []string) ([]library.Book, error) {
	var books []library.Book
	for _, term := range uniqueSearchTerms(terms) {
		found, err := searcher.SearchBooks(ctx, library.BookQuery{Title: term, MediaType: mediaType})
		if err != nil {
			if errors.Is(err, library.ErrNotFound) || errors.Is(err, library.ErrBookNotFound) {
				continue
			}
			return nil, err
		}
		books = appendUniqueBooksByID(books, found)
	}
	return books, nil
}

func bookSearchTerms(title, author string) []string {
	var terms []string
	terms = append(terms, strings.TrimSpace(title))
	terms = append(terms, leadingMatchWords(library.TitleMatchKey(title), 3))
	terms = append(terms, strings.TrimSpace(author))
	terms = append(terms, leadingMatchWords(library.ContributorMatchKey(author), 2))
	return terms
}

func uniqueSearchTerms(terms []string) []string {
	seen := map[string]struct{}{}
	unique := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		key := library.NormalizeKey(term)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, term)
	}
	return unique
}

func leadingMatchWords(value string, count int) string {
	fields := strings.Fields(value)
	if len(fields) <= count {
		return strings.Join(fields, " ")
	}
	return strings.Join(fields[:count], " ")
}

func appendUniqueBooksByID(books []library.Book, extra []library.Book) []library.Book {
	seen := make(map[int64]struct{}, len(books))
	for _, book := range books {
		if book.ID != 0 {
			seen[book.ID] = struct{}{}
		}
	}
	for _, book := range extra {
		if book.ID != 0 {
			if _, ok := seen[book.ID]; ok {
				continue
			}
			seen[book.ID] = struct{}{}
		}
		books = append(books, book)
	}
	return books
}

type EditionResolver struct {
	catalog Catalog
}

func (r *EditionResolver) Resolve(ctx context.Context, candidate ImportCandidate, book ResolvedBook, state *batchState) (ResolvedEdition, error) {
	title := strings.TrimSpace(candidate.Metadata.SelectedTitle)
	if state != nil {
		if planned := state.findPlannedBook(candidate); planned != nil {
			var existing *library.Edition
			if planned.Edition.Existing != nil {
				copy := *planned.Edition.Existing
				existing = &copy
			}
			var proposed *library.Edition
			if planned.Edition.Proposed != nil {
				copy := *planned.Edition.Proposed
				proposed = &copy
			}
			return ResolvedEdition{
				Action:     EditionActionReuse,
				Existing:   existing,
				Proposed:   proposed,
				Confidence: library.ConfidenceHigh,
				Evidence: []PlanningEvidence{{
					Signal:      "planned_batch_edition_match",
					Value:       title,
					Source:      "planner_batch",
					Confidence:  library.ConfidenceHigh,
					Explanation: "Candidate matched an edition already planned in this import batch",
				}},
			}, nil
		}
	}
	if book.Action == BookActionCreate {
		edition := &library.Edition{
			Title:    title,
			Language: candidate.Metadata.Language,
		}
		return ResolvedEdition{
			Action:     EditionActionCreate,
			Proposed:   edition,
			Confidence: library.ConfidenceHigh,
			Evidence: []PlanningEvidence{{
				Signal:      "new_edition",
				Value:       title,
				Source:      "planner",
				Confidence:  library.ConfidenceHigh,
				Explanation: "New book requires a first edition record",
			}},
		}, nil
	}
	if book.Existing == nil {
		return ResolvedEdition{
			Action: EditionActionNeedsManualReview,
			Evidence: []PlanningEvidence{{
				Signal:      "missing_book_context",
				Source:      "planner",
				Confidence:  library.ConfidenceLow,
				Explanation: "Planner could not resolve a stable book for edition matching",
			}},
		}, nil
	}

	edition, err := r.catalog.FindEdition(ctx, book.Existing.ID, title)
	if err == nil && edition != nil {
		return ResolvedEdition{
			Action:     EditionActionReuse,
			Existing:   edition,
			Confidence: library.ConfidenceHigh,
			Evidence: []PlanningEvidence{{
				Signal:      "edition_title_match",
				Value:       title,
				Source:      "catalog",
				Confidence:  library.ConfidenceHigh,
				Explanation: "Existing edition matched by exact title under the selected book",
			}},
		}, nil
	}
	if err != nil && !errors.Is(err, library.ErrNotFound) {
		return ResolvedEdition{}, err
	}

	proposed := &library.Edition{
		BookID:   book.Existing.ID,
		Title:    title,
		Language: candidate.Metadata.Language,
	}
	return ResolvedEdition{
		Action:     EditionActionCreate,
		Proposed:   proposed,
		Confidence: library.ConfidenceMedium,
		Evidence: []PlanningEvidence{{
			Signal:      "edition_create_under_existing_book",
			Value:       title,
			Source:      "planner",
			Confidence:  library.ConfidenceMedium,
			Explanation: "Book matched but no exact existing edition title was found",
		}},
	}, nil
}

type ContributorResolver struct {
	catalog Catalog
}

func (r *ContributorResolver) Resolve(ctx context.Context, candidate ImportCandidate, _ ResolvedBook, edition ResolvedEdition) ([]ResolvedContributor, error) {
	author := strings.TrimSpace(candidate.Metadata.SelectedAuthor)
	if author == "" {
		return nil, nil
	}
	proposed := library.Contributor{Name: author, SortName: library.NormalizeKey(author), Roles: []library.ContributorRole{library.RoleAuthor}}
	if edition.Existing != nil {
		contributors, err := r.catalog.GetEditionContributors(ctx, edition.Existing.ID)
		if err != nil && !errors.Is(err, library.ErrNotFound) {
			return nil, err
		}
		for _, contributor := range contributors {
			if library.NormalizeKey(contributor.Name) == library.NormalizeKey(author) {
				match := contributor
				return []ResolvedContributor{{
					Action:     ContributorActionReuse,
					Existing:   &match,
					Role:       library.RoleAuthor,
					Confidence: library.ConfidenceHigh,
					Evidence: []PlanningEvidence{{
						Signal:      "contributor_match",
						Value:       author,
						Source:      "catalog",
						Confidence:  library.ConfidenceHigh,
						Explanation: "Existing edition already has the selected author contributor",
					}},
				}}, nil
			}
		}
	}
	return []ResolvedContributor{{
		Action:     ContributorActionCreate,
		Proposed:   &proposed,
		Role:       library.RoleAuthor,
		Confidence: library.ConfidenceMedium,
		Evidence: []PlanningEvidence{{
			Signal:      "new_contributor",
			Value:       author,
			Source:      "planner",
			Confidence:  library.ConfidenceMedium,
			Explanation: "Planner would create or attach an author contributor",
		}},
	}}, nil
}

type DuplicateMatch struct {
	File   *library.BookFile
	Reason string
}

type DuplicateDetector struct {
	catalog Catalog
}

func (d *DuplicateDetector) Detect(ctx context.Context, candidate ImportCandidate) (*DuplicateMatch, error) {
	if file, err := d.catalog.FindFileByPath(ctx, candidate.Path); err == nil && file != nil {
		return &DuplicateMatch{File: file, Reason: "path"}, nil
	} else if err != nil && !errors.Is(err, library.ErrNotFound) {
		return nil, err
	}
	if candidate.ContentHash != "" {
		files, err := d.catalog.FindFilesByContentHash(ctx, candidate.ContentHash)
		if err == nil && len(files) > 0 {
			file := files[0]
			return &DuplicateMatch{File: &file, Reason: "content_hash"}, nil
		} else if err != nil && !errors.Is(err, library.ErrNotFound) {
			return nil, err
		}
	}
	return nil, nil
}

type FilePlanner struct {
	catalog Catalog
}

func (p *FilePlanner) Resolve(ctx context.Context, candidate ImportCandidate, book ResolvedBook, edition ResolvedEdition, duplicate *DuplicateMatch, state *batchState) (FileDecision, error) {
	if duplicate != nil {
		return FileDecision{
			Action:     FileActionIgnoreDuplicate,
			Existing:   duplicate.File,
			Confidence: library.ConfidenceExact,
			Evidence: []PlanningEvidence{{
				Signal:      "duplicate_" + duplicate.Reason,
				Value:       candidate.Path,
				Source:      "catalog",
				Confidence:  library.ConfidenceExact,
				Explanation: "Existing file already matches this candidate",
			}},
		}, nil
	}
	if book.Action == BookActionNeedsManualReview || edition.Action == EditionActionNeedsManualReview {
		return FileDecision{
			Action:     FileActionNeedsManualReview,
			Confidence: library.ConfidenceLow,
			Evidence: []PlanningEvidence{{
				Signal:      "manual_review_required",
				Value:       candidate.Path,
				Source:      "planner",
				Confidence:  library.ConfidenceLow,
				Explanation: "Book or edition matching remained ambiguous",
			}},
		}, nil
	}
	if book.Action == BookActionConflict || edition.Action == EditionActionConflict {
		return FileDecision{
			Action:     FileActionConflict,
			Confidence: library.ConfidenceLow,
			Evidence: []PlanningEvidence{{
				Signal:      "resolution_conflict",
				Value:       candidate.Path,
				Source:      "planner",
				Confidence:  library.ConfidenceLow,
				Explanation: "Planner found a conflicting match state before file planning",
			}},
		}, nil
	}

	var existingFiles []library.BookFile
	var err error
	if book.Existing != nil {
		existingFiles, err = p.catalog.GetBookFiles(ctx, book.Existing.ID)
		if err != nil && !errors.Is(err, library.ErrNotFound) {
			return FileDecision{}, err
		}
	}
	for _, existing := range existingFiles {
		if strings.EqualFold(existing.Format, candidate.Format) {
			return FileDecision{
				Action:     FileActionConflict,
				Existing:   &existing,
				Confidence: library.ConfidenceMedium,
				Evidence: []PlanningEvidence{{
					Signal:      "duplicate_format",
					Value:       candidate.Format,
					Source:      "catalog",
					Confidence:  library.ConfidenceMedium,
					Explanation: "A file with the same format already exists for the resolved book",
				}},
			}, nil
		}
	}
	if state != nil {
		if planned := state.findPlannedBook(candidate); planned != nil {
			for _, existing := range plannedPlansFiles(planned) {
				if strings.EqualFold(existing.Format, candidate.Format) {
					copy := existing
					return FileDecision{
						Action:     FileActionConflict,
						Existing:   &copy,
						Confidence: library.ConfidenceMedium,
						Evidence: []PlanningEvidence{{
							Signal:      "planned_duplicate_format",
							Value:       candidate.Format,
							Source:      "planner_batch",
							Confidence:  library.ConfidenceMedium,
							Explanation: "A file with the same format is already planned in this import batch",
						}},
					}, nil
				}
			}
		}
	}

	file := &library.BookFile{
		MediaType:    candidate.MediaType,
		Format:       candidate.Format,
		Path:         candidate.Path,
		OriginalPath: candidate.Path,
		Size:         candidate.Size,
		ContentHash:  candidate.ContentHash,
		SourceID:     candidate.RelativePath,
		SourceType:   string(candidate.MediaType),
		Managed:      false,
		EmbeddedMetadata: map[string]string{
			"title":  candidate.Metadata.SelectedTitle,
			"author": candidate.Metadata.SelectedAuthor,
		},
	}
	action := FileActionCreate
	explanation := "Planner would create a first managed file for this logical book"
	if book.Existing != nil {
		action = FileActionAttachNewFormat
		explanation = "Planner would attach this format to an existing logical book"
	}
	return FileDecision{
		Action:     action,
		Proposed:   file,
		Confidence: library.ConfidenceHigh,
		Evidence: []PlanningEvidence{{
			Signal:      string(action),
			Value:       candidate.Format,
			Source:      "planner",
			Confidence:  library.ConfidenceHigh,
			Explanation: explanation,
		}},
	}, nil
}

func plannedPlansFiles(plan *ImportPlan) []library.BookFile {
	var files []library.BookFile
	if plan.File.Existing != nil {
		files = append(files, *plan.File.Existing)
	}
	if plan.File.Proposed != nil {
		files = append(files, *plan.File.Proposed)
	}
	return files
}
