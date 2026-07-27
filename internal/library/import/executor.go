package libraryimport

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/library"
)

type ImportExecutor struct {
	writer RepositoryWriter
}

func NewImportExecutor(writer RepositoryWriter) *ImportExecutor {
	return &ImportExecutor{writer: writer}
}

func (e *ImportExecutor) Execute(ctx context.Context, ec ExecutionContext, plans []ImportPlan) (ExecutionSummary, error) {
	if e == nil || e.writer == nil {
		return ExecutionSummary{}, fmt.Errorf("repository writer is required")
	}
	summary := ExecutionSummary{
		Results: make([]ExecutionResult, 0, len(plans)),
		Counts:  map[string]int{},
	}
	for _, plan := range plans {
		result := e.ExecutePlan(ctx, ec, plan)
		summary.Results = append(summary.Results, result)
		summary.Counts[string(result.Status)]++
	}
	return summary, nil
}

func (e *ImportExecutor) ExecutePlan(ctx context.Context, ec ExecutionContext, plan ImportPlan) ExecutionResult {
	result := ExecutionResult{Plan: plan}
	switch plan.Disposition {
	case DispositionNeedsManualReview:
		result.Status = ExecutionStatusSkipped
		result.Reason = "plan requires manual review"
		return result
	case DispositionConflict:
		result.Status = ExecutionStatusConflict
		result.Reason = "plan contains a conflict"
		return result
	case DispositionIgnoreDuplicate:
		result.Status = ExecutionStatusDuplicate
		result.Reason = "plan is already marked as duplicate"
		if plan.File.Existing != nil {
			result.BookID = plan.File.Existing.BookID
			result.EditionID = plan.File.Existing.EditionID
			result.FileID = plan.File.Existing.ID
		}
		return result
	}

	now := ec.Now
	if now.IsZero() {
		now = time.Now()
	}
	result.Transaction.StartedAt = now

	var txState executionState
	err := e.writer.WithinTransaction(ctx, func(txCtx context.Context) error {
		txState.now = now
		return e.executeWritablePlan(txCtx, plan, &txState)
	})
	result.Transaction.FinishedAt = time.Now()
	if err != nil {
		result.Status = ExecutionStatusRolledBack
		result.Transaction.RolledBack = true
		var execErr *ExecutionError
		if errors.As(err, &execErr) {
			result.Error = execErr
		} else {
			result.Error = &ExecutionError{Stage: "transaction", Message: err.Error()}
		}
		return result
	}

	result.Status = txState.status
	result.Reason = txState.reason
	result.BookID = txState.bookID
	result.EditionID = txState.editionID
	result.FileID = txState.fileID
	if txState.status == "" {
		result.Status = ExecutionStatusSuccess
	}
	return result
}

type executionState struct {
	now       time.Time
	status    ExecutionStatus
	reason    string
	bookID    int64
	editionID int64
	fileID    int64
}

func (e *ImportExecutor) executeWritablePlan(ctx context.Context, plan ImportPlan, state *executionState) error {
	book, err := e.resolveBook(ctx, plan)
	if err != nil {
		return err
	}
	state.bookID = book.ID

	edition, err := e.resolveEdition(ctx, plan, *book)
	if err != nil {
		return err
	}
	state.editionID = edition.ID

	status, reason, existingFile, err := e.preflightFile(ctx, plan, *book, *edition)
	if err != nil {
		return err
	}
	if status != "" {
		state.status = status
		state.reason = reason
		if existingFile != nil {
			state.fileID = existingFile.ID
			if state.bookID == 0 {
				state.bookID = existingFile.BookID
			}
			if state.editionID == 0 {
				state.editionID = existingFile.EditionID
			}
		}
		return nil
	}

	for _, resolved := range plan.Contributors {
		if err := e.ensureContributor(ctx, edition.ID, resolved); err != nil {
			return err
		}
	}
	for _, identifier := range plan.Candidate.Metadata.Identifiers {
		if err := e.ensureIdentifier(ctx, identifier, book.ID, edition.ID); err != nil {
			return err
		}
	}

	file, err := e.attachFile(ctx, plan, edition.ID, state.now)
	if err != nil {
		return err
	}
	state.fileID = file.ID
	state.status = ExecutionStatusSuccess
	state.reason = "import plan executed"
	return nil
}

func (e *ImportExecutor) resolveBook(ctx context.Context, plan ImportPlan) (*library.Book, error) {
	if plan.Book.Existing != nil && plan.Book.Existing.ID != 0 {
		book, err := e.writer.FindBookByID(ctx, plan.Book.Existing.ID)
		if err == nil {
			return book, nil
		}
		if !errors.Is(err, library.ErrBookNotFound) {
			return nil, &ExecutionError{Stage: "book_lookup", Message: err.Error()}
		}
	}

	for _, identifier := range plan.Candidate.Metadata.Identifiers {
		book, err := e.writer.FindBookByIdentifier(ctx, identifier)
		if err == nil {
			return book, nil
		}
		if err != nil && !errors.Is(err, library.ErrBookNotFound) {
			return nil, &ExecutionError{Stage: "book_identifier_lookup", Message: err.Error()}
		}
	}

	title := strings.TrimSpace(plan.Candidate.Metadata.SelectedTitle)
	author := strings.TrimSpace(plan.Candidate.Metadata.SelectedAuthor)
	books, err := e.writer.SearchBooks(ctx, library.BookQuery{Title: title, MediaType: plan.Candidate.MediaType})
	if err != nil && !errors.Is(err, library.ErrBookNotFound) {
		return nil, &ExecutionError{Stage: "book_search", Message: err.Error()}
	}
	if matched := exactBookMatch(ctx, e.writer, books, title, author, plan.Candidate.MediaType); matched != nil {
		return matched, nil
	}

	proposed := plan.Book.Proposed
	if proposed == nil {
		proposed = &library.Book{
			Title:     title,
			SortTitle: library.NormalizeKey(title),
			MediaType: plan.Candidate.MediaType,
		}
	}
	applyCandidateBookMetadata(proposed, plan.Candidate)
	if proposed.MediaType == "" {
		proposed.MediaType = plan.Candidate.MediaType
	}
	book, err := e.writer.CreateBook(ctx, *proposed)
	if err != nil {
		return nil, &ExecutionError{Stage: "book_create", Message: err.Error()}
	}
	return book, nil
}

func (e *ImportExecutor) resolveEdition(ctx context.Context, plan ImportPlan, book library.Book) (*library.Edition, error) {
	if plan.Edition.Existing != nil && plan.Edition.Existing.ID != 0 {
		edition, err := e.writer.GetEdition(ctx, plan.Edition.Existing.ID)
		if err == nil {
			return edition, nil
		}
		if err != nil && !errors.Is(err, library.ErrBookNotFound) {
			return nil, &ExecutionError{Stage: "edition_lookup", Message: err.Error()}
		}
	}

	title := strings.TrimSpace(plan.Candidate.Metadata.SelectedTitle)
	edition, err := e.writer.FindEdition(ctx, book.ID, title)
	if err == nil {
		return edition, nil
	}
	if err != nil && !errors.Is(err, library.ErrBookNotFound) {
		return nil, &ExecutionError{Stage: "edition_search", Message: err.Error()}
	}

	proposed := plan.Edition.Proposed
	if proposed == nil {
		proposed = &library.Edition{Title: title}
	}
	applyCandidateEditionMetadata(proposed, plan.Candidate)
	proposed.BookID = book.ID
	edition, err = e.writer.CreateEdition(ctx, *proposed)
	if err != nil {
		return nil, &ExecutionError{Stage: "edition_create", Message: err.Error()}
	}
	return edition, nil
}

func (e *ImportExecutor) preflightFile(ctx context.Context, plan ImportPlan, book library.Book, edition library.Edition) (ExecutionStatus, string, *library.BookFile, error) {
	if file, err := e.findDuplicateFile(ctx, plan); err != nil {
		return "", "", nil, err
	} else if file != nil {
		return ExecutionStatusDuplicate, "file already exists", file, nil
	}

	files, err := e.writer.GetBookFiles(ctx, book.ID)
	if err != nil && !errors.Is(err, library.ErrBookNotFound) {
		return "", "", nil, &ExecutionError{Stage: "book_files", Message: err.Error()}
	}
	for _, file := range files {
		if !strings.EqualFold(file.Format, plan.Candidate.Format) {
			continue
		}
		if samePhysicalFile(file, plan) {
			copy := file
			return ExecutionStatusDuplicate, "same format already attached to book", &copy, nil
		}
		copy := file
		return ExecutionStatusConflict, "different file with same format already exists for book", &copy, nil
	}
	_ = edition
	return "", "", nil, nil
}

func (e *ImportExecutor) findDuplicateFile(ctx context.Context, plan ImportPlan) (*library.BookFile, error) {
	if path := strings.TrimSpace(plan.Candidate.Path); path != "" {
		file, err := e.writer.FindFileByPath(ctx, path)
		if err == nil {
			return file, nil
		}
		if err != nil && !errors.Is(err, library.ErrBookNotFound) {
			return nil, &ExecutionError{Stage: "file_path_lookup", Message: err.Error()}
		}
	}
	sourceID := strings.TrimSpace(plan.Candidate.RelativePath)
	if sourceID != "" {
		file, err := e.writer.FindFileBySourceID(ctx, sourceID)
		if err == nil {
			return file, nil
		}
		if err != nil && !errors.Is(err, library.ErrBookNotFound) {
			return nil, &ExecutionError{Stage: "file_source_lookup", Message: err.Error()}
		}
	}
	if hash := strings.TrimSpace(plan.Candidate.ContentHash); hash != "" {
		files, err := e.writer.FindFilesByContentHash(ctx, hash)
		if err == nil && len(files) > 0 {
			file := files[0]
			return &file, nil
		}
		if err != nil && !errors.Is(err, library.ErrBookNotFound) {
			return nil, &ExecutionError{Stage: "file_hash_lookup", Message: err.Error()}
		}
	}
	return nil, nil
}

func (e *ImportExecutor) ensureContributor(ctx context.Context, editionID int64, resolved ResolvedContributor) error {
	contributor := resolved.Proposed
	if contributor == nil && resolved.Existing != nil {
		copy := *resolved.Existing
		contributor = &copy
	}
	if contributor == nil {
		return nil
	}
	if len(contributor.Roles) == 0 {
		contributor.Roles = []library.ContributorRole{resolved.Role}
	}
	merged, err := e.writer.MergeContributor(ctx, *contributor)
	if err != nil {
		return &ExecutionError{Stage: "contributor_merge", Message: err.Error()}
	}
	merged.Roles = contributor.Roles
	merged.Position = contributor.Position
	if err := e.writer.AttachContributor(ctx, editionID, *merged); err != nil {
		return &ExecutionError{Stage: "contributor_attach", Message: err.Error()}
	}
	return nil
}

func (e *ImportExecutor) ensureIdentifier(ctx context.Context, identifier library.Identifier, bookID, editionID int64) error {
	switch identifier.Scope {
	case library.IdentifierScopeBook:
		identifier.Source = fmt.Sprint(bookID)
	case library.IdentifierScopeEdition:
		identifier.Source = fmt.Sprint(editionID)
	default:
		return nil
	}
	if _, err := e.writer.AddIdentifier(ctx, identifier); err != nil {
		if !errors.Is(err, library.ErrDuplicateBook) {
			return &ExecutionError{Stage: "identifier_add", Message: err.Error()}
		}
		matches, matchErr := e.writer.FindIdentifierMatches(ctx, identifier)
		if matchErr != nil {
			return &ExecutionError{Stage: "identifier_lookup", Message: matchErr.Error()}
		}
		for _, match := range matches {
			if identifier.Scope == library.IdentifierScopeBook && match.BookID == bookID {
				return nil
			}
			if identifier.Scope == library.IdentifierScopeEdition && match.EditionID == editionID {
				return nil
			}
		}
		return &ExecutionError{Stage: "identifier_conflict", Message: "identifier already belongs to a different record"}
	}
	return nil
}

func (e *ImportExecutor) attachFile(ctx context.Context, plan ImportPlan, editionID int64, now time.Time) (*library.BookFile, error) {
	file := plan.File.Proposed
	if file == nil {
		file = &library.BookFile{}
	}
	copy := *file
	copy.EditionID = editionID
	if copy.MediaType == "" {
		copy.MediaType = plan.Candidate.MediaType
	}
	if strings.TrimSpace(copy.Format) == "" {
		copy.Format = plan.Candidate.Format
	}
	if strings.TrimSpace(copy.Path) == "" {
		copy.Path = plan.Candidate.Path
	}
	if strings.TrimSpace(copy.OriginalPath) == "" {
		copy.OriginalPath = plan.Candidate.Path
	}
	if copy.Size == 0 {
		copy.Size = plan.Candidate.Size
	}
	if strings.TrimSpace(copy.ContentHash) == "" {
		copy.ContentHash = plan.Candidate.ContentHash
	}
	if strings.TrimSpace(copy.SourceID) == "" {
		copy.SourceID = plan.Candidate.RelativePath
	}
	if strings.TrimSpace(copy.SourceType) == "" {
		copy.SourceType = string(plan.Candidate.MediaType)
	}
	copy.ImportedAt = now
	if copy.EmbeddedMetadata == nil {
		copy.EmbeddedMetadata = map[string]string{}
	}
	if title := strings.TrimSpace(plan.Candidate.Metadata.SelectedTitle); title != "" {
		copy.EmbeddedMetadata["title"] = title
	}
	if author := strings.TrimSpace(plan.Candidate.Metadata.SelectedAuthor); author != "" {
		copy.EmbeddedMetadata["author"] = author
	}
	addEmbeddedMetadata(copy.EmbeddedMetadata, "subtitle", plan.Candidate.Metadata.Subtitle)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "series", plan.Candidate.Metadata.Series)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "series_number", plan.Candidate.Metadata.SeriesNumber)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "publisher", plan.Candidate.Metadata.Publisher)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "publication_year", plan.Candidate.Metadata.PublicationYear)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "isbn", plan.Candidate.Metadata.ISBN)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "language", plan.Candidate.Metadata.Language)
	addEmbeddedMetadata(copy.EmbeddedMetadata, "description", plan.Candidate.Metadata.Description)
	if len(plan.Candidate.Metadata.Tags) > 0 {
		addEmbeddedMetadata(copy.EmbeddedMetadata, "tags", strings.Join(plan.Candidate.Metadata.Tags, ", "))
	}

	attached, err := e.writer.AttachFile(ctx, copy)
	if err != nil {
		return nil, &ExecutionError{Stage: "file_attach", Message: err.Error()}
	}
	if len(copy.EmbeddedMetadata) > 0 {
		if err := e.writer.SaveEmbeddedMetadata(ctx, attached.ID, copy.EmbeddedMetadata); err != nil {
			return nil, &ExecutionError{Stage: "file_metadata", Message: err.Error()}
		}
	}
	return attached, nil
}

func applyCandidateBookMetadata(book *library.Book, candidate ImportCandidate) {
	if book == nil {
		return
	}
	if value := strings.TrimSpace(candidate.Metadata.SelectedTitle); value != "" {
		book.Title = value
		book.SortTitle = library.NormalizeKey(value)
	}
	if value := strings.TrimSpace(candidate.Metadata.Description); value != "" {
		book.Description = value
	}
	if value := strings.TrimSpace(candidate.Metadata.Language); value != "" {
		book.Language = value
	}
	if year := parsePublicationYear(candidate.Metadata.PublicationYear); year > 0 {
		book.PublicationYear = year
	}
	if book.MediaType == "" {
		book.MediaType = candidate.MediaType
	}
}

func applyCandidateEditionMetadata(edition *library.Edition, candidate ImportCandidate) {
	if edition == nil {
		return
	}
	if value := strings.TrimSpace(candidate.Metadata.SelectedTitle); value != "" {
		edition.Title = value
	}
	if value := strings.TrimSpace(candidate.Metadata.Subtitle); value != "" {
		edition.Subtitle = value
	}
	if value := strings.TrimSpace(candidate.Metadata.Description); value != "" {
		edition.Description = value
	}
	if value := strings.TrimSpace(candidate.Metadata.Publisher); value != "" {
		edition.Publisher = value
	}
	if value := strings.TrimSpace(candidate.Metadata.PublicationYear); value != "" {
		edition.PublicationDate = value
	}
	if value := strings.TrimSpace(candidate.Metadata.Language); value != "" {
		edition.Language = value
	}
}

func parsePublicationYear(value string) int {
	year, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || year < 1 {
		return 0
	}
	return year
}

func addEmbeddedMetadata(metadata map[string]string, key, value string) {
	if metadata == nil {
		return
	}
	if value = strings.TrimSpace(value); value != "" {
		metadata[key] = value
	}
}

func exactBookMatch(ctx context.Context, writer RepositoryWriter, books []library.Book, title, author string, mediaType library.MediaType) *library.Book {
	titleKey := library.NormalizeKey(title)
	authorKey := library.NormalizeKey(author)
	var exactTitleMatches []library.Book
	var exactAuthorMatches []library.Book
	for _, book := range books {
		fullBook := book
		if fullBook.ID != 0 {
			if loaded, err := writer.FindBookByID(ctx, fullBook.ID); err == nil && loaded != nil {
				fullBook = *loaded
			}
		}
		if fullBook.MediaType != "" && mediaType != "" && fullBook.MediaType != mediaType {
			continue
		}
		if library.NormalizeKey(fullBook.Title) != titleKey {
			continue
		}
		exactTitleMatches = append(exactTitleMatches, fullBook)
		if authorKey != "" && library.NormalizeKey(primaryContributorName(&fullBook)) == authorKey {
			exactAuthorMatches = append(exactAuthorMatches, fullBook)
		}
	}
	if len(exactAuthorMatches) == 1 {
		book := exactAuthorMatches[0]
		return &book
	}
	if authorKey == "" && len(exactTitleMatches) == 1 {
		book := exactTitleMatches[0]
		return &book
	}
	return nil
}

func samePhysicalFile(existing library.BookFile, plan ImportPlan) bool {
	if strings.TrimSpace(existing.Path) != "" && strings.TrimSpace(existing.Path) == strings.TrimSpace(plan.Candidate.Path) {
		return true
	}
	if strings.TrimSpace(existing.SourceID) != "" && strings.TrimSpace(existing.SourceID) == strings.TrimSpace(plan.Candidate.RelativePath) {
		return true
	}
	if strings.TrimSpace(existing.ContentHash) != "" && strings.TrimSpace(existing.ContentHash) == strings.TrimSpace(plan.Candidate.ContentHash) {
		return true
	}
	return false
}
