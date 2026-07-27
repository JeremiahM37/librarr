package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/models"
)

type LibraryMigrationEngine struct {
	legacy LegacyStore
	state  StateStore
	repo   Repository
}

func NewEngine(legacy LegacyStore, state StateStore, repo Repository) (*LibraryMigrationEngine, error) {
	if legacy == nil {
		return nil, fmt.Errorf("legacy store is required")
	}
	if state == nil {
		return nil, fmt.Errorf("state store is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("normalized repository is required")
	}
	return &LibraryMigrationEngine{legacy: legacy, state: state, repo: repo}, nil
}

func NewLibraryMigrationEngine(legacy LegacyStore, state StateStore, repo Repository) (*LibraryMigrationEngine, error) {
	return NewEngine(legacy, state, repo)
}

func (e *LibraryMigrationEngine) Plan(ctx context.Context, opts Options) (Plan, error) {
	count, err := e.legacy.CountLegacyItems(ctx)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{LegacyRows: count, DryRun: opts.DryRun}
	if count == 0 {
		plan.Warnings = append(plan.Warnings, "no legacy library_items rows found")
	}
	return plan, nil
}

func (e *LibraryMigrationEngine) DryRun(ctx context.Context) (Report, error) {
	return e.run(ctx, Options{DryRun: true})
}

func (e *LibraryMigrationEngine) Run(ctx context.Context, opts Options) (Report, error) {
	opts.DryRun = false
	return e.run(ctx, opts)
}

func (e *LibraryMigrationEngine) Resume(ctx context.Context, runID int64) (Report, error) {
	return e.run(ctx, Options{ResumeFromRunID: runID})
}

func (e *LibraryMigrationEngine) GenerateReport(_ context.Context, report Report) (string, error) {
	return report.Summary(), nil
}

func (e *LibraryMigrationEngine) Validate(ctx context.Context) (ValidationResult, error) {
	snapshot, err := e.state.ValidationSnapshot(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	result := ValidationResult{OK: true, Snapshot: snapshot}
	if snapshot.CompletedMappings != snapshot.LegacyItems {
		result.Warnings = append(result.Warnings, fmt.Sprintf("completed mappings %d do not match legacy rows %d", snapshot.CompletedMappings, snapshot.LegacyItems))
	}
	checks := []struct {
		n       int
		message string
	}{
		{snapshot.OrphanFiles, "normalized files without editions"},
		{snapshot.DuplicateFilePaths, "duplicate normalized file paths"},
		{snapshot.DuplicateIdentifiers, "duplicate normalized identifiers"},
		{snapshot.MissingMappedBooks, "completed mappings with missing books"},
		{snapshot.MissingMappedEditions, "completed mappings with missing editions"},
		{snapshot.MissingMappedFiles, "completed mappings with missing files"},
	}
	for _, check := range checks {
		if check.n > 0 {
			result.OK = false
			result.Errors = append(result.Errors, fmt.Sprintf("%d %s", check.n, check.message))
		}
	}
	return result, nil
}

func (e *LibraryMigrationEngine) run(ctx context.Context, opts Options) (Report, error) {
	report := NewReport(opts.DryRun)
	items, err := e.legacy.ListLegacyItems(ctx)
	if err != nil {
		return report, err
	}
	report.LegacyRowsTotal = len(items)

	var runID int64
	if !opts.DryRun {
		runID, err = e.state.StartRun(ctx, opts)
		if err != nil {
			return report, err
		}
		report.RunID = runID
	}

	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if opts.StopAfterRows > 0 && report.RowsProcessed >= opts.StopAfterRows {
			report.addWarning(fmt.Sprintf("stopped after %d rows", opts.StopAfterRows))
			break
		}
		row := e.migrateItem(ctx, runID, item, opts.DryRun, &report)
		report.Rows = append(report.Rows, row)
		report.RowsProcessed++
		switch row.Action {
		case "completed", "would_complete", "reused":
			report.RowsCompleted++
		case "skipped":
			report.RowsSkipped++
		case "failed":
			report.RowsFailed++
		}
	}

	validation, err := e.Validate(ctx)
	if opts.DryRun {
		validation.OK = len(validation.Errors) == 0
		if err != nil {
			validation = ValidationResult{OK: false, Errors: []string{sanitizeError(err)}}
		}
		report.Validation = validation
	} else if err != nil {
		report.addError(sanitizeError(err))
	} else {
		report.Validation = validation
	}
	if !opts.DryRun && err == nil {
		report.Validation = validation
	}
	report.Finish()
	if !opts.DryRun {
		if err := e.state.FinishRun(ctx, runID, report); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (e *LibraryMigrationEngine) migrateItem(ctx context.Context, runID int64, item models.LibraryItem, dryRun bool, report *Report) RowResult {
	row := RowResult{LegacyItemID: item.ID}
	existing, err := e.state.GetMapping(ctx, item.ID)
	if err != nil {
		row.Action = "failed"
		row.Error = sanitizeError(err)
		report.addError(row.Error)
		return row
	}
	if existing != nil && existing.Status == "completed" && !dryRun {
		row.BookID = existing.BookID
		row.EditionID = existing.EditionID
		row.FileID = existing.FileID
		row.Action = "reused"
		row.Reason = "existing migration map"
		report.DuplicatesMerged++
		return row
	}

	if dryRun {
		row.Action = "would_complete"
		row.Reason = "dry run"
		e.planItem(ctx, item, &row, report)
		return row
	}

	err = e.repo.WithinTransaction(ctx, func(txCtx context.Context) error {
		return e.applyItem(txCtx, item, &row, report)
	})
	if err != nil {
		row.Action = "failed"
		row.Error = sanitizeError(err)
		report.addError(row.Error)
		_ = e.state.SaveMapping(ctx, runID, Mapping{LegacyItemID: item.ID, Status: "error", Reason: row.Error})
		return row
	}
	row.Action = "completed"
	if err := e.state.SaveMapping(ctx, runID, Mapping{
		LegacyItemID: item.ID,
		BookID:       row.BookID,
		EditionID:    row.EditionID,
		FileID:       row.FileID,
		Status:       "completed",
		Reason:       row.Reason,
	}); err != nil {
		row.Action = "failed"
		row.Error = sanitizeError(err)
		report.addError(row.Error)
	}
	return row
}

func (e *LibraryMigrationEngine) planItem(ctx context.Context, item models.LibraryItem, row *RowResult, report *Report) {
	if book, err := e.findBook(ctx, item); err == nil {
		row.BookID = book.ID
		report.BooksReused++
	} else {
		report.BooksCreated++
	}
	if item.Metadata != "" && !json.Valid([]byte(item.Metadata)) {
		report.addWarning(fmt.Sprintf("legacy item %d has invalid metadata JSON", item.ID))
	}
}

func (e *LibraryMigrationEngine) applyItem(ctx context.Context, item models.LibraryItem, row *RowResult, report *Report) error {
	book, err := e.findBook(ctx, item)
	if err != nil {
		book, err = e.repo.CreateBook(ctx, library.Book{
			Title:     displayTitle(item),
			SortTitle: library.NormalizeKey(displayTitle(item)),
			MediaType: mediaType(item),
			Status:    library.BookStatusOwned,
		})
		if err != nil {
			return err
		}
		report.BooksCreated++
	} else {
		report.BooksReused++
	}
	row.BookID = book.ID

	edition, err := e.repo.FindEdition(ctx, book.ID, displayTitle(item))
	if err != nil {
		edition, err = e.repo.CreateEdition(ctx, library.Edition{BookID: book.ID, Title: displayTitle(item)})
		if err != nil {
			return err
		}
		report.EditionsCreated++
	} else {
		report.EditionsReused++
	}
	row.EditionID = edition.ID

	if author := strings.TrimSpace(item.Author); author != "" {
		if e.hasContributor(ctx, edition.ID, author) {
			report.ContributorsReused++
		} else {
			report.ContributorsCreated++
		}
		if err := e.repo.AttachContributor(ctx, edition.ID, library.Contributor{Name: strings.TrimSpace(item.Author), Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
			return err
		}
	}

	file, reused, err := e.findFile(ctx, item)
	if err != nil {
		return err
	}
	if file == nil {
		file, err = e.repo.AttachFile(ctx, legacyFile(item, edition.ID))
		if err != nil {
			return err
		}
		report.FilesCreated++
	} else if reused {
		report.FilesReused++
		report.DuplicatesMerged++
	}
	row.FileID = file.ID

	if strings.TrimSpace(item.Source) != "" && strings.TrimSpace(item.SourceID) != "" {
		_, err := e.repo.AddIdentifier(ctx, library.Identifier{
			Scope:    library.IdentifierScopeBook,
			Provider: strings.TrimSpace(item.Source),
			Value:    strings.TrimSpace(item.SourceID),
			Source:   fmt.Sprint(book.ID),
		})
		if err == nil {
			report.IdentifiersCreated++
		} else if !errors.Is(err, library.ErrDuplicateBook) {
			return err
		}
	}
	return nil
}

func (e *LibraryMigrationEngine) findBook(ctx context.Context, item models.LibraryItem) (*library.Book, error) {
	title := displayTitle(item)
	books, err := e.repo.SearchBooks(ctx, library.BookQuery{Title: title, Author: strings.TrimSpace(item.Author), MediaType: mediaType(item)})
	if err != nil {
		return nil, err
	}
	key := library.NormalizeKey(title)
	for i := range books {
		if books[i].MediaType == mediaType(item) && library.NormalizeKey(books[i].Title) == key {
			return &books[i], nil
		}
	}
	return nil, library.ErrNotFound
}

func (e *LibraryMigrationEngine) findFile(ctx context.Context, item models.LibraryItem) (*library.BookFile, bool, error) {
	if strings.TrimSpace(item.FilePath) != "" {
		if file, err := e.repo.FindFileByPath(ctx, item.FilePath); err == nil {
			return file, true, nil
		} else if !errors.Is(err, library.ErrNotFound) {
			return nil, false, err
		}
	}
	if strings.TrimSpace(item.SourceID) != "" {
		if file, err := e.repo.FindFileBySourceID(ctx, item.SourceID); err == nil {
			if sameSourceType(file.SourceType, item.Source) {
				return file, true, nil
			}
		} else if !errors.Is(err, library.ErrNotFound) {
			return nil, false, err
		}
	}
	if strings.TrimSpace(item.ContentHash) != "" {
		files, err := e.repo.FindFilesByContentHash(ctx, item.ContentHash)
		if err == nil {
			for _, file := range files {
				if strings.EqualFold(file.Format, effectiveFormat(item)) && file.MediaType == mediaType(item) {
					return &file, true, nil
				}
			}
		} else if !errors.Is(err, library.ErrNotFound) {
			return nil, false, err
		}
	}
	return nil, false, nil
}

func (e *LibraryMigrationEngine) hasContributor(ctx context.Context, editionID int64, author string) bool {
	contributors, err := e.repo.GetEditionContributors(ctx, editionID)
	if err != nil {
		return false
	}
	key := library.NormalizeKey(author)
	for _, contributor := range contributors {
		if library.NormalizeKey(contributor.Name) == key {
			return true
		}
	}
	return false
}

func sameSourceType(existing, legacy string) bool {
	existing = strings.TrimSpace(existing)
	legacy = strings.TrimSpace(legacy)
	return existing == "" || legacy == "" || strings.EqualFold(existing, legacy)
}

func legacyFile(item models.LibraryItem, editionID int64) library.BookFile {
	importedAt := item.AddedAt
	return library.BookFile{
		EditionID:            editionID,
		MediaType:            mediaType(item),
		Format:               effectiveFormat(item),
		Path:                 item.FilePath,
		OriginalPath:         item.OriginalPath,
		Size:                 item.FileSize,
		ContentHash:          strings.ToLower(strings.TrimSpace(item.ContentHash)),
		SourceID:             strings.TrimSpace(item.SourceID),
		SourceType:           strings.TrimSpace(item.Source),
		Managed:              true,
		EmbeddedMetadataJSON: validMetadata(item.Metadata),
		ImportedAt:           importedAt,
	}
}

func displayTitle(item models.LibraryItem) string {
	title := strings.TrimSpace(item.Title)
	if title != "" {
		return title
	}
	if item.FilePath != "" {
		return strings.TrimSuffix(filepath.Base(item.FilePath), filepath.Ext(item.FilePath))
	}
	return "Unknown Title"
}

func mediaType(item models.LibraryItem) library.MediaType {
	if mt := library.MediaType(strings.TrimSpace(item.MediaType)); mt.Valid() {
		return mt
	}
	return library.MediaTypeEbook
}

func effectiveFormat(item models.LibraryItem) string {
	format := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(item.FileFormat)), ".")
	if format != "" {
		return format
	}
	return strings.TrimPrefix(strings.ToLower(filepath.Ext(item.FilePath)), ".")
}

func validMetadata(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}"
	}
	if json.Valid([]byte(raw)) {
		return raw
	}
	return "{}"
}
