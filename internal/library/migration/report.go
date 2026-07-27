package migration

import (
	"fmt"
	"strings"
	"time"
)

func NewReport(dryRun bool) Report {
	now := time.Now().UTC()
	return Report{Version: Version, DryRun: dryRun, StartedAt: now}
}

func (r *Report) Finish() {
	r.FinishedAt = time.Now().UTC()
	r.Elapsed = r.FinishedAt.Sub(r.StartedAt)
}

func (r Report) Summary() string {
	status := "completed"
	if len(r.Errors) > 0 || !r.Validation.OK {
		status = "completed with errors"
	}
	mode := "run"
	if r.DryRun {
		mode = "dry run"
	}
	lines := []string{
		fmt.Sprintf("Librarr backfill %s %s", mode, status),
		fmt.Sprintf("legacy rows: %d", r.LegacyRowsTotal),
		fmt.Sprintf("processed: %d completed: %d skipped: %d failed: %d", r.RowsProcessed, r.RowsCompleted, r.RowsSkipped, r.RowsFailed),
		fmt.Sprintf("books created/reused: %d/%d", r.BooksCreated, r.BooksReused),
		fmt.Sprintf("editions created/reused: %d/%d", r.EditionsCreated, r.EditionsReused),
		fmt.Sprintf("files created/reused: %d/%d", r.FilesCreated, r.FilesReused),
		fmt.Sprintf("contributors created/reused: %d/%d", r.ContributorsCreated, r.ContributorsReused),
		fmt.Sprintf("identifiers created: %d covers created: %d", r.IdentifiersCreated, r.CoversCreated),
		fmt.Sprintf("elapsed: %s", r.Elapsed.Round(time.Millisecond)),
	}
	for _, warning := range r.Warnings {
		lines = append(lines, "warning: "+warning)
	}
	for _, err := range r.Errors {
		lines = append(lines, "error: "+err)
	}
	return strings.Join(lines, "\n")
}

func (r *Report) addWarning(message string) {
	r.Warnings = append(r.Warnings, message)
}

func (r *Report) addError(message string) {
	r.Errors = append(r.Errors, message)
}
