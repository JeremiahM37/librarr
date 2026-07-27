package library

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	librarrdb "github.com/jamie75/librarr/internal/db"
)

const (
	RepositoryModeLegacy     = "legacy"
	RepositoryModeNormalized = "normalized"

	// Keep in sync with internal/library/migration.Version. The migration
	// package imports library, so importing it here would create a cycle.
	requiredBackfillVersion = "librarr-2-backfill-v1"
)

type RepositorySelection struct {
	Mode              string
	Implementation    string
	Readiness         NormalizedReadiness
	LibraryService    *LibraryService
	Repository        LibraryRepository
	Compatibility     LegacyCompatibilityRepository
	ActivationSkipped bool
}

type NormalizedReadiness struct {
	Requested        bool
	Ready            bool
	FreshInstall     bool
	MigrationsOK     bool
	BackfillComplete bool
	ValidationOK     bool
	StateOK          bool
	BackfillRunID    int64
	LegacyItems      int
	CompletedStates  int
	Issues           []string
}

func NewConfiguredLibraryService(ctx context.Context, cfg *config.Config, database *librarrdb.DB) (RepositorySelection, error) {
	mode, err := cfg.NormalizedLibraryRepositoryMode()
	if err != nil {
		slog.Error("library repository activation rejected", "configured_mode", cfg.LibraryRepositoryMode, "error", err)
		return RepositorySelection{}, err
	}
	if database == nil {
		return RepositorySelection{}, fmt.Errorf("database is required")
	}
	sqlDB := database.SQLDB()

	slog.Info("library repository mode configured", "mode", mode)
	if mode == RepositoryModeLegacy {
		repo := NewLegacyLibraryRepository(database)
		compat := LegacyCompatibilityRepository(repo)
		svc, err := serviceForRepository(repo, compat)
		if err != nil {
			return RepositorySelection{}, err
		}
		slog.Info("library repository selected", "mode", mode, "implementation", "legacy")
		return RepositorySelection{
			Mode:              mode,
			Implementation:    "legacy",
			LibraryService:    svc,
			Repository:        repo,
			Compatibility:     compat,
			ActivationSkipped: true,
		}, nil
	}

	readiness := CheckNormalizedReadiness(ctx, sqlDB)
	logNormalizedReadiness(readiness)
	if !readiness.Ready {
		err := fmt.Errorf("normalized library repository is not ready: %s. Run the Librarr 2.0 backfill, ensure it completes successfully, and rerun validation before setting LIBRARR_LIBRARY_REPOSITORY_MODE=normalized", strings.Join(readiness.Issues, "; "))
		slog.Error("library repository activation rejected", "configured_mode", mode, "error", err)
		return RepositorySelection{Mode: mode, Readiness: readiness}, err
	}

	repo, err := NewRepository(NormalizedRepositoryMode, sqlDB)
	if err != nil {
		return RepositorySelection{}, err
	}
	normalized, ok := repo.(*NormalizedRepository)
	if !ok {
		return RepositorySelection{}, fmt.Errorf("normalized repository factory returned %T", repo)
	}
	compat := NewNormalizedCompatibilityRepository(normalized)
	svc, err := serviceForRepository(repo, compat)
	if err != nil {
		return RepositorySelection{}, err
	}
	slog.Info("library repository selected", "mode", mode, "implementation", "normalized", "backfill_run_id", readiness.BackfillRunID)
	return RepositorySelection{
		Mode:           mode,
		Implementation: "normalized",
		Readiness:      readiness,
		LibraryService: svc,
		Repository:     repo,
		Compatibility:  compat,
	}, nil
}

func CheckNormalizedReadiness(ctx context.Context, db *sql.DB) NormalizedReadiness {
	readiness := NormalizedReadiness{Requested: true}
	if db == nil {
		readiness.Issues = append(readiness.Issues, "database is not available")
		return readiness
	}

	readiness.MigrationsOK = requiredSchemaMigrationsApplied(ctx, db, &readiness)
	if !countLegacyItems(ctx, db, &readiness) {
		return readiness
	}
	if readiness.MigrationsOK && readiness.LegacyItems == 0 {
		readiness.FreshInstall = true
		readiness.BackfillComplete = true
		readiness.ValidationOK = true
		readiness.StateOK = true
		readiness.Ready = true
		return readiness
	}
	readiness.BackfillComplete = completedBackfillRun(ctx, db, &readiness)
	readiness.StateOK = migrationStateComplete(ctx, db, &readiness)
	readiness.Ready = readiness.MigrationsOK && readiness.BackfillComplete && readiness.ValidationOK && readiness.StateOK
	return readiness
}

func countLegacyItems(ctx context.Context, db *sql.DB, readiness *NormalizedReadiness) bool {
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM library_items`).Scan(&readiness.LegacyItems); err != nil {
		readiness.Issues = append(readiness.Issues, "legacy library item count could not be checked")
		return false
	}
	return true
}

func requiredSchemaMigrationsApplied(ctx context.Context, db *sql.DB, readiness *NormalizedReadiness) bool {
	required := []int{1, 2, 3, 4}
	for _, version := range required {
		var found int
		err := db.QueryRowContext(ctx, `SELECT 1 FROM schema_migrations WHERE version = ?`, version).Scan(&found)
		if err != nil {
			readiness.Issues = append(readiness.Issues, fmt.Sprintf("schema migration %d has not completed", version))
			return false
		}
	}
	return true
}

func completedBackfillRun(ctx context.Context, db *sql.DB, readiness *NormalizedReadiness) bool {
	var reportJSON string
	err := db.QueryRowContext(ctx, `SELECT id, report_json FROM backfill_runs
		WHERE version = ? AND dry_run = 0 AND status = 'completed'
		ORDER BY id DESC LIMIT 1`, requiredBackfillVersion).Scan(&readiness.BackfillRunID, &reportJSON)
	if err != nil {
		readiness.Issues = append(readiness.Issues, "no completed non-dry-run Librarr 2.0 backfill run was found")
		return false
	}

	var report struct {
		Validation struct {
			OK     bool     `json:"ok"`
			Errors []string `json:"errors"`
		} `json:"validation"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal([]byte(reportJSON), &report); err != nil {
		readiness.Issues = append(readiness.Issues, "latest completed backfill report could not be parsed")
		return false
	}
	if !report.Validation.OK || len(report.Validation.Errors) > 0 || len(report.Errors) > 0 {
		readiness.Issues = append(readiness.Issues, "latest completed backfill validation did not pass")
		return true
	}
	readiness.ValidationOK = true
	return true
}

func migrationStateComplete(ctx context.Context, db *sql.DB, readiness *NormalizedReadiness) bool {
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backfill_state WHERE status = 'completed'`).Scan(&readiness.CompletedStates); err != nil {
		readiness.Issues = append(readiness.Issues, "backfill state could not be checked")
		return false
	}
	if readiness.CompletedStates < readiness.LegacyItems {
		readiness.Issues = append(readiness.Issues, fmt.Sprintf("backfill state is incomplete: %d of %d legacy rows completed", readiness.CompletedStates, readiness.LegacyItems))
		return false
	}

	var badStates int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backfill_state WHERE status <> 'completed'`).Scan(&badStates); err != nil {
		readiness.Issues = append(readiness.Issues, "backfill state statuses could not be checked")
		return false
	}
	if badStates > 0 {
		readiness.Issues = append(readiness.Issues, fmt.Sprintf("backfill state contains %d incomplete or failed rows", badStates))
		return false
	}

	checks := []struct {
		name  string
		query string
	}{
		{"orphan normalized files", `SELECT COUNT(*) FROM files f LEFT JOIN editions e ON e.id = f.edition_id WHERE e.id IS NULL`},
		{"duplicate normalized file paths", `SELECT COUNT(*) FROM (SELECT file_path FROM files WHERE file_path IS NOT NULL AND file_path <> '' GROUP BY file_path HAVING COUNT(*) > 1)`},
		{"duplicate normalized identifiers", `SELECT COUNT(*) FROM (SELECT COALESCE(book_id, 0), COALESCE(edition_id, 0), provider, identifier FROM identifiers GROUP BY COALESCE(book_id, 0), COALESCE(edition_id, 0), provider, identifier HAVING COUNT(*) > 1)`},
		{"completed mappings missing books", `SELECT COUNT(*) FROM library_item_migration_map m LEFT JOIN books b ON b.id = m.book_id WHERE m.status = 'completed' AND b.id IS NULL`},
		{"completed mappings missing editions", `SELECT COUNT(*) FROM library_item_migration_map m LEFT JOIN editions e ON e.id = m.edition_id WHERE m.status = 'completed' AND e.id IS NULL`},
		{"completed mappings missing files", `SELECT COUNT(*) FROM library_item_migration_map m LEFT JOIN files f ON f.id = m.file_id WHERE m.status = 'completed' AND f.id IS NULL`},
	}
	for _, check := range checks {
		var count int
		if err := db.QueryRowContext(ctx, check.query).Scan(&count); err != nil {
			readiness.Issues = append(readiness.Issues, check.name+" could not be checked")
			return false
		}
		if count > 0 {
			readiness.Issues = append(readiness.Issues, fmt.Sprintf("%s: %d", check.name, count))
			return false
		}
	}
	return true
}

func logNormalizedReadiness(readiness NormalizedReadiness) {
	slog.Info("normalized repository readiness checked",
		"ready", readiness.Ready,
		"fresh_install", readiness.FreshInstall,
		"migrations_ok", readiness.MigrationsOK,
		"backfill_complete", readiness.BackfillComplete,
		"validation_ok", readiness.ValidationOK,
		"state_ok", readiness.StateOK,
		"backfill_run_id", readiness.BackfillRunID,
		"legacy_items", readiness.LegacyItems,
		"completed_states", readiness.CompletedStates,
	)
}

func serviceForRepository(repo LibraryRepository, compat LegacyCompatibilityRepository) (*LibraryService, error) {
	var editions EditionRepository
	if editionRepo, ok := repo.(EditionRepository); ok {
		editions = editionRepo
	}
	return NewLibraryService(ServiceOptions{
		BookRepository:        repo,
		EditionRepository:     editions,
		FileRepository:        repo,
		MetadataRepository:    repo,
		SeriesRepository:      repo,
		ContributorRepository: repo,
		IdentifierRepository:  repo,
		CoverRepository:       repo,
		TransactionManager:    transactionManager(repo),
		LegacyCompatibility:   compat,
	})
}

func transactionManager(repo LibraryRepository) TransactionManager {
	if tx, ok := repo.(TransactionManager); ok {
		return tx
	}
	return NoopTransactionManager{}
}
