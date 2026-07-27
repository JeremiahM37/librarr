package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

type SQLStore struct {
	db *sql.DB
}

func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

func (s *SQLStore) ListLegacyItems(ctx context.Context) ([]models.LibraryItem, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, title, author, file_path, original_path, file_size,
		file_format, media_type, source, source_id, metadata, content_hash, added_at
		FROM library_items ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []models.LibraryItem
	for rows.Next() {
		var item models.LibraryItem
		var added float64
		if err := rows.Scan(&item.ID, &item.Title, &item.Author, &item.FilePath, &item.OriginalPath,
			&item.FileSize, &item.FileFormat, &item.MediaType, &item.Source, &item.SourceID,
			&item.Metadata, &item.ContentHash, &added); err != nil {
			return nil, err
		}
		item.AddedAt = time.Unix(int64(added), 0).UTC()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *SQLStore) CountLegacyItems(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM library_items`).Scan(&count)
	return count, err
}

func (s *SQLStore) StartRun(ctx context.Context, opts Options) (int64, error) {
	res, err := s.db.ExecContext(ctx, `INSERT INTO backfill_runs (version, status, dry_run)
		VALUES (?, 'running', ?)`, Version, boolInt(opts.DryRun))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *SQLStore) FinishRun(ctx context.Context, runID int64, report Report) error {
	status := "completed"
	if len(report.Errors) > 0 || !report.Validation.OK {
		status = "completed_with_errors"
	}
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE backfill_runs SET completed_at = datetime('now'), status = ?,
		rows_processed = ?, rows_migrated = ?, rows_skipped = ?, errors = ?, resume_checkpoint = ?,
		report_json = ? WHERE id = ?`,
		status, report.RowsProcessed, report.RowsCompleted, report.RowsSkipped, report.RowsFailed,
		lastProcessedID(report), string(data), runID)
	return err
}

func (s *SQLStore) GetMapping(ctx context.Context, legacyItemID int64) (*Mapping, error) {
	var m Mapping
	var bookID, editionID, fileID sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT library_item_id, book_id, edition_id, file_id, status, COALESCE(reason, '')
		FROM library_item_migration_map WHERE library_item_id = ?`, legacyItemID).
		Scan(&m.LegacyItemID, &bookID, &editionID, &fileID, &m.Status, &m.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m.BookID = bookID.Int64
	m.EditionID = editionID.Int64
	m.FileID = fileID.Int64
	return &m, nil
}

func (s *SQLStore) SaveMapping(ctx context.Context, runID int64, m Mapping) error {
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	_, err := s.db.ExecContext(ctx, `INSERT INTO library_item_migration_map
		(library_item_id, book_id, edition_id, file_id, status, reason, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(library_item_id) DO UPDATE SET
			book_id = excluded.book_id,
			edition_id = excluded.edition_id,
			file_id = excluded.file_id,
			status = excluded.status,
			reason = excluded.reason,
			updated_at = excluded.updated_at`,
		m.LegacyItemID, nullableID(m.BookID), nullableID(m.EditionID), nullableID(m.FileID),
		m.Status, m.Reason, now, now)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO backfill_state
		(legacy_item_id, run_id, status, book_id, edition_id, file_id, error, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(legacy_item_id) DO UPDATE SET
			run_id = excluded.run_id,
			status = excluded.status,
			book_id = excluded.book_id,
			edition_id = excluded.edition_id,
			file_id = excluded.file_id,
			error = excluded.error,
			updated_at = excluded.updated_at`,
		m.LegacyItemID, runID, m.Status, nullableID(m.BookID), nullableID(m.EditionID), nullableID(m.FileID),
		m.Reason, now)
	return err
}

func (s *SQLStore) ValidationSnapshot(ctx context.Context) (ValidationSnapshot, error) {
	var snap ValidationSnapshot
	checks := []struct {
		query string
		dest  *int
	}{
		{`SELECT COUNT(*) FROM library_items`, &snap.LegacyItems},
		{`SELECT COUNT(*) FROM library_item_migration_map WHERE status = 'completed'`, &snap.CompletedMappings},
		{`SELECT COUNT(*) FROM files`, &snap.Files},
		{`SELECT COUNT(*) FROM files f LEFT JOIN editions e ON e.id = f.edition_id WHERE e.id IS NULL`, &snap.OrphanFiles},
		{`SELECT COUNT(*) FROM (SELECT file_path FROM files WHERE file_path IS NOT NULL AND file_path <> '' GROUP BY file_path HAVING COUNT(*) > 1)`, &snap.DuplicateFilePaths},
		{`SELECT COUNT(*) FROM (SELECT COALESCE(book_id, 0), COALESCE(edition_id, 0), provider, identifier FROM identifiers GROUP BY COALESCE(book_id, 0), COALESCE(edition_id, 0), provider, identifier HAVING COUNT(*) > 1)`, &snap.DuplicateIdentifiers},
		{`SELECT COUNT(*) FROM library_item_migration_map m LEFT JOIN books b ON b.id = m.book_id WHERE m.status = 'completed' AND b.id IS NULL`, &snap.MissingMappedBooks},
		{`SELECT COUNT(*) FROM library_item_migration_map m LEFT JOIN editions e ON e.id = m.edition_id WHERE m.status = 'completed' AND e.id IS NULL`, &snap.MissingMappedEditions},
		{`SELECT COUNT(*) FROM library_item_migration_map m LEFT JOIN files f ON f.id = m.file_id WHERE m.status = 'completed' AND f.id IS NULL`, &snap.MissingMappedFiles},
	}
	for _, check := range checks {
		if err := s.db.QueryRowContext(ctx, check.query).Scan(check.dest); err != nil {
			return snap, fmt.Errorf("validation query failed: %w", err)
		}
	}
	return snap, nil
}

func lastProcessedID(report Report) int64 {
	if len(report.Rows) == 0 {
		return 0
	}
	return report.Rows[len(report.Rows)-1].LegacyItemID
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullableID(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	message = strings.ReplaceAll(message, "\n", " ")
	if len(message) > 500 {
		message = message[:500]
	}
	return message
}
