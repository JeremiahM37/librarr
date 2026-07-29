package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

var ErrWantedBookExists = errors.New("wanted book already exists")

func normalizeWantedBook(in models.WantedBook) models.WantedBook {
	in.Title = strings.TrimSpace(in.Title)
	in.Author = strings.TrimSpace(in.Author)
	in.ISBN = strings.TrimSpace(in.ISBN)
	in.ASIN = strings.TrimSpace(in.ASIN)
	in.Series = strings.TrimSpace(in.Series)
	in.Publisher = strings.TrimSpace(in.Publisher)
	in.Language = strings.TrimSpace(in.Language)
	in.CoverURL = strings.TrimSpace(in.CoverURL)
	in.Description = strings.TrimSpace(in.Description)
	in.Source = strings.TrimSpace(in.Source)
	in.MediaType = strings.TrimSpace(strings.ToLower(in.MediaType))
	in.PreferredFormat = strings.TrimSpace(strings.ToLower(in.PreferredFormat))
	in.OriginSource = strings.TrimSpace(in.OriginSource)
	in.OriginReleaseTitle = strings.TrimSpace(in.OriginReleaseTitle)
	in.OriginIndexer = strings.TrimSpace(in.OriginIndexer)
	in.SourceID = strings.TrimSpace(in.SourceID)
	in.Status = strings.TrimSpace(strings.ToLower(in.Status))
	if in.MediaType == "" {
		in.MediaType = "ebook"
	}
	if in.Status == "" {
		in.Status = "wanted"
	}
	return in
}

type WantedSearchUpdate struct {
	Status          string
	LastSearch      time.Time
	LastResultCount int
	LastSuccess     bool
	LastError       string
	BestMatchScore  float64
	LastMatchTitle  string
	Query           string
}

const wantedBookSelectColumns = `id, title, author, isbn, asin, series, publisher, language, cover_url, description, source, media_type, preferred_format, origin_source, origin_release_title, origin_indexer, source_id, monitored, status, last_search, last_result_count, last_success, last_error, best_match_score, last_match_title, selected_release_id, selected_release_title, download_job_id, download_client, download_hash, download_started_at, download_error, added_at, updated_at`

const wantedReleaseSelectColumns = `id, wanted_book_id, title, guid, indexer, protocol, publish_date, size, size_human, seeders, leechers, grabs, language, format, download_url, categories, score, search_query, search_time`

func (d *DB) CreateWantedBook(book models.WantedBook) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	book = normalizeWantedBook(book)
	if book.Title == "" {
		return nil, fmt.Errorf("title is required")
	}

	result, err := d.db.Exec(`INSERT INTO wanted_books (
		title, author, isbn, asin, series, publisher, language, cover_url, description, source, media_type, preferred_format, origin_source, origin_release_title, origin_indexer, source_id, monitored, status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		book.Title, book.Author, book.ISBN, book.ASIN, book.Series, book.Publisher, book.Language, book.CoverURL, book.Description, book.Source, book.MediaType, book.PreferredFormat, book.OriginSource, book.OriginReleaseTitle, book.OriginIndexer, book.SourceID, wantedBoolToInt(book.Monitored), book.Status,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, ErrWantedBookExists
		}
		return nil, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) GetWantedBook(id int64) (*models.WantedBook, error) {
	row := d.db.QueryRow(`SELECT `+wantedBookSelectColumns+`
		FROM wanted_books WHERE id = ?`, id)
	return scanWantedBook(row)
}

func (d *DB) ListWantedBooks() ([]models.WantedBook, error) {
	rows, err := d.db.Query(`SELECT ` + wantedBookSelectColumns + `
		FROM wanted_books ORDER BY monitored DESC, COALESCE(last_search, added_at) DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []models.WantedBook
	for rows.Next() {
		item, err := scanWantedBook(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (d *DB) ListMonitoredWantedBooks() ([]models.WantedBook, error) {
	rows, err := d.db.Query(`SELECT ` + wantedBookSelectColumns + `
		FROM wanted_books
		WHERE monitored = 1 AND status NOT IN ('ignored', 'downloading', 'downloaded', 'imported')
		ORDER BY COALESCE(last_search, added_at) ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []models.WantedBook
	for rows.Next() {
		item, err := scanWantedBook(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (d *DB) UpdateWantedBookMetadata(id int64, book models.WantedBook) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	book = normalizeWantedBook(book)
	if book.Title == "" {
		return nil, fmt.Errorf("title is required")
	}
	_, err := d.db.Exec(`UPDATE wanted_books
		SET title = ?, author = ?, isbn = ?, asin = ?, series = ?, publisher = ?, language = ?,
		    cover_url = ?, description = ?, source = ?, media_type = ?, preferred_format = ?,
		    origin_source = ?, origin_release_title = ?, origin_indexer = ?, source_id = ?, updated_at = datetime('now')
		WHERE id = ?`,
		book.Title, book.Author, book.ISBN, book.ASIN, book.Series, book.Publisher, book.Language,
		book.CoverURL, book.Description, book.Source, book.MediaType, book.PreferredFormat,
		book.OriginSource, book.OriginReleaseTitle, book.OriginIndexer, book.SourceID, id,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, ErrWantedBookExists
		}
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) UpdateWantedBook(id int64, monitored *bool, status *string) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	current, err := d.GetWantedBook(id)
	if err != nil {
		return nil, err
	}
	if monitored != nil {
		current.Monitored = *monitored
	}
	if status != nil {
		current.Status = strings.TrimSpace(strings.ToLower(*status))
	}

	_, err = d.db.Exec(`UPDATE wanted_books
		SET monitored = ?, status = ?, updated_at = datetime('now')
		WHERE id = ?`,
		wantedBoolToInt(current.Monitored), current.Status, id,
	)
	if err != nil {
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) DeleteWantedBook(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(`DELETE FROM wanted_books WHERE id = ?`, id)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (d *DB) UpdateWantedSearch(id int64, update WantedSearchUpdate) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	status := strings.TrimSpace(strings.ToLower(update.Status))
	if status == "" {
		status = "wanted"
	}
	lastError := strings.TrimSpace(update.LastError)
	lastMatchTitle := strings.TrimSpace(update.LastMatchTitle)
	_, err := d.db.Exec(`UPDATE wanted_books
		SET status = ?, last_search = ?, last_result_count = ?, last_success = ?, last_error = ?, best_match_score = ?, last_match_title = ?, updated_at = datetime('now')
		WHERE id = ?`,
		status,
		update.LastSearch.UTC().Format(time.RFC3339),
		update.LastResultCount,
		wantedBoolToInt(update.LastSuccess),
		lastError,
		update.BestMatchScore,
		lastMatchTitle,
		id,
	)
	if err != nil {
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) MarkWantedOriginFound(id int64, matchTitle string, score float64) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE wanted_books
		SET status = 'found',
		    last_result_count = 1,
		    last_success = 1,
		    last_error = '',
		    best_match_score = ?,
		    last_match_title = ?,
		    updated_at = datetime('now')
		WHERE id = ?`,
		score,
		strings.TrimSpace(matchTitle),
		id,
	)
	if err != nil {
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) MarkWantedDownloading(id, releaseID int64, releaseTitle, client, downloadHash string, startedAt time.Time) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	_, err := d.db.Exec(`UPDATE wanted_books
		SET status = 'downloading',
		    selected_release_id = ?,
		    selected_release_title = ?,
		    download_client = ?,
		    download_hash = ?,
		    download_started_at = ?,
		    download_error = '',
		    updated_at = datetime('now')
		WHERE id = ?`,
		releaseID,
		strings.TrimSpace(releaseTitle),
		strings.TrimSpace(client),
		strings.TrimSpace(downloadHash),
		startedAt.UTC().Format(time.RFC3339),
		id,
	)
	if err != nil {
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) MarkWantedDownloadFailure(id int64, message string) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE wanted_books
		SET download_error = ?, updated_at = datetime('now')
		WHERE id = ?`,
		strings.TrimSpace(message),
		id,
	)
	if err != nil {
		return nil, err
	}
	return d.GetWantedBook(id)
}

func (d *DB) AddWantedSearchHistory(id int64, update WantedSearchUpdate) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`INSERT INTO wanted_search_history (
		wanted_book_id, title, author, query, status, result_count, success, error, best_match_score, best_match_title, searched_at
	) SELECT id, title, author, ?, ?, ?, ?, ?, ?, ?, ?
	  FROM wanted_books WHERE id = ?`,
		strings.TrimSpace(update.Query),
		strings.TrimSpace(strings.ToLower(update.Status)),
		update.LastResultCount,
		wantedBoolToInt(update.LastSuccess),
		strings.TrimSpace(update.LastError),
		update.BestMatchScore,
		strings.TrimSpace(update.LastMatchTitle),
		update.LastSearch.UTC().Format(time.RFC3339),
		id,
	)
	return err
}

func (d *DB) PruneWantedSearchHistory(maxRows int) error {
	if maxRows <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`DELETE FROM wanted_search_history
		WHERE id IN (
			SELECT id FROM (
				SELECT id,
				       ROW_NUMBER() OVER (PARTITION BY wanted_book_id ORDER BY searched_at DESC, id DESC) AS row_num
				FROM wanted_search_history
			) ranked
			WHERE ranked.row_num > ?
		)`, maxRows)
	return err
}

func (d *DB) ListWantedSearchHistory(limit int) ([]models.WantedSearchHistory, error) {
	query := `SELECT id, wanted_book_id, title, author, query, status, result_count, success, error, best_match_score, best_match_title, searched_at
		FROM wanted_search_history
		ORDER BY searched_at DESC, id DESC`
	var rows *sql.Rows
	var err error
	if limit > 0 {
		rows, err = d.db.Query(query+` LIMIT ?`, limit)
	} else {
		rows, err = d.db.Query(query)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []models.WantedSearchHistory
	for rows.Next() {
		item, err := scanWantedSearchHistory(rows)
		if err != nil {
			return nil, err
		}
		history = append(history, *item)
	}
	return history, rows.Err()
}

func (d *DB) ReplaceWantedReleases(id int64, releases []models.WantedRelease) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.Exec(`DELETE FROM wanted_search_releases WHERE wanted_book_id = ?`, id); err != nil {
		return err
	}
	sort.SliceStable(releases, func(i, j int) bool {
		if releases[i].Score == releases[j].Score {
			return releases[i].Title < releases[j].Title
		}
		return releases[i].Score > releases[j].Score
	})
	for _, release := range releases {
		categories, err := json.Marshal(release.Categories)
		if err != nil {
			return err
		}
		if release.SearchTime.IsZero() {
			release.SearchTime = time.Now().UTC()
		}
		_, err = tx.Exec(`INSERT INTO wanted_search_releases (
			wanted_book_id, title, guid, indexer, protocol, publish_date, size, size_human,
			seeders, leechers, grabs, language, format, download_url, categories, score, search_query, search_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id,
			strings.TrimSpace(release.Title),
			strings.TrimSpace(release.GUID),
			strings.TrimSpace(release.Indexer),
			strings.TrimSpace(strings.ToLower(release.Protocol)),
			strings.TrimSpace(release.PublishDate),
			release.Size,
			strings.TrimSpace(release.SizeHuman),
			release.Seeders,
			release.Leechers,
			release.Grabs,
			strings.TrimSpace(strings.ToLower(release.Language)),
			strings.TrimSpace(strings.ToLower(release.Format)),
			strings.TrimSpace(release.DownloadURL),
			string(categories),
			release.Score,
			strings.TrimSpace(release.SearchQuery),
			release.SearchTime.UTC().Format(time.RFC3339),
		)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (d *DB) ListWantedReleases(id int64) ([]models.WantedRelease, error) {
	rows, err := d.db.Query(`SELECT `+wantedReleaseSelectColumns+`
		FROM wanted_search_releases
		WHERE wanted_book_id = ?
		ORDER BY score DESC, id ASC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var releases []models.WantedRelease
	for rows.Next() {
		release, err := scanWantedRelease(rows)
		if err != nil {
			return nil, err
		}
		releases = append(releases, *release)
	}
	return releases, rows.Err()
}

func (d *DB) GetWantedRelease(wantedID, releaseID int64) (*models.WantedRelease, error) {
	row := d.db.QueryRow(`SELECT `+wantedReleaseSelectColumns+`
		FROM wanted_search_releases
		WHERE wanted_book_id = ? AND id = ?`, wantedID, releaseID)
	return scanWantedRelease(row)
}

type wantedBookScanner interface {
	Scan(dest ...any) error
}

func scanWantedBook(scanner wantedBookScanner) (*models.WantedBook, error) {
	var item models.WantedBook
	var monitored int
	var lastSuccess int
	var lastSearch sql.NullTime
	var downloadStartedAt sql.NullTime
	if err := scanner.Scan(
		&item.ID,
		&item.Title,
		&item.Author,
		&item.ISBN,
		&item.ASIN,
		&item.Series,
		&item.Publisher,
		&item.Language,
		&item.CoverURL,
		&item.Description,
		&item.Source,
		&item.MediaType,
		&item.PreferredFormat,
		&item.OriginSource,
		&item.OriginReleaseTitle,
		&item.OriginIndexer,
		&item.SourceID,
		&monitored,
		&item.Status,
		&lastSearch,
		&item.LastResultCount,
		&lastSuccess,
		&item.LastError,
		&item.BestMatchScore,
		&item.LastMatchTitle,
		&item.SelectedReleaseID,
		&item.SelectedReleaseTitle,
		&item.DownloadJobID,
		&item.DownloadClient,
		&item.DownloadHash,
		&downloadStartedAt,
		&item.DownloadError,
		&item.AddedAt,
		&item.UpdatedAt,
	); err != nil {
		return nil, err
	}
	item.Monitored = monitored == 1
	item.LastSuccess = lastSuccess == 1
	if lastSearch.Valid {
		timestamp := lastSearch.Time
		item.LastSearch = &timestamp
	}
	if downloadStartedAt.Valid {
		timestamp := downloadStartedAt.Time
		item.DownloadStartedAt = &timestamp
	}
	return &item, nil
}

func scanWantedSearchHistory(scanner wantedBookScanner) (*models.WantedSearchHistory, error) {
	var item models.WantedSearchHistory
	var success int
	if err := scanner.Scan(
		&item.ID,
		&item.WantedBookID,
		&item.Title,
		&item.Author,
		&item.Query,
		&item.Status,
		&item.ResultCount,
		&success,
		&item.Error,
		&item.BestMatchScore,
		&item.BestMatchTitle,
		&item.SearchedAt,
	); err != nil {
		return nil, err
	}
	item.Success = success == 1
	return &item, nil
}

func scanWantedRelease(scanner wantedBookScanner) (*models.WantedRelease, error) {
	var item models.WantedRelease
	var categories string
	if err := scanner.Scan(
		&item.ID,
		&item.WantedBookID,
		&item.Title,
		&item.GUID,
		&item.Indexer,
		&item.Protocol,
		&item.PublishDate,
		&item.Size,
		&item.SizeHuman,
		&item.Seeders,
		&item.Leechers,
		&item.Grabs,
		&item.Language,
		&item.Format,
		&item.DownloadURL,
		&categories,
		&item.Score,
		&item.SearchQuery,
		&item.SearchTime,
	); err != nil {
		return nil, err
	}
	if strings.TrimSpace(categories) != "" {
		_ = json.Unmarshal([]byte(categories), &item.Categories)
	}
	return &item, nil
}

func wantedBoolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
