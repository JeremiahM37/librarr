package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
	in.Status = strings.TrimSpace(strings.ToLower(in.Status))
	if in.MediaType == "" {
		in.MediaType = "ebook"
	}
	if in.Status == "" {
		in.Status = "wanted"
	}
	return in
}

func (d *DB) CreateWantedBook(book models.WantedBook) (*models.WantedBook, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	book = normalizeWantedBook(book)
	if book.Title == "" {
		return nil, fmt.Errorf("title is required")
	}

	result, err := d.db.Exec(`INSERT INTO wanted_books (
		title, author, isbn, asin, series, publisher, language, cover_url, description, source, media_type, monitored, status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		book.Title, book.Author, book.ISBN, book.ASIN, book.Series, book.Publisher, book.Language, book.CoverURL, book.Description, book.Source, book.MediaType, wantedBoolToInt(book.Monitored), book.Status,
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
	row := d.db.QueryRow(`SELECT id, title, author, isbn, asin, series, publisher, language, cover_url, description, source, media_type, monitored, status, added_at, updated_at
		FROM wanted_books WHERE id = ?`, id)
	return scanWantedBook(row)
}

func (d *DB) ListWantedBooks() ([]models.WantedBook, error) {
	rows, err := d.db.Query(`SELECT id, title, author, isbn, asin, series, publisher, language, cover_url, description, source, media_type, monitored, status, added_at, updated_at
		FROM wanted_books ORDER BY monitored DESC, added_at DESC, id DESC`)
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

type wantedBookScanner interface {
	Scan(dest ...any) error
}

func scanWantedBook(scanner wantedBookScanner) (*models.WantedBook, error) {
	var item models.WantedBook
	var monitored int
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
		&monitored,
		&item.Status,
		&item.AddedAt,
		&item.UpdatedAt,
	); err != nil {
		return nil, err
	}
	item.Monitored = monitored == 1
	return &item, nil
}

func wantedBoolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
