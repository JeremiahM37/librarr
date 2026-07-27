package db

import (
	"database/sql"
	"fmt"
	"time"
)

type Book struct {
	ID              int64
	Title           string
	SortTitle       string
	Description     string
	PublicationYear sql.NullInt64
	Language        string
	MediaType       string
	Monitored       bool
	Status          string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Edition struct {
	ID              int64
	BookID          int64
	Title           string
	Subtitle        string
	Publisher       string
	PublicationDate string
	Language        string
	PageCount       sql.NullInt64
	EditionName     string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Contributor struct {
	ID        int64
	Name      string
	SortName  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type EditionContributor struct {
	EditionID     int64
	ContributorID int64
	Role          string
	Position      int
}

type Series struct {
	ID          int64
	Title       string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type BookSeries struct {
	BookID          int64
	SeriesID        int64
	Position        sql.NullFloat64
	DisplayPosition string
}

type File struct {
	ID           int64
	EditionID    int64
	MediaType    string
	Format       string
	FilePath     string
	OriginalPath string
	FileSize     int64
	ContentHash  string
	SourceID     string
	SourceType   string
	Quality      string
	IsManaged    bool
	ImportedAt   sql.NullTime
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Identifier struct {
	ID         int64
	BookID     sql.NullInt64
	EditionID  sql.NullInt64
	Provider   string
	Identifier string
	CreatedAt  time.Time
}

type Cover struct {
	ID        int64
	BookID    sql.NullInt64
	EditionID sql.NullInt64
	Source    string
	SourceURL string
	LocalPath string
	MimeType  string
	Width     sql.NullInt64
	Height    sql.NullInt64
	IsPrimary bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type LibraryItemMigrationMap struct {
	LibraryItemID int64
	BookID        sql.NullInt64
	EditionID     sql.NullInt64
	FileID        sql.NullInt64
	Status        string
	Reason        sql.NullString
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (d *DB) CreateBook(book *Book) (int64, error) {
	if book == nil {
		return 0, fmt.Errorf("book is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO books (title, sort_title, description, publication_year, language, media_type, monitored, status)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		book.Title, book.SortTitle, book.Description, nullIntValue(book.PublicationYear),
		book.Language, book.MediaType, boolToInt(book.Monitored), book.Status,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetBook(id int64) (*Book, error) {
	var book Book
	var createdAt, updatedAt string
	var monitored int
	err := d.db.QueryRow(
		`SELECT id, title, sort_title, description, publication_year, language, media_type, monitored, status, created_at, updated_at
		 FROM books WHERE id = ?`,
		id,
	).Scan(&book.ID, &book.Title, &book.SortTitle, &book.Description, &book.PublicationYear,
		&book.Language, &book.MediaType, &monitored, &book.Status, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	book.Monitored = monitored != 0
	book.CreatedAt = parseDBTime(createdAt)
	book.UpdatedAt = parseDBTime(updatedAt)
	return &book, nil
}

func (d *DB) CreateEdition(edition *Edition) (int64, error) {
	if edition == nil {
		return 0, fmt.Errorf("edition is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO editions (book_id, title, subtitle, publisher, publication_date, language, page_count, edition_name)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		edition.BookID, edition.Title, edition.Subtitle, edition.Publisher, edition.PublicationDate,
		edition.Language, nullIntValue(edition.PageCount), edition.EditionName,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) CreateContributor(contributor *Contributor) (int64, error) {
	if contributor == nil {
		return 0, fmt.Errorf("contributor is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO contributors (name, sort_name) VALUES (?, ?)`,
		contributor.Name, contributor.SortName,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) CreateFile(file *File) (int64, error) {
	if file == nil {
		return 0, fmt.Errorf("file is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO files (edition_id, media_type, format, file_path, original_path, file_size, content_hash, source_id, source_type, quality, is_managed, imported_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		file.EditionID, file.MediaType, file.Format, nullableString(file.FilePath), file.OriginalPath,
		file.FileSize, file.ContentHash, file.SourceID, file.SourceType, file.Quality,
		boolToInt(file.IsManaged), nullTimeValue(file.ImportedAt),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) AddEditionContributor(link EditionContributor) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(
		`INSERT INTO edition_contributors (edition_id, contributor_id, role, position) VALUES (?, ?, ?, ?)`,
		link.EditionID, link.ContributorID, link.Role, link.Position,
	)
	return err
}

func (d *DB) AddBookSeries(link BookSeries) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(
		`INSERT INTO book_series (book_id, series_id, position, display_position) VALUES (?, ?, ?, ?)`,
		link.BookID, link.SeriesID, nullFloatValue(link.Position), link.DisplayPosition,
	)
	return err
}

func (d *DB) CreateSeries(series *Series) (int64, error) {
	if series == nil {
		return 0, fmt.Errorf("series is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO series (title, description) VALUES (?, ?)`,
		series.Title, series.Description,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) AddIdentifier(identifier *Identifier) (int64, error) {
	if identifier == nil {
		return 0, fmt.Errorf("identifier is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO identifiers (book_id, edition_id, provider, identifier) VALUES (?, ?, ?, ?)`,
		nullIntValue(identifier.BookID), nullIntValue(identifier.EditionID), identifier.Provider, identifier.Identifier,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) AddCover(cover *Cover) (int64, error) {
	if cover == nil {
		return 0, fmt.Errorf("cover is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO covers (book_id, edition_id, source, source_url, local_path, mime_type, width, height, is_primary)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullIntValue(cover.BookID), nullIntValue(cover.EditionID), cover.Source, cover.SourceURL,
		cover.LocalPath, cover.MimeType, nullIntValue(cover.Width), nullIntValue(cover.Height),
		boolToInt(cover.IsPrimary),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func nullIntValue(value sql.NullInt64) interface{} {
	if value.Valid {
		return value.Int64
	}
	return nil
}

func nullFloatValue(value sql.NullFloat64) interface{} {
	if value.Valid {
		return value.Float64
	}
	return nil
}

func nullTimeValue(value sql.NullTime) interface{} {
	if value.Valid {
		return value.Time
	}
	return nil
}

func nullableString(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func parseDBTime(value string) time.Time {
	if t, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t
	}
	return time.Time{}
}
