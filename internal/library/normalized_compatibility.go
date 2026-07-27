package library

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

type NormalizedCompatibilityRepository struct {
	repo *NormalizedRepository
}

func NewNormalizedCompatibilityRepository(repo *NormalizedRepository) *NormalizedCompatibilityRepository {
	return &NormalizedCompatibilityRepository{repo: repo}
}

func (r *NormalizedCompatibilityRepository) ListLegacyItems(ctx context.Context, mediaType string, limit, offset int) ([]models.LibraryItem, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.repo.db.QueryContext(ctx, `SELECT f.id, b.title, COALESCE(c.name, ''), COALESCE(f.file_path, ''),
		f.original_path, f.file_size, f.format, f.media_type, f.source_type, f.source_id,
		f.embedded_metadata_json, f.content_hash, COALESCE(f.imported_at, f.created_at, b.created_at)
		FROM files f
		JOIN editions e ON e.id = f.edition_id
		JOIN books b ON b.id = e.book_id
		LEFT JOIN edition_contributors ec ON ec.edition_id = e.id AND ec.role = 'author'
		LEFT JOIN contributors c ON c.id = ec.contributor_id
		WHERE (? = '' OR f.media_type = ?)
		GROUP BY f.id
		ORDER BY datetime(COALESCE(f.imported_at, f.created_at, b.created_at)) DESC, f.id DESC
		LIMIT ? OFFSET ?`, mediaType, mediaType, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []models.LibraryItem
	for rows.Next() {
		item, err := scanNormalizedLegacyItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if items == nil {
		items = []models.LibraryItem{}
	}
	return items, rows.Err()
}

func (r *NormalizedCompatibilityRepository) CountLegacyItems(ctx context.Context, mediaType string) (int, error) {
	var count int
	err := r.repo.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM files WHERE (? = '' OR media_type = ?)`, mediaType, mediaType).Scan(&count)
	return count, err
}

func (r *NormalizedCompatibilityRepository) LegacyStats(ctx context.Context) (map[string]interface{}, error) {
	ebookCount, _ := r.CountLegacyItems(ctx, "ebook")
	audiobookCount, _ := r.CountLegacyItems(ctx, "audiobook")
	mangaCount, _ := r.CountLegacyItems(ctx, "manga")
	totalCount, _ := r.CountLegacyItems(ctx, "")
	return map[string]interface{}{
		"total_items": totalCount,
		"ebooks":      ebookCount,
		"audiobooks":  audiobookCount,
		"manga":       mangaCount,
	}, nil
}

func (r *NormalizedCompatibilityRepository) GetLegacyItem(ctx context.Context, id int64) (models.LibraryItem, error) {
	rows, err := r.repo.db.QueryContext(ctx, `SELECT f.id, b.title, COALESCE(c.name, ''), COALESCE(f.file_path, ''),
		f.original_path, f.file_size, f.format, f.media_type, f.source_type, f.source_id,
		f.embedded_metadata_json, f.content_hash, COALESCE(f.imported_at, f.created_at, b.created_at)
		FROM files f
		JOIN editions e ON e.id = f.edition_id
		JOIN books b ON b.id = e.book_id
		LEFT JOIN edition_contributors ec ON ec.edition_id = e.id AND ec.role = 'author'
		LEFT JOIN contributors c ON c.id = ec.contributor_id
		WHERE f.id = ?
		GROUP BY f.id`, id)
	if err != nil {
		return models.LibraryItem{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return models.LibraryItem{}, err
		}
		return models.LibraryItem{}, ErrNotFound
	}
	return scanNormalizedLegacyItem(rows)
}

func (r *NormalizedCompatibilityRepository) ImportLegacyItem(ctx context.Context, item models.LibraryItem) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var fileID int64
	err := r.repo.WithinTransaction(ctx, func(txCtx context.Context) error {
		mediaType := MediaType(item.MediaType)
		if mediaType == "" {
			mediaType = MediaTypeEbook
		}
		title := strings.TrimSpace(item.Title)
		if title == "" {
			title = "Unknown"
		}
		book, err := r.repo.CreateBook(txCtx, Book{
			Title:     title,
			SortTitle: NormalizeKey(title),
			MediaType: mediaType,
			Status:    BookStatusOwned,
		})
		if err != nil {
			return err
		}
		edition, err := r.repo.CreateEdition(txCtx, Edition{BookID: book.ID, Title: title})
		if err != nil {
			return err
		}
		if author := strings.TrimSpace(item.Author); author != "" {
			if err := r.repo.AttachContributor(txCtx, edition.ID, Contributor{Name: author, Roles: []ContributorRole{RoleAuthor}}); err != nil {
				return err
			}
		}
		format := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(item.FileFormat)), ".")
		if format == "" {
			format = fileExt(item.FilePath)
		}
		if format == "" {
			format = "unknown"
		}
		file, err := r.repo.AttachFile(txCtx, BookFile{
			BookID:               book.ID,
			EditionID:            edition.ID,
			MediaType:            mediaType,
			Format:               format,
			Path:                 item.FilePath,
			OriginalPath:         item.OriginalPath,
			Size:                 item.FileSize,
			ContentHash:          item.ContentHash,
			SourceID:             item.SourceID,
			SourceType:           item.Source,
			Managed:              true,
			EmbeddedMetadataJSON: item.Metadata,
			ImportedAt:           item.AddedAt,
		})
		if err != nil {
			return err
		}
		fileID = file.ID
		return nil
	})
	return fileID, err
}

func (r *NormalizedCompatibilityRepository) DeleteLegacyItem(ctx context.Context, id int64) error {
	err := r.repo.DeleteFile(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	return err
}

func (r *NormalizedCompatibilityRepository) DeleteLegacyItemBySourceID(ctx context.Context, sourceID string) error {
	file, err := r.repo.FindFileBySourceID(ctx, sourceID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.repo.DeleteFile(ctx, file.ID)
}

func (r *NormalizedCompatibilityRepository) HasLegacySourceID(ctx context.Context, sourceID string) (bool, error) {
	_, err := r.repo.FindFileBySourceID(ctx, sourceID)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func scanNormalizedLegacyItem(scanner interface {
	Scan(dest ...any) error
}) (models.LibraryItem, error) {
	var item models.LibraryItem
	var added string
	if err := scanner.Scan(&item.ID, &item.Title, &item.Author, &item.FilePath, &item.OriginalPath,
		&item.FileSize, &item.FileFormat, &item.MediaType, &item.Source, &item.SourceID,
		&item.Metadata, &item.ContentHash, &added); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.LibraryItem{}, ErrNotFound
		}
		return models.LibraryItem{}, err
	}
	item.AddedAt = parseTime(added)
	if item.AddedAt.IsZero() {
		item.AddedAt = time.Now()
	}
	return item, nil
}

var _ LegacyCompatibilityRepository = (*NormalizedCompatibilityRepository)(nil)
