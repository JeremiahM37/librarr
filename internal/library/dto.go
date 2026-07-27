package library

import (
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

type LegacyLibraryItemDTO = models.LibraryItem

func LegacyItemToBook(item models.LibraryItem) Book {
	return Book{
		ID:           item.ID,
		Title:        item.Title,
		SortTitle:    NormalizeKey(item.Title),
		MediaType:    MediaType(item.MediaType),
		Status:       BookStatusOwned,
		CreatedAt:    item.AddedAt,
		UpdatedAt:    item.AddedAt,
		Contributors: legacyContributors(item),
		PreferredEdition: &Edition{
			ID:        item.ID,
			BookID:    item.ID,
			Title:     item.Title,
			CreatedAt: item.AddedAt,
			UpdatedAt: item.AddedAt,
		},
	}
}

func LegacyItemToFile(item models.LibraryItem) BookFile {
	format := item.FileFormat
	if format == "" {
		format = fileExt(item.FilePath)
	}
	return BookFile{
		ID:           item.ID,
		BookID:       item.ID,
		EditionID:    item.ID,
		MediaType:    MediaType(item.MediaType),
		Format:       strings.TrimPrefix(strings.ToLower(format), "."),
		Path:         item.FilePath,
		OriginalPath: item.OriginalPath,
		Size:         item.FileSize,
		ContentHash:  item.ContentHash,
		SourceID:     item.SourceID,
		SourceType:   item.Source,
		Managed:      true,
		ImportedAt:   item.AddedAt,
		CreatedAt:    item.AddedAt,
		UpdatedAt:    item.AddedAt,
	}
}

func ToLegacyLibraryItem(book Book, file BookFile) models.LibraryItem {
	addedAt := file.ImportedAt
	if addedAt.IsZero() {
		addedAt = firstTime(file.CreatedAt, book.CreatedAt, time.Now())
	}
	return models.LibraryItem{
		ID:           file.ID,
		Title:        book.Title,
		Author:       primaryAuthor(book),
		FilePath:     file.Path,
		OriginalPath: file.OriginalPath,
		FileSize:     file.Size,
		FileFormat:   file.Format,
		MediaType:    string(nonEmptyMediaType(file.MediaType, book.MediaType)),
		Source:       file.SourceType,
		SourceID:     file.SourceID,
		ContentHash:  file.ContentHash,
		AddedAt:      addedAt,
	}
}

func legacyContributors(item models.LibraryItem) []Contributor {
	if strings.TrimSpace(item.Author) == "" {
		return nil
	}
	return []Contributor{{Name: item.Author, Roles: []ContributorRole{RoleAuthor}, Position: 1}}
}

func primaryAuthor(book Book) string {
	for _, contributor := range book.Contributors {
		if contributor.Name == "" {
			continue
		}
		if len(contributor.Roles) == 0 {
			return contributor.Name
		}
		for _, role := range contributor.Roles {
			if role == RoleAuthor {
				return contributor.Name
			}
		}
	}
	return ""
}

func nonEmptyMediaType(values ...MediaType) MediaType {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return MediaTypeEbook
}

func firstTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func fileExt(path string) string {
	idx := strings.LastIndex(path, ".")
	if idx < 0 || idx == len(path)-1 {
		return ""
	}
	return path[idx+1:]
}
