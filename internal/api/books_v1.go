package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/library"
)

const normalizedReadAPIError = "Librarr v1 book API requires LIBRARR_LIBRARY_REPOSITORY_MODE=normalized"

type v1BookListResponse struct {
	Items      []v1BookSummary   `json:"items"`
	Pagination v1PaginationBlock `json:"pagination"`
}

type v1PaginationBlock struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

type v1SeriesSummary struct {
	Name     *string  `json:"name"`
	Position *float64 `json:"position"`
}

type v1ContributorSummary struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
}

type v1PrimaryAuthorSummary struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type v1IdentifierSummary struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type v1CoverSummary struct {
	Available bool    `json:"available"`
	URL       *string `json:"url"`
}

type v1BookSummary struct {
	ID            int64                   `json:"id"`
	Title         string                  `json:"title"`
	SortTitle     string                  `json:"sort_title"`
	MediaType     string                  `json:"media_type"`
	Description   string                  `json:"description"`
	Series        v1SeriesSummary         `json:"series"`
	PrimaryAuthor *v1PrimaryAuthorSummary `json:"primary_author"`
	Contributors  []v1ContributorSummary  `json:"contributors"`
	Identifiers   []v1IdentifierSummary   `json:"identifiers"`
	Formats       []string                `json:"formats"`
	EditionCount  int                     `json:"edition_count"`
	FileCount     int                     `json:"file_count"`
	Cover         v1CoverSummary          `json:"cover"`
	CreatedAt     string                  `json:"created_at"`
	UpdatedAt     string                  `json:"updated_at"`
}

type v1BookDetailResponse struct {
	ID              int64                   `json:"id"`
	Title           string                  `json:"title"`
	SortTitle       string                  `json:"sort_title"`
	MediaType       string                  `json:"media_type"`
	Description     string                  `json:"description"`
	PublicationYear int                     `json:"publication_year,omitempty"`
	Language        string                  `json:"language,omitempty"`
	Series          v1SeriesSummary         `json:"series"`
	PrimaryAuthor   *v1PrimaryAuthorSummary `json:"primary_author"`
	Contributors    []v1ContributorSummary  `json:"contributors"`
	Identifiers     []v1IdentifierSummary   `json:"identifiers"`
	Formats         []string                `json:"formats"`
	FileCount       int                     `json:"file_count"`
	EditionCount    int                     `json:"edition_count"`
	Editions        []v1EditionSummary      `json:"editions"`
	Cover           v1CoverSummary          `json:"cover"`
	CreatedAt       string                  `json:"created_at"`
	UpdatedAt       string                  `json:"updated_at"`
}

type v1EditionSummary struct {
	ID              int64                  `json:"id"`
	Title           string                 `json:"title"`
	Subtitle        string                 `json:"subtitle,omitempty"`
	Publisher       string                 `json:"publisher,omitempty"`
	PublicationDate string                 `json:"publication_date,omitempty"`
	Language        string                 `json:"language,omitempty"`
	Contributors    []v1ContributorSummary `json:"contributors,omitempty"`
	Identifiers     []v1IdentifierSummary  `json:"identifiers"`
	Formats         []string               `json:"formats"`
	FileCount       int                    `json:"file_count"`
}

type v1FileSummary struct {
	ID           int64  `json:"id"`
	EditionID    int64  `json:"edition_id"`
	Path         string `json:"path,omitempty"`
	OriginalPath string `json:"original_path,omitempty"`
	Format       string `json:"format"`
	MediaType    string `json:"media_type"`
	Size         int64  `json:"size"`
	SourceID     string `json:"source_id,omitempty"`
	ContentHash  string `json:"content_hash,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type v1MetadataFieldSummary struct {
	Value           string `json:"value"`
	Source          string `json:"source"`
	Confidence      string `json:"confidence"`
	ConfidenceScore int    `json:"confidence_score"`
	UpdatedAt       string `json:"updated_at"`
	ManualOverride  bool   `json:"manual_override"`
}

type v1MetadataEvidenceSummary struct {
	Value           string `json:"value"`
	Source          string `json:"source"`
	Confidence      string `json:"confidence"`
	ConfidenceScore int    `json:"confidence_score"`
	UpdatedAt       string `json:"updated_at"`
	ManualOverride  bool   `json:"manual_override"`
	Selected        bool   `json:"selected"`
}

type v1MetadataContributorSummary struct {
	Name            string `json:"name"`
	Role            string `json:"role"`
	Source          string `json:"source"`
	Confidence      string `json:"confidence"`
	ConfidenceScore int    `json:"confidence_score"`
	UpdatedAt       string `json:"updated_at"`
	ManualOverride  bool   `json:"manual_override"`
}

type v1MetadataIdentifierSummary struct {
	Type            string `json:"type"`
	Value           string `json:"value"`
	Source          string `json:"source"`
	Confidence      string `json:"confidence"`
	ConfidenceScore int    `json:"confidence_score"`
	UpdatedAt       string `json:"updated_at"`
	ManualOverride  bool   `json:"manual_override"`
}

func (s *Server) handleV1Books(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}

	sortValue := strings.TrimSpace(r.URL.Query().Get("sort"))
	if sortValue == "" {
		sortValue = "recently_added"
	}
	if !slices.Contains([]string{"title", "author", "recently_added", "recently_updated"}, sortValue) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "Invalid sort value",
		})
		return
	}

	orderValue := strings.TrimSpace(r.URL.Query().Get("order"))
	if orderValue == "" {
		if sortValue == "title" || sortValue == "author" {
			orderValue = "asc"
		} else {
			orderValue = "desc"
		}
	}
	if !slices.Contains([]string{"asc", "desc"}, strings.ToLower(orderValue)) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "Invalid order value",
		})
		return
	}

	query := library.ListBooksQuery{
		MediaType: library.MediaType(strings.TrimSpace(r.URL.Query().Get("media_type"))),
		Search:    strings.TrimSpace(r.URL.Query().Get("search")),
		Format:    strings.TrimSpace(r.URL.Query().Get("format")),
		Sort:      sortValue,
		Order:     strings.ToLower(orderValue),
		Limit:     queryBoundedInt(r, "limit", 50, 1, 500),
		Offset:    queryBoundedInt(r, "offset", 0, 0, 1_000_000),
	}
	if query.MediaType != "" && !query.MediaType.Valid() {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "Invalid media_type value",
		})
		return
	}

	total, err := s.library().CountListedBooks(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to count books", err)
		return
	}

	items, err := s.library().ListBookReadModels(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load books", err)
		return
	}

	responseItems := make([]v1BookSummary, 0, len(items))
	for _, item := range items {
		responseItems = append(responseItems, buildV1BookSummary(item))
	}

	writeJSON(w, http.StatusOK, v1BookListResponse{
		Items: responseItems,
		Pagination: v1PaginationBlock{
			Limit:  query.Limit,
			Offset: query.Offset,
			Total:  total,
		},
	})
}

func (s *Server) handleV1Book(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}

	book, err := s.library().GetBook(r.Context(), bookID)
	if errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book", err)
		return
	}

	detail, err := s.buildV1BookDetail(r.Context(), *book)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to build book response", err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleV1BookFiles(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	if _, err := s.library().GetBook(r.Context(), bookID); errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book", err)
		return
	}

	files, err := s.library().GetBookFiles(r.Context(), bookID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load files", err)
		return
	}
	items := make([]v1FileSummary, 0, len(files))
	for _, file := range files {
		items = append(items, v1FileSummary{
			ID:           file.ID,
			EditionID:    file.EditionID,
			Path:         file.Path,
			OriginalPath: file.OriginalPath,
			Format:       file.Format,
			MediaType:    string(file.MediaType),
			Size:         file.Size,
			SourceID:     file.SourceID,
			ContentHash:  file.ContentHash,
			CreatedAt:    formatAPITime(file.CreatedAt),
			UpdatedAt:    formatAPITime(file.UpdatedAt),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleV1BookEditions(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	if _, err := s.library().GetBook(r.Context(), bookID); errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book", err)
		return
	}

	editions, err := s.buildV1Editions(r.Context(), bookID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load editions", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": editions})
}

func (s *Server) handleV1LibrarySummary(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	summary, err := s.library().GetLibrarySummary(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library summary", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total_books":         summary.TotalBooks,
		"total_editions":      summary.TotalEditions,
		"total_files":         summary.TotalFiles,
		"authors":             summary.AuthorCount,
		"ebooks":              summary.EbookCount,
		"audiobooks":          summary.AudiobookCount,
		"manga":               summary.MangaCount,
		"recently_added":      summary.RecentAddedCount,
		"format_distribution": summary.FormatCounts,
	})
}

func (s *Server) handleV1BookCover(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	if _, err := s.library().GetBook(r.Context(), bookID); errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book", err)
		return
	}
	cover, err := s.library().GetPrimaryCover(r.Context(), bookID)
	if errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Cover not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load cover", err)
		return
	}
	if strings.TrimSpace(cover.LocalPath) == "" {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Cover not found"})
		return
	}
	file, err := os.Open(cover.LocalPath)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Cover not found"})
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.IsDir() {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Cover not found"})
		return
	}
	header := make([]byte, 512)
	n, _ := io.ReadFull(file, header)
	contentType := strings.TrimSpace(cover.MimeType)
	if contentType == "" {
		contentType = http.DetectContentType(header[:n])
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Cover not found"})
		return
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(w, r, "cover", info.ModTime(), file)
}

func (s *Server) handleV1BookMetadata(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	metadata, err := s.library().GetBookMetadata(r.Context(), bookID)
	if errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book metadata", err)
		return
	}
	writeJSON(w, http.StatusOK, mapV1BookMetadata(metadata))
}

func (s *Server) handleV1BookMetadataPatch(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	var req struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request body"})
		return
	}
	patch, err := parseBookMetadataPatch(req.Fields)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": err.Error()})
		return
	}
	metadata, err := s.library().PatchBookMetadata(r.Context(), bookID, patch)
	if errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to update book metadata", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"metadata": mapV1BookMetadata(metadata),
	})
}

func (s *Server) handleV1BookProvenance(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	provenance, err := s.library().GetBookProvenance(r.Context(), bookID)
	if errors.Is(err, library.ErrBookNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Book not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book provenance", err)
		return
	}
	writeJSON(w, http.StatusOK, mapV1BookProvenance(provenance))
}

func (s *Server) ensureNormalizedReadAPI(w http.ResponseWriter) bool {
	mode, err := s.cfg.NormalizedLibraryRepositoryMode()
	if err != nil || mode != "normalized" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   normalizedReadAPIError,
		})
		return false
	}
	return true
}

func parseIDPathValue(w http.ResponseWriter, r *http.Request, key, message string) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue(key), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   message,
		})
		return 0, false
	}
	return id, true
}

func buildV1BookSummary(item library.BookReadModel) v1BookSummary {
	return v1BookSummary{
		ID:            item.Book.ID,
		Title:         item.Book.Title,
		SortTitle:     item.Book.SortTitle,
		MediaType:     string(item.Book.MediaType),
		Description:   item.Book.Description,
		Series:        mapSeries(item.Series),
		PrimaryAuthor: mapPrimaryAuthor(item.PrimaryAuthor),
		Contributors:  mapContributors(item.Contributors),
		Identifiers:   mapIdentifiers(item.Identifiers),
		Formats:       item.Formats,
		EditionCount:  item.EditionCount,
		FileCount:     item.FileCount,
		Cover:         mapBookCover(item.Book.ID, item.LocalCover),
		CreatedAt:     formatAPITime(item.Book.CreatedAt),
		UpdatedAt:     formatAPITime(item.Book.UpdatedAt),
	}
}

func (s *Server) buildV1BookDetail(ctx context.Context, book library.Book) (v1BookDetailResponse, error) {
	editions, err := s.buildV1Editions(ctx, book.ID)
	if err != nil {
		return v1BookDetailResponse{}, err
	}
	files, err := s.library().GetBookFiles(ctx, book.ID)
	if err != nil {
		return v1BookDetailResponse{}, err
	}
	contributors, primaryAuthor := aggregateContributors(editions)
	formats := uniqueFormats(files)
	return v1BookDetailResponse{
		ID:              book.ID,
		Title:           book.Title,
		SortTitle:       book.SortTitle,
		MediaType:       string(book.MediaType),
		Description:     book.Description,
		PublicationYear: book.PublicationYear,
		Language:        book.Language,
		Series:          mapSeries(book.Series),
		PrimaryAuthor:   primaryAuthor,
		Contributors:    contributors,
		Identifiers:     mapIdentifiers(book.Identifiers),
		Formats:         formats,
		FileCount:       len(files),
		EditionCount:    len(editions),
		Editions:        editions,
		Cover:           mapBookCover(book.ID, primaryLocalBookCover(book.Covers)),
		CreatedAt:       formatAPITime(book.CreatedAt),
		UpdatedAt:       formatAPITime(book.UpdatedAt),
	}, nil
}

func (s *Server) buildV1Editions(ctx context.Context, bookID int64) ([]v1EditionSummary, error) {
	editions, err := s.library().ListBookEditions(ctx, bookID)
	if err != nil {
		return nil, err
	}
	allFiles, err := s.library().GetBookFiles(ctx, bookID)
	if err != nil {
		return nil, err
	}
	summaries := make([]v1EditionSummary, 0, len(editions))
	for _, edition := range editions {
		full, err := s.library().GetEdition(ctx, edition.ID)
		if err != nil {
			return nil, err
		}
		var editionFiles []library.BookFile
		for _, file := range allFiles {
			if file.EditionID == edition.ID {
				editionFiles = append(editionFiles, file)
			}
		}
		summaries = append(summaries, v1EditionSummary{
			ID:              full.ID,
			Title:           full.Title,
			Subtitle:        full.Subtitle,
			Publisher:       full.Publisher,
			PublicationDate: full.PublicationDate,
			Language:        full.Language,
			Contributors:    mapContributors(full.Contributors),
			Identifiers:     mapIdentifiers(full.Identifiers),
			Formats:         uniqueFormats(editionFiles),
			FileCount:       len(editionFiles),
		})
	}
	return summaries, nil
}

func mapSeries(series []library.BookSeries) v1SeriesSummary {
	if len(series) == 0 {
		return v1SeriesSummary{}
	}
	name := series[0].Series.Title
	var position *float64
	if series[0].Position != 0 {
		pos := series[0].Position
		position = &pos
	}
	return v1SeriesSummary{
		Name:     &name,
		Position: position,
	}
}

func mapContributors(contributors []library.Contributor) []v1ContributorSummary {
	items := make([]v1ContributorSummary, 0, len(contributors))
	for _, contributor := range contributors {
		role := ""
		if len(contributor.Roles) > 0 {
			role = string(contributor.Roles[0])
		}
		items = append(items, v1ContributorSummary{
			ID:   contributor.ID,
			Name: contributor.Name,
			Role: role,
		})
	}
	return items
}

func mapIdentifiers(identifiers []library.Identifier) []v1IdentifierSummary {
	items := make([]v1IdentifierSummary, 0, len(identifiers))
	for _, identifier := range identifiers {
		items = append(items, v1IdentifierSummary{
			Type:  identifier.Provider,
			Value: identifier.Value,
		})
	}
	return items
}

func mapPrimaryAuthor(contributor *library.Contributor) *v1PrimaryAuthorSummary {
	if contributor == nil {
		return nil
	}
	return &v1PrimaryAuthorSummary{ID: contributor.ID, Name: contributor.Name}
}

func mapBookCover(bookID int64, cover *library.Cover) v1CoverSummary {
	if cover == nil || strings.TrimSpace(cover.LocalPath) == "" {
		return v1CoverSummary{}
	}
	url := "/api/v1/books/" + strconv.FormatInt(bookID, 10) + "/cover"
	return v1CoverSummary{Available: true, URL: &url}
}

func primaryLocalBookCover(covers []library.Cover) *library.Cover {
	for _, cover := range covers {
		if strings.TrimSpace(cover.LocalPath) != "" {
			copyCover := cover
			return &copyCover
		}
	}
	return nil
}

func aggregateContributors(editions []v1EditionSummary) ([]v1ContributorSummary, *v1PrimaryAuthorSummary) {
	seen := make(map[string]struct{})
	contributors := make([]v1ContributorSummary, 0)
	var primary *v1PrimaryAuthorSummary
	for _, edition := range editions {
		for _, contributor := range edition.Contributors {
			key := fmt.Sprintf("%d:%s:%s", contributor.ID, contributor.Name, contributor.Role)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			contributors = append(contributors, contributor)
			if primary == nil && strings.EqualFold(contributor.Role, string(library.RoleAuthor)) {
				primary = &v1PrimaryAuthorSummary{ID: contributor.ID, Name: contributor.Name}
			}
		}
	}
	if primary == nil && len(contributors) > 0 {
		primary = &v1PrimaryAuthorSummary{ID: contributors[0].ID, Name: contributors[0].Name}
	}
	return contributors, primary
}

func uniqueFormats(files []library.BookFile) []string {
	seen := map[string]struct{}{}
	formats := make([]string, 0, len(files))
	for _, file := range files {
		format := strings.TrimSpace(strings.ToLower(file.Format))
		if format == "" {
			continue
		}
		if _, ok := seen[format]; ok {
			continue
		}
		seen[format] = struct{}{}
		formats = append(formats, format)
	}
	slices.Sort(formats)
	return formats
}

func formatAPITime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func parseBookMetadataPatch(fields map[string]any) (library.BookMetadataPatch, error) {
	patch := library.BookMetadataPatch{Fields: map[library.MetadataField]string{}}
	for key, raw := range fields {
		field := library.MetadataField(strings.TrimSpace(key))
		if !field.Valid() {
			return library.BookMetadataPatch{}, fmt.Errorf("unsupported metadata field %q", key)
		}
		switch typed := raw.(type) {
		case string:
			patch.Fields[field] = typed
		case []any:
			if field != library.MetadataFieldGenres {
				return library.BookMetadataPatch{}, fmt.Errorf("metadata field %q must be a string", key)
			}
			values := make([]string, 0, len(typed))
			for _, item := range typed {
				str, ok := item.(string)
				if !ok {
					return library.BookMetadataPatch{}, fmt.Errorf("metadata field %q must contain strings", key)
				}
				values = append(values, strings.TrimSpace(str))
			}
			patch.Fields[field] = strings.Join(values, ", ")
		default:
			return library.BookMetadataPatch{}, fmt.Errorf("metadata field %q has an unsupported value", key)
		}
	}
	return patch, nil
}

func mapV1BookMetadata(metadata *library.BookMetadata) map[string]any {
	fields := make(map[string]v1MetadataFieldSummary, len(metadata.Fields))
	for _, field := range library.BookMetadataFieldOrder() {
		entry, ok := metadata.Fields[field]
		if !ok {
			continue
		}
		fields[string(field)] = v1MetadataFieldSummary{
			Value:           entry.Value,
			Source:          entry.Source,
			Confidence:      string(entry.Confidence),
			ConfidenceScore: library.MetadataConfidenceScore(entry.Confidence),
			UpdatedAt:       formatAPITime(entry.UpdatedAt),
			ManualOverride:  entry.ManualOverride,
		}
	}
	return map[string]any{
		"book_id":              metadata.BookID,
		"effective_edition_id": metadata.EffectiveEditionID,
		"fields":               fields,
		"contributors":         mapV1MetadataContributors(metadata.Contributors),
		"identifiers":          mapV1MetadataIdentifiers(metadata.Identifiers),
	}
}

func mapV1BookProvenance(provenance *library.BookMetadataProvenance) map[string]any {
	fields := make(map[string][]v1MetadataEvidenceSummary, len(provenance.Fields))
	for _, field := range library.BookMetadataFieldOrder() {
		entries, ok := provenance.Fields[field]
		if !ok {
			continue
		}
		mapped := make([]v1MetadataEvidenceSummary, 0, len(entries))
		for _, entry := range entries {
			mapped = append(mapped, v1MetadataEvidenceSummary{
				Value:           entry.Value,
				Source:          entry.Source,
				Confidence:      string(entry.Confidence),
				ConfidenceScore: library.MetadataConfidenceScore(entry.Confidence),
				UpdatedAt:       formatAPITime(entry.UpdatedAt),
				ManualOverride:  entry.ManualOverride,
				Selected:        entry.Selected,
			})
		}
		fields[string(field)] = mapped
	}
	return map[string]any{
		"book_id":              provenance.BookID,
		"effective_edition_id": provenance.EffectiveEditionID,
		"fields":               fields,
		"contributors":         mapV1MetadataContributors(provenance.Contributors),
		"identifiers":          mapV1MetadataIdentifiers(provenance.Identifiers),
	}
}

func mapV1MetadataContributors(items []library.MetadataContributor) []v1MetadataContributorSummary {
	result := make([]v1MetadataContributorSummary, 0, len(items))
	for _, item := range items {
		result = append(result, v1MetadataContributorSummary{
			Name:            item.Name,
			Role:            string(item.Role),
			Source:          item.Source,
			Confidence:      string(item.Confidence),
			ConfidenceScore: library.MetadataConfidenceScore(item.Confidence),
			UpdatedAt:       formatAPITime(item.UpdatedAt),
			ManualOverride:  item.ManualOverride,
		})
	}
	return result
}

func mapV1MetadataIdentifiers(items []library.MetadataIdentifier) []v1MetadataIdentifierSummary {
	result := make([]v1MetadataIdentifierSummary, 0, len(items))
	for _, item := range items {
		result = append(result, v1MetadataIdentifierSummary{
			Type:            item.Type,
			Value:           item.Value,
			Source:          item.Source,
			Confidence:      string(item.Confidence),
			ConfidenceScore: library.MetadataConfidenceScore(item.Confidence),
			UpdatedAt:       formatAPITime(item.UpdatedAt),
			ManualOverride:  item.ManualOverride,
		})
	}
	return result
}
