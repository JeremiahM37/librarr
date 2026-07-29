package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/metadata"
	"github.com/jamie75/librarr/internal/organize"
)

const (
	metadataProposalTTL       = 30 * time.Minute
	maxOnlineCoverBytes int64 = 8 << 20
)

type metadataProposal struct {
	Token       string                  `json:"token"`
	BookID      int64                   `json:"book_id"`
	EditionID   int64                   `json:"edition_id,omitempty"`
	Source      string                  `json:"source"`
	Provider    string                  `json:"provider,omitempty"`
	Confidence  string                  `json:"confidence"`
	Score       int                     `json:"score"`
	Reason      string                  `json:"reason"`
	Fields      map[string]string       `json:"fields"`
	Author      string                  `json:"author,omitempty"`
	Identifiers []v1IdentifierSummary   `json:"identifiers,omitempty"`
	Subjects    []string                `json:"subjects,omitempty"`
	Series      *metadataSeriesProposal `json:"series,omitempty"`
	Cover       *metadataCoverProposal  `json:"cover,omitempty"`
	FileID      int64                   `json:"file_id,omitempty"`
	FileFormat  string                  `json:"file_format,omitempty"`
	FileLabel   string                  `json:"file_label,omitempty"`
	CreatedAt   time.Time               `json:"-"`
}

type metadataSeriesProposal struct {
	Name     string `json:"name"`
	Position string `json:"position,omitempty"`
}

type metadataCoverProposal struct {
	Available bool   `json:"available"`
	Source    string `json:"source"`
	URL       string `json:"url,omitempty"`
	MimeType  string `json:"mime_type,omitempty"`
	Width     int    `json:"width,omitempty"`
	Height    int    `json:"height,omitempty"`
}

type metadataApplyRequest struct {
	Token          string   `json:"token"`
	SelectedFields []string `json:"selected_fields"`
}

func (s *Server) handleV1BookMetadataExtract(w http.ResponseWriter, r *http.Request) {
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
	files, err := s.library().GetBookFiles(r.Context(), bookID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load book files", err)
		return
	}
	file := preferredMetadataFile(files)
	if file == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "No supported local ebook file is attached to this book"})
		return
	}
	proposal, err := s.extractMetadataProposal(r.Context(), *book, *file)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"success": false, "error": err.Error()})
		return
	}
	s.storeMetadataProposal(proposal)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "proposal": proposal})
}

func (s *Server) handleV1BookMetadataMatches(w http.ResponseWriter, r *http.Request) {
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
	metadataDoc, _ := s.library().GetBookMetadata(r.Context(), bookID)
	query := metadata.MatchQuery{
		Title:  currentMetadataValue(metadataDoc, library.MetadataFieldTitle, book.Title),
		Author: firstMetadataAuthor(metadataDoc, book.Contributors),
		ISBN:   firstMetadataISBN(metadataDoc, book.Identifiers),
		Limit:  5,
	}
	client := s.metadataClient
	if client == nil {
		client = metadata.NewClient(&http.Client{Timeout: 15 * time.Second})
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	matches, err := client.SearchMatches(ctx, query)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": "Metadata provider failed: " + sanitizeProviderError(err)})
		return
	}
	proposals := make([]metadataProposal, 0, len(matches))
	for _, match := range matches {
		proposal := proposalFromOpenLibraryMatch(book.ID, metadataDoc, match)
		s.storeMetadataProposal(proposal)
		proposals = append(proposals, proposal)
	}
	slices.SortStableFunc(proposals, func(a, b metadataProposal) int { return b.Score - a.Score })
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "candidates": proposals})
}

func (s *Server) handleV1BookMetadataApply(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNormalizedReadAPI(w) {
		return
	}
	bookID, ok := parseIDPathValue(w, r, "id", "Invalid book ID")
	if !ok {
		return
	}
	var req metadataApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request body"})
		return
	}
	proposal, ok := s.loadMetadataProposal(req.Token)
	if !ok || proposal.BookID != bookID {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Metadata proposal expired or not found"})
		return
	}
	selected := cleanSelectedMetadataFields(req.SelectedFields)
	if len(selected) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Select at least one metadata field to apply"})
		return
	}
	if err := s.applyMetadataProposal(r.Context(), proposal, selected); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to apply metadata proposal", err)
		return
	}
	metadataDoc, err := s.library().GetBookMetadata(r.Context(), bookID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to reload book metadata", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "metadata": mapV1BookMetadata(metadataDoc)})
}

func (s *Server) extractMetadataProposal(ctx context.Context, book library.Book, file library.BookFile) (metadataProposal, error) {
	if !strings.EqualFold(file.Format, "epub") && !strings.EqualFold(filepath.Ext(file.Path), ".epub") {
		return metadataProposal{}, fmt.Errorf("%s metadata extraction is not supported yet", strings.ToUpper(firstNonEmpty(file.Format, filepath.Ext(file.Path), "file")))
	}
	path := strings.TrimSpace(file.Path)
	if path == "" {
		return metadataProposal{}, fmt.Errorf("attached file has no managed path")
	}
	meta, err := organize.ReadEPUBPackageMetadata(path)
	if err != nil {
		return metadataProposal{}, fmt.Errorf("EPUB metadata could not be read: %w", err)
	}
	fields := fieldsFromEbookMetadata(meta)
	if len(fields) == 0 && meta.Author == "" && meta.ISBN == "" && len(meta.Subjects) == 0 && meta.Series == "" {
		return metadataProposal{}, fmt.Errorf("EPUB contains no supported metadata fields")
	}
	proposal := metadataProposal{
		Token:       newMetadataProposalToken(),
		BookID:      book.ID,
		EditionID:   file.EditionID,
		Source:      "epub",
		Provider:    "epub",
		Confidence:  string(library.ConfidenceHigh),
		Score:       90,
		Reason:      "Embedded EPUB metadata",
		Fields:      fields,
		Author:      meta.Author,
		Identifiers: identifiersFromMetadata(meta),
		Subjects:    meta.Subjects,
		FileID:      file.ID,
		FileFormat:  strings.ToLower(firstNonEmpty(file.Format, "epub")),
		FileLabel:   filepath.Base(path),
		CreatedAt:   time.Now().UTC(),
	}
	if meta.Series != "" {
		proposal.Series = &metadataSeriesProposal{Name: meta.Series, Position: meta.SeriesIndex}
	}
	if cover, err := organize.ExtractEmbeddedCover(path); err == nil && cover != nil {
		proposal.Cover = &metadataCoverProposal{Available: true, Source: "embedded_epub", MimeType: cover.MimeType, Width: cover.Width, Height: cover.Height}
	} else if err != nil {
		slog.Debug("metadata extract cover unavailable", "book_id", book.ID, "file_id", file.ID, "error", err)
	}
	_ = ctx
	return proposal, nil
}

func fieldsFromEbookMetadata(meta *organize.EbookMetadata) map[string]string {
	fields := map[string]string{}
	if meta == nil {
		return fields
	}
	add := func(field library.MetadataField, value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			fields[string(field)] = value
		}
	}
	add(library.MetadataFieldTitle, meta.Title)
	add(library.MetadataFieldEditionTitle, meta.Title)
	add(library.MetadataFieldSubtitle, meta.Subtitle)
	add(library.MetadataFieldDescription, meta.Description)
	add(library.MetadataFieldLanguage, meta.Language)
	add(library.MetadataFieldPublicationDate, meta.PublicationDate)
	add(library.MetadataFieldPublisher, meta.Publisher)
	if len(meta.Subjects) > 0 {
		add(library.MetadataFieldGenres, strings.Join(meta.Subjects, ", "))
	}
	return fields
}

func identifiersFromMetadata(meta *organize.EbookMetadata) []v1IdentifierSummary {
	if meta == nil {
		return nil
	}
	seen := map[string]struct{}{}
	items := make([]v1IdentifierSummary, 0)
	add := func(kind, value string) {
		kind = strings.TrimSpace(strings.ToLower(kind))
		value = strings.TrimSpace(value)
		if kind == "" || value == "" {
			return
		}
		key := kind + "\x00" + value
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		items = append(items, v1IdentifierSummary{Type: kind, Value: value})
	}
	add("isbn", meta.ISBN)
	for kind, value := range meta.Identifiers {
		add(kind, value)
	}
	return items
}

func proposalFromOpenLibraryMatch(bookID int64, current *library.BookMetadata, match metadata.BookMetadata) metadataProposal {
	fields := map[string]string{}
	add := func(field library.MetadataField, value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			fields[string(field)] = sanitizeMetadataDescription(value)
		}
	}
	add(library.MetadataFieldTitle, match.Title)
	add(library.MetadataFieldEditionTitle, match.Title)
	add(library.MetadataFieldDescription, match.Description)
	add(library.MetadataFieldLanguage, match.Language)
	add(library.MetadataFieldPublicationDate, match.Year)
	add(library.MetadataFieldPublisher, match.Publisher)
	if len(match.Subjects) > 0 {
		add(library.MetadataFieldGenres, strings.Join(uniqueStrings(match.Subjects), ", "))
	}
	confidence := confidenceFromScore(match.Confidence)
	proposal := metadataProposal{
		Token:      newMetadataProposalToken(),
		BookID:     bookID,
		EditionID:  currentEffectiveEditionID(current),
		Source:     "openlibrary",
		Provider:   "openlibrary",
		Confidence: string(confidence),
		Score:      match.Confidence,
		Reason:     firstNonEmpty(match.MatchReason, "Open Library candidate"),
		Fields:     fields,
		Author:     match.Author,
		Subjects:   uniqueStrings(match.Subjects),
		CreatedAt:  time.Now().UTC(),
	}
	if match.ISBN != "" {
		proposal.Identifiers = append(proposal.Identifiers, v1IdentifierSummary{Type: "isbn", Value: match.ISBN})
	}
	if match.OLID != "" {
		proposal.Identifiers = append(proposal.Identifiers, v1IdentifierSummary{Type: "openlibrary", Value: strings.TrimPrefix(match.OLID, "/")})
	}
	if match.Series != "" {
		proposal.Series = &metadataSeriesProposal{Name: match.Series}
	}
	if validOpenLibraryCoverURL(match.CoverURL) {
		proposal.Cover = &metadataCoverProposal{Available: true, Source: "openlibrary", URL: match.CoverURL}
	}
	return proposal
}

func (s *Server) applyMetadataProposal(ctx context.Context, proposal metadataProposal, selected map[string]struct{}) error {
	return s.library().WithinTransaction(ctx, func(txCtx context.Context) error {
		fields := map[library.MetadataField]string{}
		for raw, value := range proposal.Fields {
			if _, ok := selected[raw]; !ok {
				continue
			}
			field := library.MetadataField(raw)
			if field.Valid() {
				fields[field] = value
			}
		}
		if len(fields) > 0 {
			if _, err := s.library().ApplyBookMetadataSource(txCtx, library.MetadataUpdate{
				BookID:     proposal.BookID,
				Source:     proposal.Source,
				Confidence: library.Confidence(proposal.Confidence),
				Fields:     fields,
			}); err != nil {
				return err
			}
		}
		if _, ok := selected["author"]; ok && strings.TrimSpace(proposal.Author) != "" && proposal.EditionID != 0 {
			if err := s.replaceEditionAuthor(txCtx, proposal.EditionID, proposal.Author); err != nil {
				return err
			}
		}
		if _, ok := selected["identifiers"]; ok {
			if err := s.applyProposalIdentifiers(txCtx, proposal); err != nil {
				return err
			}
		}
		if _, ok := selected["series"]; ok && proposal.Series != nil && strings.TrimSpace(proposal.Series.Name) != "" {
			position := parseFloat(proposal.Series.Position)
			if err := s.library().AttachBookToSeries(txCtx, proposal.BookID, library.BookSeries{
				Series:          library.Series{Title: proposal.Series.Name},
				Position:        position,
				DisplayPosition: strings.TrimSpace(proposal.Series.Position),
			}); err != nil {
				return err
			}
		}
		if _, ok := selected["cover"]; ok && proposal.Cover != nil && proposal.Cover.Available {
			if err := s.applyProposalCover(txCtx, proposal); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Server) replaceEditionAuthor(ctx context.Context, editionID int64, author string) error {
	contributors, err := s.library().GetEditionContributors(ctx, editionID)
	if err != nil {
		return err
	}
	for _, contributor := range contributors {
		for _, role := range contributor.Roles {
			if role == library.RoleAuthor {
				if err := s.library().DetachContributor(ctx, editionID, contributor.ID, library.RoleAuthor); err != nil {
					return err
				}
			}
		}
	}
	return s.library().AttachContributor(ctx, editionID, library.Contributor{Name: author, Roles: []library.ContributorRole{library.RoleAuthor}})
}

func (s *Server) applyProposalIdentifiers(ctx context.Context, proposal metadataProposal) error {
	for _, identifier := range proposal.Identifiers {
		provider := strings.TrimSpace(strings.ToLower(identifier.Type))
		value := strings.TrimSpace(identifier.Value)
		if provider == "" || value == "" {
			continue
		}
		scope := library.IdentifierScopeBook
		source := fmt.Sprint(proposal.BookID)
		if provider == "isbn" || strings.HasPrefix(provider, "isbn") {
			scope = library.IdentifierScopeEdition
			source = fmt.Sprint(proposal.EditionID)
		}
		if source == "0" {
			scope = library.IdentifierScopeBook
			source = fmt.Sprint(proposal.BookID)
		}
		if _, err := s.library().AddIdentifier(ctx, library.Identifier{Scope: scope, Provider: provider, Value: value, Source: source}); err != nil && !errors.Is(err, library.ErrDuplicateBook) {
			return err
		}
	}
	return nil
}

func (s *Server) applyProposalCover(ctx context.Context, proposal metadataProposal) error {
	if proposal.Cover == nil {
		return nil
	}
	if existing, err := s.library().GetPrimaryCover(ctx, proposal.BookID); err == nil && existing != nil && strings.TrimSpace(existing.LocalPath) != "" {
		if info, statErr := os.Stat(existing.LocalPath); statErr == nil && !info.IsDir() && info.Size() > 0 {
			return nil
		}
	}
	switch proposal.Cover.Source {
	case "embedded_epub":
		file, err := s.library().GetFile(ctx, proposal.FileID)
		if err != nil {
			return err
		}
		_, err = s.coverCache.AttachBookCover(ctx, s.library(), proposal.BookID, file.Path)
		return err
	case "openlibrary":
		cover, err := fetchOnlineCover(ctx, proposal.Cover.URL)
		if err != nil {
			return err
		}
		if s.coverCache == nil {
			return fmt.Errorf("cover cache is not configured")
		}
		filename := safeMetadataCoverFilename(proposal.BookID, proposal.Token, cover.Ext)
		targetPath := filepath.Join(s.coverCache.Dir(), "books", filename)
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(targetPath, cover.Data, 0644); err != nil {
			return err
		}
		_, err = s.library().AttachCover(ctx, library.Cover{
			BookID:    proposal.BookID,
			Source:    "openlibrary",
			SourceURL: proposal.Cover.URL,
			LocalPath: targetPath,
			MimeType:  cover.MimeType,
			Width:     cover.Width,
			Height:    cover.Height,
			IsPrimary: true,
		})
		return err
	default:
		return fmt.Errorf("unsupported cover source")
	}
}

type downloadedCover struct {
	Data     []byte
	MimeType string
	Ext      string
	Width    int
	Height   int
}

func fetchOnlineCover(ctx context.Context, rawURL string) (*downloadedCover, error) {
	if !validOpenLibraryCoverURL(rawURL) {
		return nil, fmt.Errorf("cover URL is not an approved provider URL")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("cover provider returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOnlineCoverBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxOnlineCoverBytes {
		return nil, fmt.Errorf("cover image is too large")
	}
	mimeType := http.DetectContentType(data)
	if !strings.HasPrefix(mimeType, "image/") {
		return nil, fmt.Errorf("cover response is not an image")
	}
	width, height := imageSize(data)
	ext := ".jpg"
	if mimeType == "image/png" {
		ext = ".png"
	}
	return &downloadedCover{Data: data, MimeType: mimeType, Ext: ext, Width: width, Height: height}, nil
}

func preferredMetadataFile(files []library.BookFile) *library.BookFile {
	for _, file := range files {
		if strings.EqualFold(file.Format, "epub") || strings.EqualFold(filepath.Ext(file.Path), ".epub") {
			fileCopy := file
			return &fileCopy
		}
	}
	for _, file := range files {
		switch strings.ToLower(file.Format) {
		case "pdf", "mobi", "azw3":
			fileCopy := file
			return &fileCopy
		}
	}
	return nil
}

func (s *Server) storeMetadataProposal(proposal metadataProposal) {
	s.metadataProposalMu.Lock()
	defer s.metadataProposalMu.Unlock()
	if s.metadataProposals == nil {
		s.metadataProposals = map[string]metadataProposal{}
	}
	now := time.Now().UTC()
	for token, cached := range s.metadataProposals {
		if now.Sub(cached.CreatedAt) > metadataProposalTTL {
			delete(s.metadataProposals, token)
		}
	}
	if proposal.CreatedAt.IsZero() {
		proposal.CreatedAt = now
	}
	s.metadataProposals[proposal.Token] = proposal
}

func (s *Server) loadMetadataProposal(token string) (metadataProposal, bool) {
	s.metadataProposalMu.Lock()
	defer s.metadataProposalMu.Unlock()
	proposal, ok := s.metadataProposals[strings.TrimSpace(token)]
	if !ok || time.Since(proposal.CreatedAt) > metadataProposalTTL {
		return metadataProposal{}, false
	}
	return proposal, true
}

func cleanSelectedMetadataFields(fields []string) map[string]struct{} {
	allowed := map[string]struct{}{
		"title": {}, "edition_title": {}, "subtitle": {}, "description": {}, "genres": {}, "language": {},
		"publication_date": {}, "publisher": {}, "author": {}, "identifiers": {}, "subjects": {}, "series": {}, "cover": {},
	}
	selected := map[string]struct{}{}
	for _, field := range fields {
		field = strings.TrimSpace(strings.ToLower(field))
		if _, ok := allowed[field]; ok {
			if field == "subjects" {
				field = "genres"
			}
			selected[field] = struct{}{}
		}
	}
	return selected
}

func currentMetadataValue(metadata *library.BookMetadata, field library.MetadataField, fallback string) string {
	if metadata != nil {
		if entry, ok := metadata.Fields[field]; ok && strings.TrimSpace(entry.Value) != "" {
			return entry.Value
		}
	}
	return fallback
}

func firstMetadataAuthor(metadata *library.BookMetadata, contributors []library.Contributor) string {
	if metadata != nil {
		for _, c := range metadata.Contributors {
			if c.Role == library.RoleAuthor && strings.TrimSpace(c.Name) != "" {
				return c.Name
			}
		}
	}
	for _, c := range contributors {
		for _, role := range c.Roles {
			if role == library.RoleAuthor && strings.TrimSpace(c.Name) != "" {
				return c.Name
			}
		}
	}
	return ""
}

func firstMetadataISBN(metadata *library.BookMetadata, identifiers []library.Identifier) string {
	if metadata != nil {
		for _, id := range metadata.Identifiers {
			if strings.Contains(strings.ToLower(id.Type), "isbn") {
				return id.Value
			}
		}
	}
	for _, id := range identifiers {
		if strings.Contains(strings.ToLower(id.Provider), "isbn") {
			return id.Value
		}
	}
	return ""
}

func currentEffectiveEditionID(metadata *library.BookMetadata) int64 {
	if metadata == nil {
		return 0
	}
	return metadata.EffectiveEditionID
}

func confidenceFromScore(score int) library.Confidence {
	switch {
	case score >= 95:
		return library.ConfidenceExact
	case score >= 85:
		return library.ConfidenceHigh
	case score >= 60:
		return library.ConfidenceMedium
	case score > 0:
		return library.ConfidenceLow
	default:
		return library.ConfidenceNone
	}
}

func newMetadataProposalToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func sanitizeProviderError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 180 {
		msg = msg[:180]
	}
	return msg
}

func sanitizeMetadataDescription(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\x00", "")
	return html.UnescapeString(value)
}

func validOpenLibraryCoverURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u == nil {
		return false
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == "covers.openlibrary.org" || host == "localhost" || host == "127.0.0.1"
}

func imageSize(data []byte) (int, int) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0
	}
	return cfg.Width, cfg.Height
}

func safeMetadataCoverFilename(bookID int64, token, ext string) string {
	if ext == "" {
		ext = ".jpg"
	}
	return fmt.Sprintf("metadata-%d-%s%s", bookID, strings.TrimSpace(token), ext)
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}

func parseFloat(value string) float64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	var f float64
	_, _ = fmt.Sscanf(value, "%f", &f)
	return f
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func init() {
	image.RegisterFormat("jpeg", "\xff\xd8", jpeg.Decode, jpeg.DecodeConfig)
}
