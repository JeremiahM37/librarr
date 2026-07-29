package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/download"
	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/netutil"
	wantedmeta "github.com/jamie75/librarr/internal/wanted"
)

type wantedListResponse struct {
	Items  []models.WantedBook `json:"items"`
	Counts map[string]int      `json:"counts"`
}

type wantedHistoryResponse struct {
	Items []models.WantedSearchHistory `json:"items"`
}

type wantedReleasesResponse struct {
	Items []models.WantedRelease `json:"items"`
	Total int                    `json:"total"`
}

type wantedReleaseDownloadResponse struct {
	Success       bool               `json:"success"`
	WantedID      int64              `json:"wanted_id"`
	ReleaseID     int64              `json:"release_id"`
	DownloadJobID string             `json:"download_job_id,omitempty"`
	DownloadHash  string             `json:"download_hash,omitempty"`
	Status        string             `json:"status"`
	Message       string             `json:"message,omitempty"`
	Warning       string             `json:"warning,omitempty"`
	Item          *models.WantedBook `json:"item,omitempty"`
}

type wantedCreateRequest struct {
	Title              string   `json:"title"`
	Author             string   `json:"author"`
	ISBN               string   `json:"isbn"`
	ASIN               string   `json:"asin"`
	Series             string   `json:"series"`
	Publisher          string   `json:"publisher"`
	Language           string   `json:"language"`
	CoverURL           string   `json:"cover_url"`
	Description        string   `json:"description"`
	Source             string   `json:"source"`
	MediaType          string   `json:"media_type"`
	Format             string   `json:"format"`
	PreferredFormat    string   `json:"preferred_format"`
	OriginSource       string   `json:"origin_source"`
	OriginReleaseTitle string   `json:"origin_release_title"`
	OriginIndexer      string   `json:"origin_indexer"`
	Indexer            string   `json:"indexer"`
	SourceID           string   `json:"source_id"`
	GUID               string   `json:"guid"`
	DownloadURL        string   `json:"download_url"`
	MagnetURL          string   `json:"magnet_url"`
	URL                string   `json:"url"`
	InfoHash           string   `json:"info_hash"`
	DownloadProtocol   string   `json:"download_protocol"`
	Size               int64    `json:"size"`
	SizeHuman          string   `json:"size_human"`
	Seeders            int      `json:"seeders"`
	Leechers           int      `json:"leechers"`
	Grabs              int      `json:"grabs"`
	PublishDate        string   `json:"publish_date"`
	Categories         []string `json:"categories"`
	Score              float64  `json:"score"`
	Monitored          *bool    `json:"monitored"`
	Status             string   `json:"status"`
}

type wantedPatchRequest struct {
	Monitored *bool   `json:"monitored"`
	Status    *string `json:"status"`
}

func (s *Server) handleV1WantedList(w http.ResponseWriter, r *http.Request) {
	items, err := s.db.ListWantedBooks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to load wanted books"})
		return
	}
	items = s.reconcileWantedImportedStatus(r.Context(), items)

	counts := map[string]int{
		"total":       len(items),
		"wanted":      0,
		"ignored":     0,
		"downloaded":  0,
		"downloading": 0,
		"found":       0,
		"missing":     0,
		"searching":   0,
		"imported":    0,
		"monitored":   0,
	}
	for _, item := range items {
		counts[strings.TrimSpace(strings.ToLower(item.Status))]++
		if item.Monitored {
			counts["monitored"]++
		}
	}
	writeJSON(w, http.StatusOK, wantedListResponse{Items: items, Counts: counts})
}

func (s *Server) reconcileWantedImportedStatus(ctx context.Context, items []models.WantedBook) []models.WantedBook {
	if s == nil || s.db == nil || len(items) == 0 || s.libraryService == nil {
		return items
	}
	for i := range items {
		status := strings.TrimSpace(strings.ToLower(items[i].Status))
		if status == "ignored" || status == "downloaded" || status == "imported" {
			continue
		}
		matched, err := s.wantedBookInLibrary(ctx, items[i])
		if err != nil {
			slog.Debug("wanted library reconciliation skipped", "wanted_id", items[i].ID, "error", err)
			continue
		}
		if !matched {
			continue
		}
		imported := "imported"
		updated, err := s.db.UpdateWantedBook(items[i].ID, nil, &imported)
		if err != nil {
			slog.Warn("wanted library reconciliation failed", "wanted_id", items[i].ID, "error", err)
			items[i].Status = imported
			continue
		}
		items[i] = *updated
	}
	return items
}

func (s *Server) wantedBookInLibrary(ctx context.Context, item models.WantedBook) (bool, error) {
	titleKey := library.TitleMatchKey(item.Title)
	authorKey := library.ContributorMatchKey(item.Author)
	if titleKey == "" {
		return false, nil
	}
	mediaType := library.MediaType(strings.TrimSpace(strings.ToLower(item.MediaType)))
	if mediaType == "" {
		mediaType = library.MediaTypeEbook
	}
	queries := []string{item.Title, leadingWords(titleKey, 3), item.Author}
	seen := map[string]struct{}{}
	for _, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		if _, ok := seen[strings.ToLower(query)]; ok {
			continue
		}
		seen[strings.ToLower(query)] = struct{}{}
		books, err := s.libraryService.ListBookReadModels(ctx, library.ListBooksQuery{MediaType: mediaType, Search: query, Limit: 500})
		if err != nil {
			return false, err
		}
		for _, book := range books {
			if library.TitleMatchKey(book.Book.Title) != titleKey {
				continue
			}
			if authorKey != "" && library.ContributorMatchKey(primaryReadModelAuthor(book)) != authorKey {
				continue
			}
			return true, nil
		}
	}
	return false, nil
}

func (s *Server) handleV1WantedCreate(w http.ResponseWriter, r *http.Request) {
	var req wantedCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request body"})
		return
	}
	if strings.TrimSpace(req.Title) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Title is required"})
		return
	}

	monitored := true
	if req.Monitored != nil {
		monitored = *req.Monitored
	}
	status := strings.TrimSpace(strings.ToLower(req.Status))
	if status == "" {
		status = "wanted"
	}
	if !isValidWantedStatus(status) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted status"})
		return
	}

	book := models.WantedBook{
		Title:              req.Title,
		Author:             req.Author,
		ISBN:               req.ISBN,
		ASIN:               req.ASIN,
		Series:             req.Series,
		Publisher:          req.Publisher,
		Language:           req.Language,
		CoverURL:           req.CoverURL,
		Description:        req.Description,
		Source:             req.Source,
		MediaType:          req.MediaType,
		PreferredFormat:    wantedFirstNonBlank(req.PreferredFormat, req.Format),
		OriginSource:       wantedFirstNonBlank(req.OriginSource, req.Source),
		OriginReleaseTitle: req.OriginReleaseTitle,
		OriginIndexer:      wantedFirstNonBlank(req.OriginIndexer, req.Indexer),
		SourceID:           wantedFirstNonBlank(req.SourceID, req.GUID),
		Monitored:          monitored,
		Status:             status,
	}
	if strings.TrimSpace(book.OriginReleaseTitle) == "" && looksWantedReleaseSource(book.Source, book.OriginSource) {
		book.OriginReleaseTitle = book.Title
	}
	normalization := wantedmeta.NormalizeBook(book)
	if strings.TrimSpace(normalization.Normalized.Title) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Title is required"})
		return
	}
	if conflicts, err := s.wantedCanonicalConflict(0, normalization.Normalized); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to validate wanted book"})
		return
	} else if conflicts {
		writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": "Wanted book already exists"})
		return
	}

	item, err := s.db.CreateWantedBook(normalization.Normalized)
	if err != nil {
		if errors.Is(err, db.ErrWantedBookExists) {
			writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": "Wanted book already exists"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to create wanted book"})
		return
	}
	item, err = s.seedWantedOriginRelease(item, req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to store origin release"})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"success": true, "item": item})
}

func (s *Server) seedWantedOriginRelease(item *models.WantedBook, req wantedCreateRequest) (*models.WantedBook, error) {
	if item == nil {
		return item, nil
	}
	release := wantedReleaseFromCreateRequest(item.ID, req)
	if strings.TrimSpace(release.Title) == "" || !wantedReleaseDownloadAvailable(release) {
		return item, nil
	}
	if release.Score <= 0 {
		release.Score = 100
	}
	if release.SearchTime.IsZero() {
		release.SearchTime = time.Now().UTC()
	}
	if err := s.db.ReplaceWantedReleases(item.ID, []models.WantedRelease{release}); err != nil {
		return nil, err
	}
	return s.db.MarkWantedOriginFound(item.ID, release.Title, release.Score)
}

func wantedReleaseFromCreateRequest(wantedBookID int64, req wantedCreateRequest) models.WantedRelease {
	protocol := strings.TrimSpace(strings.ToLower(req.DownloadProtocol))
	if protocol == "" {
		protocol = strings.TrimSpace(strings.ToLower(req.Source))
	}
	if protocol == "prowlarr" {
		protocol = "torrent"
	}
	if protocol == "" {
		protocol = "torrent"
	}
	format := strings.TrimSpace(strings.ToLower(wantedFirstNonBlank(req.PreferredFormat, req.Format)))
	language := strings.TrimSpace(strings.ToLower(req.Language))
	return models.WantedRelease{
		WantedBookID: wantedBookID,
		Title:        wantedFirstNonBlank(req.OriginReleaseTitle, req.Title),
		GUID:         wantedFirstNonBlank(req.GUID, req.InfoHash, req.SourceID),
		Indexer:      wantedFirstNonBlank(req.OriginIndexer, req.Indexer),
		Protocol:     protocol,
		PublishDate:  strings.TrimSpace(req.PublishDate),
		Size:         req.Size,
		SizeHuman:    strings.TrimSpace(req.SizeHuman),
		Seeders:      req.Seeders,
		Leechers:     req.Leechers,
		Grabs:        req.Grabs,
		Language:     language,
		Format:       format,
		DownloadURL:  wantedFirstNonBlank(req.DownloadURL, req.MagnetURL, req.URL),
		Categories:   append([]string(nil), req.Categories...),
		Score:        req.Score,
		SearchQuery:  strings.TrimSpace(req.Title),
		SearchTime:   time.Now().UTC(),
	}
}

func (s *Server) handleV1WantedDelete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted id"})
		return
	}
	if err := s.db.DeleteWantedBook(id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Wanted book not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to delete wanted book"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleV1WantedPatch(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted id"})
		return
	}

	var req wantedPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request body"})
		return
	}
	if req.Monitored == nil && req.Status == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "No wanted fields provided"})
		return
	}
	if req.Status != nil && !isValidWantedStatus(strings.TrimSpace(strings.ToLower(*req.Status))) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted status"})
		return
	}

	item, err := s.db.UpdateWantedBook(id, req.Monitored, req.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Wanted book not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to update wanted book"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "item": item})
}

func looksWantedReleaseSource(values ...string) bool {
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if strings.Contains(value, "torrent") || strings.Contains(value, "prowlarr") || strings.Contains(value, "release") {
			return true
		}
	}
	return false
}

func wantedFirstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Server) wantedCanonicalConflict(ignoreID int64, candidate models.WantedBook) (bool, error) {
	items, err := s.db.ListWantedBooks()
	if err != nil {
		return false, err
	}
	candidateISBN := strings.ToLower(strings.TrimSpace(candidate.ISBN))
	candidateASIN := strings.ToLower(strings.TrimSpace(candidate.ASIN))
	candidateTitle := library.TitleMatchKey(candidate.Title)
	candidateAuthor := library.ContributorMatchKey(candidate.Author)
	candidateMedia := strings.ToLower(strings.TrimSpace(candidate.MediaType))
	if candidateMedia == "" {
		candidateMedia = "ebook"
	}
	for _, item := range items {
		if item.ID == ignoreID {
			continue
		}
		itemMedia := strings.ToLower(strings.TrimSpace(item.MediaType))
		if itemMedia == "" {
			itemMedia = "ebook"
		}
		if itemMedia != candidateMedia {
			continue
		}
		if candidateISBN != "" && strings.EqualFold(strings.TrimSpace(item.ISBN), candidateISBN) {
			return true, nil
		}
		if candidateASIN != "" && strings.EqualFold(strings.TrimSpace(item.ASIN), candidateASIN) {
			return true, nil
		}
		if candidateTitle != "" && candidateTitle == library.TitleMatchKey(item.Title) &&
			candidateAuthor != "" && candidateAuthor == library.ContributorMatchKey(item.Author) {
			return true, nil
		}
	}
	return false, nil
}

func isValidWantedStatus(status string) bool {
	switch status {
	case "wanted", "searching", "found", "missing", "downloading", "downloaded", "ignored", "imported":
		return true
	default:
		return false
	}
}

func (s *Server) handleV1WantedHistory(w http.ResponseWriter, r *http.Request) {
	items, err := s.db.ListWantedSearchHistory(100)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to load wanted search history"})
		return
	}
	writeJSON(w, http.StatusOK, wantedHistoryResponse{Items: items})
}

func (s *Server) handleV1WantedReleases(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted id"})
		return
	}
	item, err := s.db.GetWantedBook(id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Wanted book not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to load wanted book"})
		return
	}
	items, err := s.db.ListWantedReleases(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to load wanted releases"})
		return
	}
	for i := range items {
		items[i].DownloadAvailable = wantedReleaseDownloadAvailable(items[i])
		items[i].Selected = item.SelectedReleaseID > 0 && item.SelectedReleaseID == items[i].ID
	}
	writeJSON(w, http.StatusOK, wantedReleasesResponse{Items: items, Total: len(items)})
}

func (s *Server) handleV1WantedReleaseDownload(w http.ResponseWriter, r *http.Request) {
	wantedID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || wantedID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted id"})
		return
	}
	releaseID, err := strconv.ParseInt(r.PathValue("release_id"), 10, 64)
	if err != nil || releaseID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid release id"})
		return
	}
	if s.downloadMgr == nil || s.cfg == nil || !s.cfg.HasTorrentClient() {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"success": false, "error": "No torrent download client configured"})
		return
	}

	item, err := s.db.GetWantedBook(wantedID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Wanted book not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to load wanted book"})
		return
	}
	switch strings.TrimSpace(strings.ToLower(item.Status)) {
	case "downloading", "downloaded", "imported":
		writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": "Wanted book is already downloading or complete"})
		return
	}

	release, err := s.db.GetWantedRelease(wantedID, releaseID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "Wanted release not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Failed to load wanted release"})
		return
	}
	if !wantedReleaseDownloadAvailable(*release) {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"success": false, "error": "Release does not have a supported torrent or magnet acquisition URL"})
		return
	}

	clientName := s.cfg.ActiveTorrentClient()
	if clientName == "" {
		clientName = "torrent"
	}
	logAttrs := []any{
		"wanted_id", wantedID,
		"release_id", releaseID,
		"title", netutil.SanitizeLogValue(release.Title),
		"indexer", netutil.SanitizeLogValue(release.Indexer),
		"protocol", netutil.SanitizeLogValue(release.Protocol),
		"download_client", clientName,
	}
	slog.Info("submitting wanted release to torrent client", logAttrs...)

	warning, err := s.submitWantedReleaseToTorrent(r.Context(), *item, *release)
	if err != nil {
		_, _ = s.db.MarkWantedDownloadFailure(wantedID, errString(err))
		slog.Warn("wanted release torrent submission failed", append(logAttrs, "error", errString(err))...)
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": "Failed to send release to download client: " + errString(err)})
		return
	}

	downloadHash := wantedReleaseDownloadHash(*release)
	updated, err := s.db.MarkWantedDownloading(wantedID, releaseID, release.Title, clientName, downloadHash, time.Now().UTC())
	if err != nil {
		slog.Warn("wanted release submitted but status update failed", append(logAttrs, "error", errString(err))...)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "Release was sent, but Wanted status could not be updated"})
		return
	}
	slog.Info("wanted release sent to torrent client", append(logAttrs, "status", updated.Status)...)
	writeJSON(w, http.StatusOK, wantedReleaseDownloadResponse{
		Success:      true,
		WantedID:     wantedID,
		ReleaseID:    releaseID,
		DownloadHash: downloadHash,
		Status:       updated.Status,
		Message:      "Release sent to " + clientName,
		Warning:      warning,
		Item:         updated,
	})
}

func wantedReleaseDownloadHash(release models.WantedRelease) string {
	for _, candidate := range []string{release.DownloadURL, release.GUID} {
		candidate = strings.TrimSpace(candidate)
		if !strings.HasPrefix(strings.ToLower(candidate), "magnet:") {
			continue
		}
		parsed, err := url.Parse(candidate)
		if err != nil {
			continue
		}
		for _, xt := range parsed.Query()["xt"] {
			const prefix = "urn:btih:"
			if strings.HasPrefix(strings.ToLower(xt), prefix) {
				return strings.TrimSpace(xt[len(prefix):])
			}
		}
	}
	return ""
}

func (s *Server) submitWantedReleaseToTorrent(ctx context.Context, item models.WantedBook, release models.WantedRelease) (string, error) {
	url, err := s.resolveTorrentURL(ctx, models.DownloadRequest{
		Source:           "torrent",
		Title:            firstNonEmptyString(release.Title, item.Title),
		Author:           item.Author,
		DownloadURL:      release.DownloadURL,
		GUID:             release.GUID,
		MediaType:        item.MediaType,
		DownloadProtocol: release.Protocol,
	}, models.SearchResult{})
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(url) == "" {
		return "", errors.New("release has no stored download URL or magnet link")
	}

	savePath := s.cfg.QBSavePath
	category := s.cfg.QBCategory
	switch strings.TrimSpace(strings.ToLower(item.MediaType)) {
	case "audiobook":
		savePath = s.cfg.QBAudiobookSavePath
		category = s.cfg.QBAudiobookCategory
	case "manga":
		savePath = s.cfg.QBMangaSavePath
		category = s.cfg.QBMangaCategory
	}

	err = s.downloadMgr.StartTorrentDownload(url, firstNonEmptyString(release.Title, item.Title), savePath, category, "")
	var verificationWarning *download.TorrentVerificationWarning
	if errors.As(err, &verificationWarning) {
		return errString(err), nil
	}
	return "", err
}

func wantedReleaseDownloadAvailable(release models.WantedRelease) bool {
	protocol := strings.TrimSpace(strings.ToLower(release.Protocol))
	if protocol != "" && protocol != "torrent" && protocol != "magnet" {
		return false
	}
	downloadURL := strings.TrimSpace(release.DownloadURL)
	guid := strings.TrimSpace(release.GUID)
	if downloadURL == "" && guid == "" {
		return false
	}
	return strings.HasPrefix(strings.ToLower(downloadURL), "magnet:") ||
		strings.HasPrefix(strings.ToLower(guid), "magnet:") ||
		strings.HasPrefix(strings.ToLower(downloadURL), "http://") ||
		strings.HasPrefix(strings.ToLower(downloadURL), "https://")
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *Server) handleV1WantedSearchAll(w http.ResponseWriter, r *http.Request) {
	if s.wantedMonitor == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": "Wanted monitor is unavailable"})
		return
	}
	summary, err := s.wantedMonitor.SearchAll(r.Context(), true)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "already running") {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "summary": summary, "items": summary.Updated})
}

func (s *Server) handleV1WantedSearchOne(w http.ResponseWriter, r *http.Request) {
	if s.wantedMonitor == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": "Wanted monitor is unavailable"})
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid wanted id"})
		return
	}
	item, err := s.wantedMonitor.SearchOne(r.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "already running") {
			status = http.StatusConflict
		}
		if errors.Is(err, sql.ErrNoRows) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "item": item})
}
