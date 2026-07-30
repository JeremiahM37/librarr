package organize

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

// LibraryTargets handles importing organized files into external libraries.
type LibraryTargets struct {
	cfg    *config.Config
	client *http.Client
}

// NewLibraryTargets creates a new library targets handler.
func NewLibraryTargets(cfg *config.Config) *LibraryTargets {
	return &LibraryTargets{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ImportEbook copies to Calibre library and triggers scans on ABS and Kavita.
func (lt *LibraryTargets) ImportEbook(filePath, title, author string) {
	lt.copyToCalibre(filePath, title, author)
	lt.copyToKavitaEbook(filePath, title, author)
	lt.scanABSEbookLibrary()
}

// ImportAudiobook triggers ABS library scan.
func (lt *LibraryTargets) ImportAudiobook() {
	lt.scanABSAudiobookLibrary()
}

// ImportManga copies to Kavita and Komga manga libraries and triggers scans.
func (lt *LibraryTargets) ImportManga(filePath, seriesTitle string) {
	lt.copyToKavitaManga(filePath, seriesTitle)
	lt.scanKavita()
	lt.copyToKomga(filePath, seriesTitle)
	lt.scanKomga()
}

// copyToCalibre copies an ebook to the Calibre library directory.
func (lt *LibraryTargets) copyToCalibre(filePath, title, author string) {
	if !lt.cfg.HasCalibre() {
		return
	}
	if author == "" {
		author = "Unknown"
	}

	destDir := filepath.Join(lt.cfg.CalibreLibraryPath, sanitizePath(author, 80), sanitizePath(title, 80))
	if err := os.MkdirAll(destDir, 0755); err != nil {
		slog.Error("create calibre dir failed", "error", err)
		return
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))
	if err := copyFile(filePath, destPath); err != nil {
		slog.Error("copy to calibre failed", "error", err)
		return
	}
	slog.Info("copied to calibre library", "path", destPath)
}

// copyToKavitaEbook copies an ebook to the Kavita ebook library.
func (lt *LibraryTargets) copyToKavitaEbook(filePath, title, author string) {
	if lt.cfg.KavitaLibraryPath == "" {
		return
	}
	if author == "" {
		author = "Unknown"
	}

	destDir := filepath.Join(lt.cfg.KavitaLibraryPath, sanitizePath(author, 80), sanitizePath(title, 80))
	if err := os.MkdirAll(destDir, 0755); err != nil {
		slog.Error("create kavita ebook dir failed", "error", err)
		return
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))
	if err := copyFile(filePath, destPath); err != nil {
		slog.Error("copy to kavita ebook library failed", "error", err)
		return
	}
	slog.Info("copied to kavita ebook library", "path", destPath)
}

// copyToKavitaManga copies manga to the Kavita manga library.
func (lt *LibraryTargets) copyToKavitaManga(filePath, seriesTitle string) {
	if lt.cfg.KavitaMangaLibraryPath == "" {
		return
	}

	destDir := filepath.Join(lt.cfg.KavitaMangaLibraryPath, sanitizePath(seriesTitle, 80))
	if err := os.MkdirAll(destDir, 0755); err != nil {
		slog.Error("create kavita manga dir failed", "error", err)
		return
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))
	if err := copyFile(filePath, destPath); err != nil {
		slog.Error("copy to kavita manga library failed", "error", err)
		return
	}
	slog.Info("copied to kavita manga library", "path", destPath)
}

// scanABSAudiobookLibrary triggers an Audiobookshelf audiobook library scan.
func (lt *LibraryTargets) scanABSAudiobookLibrary() {
	if !lt.cfg.HasAudiobookshelf() || lt.cfg.ABSLibraryID == "" {
		return
	}
	lt.absLibraryScan(lt.cfg.ABSLibraryID)
}

// scanABSEbookLibrary triggers an Audiobookshelf ebook library scan.
func (lt *LibraryTargets) scanABSEbookLibrary() {
	if !lt.cfg.HasAudiobookshelf() || lt.cfg.ABSEbookLibraryID == "" {
		return
	}
	lt.absLibraryScan(lt.cfg.ABSEbookLibraryID)
}

func (lt *LibraryTargets) absLibraryScan(libraryID string) {
	url := fmt.Sprintf("%s/api/libraries/%s/scan", lt.cfg.ABSURL, libraryID)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		slog.Error("abs scan request creation failed", "error", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+lt.cfg.ABSToken)

	resp, err := lt.client.Do(req)
	if err != nil {
		// Include the configured base URL: the most common cause of a
		// failure here is a malformed ABS_URL, and the bare net/http error
		// ("unsupported protocol scheme") never says which URL it came from.
		slog.Error("abs scan failed", "library_id", libraryID, "abs_url", lt.cfg.ABSURL, "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 300 {
		slog.Info("abs library scan triggered", "library_id", libraryID)
	} else {
		slog.Warn("abs scan returned non-success", "library_id", libraryID, "status", resp.StatusCode)
	}
}

// scanKavita triggers a Kavita library scan.
func (lt *LibraryTargets) scanKavita() {
	if !lt.cfg.HasKavita() {
		return
	}

	// Login to get JWT token.
	token, err := lt.kavitaLogin()
	if err != nil {
		slog.Error("kavita login failed", "error", err)
		return
	}

	url := fmt.Sprintf("%s/api/Library/scan", lt.cfg.KavitaURL)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		slog.Error("kavita scan request creation failed", "error", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := lt.client.Do(req)
	if err != nil {
		slog.Error("kavita scan failed", "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 300 {
		slog.Info("kavita library scan triggered")
	} else {
		slog.Warn("kavita scan returned non-success", "status", resp.StatusCode)
	}
}

func (lt *LibraryTargets) kavitaLogin() (string, error) {
	payload, _ := json.Marshal(map[string]string{
		"username": lt.cfg.KavitaUser,
		"password": lt.cfg.KavitaPass,
	})

	url := fmt.Sprintf("%s/api/Account/login", lt.cfg.KavitaURL)
	resp, err := lt.client.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("kavita login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("kavita login HTTP %d", resp.StatusCode)
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("kavita login decode: %w", err)
	}
	return result.Token, nil
}

// copyToKomga copies a manga file to the Komga library path.
func (lt *LibraryTargets) copyToKomga(filePath, seriesTitle string) {
	if !lt.cfg.HasKomga() || lt.cfg.KomgaLibraryPath == "" {
		return
	}

	safeTitle := sanitizePath(seriesTitle, 80)
	destDir := filepath.Join(lt.cfg.KomgaLibraryPath, safeTitle)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		slog.Error("create komga dir failed", "error", err)
		return
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))
	if err := copyFile(filePath, destPath); err != nil {
		slog.Error("copy to komga failed", "error", err)
		return
	}
	slog.Info("copied to komga library", "path", destPath)
}

// scanKomga triggers a Komga library scan.
func (lt *LibraryTargets) scanKomga() {
	if !lt.cfg.HasKomga() || lt.cfg.KomgaLibraryID == "" {
		return
	}

	url := fmt.Sprintf("%s/api/v1/libraries/%s/scan", lt.cfg.KomgaURL, lt.cfg.KomgaLibraryID)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		slog.Error("komga scan request creation failed", "error", err)
		return
	}
	req.SetBasicAuth(lt.cfg.KomgaUser, lt.cfg.KomgaPass)

	resp, err := lt.client.Do(req)
	if err != nil {
		slog.Error("komga scan failed", "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 300 {
		slog.Info("komga library scan triggered")
	} else {
		slog.Warn("komga scan returned non-success", "status", resp.StatusCode)
	}
}

// ABSAutoMatch triggers Audible metadata match for a library item.
func (lt *LibraryTargets) ABSAutoMatch(itemID string) {
	if !lt.cfg.HasAudiobookshelf() {
		return
	}

	url := fmt.Sprintf("%s/api/items/%s/match", lt.cfg.ABSURL, itemID)
	payload, _ := json.Marshal(map[string]string{"provider": "audible"})
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		slog.Error("abs match request creation failed", "error", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+lt.cfg.ABSToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := lt.client.Do(req)
	if err != nil {
		slog.Error("abs auto-match failed", "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 300 {
		slog.Info("abs auto-matched item", "item_id", itemID)
	}
}

// ABSAutoMatchNewItems scans for new audiobook items and triggers Audible match.
func (lt *LibraryTargets) ABSAutoMatchNewItems(knownIDs map[string]bool) {
	if !lt.cfg.HasAudiobookshelf() || lt.cfg.ABSLibraryID == "" {
		return
	}

	url := fmt.Sprintf("%s/api/libraries/%s/items?limit=100", lt.cfg.ABSURL, lt.cfg.ABSLibraryID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+lt.cfg.ABSToken)

	resp, err := lt.client.Do(req)
	if err != nil {
		slog.Error("abs items list failed", "error", err)
		return
	}
	defer resp.Body.Close()

	var data struct {
		Results []struct {
			ID string `json:"id"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return
	}

	for _, item := range data.Results {
		if knownIDs[item.ID] {
			continue
		}
		lt.ABSAutoMatch(item.ID)
	}
}

// ABSCleanupDuplicateEbooks checks for duplicate entries after ebook import.
// Keeps the Calibre version and removes ABS-only duplicates.
func (lt *LibraryTargets) ABSCleanupDuplicateEbooks(title string) {
	if !lt.cfg.HasAudiobookshelf() || lt.cfg.ABSEbookLibraryID == "" {
		return
	}

	url := fmt.Sprintf("%s/api/libraries/%s/items?limit=100&filter=title=%s",
		lt.cfg.ABSURL, lt.cfg.ABSEbookLibraryID, title)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+lt.cfg.ABSToken)

	resp, err := lt.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var data struct {
		Results []struct {
			ID    string `json:"id"`
			Media struct {
				LibraryFiles []struct {
					Metadata struct {
						Path string `json:"path"`
					} `json:"metadata"`
				} `json:"libraryFiles"`
			} `json:"media"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return
	}

	if len(data.Results) <= 1 {
		return
	}

	// Keep the first item, delete duplicates.
	for i := 1; i < len(data.Results); i++ {
		delURL := fmt.Sprintf("%s/api/items/%s", lt.cfg.ABSURL, data.Results[i].ID)
		delReq, _ := http.NewRequest("DELETE", delURL, nil)
		delReq.Header.Set("Authorization", "Bearer "+lt.cfg.ABSToken)
		delResp, err := lt.client.Do(delReq)
		if err == nil {
			delResp.Body.Close()
			slog.Info("abs duplicate removed", "title", title, "item_id", data.Results[i].ID)
		}
	}
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}
