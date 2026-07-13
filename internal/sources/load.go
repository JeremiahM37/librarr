package sources

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// defaultRegistryURL is the canonical librarr-sources companion repo. It is a
// var (not a const) so tests can swap it without hitting GitHub.
var defaultRegistryURL = "https://raw.githubusercontent.com/JeremiahM37/librarr-sources/main/sources.json"

// Load resolves the active registry in this order:
//
//  1. path  — local JSON file (LIBRARR_SOURCES_PATH)
//  2. url   — user-supplied HTTP(S) URL (LIBRARR_SOURCES_URL)
//  3. The built-in default URL (librarr-sources)
//  4. cachePath — on-disk copy of the last successful fetch
//
// On a successful HTTP fetch (steps 2 or 3) the body is written to cachePath
// so subsequent restarts work offline. If every step fails the function
// returns a non-nil empty Registry — callers can still range over slices,
// but search will return no results until the registry can be reached.
//
// All inputs may be empty.
func Load(path, url, cachePath string) *Registry {
	if path != "" {
		if r, err := loadFile(path); err == nil {
			slog.Info("loaded sources registry from file", "path", path, "version", r.Version)
			return r
		} else {
			slog.Warn("failed to load sources registry from file", "path", path, "error", err)
		}
	}
	if url != "" {
		if r, body, err := loadURL(url); err == nil {
			slog.Info("loaded sources registry from URL", "url", url, "version", r.Version)
			writeCache(cachePath, body)
			return r
		} else {
			slog.Warn("failed to load sources registry from URL", "url", url, "error", err)
		}
	}
	if r, body, err := loadURL(defaultRegistryURL); err == nil {
		slog.Info("loaded sources registry from default URL", "url", defaultRegistryURL, "version", r.Version)
		writeCache(cachePath, body)
		return r
	} else {
		slog.Warn("failed to load sources registry from default URL", "url", defaultRegistryURL, "error", err)
	}
	if cachePath != "" {
		if r, err := loadFile(cachePath); err == nil {
			slog.Info("loaded sources registry from on-disk cache", "path", cachePath, "version", r.Version)
			return r
		} else if !os.IsNotExist(err) {
			slog.Warn("failed to load sources registry from cache", "path", cachePath, "error", err)
		}
	}
	slog.Error("no sources registry available — search will return no results until LIBRARR_SOURCES_URL is reachable or LIBRARR_SOURCES_PATH is set")
	return &Registry{}
}

func loadFile(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decode(data)
}

func loadURL(url string) (*Registry, []byte, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return nil, nil, fmt.Errorf("url must be http(s)")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MiB cap
	if err != nil {
		return nil, nil, err
	}
	r, err := decode(body)
	if err != nil {
		return nil, nil, err
	}
	return r, body, nil
}

func decode(data []byte) (*Registry, error) {
	var r Registry
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return r.Normalize(), nil
}

// writeCache persists a successful registry fetch to disk. Failures are logged
// at WARN — caching is best-effort, not load-bearing.
func writeCache(path string, body []byte) {
	if path == "" || len(body) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		slog.Warn("could not create sources cache directory", "path", path, "error", err)
		return
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		slog.Warn("could not write sources cache", "path", path, "error", err)
	}
}

// ApplyEnvOverrides patches the registry with any legacy per-source environment
// variables that are set. Returns the (mutated) input registry for chaining.
// This preserves backwards compatibility for users who configure individual
// endpoints via env vars instead of editing the registry.
func (r *Registry) ApplyEnvOverrides(getenv func(string) string) *Registry {
	if v := getenv("ANNAS_ARCHIVE_DOMAIN"); v != "" {
		r.Annas.Domain = v
	}
	return r.Normalize()
}
