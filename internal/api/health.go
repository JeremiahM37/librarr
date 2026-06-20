package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/JeremiahM37/librarr/internal/version"
)

// Version comes from the embedded VERSION file (see internal/version).
// BuildTime may still be injected at build time via -ldflags.
var (
	Version   = version.Version
	BuildTime = "unknown"
	GoVersion = runtime.Version()
)

var startTime = time.Now()

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	uptime := time.Since(startTime)

	// Count enabled sources
	enabledSources := 0
	sourceNames := []string{}
	for _, src := range s.searchMgr.GetSources() {
		if src.Enabled() {
			enabledSources++
			sourceNames = append(sourceNames, src.Name())
		}
	}

	// Library stats
	libraryTotal := 0
	if stats, err := s.db.GetStats(); err == nil {
		if total, ok := stats["total_items"]; ok {
			if n, ok := total.(int); ok {
				libraryTotal = n
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":          "ok",
		"version":         Version,
		"build_time":      BuildTime,
		"go_version":      GoVersion,
		"uptime_seconds":  int(uptime.Seconds()),
		"uptime_human":    formatDuration(uptime),
		"sources_enabled": enabledSources,
		"sources":         sourceNames,
		"library_items":   libraryTotal,
	})
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, mins)
	}
	return fmt.Sprintf("%dm", mins)
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	// Determine if audiobook search is available (prowlarr audiobooks or ABB).
	hasAudiobookSearch := false
	for _, src := range s.searchMgr.GetSources() {
		if src.SearchTab() == "audiobook" && src.Enabled() {
			hasAudiobookSearch = true
			break
		}
	}

	// Return objects with "configured" + "url" so the frontend can display
	// both the status and the URL. Previously these were bare booleans, and
	// the frontend did data.prowlarr.url which was undefined on a bool →
	// showed "Not configured" even when the service was configured.
	configObj := func(configured bool, url string) map[string]interface{} {
		if !configured {
			return nil
		}
		return map[string]interface{}{"configured": true, "url": url}
	}

	resp := map[string]interface{}{
		"prowlarr":            configObj(s.cfg.HasProwlarr(), s.cfg.ProwlarrURL),
		"qbittorrent":         configObj(s.cfg.HasQBittorrent(), s.cfg.QBUrl),
		"transmission":        configObj(s.cfg.HasTransmission(), s.cfg.TransmissionURL),
		"torrent_client":      s.cfg.ActiveTorrentClient(),
		"audiobookshelf":      s.cfg.HasAudiobookshelf(),
		"kavita":              s.cfg.HasKavita(),
		"calibre":             s.cfg.HasCalibre(),
		"komga":               s.cfg.HasKomga(),
		"sabnzbd":             configObj(s.cfg.HasSABnzbd(), s.cfg.SABnzbdURL),
		"file_org_enabled":    s.cfg.FileOrgEnabled,
		"torznab_enabled":     s.cfg.TorznabAPIKey != "",
		"rate_limit_enabled":  s.cfg.RateLimitEnabled,
		"metrics_enabled":     s.cfg.MetricsEnabled,
		"webnovel_enabled":    s.cfg.WebNovelEnabled,
		"mangadex_enabled":    s.cfg.MangaDexEnabled,
		"audiobooks":          hasAudiobookSearch,
		"foreign_lang_filter": s.searchMgr.ForeignLangFilterEnabled(),
		"flibusta_enabled":    s.cfg.HasFlibusta(),
		"zlibrary_enabled":    s.cfg.HasZLibrary(),
	}

	// OIDC config (safe to expose — no secrets).
	if s.cfg.HasOIDC() {
		resp["oidc_enabled"] = true
		resp["oidc_provider_name"] = s.cfg.OIDCProviderName
	} else {
		resp["oidc_enabled"] = false
	}

	// Multi-user info.
	userCount, _ := s.db.CountUsers()
	resp["multi_user"] = userCount > 0
	resp["has_users"] = userCount > 0

	// Current user info from context.
	if username, ok := r.Context().Value(ctxUsername).(string); ok && username != "" {
		resp["current_user"] = username
	}
	if role, ok := r.Context().Value(ctxUserRole).(string); ok && role != "" {
		resp["current_role"] = role
	}

	if s.cfg.KavitaPublicURL != "" {
		resp["kavita_url"] = s.cfg.KavitaPublicURL
	} else if s.cfg.KavitaURL != "" {
		resp["kavita_url"] = s.cfg.KavitaURL
	}

	if s.cfg.ABSPublicURL != "" {
		resp["audiobookshelf_url"] = s.cfg.ABSPublicURL
	} else if s.cfg.ABSURL != "" {
		resp["audiobookshelf_url"] = s.cfg.ABSURL
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
