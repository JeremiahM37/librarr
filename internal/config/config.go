package config

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/sources"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// Server
	Port   int
	DBPath string

	// qBittorrent
	QBUrl               string
	QBUser              string
	QBPass              string
	QBSavePath          string
	QBCategory          string
	QBAudiobookSavePath string
	QBAudiobookCategory string
	QBMangaSavePath     string
	QBMangaCategory     string

	// Prowlarr
	ProwlarrURL    string
	ProwlarrAPIKey string

	// File Organization
	FileOrgEnabled   bool
	EbookDir         string
	AudiobookDir     string
	MangaDir         string
	IncomingDir      string
	MangaIncomingDir string

	// Torznab
	TorznabAPIKey string

	// Anna's Archive
	AnnasArchiveDomain string

	// Sources is the runtime indexer-endpoint registry. Drivers read URLs,
	// mirrors, and per-site config from here instead of from hardcoded
	// constants. Always non-nil after Load() returns.
	Sources *sources.Registry

	// Circuit Breaker
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   int // seconds

	// Download Settings
	MaxRetries          int
	RetryBackoffSeconds int

	// Search Filtering
	MinTorrentSizeBytes int64
	MaxTorrentSizeBytes int64

	// Library Import Targets
	CalibreLibraryPath     string
	CalibreURL             string
	KavitaURL              string
	KavitaUser             string
	KavitaPass             string
	KavitaLibraryPath      string
	KavitaMangaLibraryPath string
	ABSURL                 string
	ABSToken               string
	ABSLibraryID           string
	ABSEbookLibraryID      string

	// Authentication
	AuthUsername string
	AuthPassword string
	APIKey       string

	// Komga
	KomgaURL         string
	KomgaUser        string
	KomgaPass        string
	KomgaLibraryID   string
	KomgaLibraryPath string

	// ABS Public URL (for external links)
	ABSPublicURL string

	// Kavita Public URL (for external links)
	KavitaPublicURL string

	// SABnzbd (Usenet)
	SABnzbdURL      string
	SABnzbdAPIKey   string
	SABnzbdCategory string

	// Download client priority (lower = preferred)
	QBPriority  int
	SABPriority int

	// Post-import torrent handling
	RemoveTorrentAfterImport bool

	// Flibusta
	FlibustaURL     string
	FlibustaEnabled bool

	// Z-Library
	ZLibraryURL      string
	ZLibraryEmail    string
	ZLibraryPassword string
	ZLibraryEnabled  bool

	// ThePirateBay
	TPBEnabled bool

	// BookTracker
	BookTrackerURL     string
	BookTrackerUser    string
	BookTrackerPass    string
	BookTrackerEnabled bool

	// Search filtering
	ForeignLangFilter bool // filter out non-English titles (default: true for backward compat)

	// Feature toggles
	RateLimitEnabled bool
	MetricsEnabled   bool
	WebNovelEnabled  bool
	MangaDexEnabled  bool

	// lightnovel-crawler container name (for docker exec)
	LNCrawlContainer string

	// Settings persistence
	SettingsFile string

	// OIDC / SSO
	OIDCEnabled         bool
	OIDCProviderName    string
	OIDCIssuer          string
	OIDCClientID        string
	OIDCClientSecret    string
	OIDCRedirectURI     string
	OIDCAutoCreateUsers bool
	OIDCDefaultRole     string

	// Deluge
	DelugeURL      string
	DelugePassword string

	// Transmission
	TransmissionURL  string
	TransmissionUser string
	TransmissionPass string

	// User Agent
	UserAgent string

	// Webhooks (env-based defaults)
	WebhookURL  string
	WebhookType string // "discord" or "generic"

	// Scheduler
	SchedulerEnabled       bool
	SchedulerIntervalHours int
	SchedulerAutoDownload  bool
	SchedulerMinScore      int

	// Quality Profiles
	AutoUpgradeEnabled bool

	// Rename on Import
	RenameEnabled bool
	RenamePattern string

	// Author Monitoring
	AuthorMonitorEnabled    bool
	AuthorCheckIntervalDays int
}

// Load reads configuration from environment variables with sensible defaults,
// then applies overrides from the settings file (if present) so values saved
// from the UI persist across restarts.
func Load() *Config {
	cfg := buildFromEnv()
	cfg.applySettingsFileOverrides()
	cfg.probeSettingsFileWritable()
	return cfg
}

// deriveSettingsFile resolves the path the binary will read+write for UI-saved
// settings. If SETTINGS_FILE is explicitly set, that wins. Otherwise we
// co-locate settings.json next to the SQLite database so deployments that mount
// their data volume at a non-default path (e.g. /data/librarr/ instead of
// /data/) don't silently fail on every "Save Settings" click.
//
// The historical default of /data/settings.json is preserved when
// LIBRARR_DB_PATH is also at the default /data/librarr.db — so existing
// deployments with no env-var changes behave identically.
func deriveSettingsFile(explicit, dbPath string) string {
	if explicit != "" {
		return explicit
	}
	dir := filepath.Dir(dbPath)
	if dir == "" || dir == "." {
		dir = "/data"
	}
	return filepath.Join(dir, "settings.json")
}

// probeSettingsFileWritable logs a clear error at startup if the binary can't
// write SettingsFile. The previous behaviour was to discover this on the first
// /api/settings POST, hours after boot, which silently broke the UI's Save
// Settings button.
func (c *Config) probeSettingsFileWritable() {
	if c.SettingsFile == "" {
		return
	}
	dir := filepath.Dir(c.SettingsFile)
	if dir == "" {
		return
	}
	probe := filepath.Join(dir, ".librarr-settings-write-probe")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		slog.Error(
			"settings file location is not writable — UI 'Save Settings' will fail until this is fixed",
			"path", c.SettingsFile,
			"parent_dir", dir,
			"error", err,
			"hint", "set SETTINGS_FILE to a path inside a writable volume, or mount your data dir at the SettingsFile parent",
		)
		return
	}
	_ = f.Close()
	_ = os.Remove(probe)
}

// buildFromEnv returns a Config populated from environment variables and defaults.
func buildFromEnv() *Config {
	registry := sources.Load(
		getEnv("LIBRARR_SOURCES_PATH", ""),
		getEnv("LIBRARR_SOURCES_URL", ""),
	).ApplyEnvOverrides(os.Getenv)

	// Honor ANNAS_ARCHIVE_DOMAIN if set; otherwise pick up the registry value.
	annasDomain := getEnv("ANNAS_ARCHIVE_DOMAIN", "")
	if annasDomain == "" {
		annasDomain = registry.Annas.Domain
	}

	return &Config{
		Sources: registry,
		Port:    getEnvInt("LIBRARR_PORT", 5050),
		DBPath:  getEnv("LIBRARR_DB_PATH", "/data/librarr.db"),

		QBUrl:               getEnv("QB_URL", ""),
		QBUser:              getEnv("QB_USER", "admin"),
		QBPass:              getEnv("QB_PASS", ""),
		QBSavePath:          getEnv("QB_SAVE_PATH", "/downloads"),
		QBCategory:          getEnv("QB_CATEGORY", "librarr"),
		QBAudiobookSavePath: getEnv("QB_AUDIOBOOK_SAVE_PATH", "/audiobooks-incoming"),
		QBAudiobookCategory: getEnv("QB_AUDIOBOOK_CATEGORY", "audiobooks"),
		QBMangaSavePath:     getEnv("QB_MANGA_SAVE_PATH", "/manga-incoming"),
		QBMangaCategory:     getEnv("QB_MANGA_CATEGORY", "manga"),

		ProwlarrURL:    getEnv("PROWLARR_URL", ""),
		ProwlarrAPIKey: getEnv("PROWLARR_API_KEY", ""),

		FileOrgEnabled:   getEnvBool("FILE_ORG_ENABLED", true),
		EbookDir:         getEnv("EBOOK_DIR", "/books/ebooks"),
		AudiobookDir:     getEnv("AUDIOBOOK_DIR", "/books/audiobooks"),
		MangaDir:         getEnv("MANGA_DIR", "/books/manga"),
		IncomingDir:      getEnv("INCOMING_DIR", "/data/incoming"),
		MangaIncomingDir: getEnv("MANGA_INCOMING_DIR", "/data/manga-incoming"),

		TorznabAPIKey: getEnv("TORZNAB_API_KEY", ""),

		AnnasArchiveDomain: annasDomain,

		CircuitBreakerThreshold: getEnvInt("CIRCUIT_BREAKER_THRESHOLD", 3),
		CircuitBreakerTimeout:   getEnvInt("CIRCUIT_BREAKER_TIMEOUT", 300),

		MaxRetries:          getEnvInt("MAX_RETRIES", 2),
		RetryBackoffSeconds: getEnvInt("RETRY_BACKOFF_SECONDS", 60),

		MinTorrentSizeBytes: getEnvInt64("MIN_TORRENT_SIZE_BYTES", 10000),      // 10KB
		MaxTorrentSizeBytes: getEnvInt64("MAX_TORRENT_SIZE_BYTES", 2000000000), // 2GB

		CalibreLibraryPath:     getEnv("CALIBRE_LIBRARY_PATH", ""),
		CalibreURL:             getEnv("CALIBRE_URL", ""),
		KavitaURL:              getEnv("KAVITA_URL", ""),
		KavitaUser:             getEnv("KAVITA_USER", ""),
		KavitaPass:             getEnv("KAVITA_PASS", ""),
		KavitaLibraryPath:      getEnv("KAVITA_LIBRARY_PATH", ""),
		KavitaMangaLibraryPath: getEnv("KAVITA_MANGA_LIBRARY_PATH", ""),
		ABSURL:                 getEnv("ABS_URL", ""),
		ABSToken:               getEnv("ABS_TOKEN", ""),
		ABSLibraryID:           getEnv("ABS_LIBRARY_ID", ""),
		ABSEbookLibraryID:      getEnv("ABS_EBOOK_LIBRARY_ID", ""),

		AuthUsername: getEnv("AUTH_USERNAME", ""),
		AuthPassword: getEnv("AUTH_PASSWORD", ""),
		APIKey:       getEnv("API_KEY", ""),

		KomgaURL:         getEnv("KOMGA_URL", ""),
		KomgaUser:        getEnv("KOMGA_USER", ""),
		KomgaPass:        getEnv("KOMGA_PASS", ""),
		KomgaLibraryID:   getEnv("KOMGA_LIBRARY_ID", ""),
		KomgaLibraryPath: getEnv("KOMGA_LIBRARY_PATH", ""),

		ABSPublicURL: getEnv("ABS_PUBLIC_URL", ""),

		KavitaPublicURL: getEnv("KAVITA_PUBLIC_URL", ""),

		SABnzbdURL:      getEnv("SABNZBD_URL", ""),
		SABnzbdAPIKey:   getEnv("SABNZBD_API_KEY", ""),
		SABnzbdCategory: getEnv("SABNZBD_CATEGORY", "librarr"),

		QBPriority:  getEnvInt("QB_PRIORITY", 1),
		SABPriority: getEnvInt("SAB_PRIORITY", 2),

		RemoveTorrentAfterImport: getEnvBool("REMOVE_TORRENT_AFTER_IMPORT", true),

		RateLimitEnabled: getEnvBool("RATE_LIMIT_ENABLED", true),
		MetricsEnabled:   getEnvBool("METRICS_ENABLED", true),
		WebNovelEnabled:  getEnvBool("WEBNOVEL_ENABLED", true),
		MangaDexEnabled:  getEnvBool("MANGADEX_ENABLED", true),

		FlibustaURL:     getEnv("FLIBUSTA_URL", ""),
		FlibustaEnabled: getEnvBool("FLIBUSTA_ENABLED", false),

		ZLibraryURL:      getEnv("ZLIBRARY_URL", ""),
		ZLibraryEmail:    getEnv("ZLIBRARY_EMAIL", ""),
		ZLibraryPassword: getEnv("ZLIBRARY_PASSWORD", ""),
		ZLibraryEnabled:  getEnvBool("ZLIBRARY_ENABLED", false),

		TPBEnabled: getEnvBool("TPB_ENABLED", false),

		BookTrackerURL:     getEnv("BOOKTRACKER_URL", ""),
		BookTrackerUser:    getEnv("BOOKTRACKER_USER", ""),
		BookTrackerPass:    getEnv("BOOKTRACKER_PASS", ""),
		BookTrackerEnabled: getEnvBool("BOOKTRACKER_ENABLED", false),

		ForeignLangFilter: getEnvBool("FOREIGN_LANG_FILTER", true),

		LNCrawlContainer: getEnv("LNCRAWL_CONTAINER", ""),

		SettingsFile: deriveSettingsFile(os.Getenv("SETTINGS_FILE"), getEnv("LIBRARR_DB_PATH", "/data/librarr.db")),

		OIDCEnabled:         getEnvBool("OIDC_ENABLED", false),
		OIDCProviderName:    getEnv("OIDC_PROVIDER_NAME", "SSO"),
		OIDCIssuer:          getEnv("OIDC_ISSUER", ""),
		OIDCClientID:        getEnv("OIDC_CLIENT_ID", ""),
		OIDCClientSecret:    getEnv("OIDC_CLIENT_SECRET", ""),
		OIDCRedirectURI:     getEnv("OIDC_REDIRECT_URI", ""),
		OIDCAutoCreateUsers: getEnvBool("OIDC_AUTO_CREATE_USERS", true),
		OIDCDefaultRole:     getEnv("OIDC_DEFAULT_ROLE", "user"),

		DelugeURL:      getEnv("DELUGE_URL", ""),
		DelugePassword: getEnv("DELUGE_PASSWORD", ""),

		TransmissionURL:  getEnv("TRANSMISSION_URL", ""),
		TransmissionUser: getEnv("TRANSMISSION_USER", ""),
		TransmissionPass: getEnv("TRANSMISSION_PASS", ""),

		UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

		WebhookURL:  getEnv("WEBHOOK_URL", ""),
		WebhookType: getEnv("WEBHOOK_TYPE", "generic"),

		SchedulerEnabled:       getEnvBool("SCHEDULER_ENABLED", false),
		SchedulerIntervalHours: getEnvInt("SCHEDULER_INTERVAL_HOURS", 24),
		SchedulerAutoDownload:  getEnvBool("SCHEDULER_AUTO_DOWNLOAD", false),
		SchedulerMinScore:      getEnvInt("SCHEDULER_MIN_SCORE", 70),

		AutoUpgradeEnabled: getEnvBool("AUTO_UPGRADE_ENABLED", false),

		RenameEnabled: getEnvBool("RENAME_ENABLED", false),
		RenamePattern: getEnv("RENAME_PATTERN", "{author} - {title} ({year}).{ext}"),

		AuthorMonitorEnabled:    getEnvBool("AUTHOR_MONITOR_ENABLED", false),
		AuthorCheckIntervalDays: getEnvInt("AUTHOR_CHECK_INTERVAL_DAYS", 7),
	}
}

// HasOIDC returns true if OIDC/SSO is configured.
func (c *Config) HasOIDC() bool {
	return c.OIDCEnabled && c.OIDCIssuer != "" && c.OIDCClientID != "" && c.OIDCClientSecret != ""
}

// HasQBittorrent returns true if qBittorrent is configured.
func (c *Config) HasQBittorrent() bool {
	return c.QBUrl != ""
}

// HasProwlarr returns true if Prowlarr is configured.
func (c *Config) HasProwlarr() bool {
	return c.ProwlarrURL != "" && c.ProwlarrAPIKey != ""
}

// HasAudiobookshelf returns true if ABS is configured.
func (c *Config) HasAudiobookshelf() bool {
	return c.ABSURL != "" && c.ABSToken != ""
}

// HasKavita returns true if Kavita is configured.
func (c *Config) HasKavita() bool {
	return c.KavitaURL != "" && c.KavitaUser != "" && c.KavitaPass != ""
}

// HasCalibre returns true if Calibre library path is configured.
func (c *Config) HasCalibre() bool {
	return c.CalibreLibraryPath != ""
}

// HasAuth returns true if session-based auth is configured.
func (c *Config) HasAuth() bool {
	return c.AuthUsername != "" && c.AuthPassword != ""
}

// HasKomga returns true if Komga is configured.
func (c *Config) HasKomga() bool {
	return c.KomgaURL != "" && c.KomgaUser != "" && c.KomgaPass != ""
}

// HasSABnzbd returns true if SABnzbd is configured.
func (c *Config) HasSABnzbd() bool {
	return c.SABnzbdURL != "" && c.SABnzbdAPIKey != ""
}

// HasAPIKey returns true if API key auth is configured.
func (c *Config) HasAPIKey() bool {
	return c.APIKey != ""
}

// HasDeluge returns true if Deluge is configured.
func (c *Config) HasDeluge() bool {
	return c.DelugeURL != ""
}

// HasTransmission returns true if Transmission is configured.
func (c *Config) HasTransmission() bool {
	return c.TransmissionURL != ""
}

// HasFlibusta returns true if Flibusta is configured and enabled.
func (c *Config) HasFlibusta() bool {
	return c.FlibustaEnabled && c.FlibustaURL != ""
}

// HasZLibrary returns true if Z-Library is configured and enabled.
func (c *Config) HasZLibrary() bool {
	return c.ZLibraryEnabled && c.ZLibraryEmail != "" && c.ZLibraryPassword != ""
}

// HasBookTracker returns true if BookTracker is configured and enabled.
func (c *Config) HasBookTracker() bool {
	return c.BookTrackerEnabled && c.BookTrackerURL != "" && c.BookTrackerUser != "" && c.BookTrackerPass != ""
}

// applySettingsFileOverrides reads the JSON settings file at c.SettingsFile (if
// it exists) and overrides matching string/bool/int fields on the Config.
// Missing or unparseable files are silently ignored so the env-only path keeps
// working — this is best-effort layering, not a hard config source.
func (c *Config) applySettingsFileOverrides() {
	if c.SettingsFile == "" {
		return
	}
	data, err := os.ReadFile(c.SettingsFile)
	if err != nil {
		return
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}

	strPtrs := map[string]*string{
		"qb_url":                    &c.QBUrl,
		"qb_user":                   &c.QBUser,
		"qb_pass":                   &c.QBPass,
		"prowlarr_url":              &c.ProwlarrURL,
		"prowlarr_api_key":          &c.ProwlarrAPIKey,
		"sabnzbd_url":               &c.SABnzbdURL,
		"sabnzbd_api_key":           &c.SABnzbdAPIKey,
		"sabnzbd_category":          &c.SABnzbdCategory,
		"abs_url":                   &c.ABSURL,
		"abs_token":                 &c.ABSToken,
		"abs_library_id":            &c.ABSLibraryID,
		"abs_ebook_library_id":      &c.ABSEbookLibraryID,
		"abs_public_url":            &c.ABSPublicURL,
		"kavita_url":                &c.KavitaURL,
		"kavita_user":               &c.KavitaUser,
		"kavita_pass":               &c.KavitaPass,
		"kavita_library_path":       &c.KavitaLibraryPath,
		"kavita_manga_library_path": &c.KavitaMangaLibraryPath,
		"kavita_public_url":         &c.KavitaPublicURL,
		"komga_url":                 &c.KomgaURL,
		"komga_user":                &c.KomgaUser,
		"komga_pass":                &c.KomgaPass,
		"komga_library_id":          &c.KomgaLibraryID,
		"komga_library_path":        &c.KomgaLibraryPath,
		"calibre_url":               &c.CalibreURL,
		"calibre_library_path":      &c.CalibreLibraryPath,
		"annas_archive_domain":      &c.AnnasArchiveDomain,
		"ebook_dir":                 &c.EbookDir,
		"audiobook_dir":             &c.AudiobookDir,
		"manga_dir":                 &c.MangaDir,
		"incoming_dir":              &c.IncomingDir,
		"manga_incoming_dir":        &c.MangaIncomingDir,
		"flibusta_url":              &c.FlibustaURL,
	}
	for key, fieldPtr := range strPtrs {
		v, ok := raw[key]
		if !ok {
			continue
		}
		s, isStr := v.(string)
		if !isStr || s == "" {
			continue
		}
		*fieldPtr = s
	}

	boolPtrs := map[string]*bool{
		"file_org_enabled":            &c.FileOrgEnabled,
		"rate_limit_enabled":          &c.RateLimitEnabled,
		"metrics_enabled":             &c.MetricsEnabled,
		"webnovel_enabled":            &c.WebNovelEnabled,
		"mangadex_enabled":            &c.MangaDexEnabled,
		"flibusta_enabled":            &c.FlibustaEnabled,
		"zlibrary_enabled":            &c.ZLibraryEnabled,
		"remove_torrent_after_import": &c.RemoveTorrentAfterImport,
		"foreign_lang_filter":         &c.ForeignLangFilter,
	}
	for key, fieldPtr := range boolPtrs {
		v, ok := raw[key]
		if !ok {
			continue
		}
		b, isBool := v.(bool)
		if !isBool {
			continue
		}
		*fieldPtr = b
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	switch v {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	}
	return fallback
}
