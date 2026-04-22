package config

import (
	"os"
	"strconv"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// Server
	Port   int
	DBPath string

	// qBittorrent
	QBUrl              string
	QBUser             string
	QBPass             string
	QBSavePath         string
	QBCategory         string
	QBAudiobookSavePath string
	QBAudiobookCategory string
	QBMangaSavePath    string
	QBMangaCategory    string

	// Prowlarr
	ProwlarrURL    string
	ProwlarrAPIKey string

	// File Organization
	FileOrgEnabled     bool
	EbookDir           string
	AudiobookDir       string
	MangaDir           string
	IncomingDir        string
	MangaIncomingDir   string

	// Torznab
	TorznabAPIKey string

	// Anna's Archive
	AnnasArchiveDomain string

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
	CalibreLibraryPath      string
	CalibreURL              string
	KavitaURL               string
	KavitaUser              string
	KavitaPass              string
	KavitaLibraryPath       string
	KavitaMangaLibraryPath  string
	ABSURL                  string
	ABSToken                string
	ABSLibraryID            string
	ABSEbookLibraryID       string

	// Authentication
	AuthUsername   string
	AuthPassword   string
	APIKey         string
	APIKeyReadOnly string

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
	ZLibraryURL     string
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
	AuthorMonitorEnabled      bool
	AuthorCheckIntervalDays   int
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		Port:   getEnvInt("LIBRARR_PORT", 5050),
		DBPath: getEnv("LIBRARR_DB_PATH", "/data/librarr.db"),

		QBUrl:              getEnv("QB_URL", ""),
		QBUser:             getEnv("QB_USER", "admin"),
		QBPass:             getEnv("QB_PASS", ""),
		QBSavePath:         getEnv("QB_SAVE_PATH", "/downloads"),
		QBCategory:         getEnv("QB_CATEGORY", "librarr"),
		QBAudiobookSavePath: getEnv("QB_AUDIOBOOK_SAVE_PATH", "/audiobooks-incoming"),
		QBAudiobookCategory: getEnv("QB_AUDIOBOOK_CATEGORY", "audiobooks"),
		QBMangaSavePath:    getEnv("QB_MANGA_SAVE_PATH", "/manga-incoming"),
		QBMangaCategory:    getEnv("QB_MANGA_CATEGORY", "manga"),

		ProwlarrURL:    getEnv("PROWLARR_URL", ""),
		ProwlarrAPIKey: getEnv("PROWLARR_API_KEY", ""),

		FileOrgEnabled:   getEnvBool("FILE_ORG_ENABLED", true),
		EbookDir:         getEnv("EBOOK_DIR", "/books/ebooks"),
		AudiobookDir:     getEnv("AUDIOBOOK_DIR", "/books/audiobooks"),
		MangaDir:         getEnv("MANGA_DIR", "/books/manga"),
		IncomingDir:      getEnv("INCOMING_DIR", "/data/incoming"),
		MangaIncomingDir: getEnv("MANGA_INCOMING_DIR", "/data/manga-incoming"),

		TorznabAPIKey: getEnv("TORZNAB_API_KEY", ""),

		AnnasArchiveDomain: getEnv("ANNAS_ARCHIVE_DOMAIN", "annas-archive.gl"),

		CircuitBreakerThreshold: getEnvInt("CIRCUIT_BREAKER_THRESHOLD", 3),
		CircuitBreakerTimeout:   getEnvInt("CIRCUIT_BREAKER_TIMEOUT", 300),

		MaxRetries:          getEnvInt("MAX_RETRIES", 2),
		RetryBackoffSeconds: getEnvInt("RETRY_BACKOFF_SECONDS", 60),

		MinTorrentSizeBytes: getEnvInt64("MIN_TORRENT_SIZE_BYTES", 10000),       // 10KB
		MaxTorrentSizeBytes: getEnvInt64("MAX_TORRENT_SIZE_BYTES", 2000000000),  // 2GB

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

		AuthUsername:   getEnv("AUTH_USERNAME", ""),
		AuthPassword:   getEnv("AUTH_PASSWORD", ""),
		APIKey:         getEnv("API_KEY", ""),
		APIKeyReadOnly: getEnv("API_KEY_READ_ONLY", ""),

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

		ZLibraryURL:     getEnv("ZLIBRARY_URL", ""),
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

		SettingsFile: getEnv("SETTINGS_FILE", "/data/settings.json"),

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

// HasAPIKey returns true if any API key auth is configured.
func (c *Config) HasAPIKey() bool {
	return c.APIKey != "" || c.APIKeyReadOnly != ""
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
