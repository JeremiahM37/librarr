package config

import (
	"regexp"
	"strings"
)

// schemeRe matches an explicit URI scheme prefix ("http://", "https://",
// "unix://", …). A bare "host:port" does NOT match, because there is no "//"
// after the colon — which is exactly the input we need to repair.
var schemeRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*://`)

// NormalizeBaseURL cleans a service base URL that came from an env var or the
// settings file: trims whitespace and trailing slashes, and prepends "http://"
// when no scheme is present.
//
// Issue #92: a scheme-less value such as "audiobookshelf:13378" reaches
// net/http as a relative URL, so every request against it fails with the
// opaque `unsupported protocol scheme ""`. The user-visible symptoms are a
// library page stuck on "Failed to load library" and an "abs scan failed" log
// line, neither of which points at the URL as the cause. Repairing the value
// once, at load time, fixes every consumer at once.
//
// A non-http scheme is left alone — if someone configures "unix://…" that's
// their call, and mangling it into "http://unix://…" would be worse than the
// original error.
func NormalizeBaseURL(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if !schemeRe.MatchString(s) {
		s = "http://" + s
	}
	// Trim trailing slashes so callers can concatenate "/api/..." without
	// producing a double slash — but never trim into the scheme separator
	// itself, which would turn "http://" into "http:".
	scheme := schemeRe.FindString(s)
	return scheme + strings.TrimRight(s[len(scheme):], "/")
}

// BaseURLSettingKeys lists the settings.json keys that hold a service base URL
// — the subset of settings normalizeServiceURLs repairs at load time. The
// settings API normalizes the same keys on write, so the value the UI shows
// after a save matches the one the runtime will use.
var BaseURLSettingKeys = []string{
	"qb_url",
	"transmission_url",
	"prowlarr_url",
	"sabnzbd_url",
	"abs_url",
	"abs_public_url",
	"kavita_url",
	"kavita_public_url",
	"komga_url",
	"calibre_url",
	"flibusta_url",
}

// normalizeServiceURLs runs every configured service base URL through
// NormalizeBaseURL. Called from Load after both the env layer and the
// settings-file layer have been applied, so a bad value is repaired no matter
// which source it came from.
//
// Deliberately excluded: WebhookURL and the OIDC endpoints. Those are full
// endpoint URLs handed to a third party verbatim (a Discord webhook path, an
// OIDC issuer that must byte-match the provider's discovery document), not
// base URLs we append paths to.
func (c *Config) normalizeServiceURLs() {
	for _, field := range []*string{
		&c.QBUrl,
		&c.ProwlarrURL,
		&c.SABnzbdURL,
		&c.ABSURL,
		&c.ABSPublicURL,
		&c.KavitaURL,
		&c.KavitaPublicURL,
		&c.KomgaURL,
		&c.CalibreURL,
		&c.DelugeURL,
		&c.TransmissionURL,
		&c.FlibustaURL,
		&c.ZLibraryURL,
		&c.BookTrackerURL,
	} {
		*field = NormalizeBaseURL(*field)
	}
}
