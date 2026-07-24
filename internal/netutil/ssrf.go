// Package netutil provides network helpers, including SSRF-safe
// validation of user-supplied integration and outbound URLs.
package netutil

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

func parseHTTPURL(rawURL string) (*url.URL, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("URL is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("URL must use http or https")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("URL must include a host")
	}
	return u, nil
}

func isMetadataHost(host string) bool {
	lower := strings.ToLower(host)
	for _, blocked := range []string{
		"metadata.google.internal",
		"metadata.goog",
	} {
		if lower == blocked || strings.HasSuffix(lower, "."+blocked) {
			return true
		}
	}
	return false
}

// ValidateIntegrationURL checks admin-initiated integration test URLs (Prowlarr,
// Kavita, etc.). Private and loopback addresses are allowed — homelab services
// commonly run at http://192.168.x.x:port or http://localhost:port.
func ValidateIntegrationURL(rawURL string) error {
	u, err := parseHTTPURL(rawURL)
	if err != nil {
		return err
	}
	if isMetadataHost(u.Hostname()) {
		return fmt.Errorf("URL targets a restricted host")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && isCloudMetadataIP(ip) {
		return fmt.Errorf("URL targets a restricted address")
	}
	return nil
}

// allowPrivateOutbound reports whether the operator has explicitly opted out
// of the private/loopback-address SSRF guard. Some self-hosted setups serve
// downloads from LAN mirrors (a NAS libgen mirror, an internal cache); the
// hermetic e2e suite also relies on it to download from a 127.0.0.1 stub.
// Cloud-metadata addresses stay blocked even with the override — there is no
// legitimate reason for a book download to come from 169.254.169.254.
func allowPrivateOutbound() bool {
	return os.Getenv("LIBRARR_INSECURE_ALLOW_PRIVATE_URLS") == "1"
}

// ValidateOutboundURL checks that rawURL is a safe http(s) target for server-side
// requests. It rejects loopback, private, link-local, and metadata addresses
// (see LIBRARR_INSECURE_ALLOW_PRIVATE_URLS for the LAN-mirror escape hatch).
func ValidateOutboundURL(rawURL string) error {
	u, err := parseHTTPURL(rawURL)
	if err != nil {
		return err
	}
	host := u.Hostname()

	lower := strings.ToLower(host)
	if lower == "localhost" || strings.HasSuffix(lower, ".localhost") {
		return fmt.Errorf("URL targets a restricted host")
	}
	if isMetadataHost(host) {
		return fmt.Errorf("URL targets a restricted host")
	}

	if ip := net.ParseIP(host); ip != nil {
		if isCloudMetadataIP(ip) {
			return fmt.Errorf("URL targets a restricted address")
		}
		if isRestrictedIP(ip) && !allowPrivateOutbound() {
			return fmt.Errorf("URL targets a restricted address")
		}
		return nil
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		// Hostname did not resolve (offline, Docker service name, etc.).
		// Blocked hostnames were already rejected above; allow the URL.
		return nil
	}
	for _, ip := range ips {
		if isCloudMetadataIP(ip) {
			return fmt.Errorf("URL resolves to a restricted address")
		}
		if allowPrivateOutbound() {
			continue
		}
		if isRestrictedIP(ip) {
			return fmt.Errorf("URL targets a restricted address")
		}
	}
	return nil
}

// ValidateSameOriginHTTPURL parses rawURL and verifies that it has the same
// normalized host and effective port as allowedOrigin. It is intended for
// integration-owned URLs, such as Prowlarr download links, where the server may
// legitimately be private or loopback but redirects must not escape the configured
// origin.
func ValidateSameOriginHTTPURL(rawURL, allowedOrigin string) (*url.URL, error) {
	u, err := parseStrictHTTPURL(rawURL)
	if err != nil {
		return nil, err
	}
	allowed, err := parseStrictHTTPURL(allowedOrigin)
	if err != nil {
		return nil, fmt.Errorf("configured origin invalid: %w", err)
	}
	if normalizedHostname(u) != normalizedHostname(allowed) || effectivePort(u) != effectivePort(allowed) {
		return nil, fmt.Errorf("URL host or port does not match configured origin")
	}
	return u, nil
}

func parseStrictHTTPURL(rawURL string) (*url.URL, error) {
	u, err := parseHTTPURL(rawURL)
	if err != nil {
		return nil, err
	}
	if u.User != nil {
		return nil, fmt.Errorf("URL must not include credentials")
	}
	if _, err := strictEffectivePort(u); err != nil {
		return nil, err
	}
	return u, nil
}

func normalizedHostname(u *url.URL) string {
	return strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
}

func effectivePort(u *url.URL) string {
	port, _ := strictEffectivePort(u)
	return port
}

func strictEffectivePort(u *url.URL) (string, error) {
	if port := u.Port(); port != "" {
		return port, nil
	}
	host := u.Host
	lastColon := strings.LastIndex(host, ":")
	lastBracket := strings.LastIndex(host, "]")
	if lastColon > lastBracket {
		return "", fmt.Errorf("URL has malformed port")
	}
	switch u.Scheme {
	case "http":
		return "80", nil
	case "https":
		return "443", nil
	default:
		return "", fmt.Errorf("URL must use http or https")
	}
}

// SanitizeLogValue strips line breaks from remote-derived values before they
// reach structured logs, so a crafted value cannot forge additional log
// entries.
func SanitizeLogValue(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

// SanitizeSensitiveText redacts credentials and common secret query parameters
// from error strings before they are logged or returned through API responses.
// Line breaks are also removed so the result is safe to log.
func SanitizeSensitiveText(text string) string {
	text = SanitizeLogValue(text)
	parts := strings.Fields(text)
	for i, part := range parts {
		trimmed := strings.Trim(part, "\"'(),")
		if u, err := url.Parse(trimmed); err == nil && u.Scheme != "" && u.Host != "" {
			u.User = nil
			q := u.Query()
			for key := range q {
				lower := strings.ToLower(key)
				if strings.Contains(lower, "key") || strings.Contains(lower, "token") || strings.Contains(lower, "pass") || strings.Contains(lower, "sid") {
					q.Set(key, "REDACTED")
				}
			}
			u.RawQuery = q.Encode()
			parts[i] = strings.Replace(part, trimmed, u.String(), 1)
		}
	}
	return strings.Join(parts, " ")
}

func isCloudMetadataIP(ip net.IP) bool {
	ip4 := ip.To4()
	return ip4 != nil && ip4[0] == 169 && ip4[1] == 254
}

func isRestrictedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsPrivate() || ip.IsUnspecified() {
		return true
	}
	return isCloudMetadataIP(ip)
}
