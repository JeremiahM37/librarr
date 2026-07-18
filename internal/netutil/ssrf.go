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
