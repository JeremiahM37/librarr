package download

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// URLValidationConfig controls how strict outbound download URL validation is.
type URLValidationConfig struct {
	AllowedHosts  []string
	BlockPrivate  bool
	BlockMetadata bool
}

func defaultURLValidationConfig() URLValidationConfig {
	return URLValidationConfig{
		BlockPrivate:  true,
		BlockMetadata: true,
	}
}

// validateURLSafety rejects loopback/private/metadata endpoints by default.
func validateURLSafety(rawURL string, cfg URLValidationConfig) error {
	if rawURL == "" {
		return fmt.Errorf("URL is required")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("invalid URL scheme '%s': only http and https are allowed", scheme)
	}

	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("invalid URL: no hostname")
	}

	if isLoopbackHost(host) {
		return fmt.Errorf("URL targets localhost (SSRF blocked)")
	}

	if cfg.BlockPrivate {
		if ip := net.ParseIP(host); ip != nil && isPrivateOrReservedIP(ip) {
			return fmt.Errorf("URL targets private/reserved IP (SSRF blocked)")
		}
	}

	if cfg.BlockMetadata && isCloudMetadataEndpoint(host) {
		return fmt.Errorf("URL targets metadata endpoint (SSRF blocked)")
	}

	if len(cfg.AllowedHosts) > 0 && !isHostInWhitelist(host, cfg.AllowedHosts) {
		return fmt.Errorf("URL host '%s' is not in allowed hosts", host)
	}

	return nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" || host == "localhost.localdomain" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isPrivateOrReservedIP(ip net.IP) bool {
	if ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	return ip.String() == "0.0.0.0"
}

func isCloudMetadataEndpoint(host string) bool {
	lower := strings.ToLower(host)
	if lower == "169.254.169.254" || lower == "metadata.aws.internal" || lower == "metadata.google.internal" {
		return true
	}
	if strings.HasPrefix(lower, "metadata.") || strings.HasPrefix(lower, "169.254.") || strings.HasPrefix(lower, "fe80:") {
		return true
	}
	return strings.Contains(lower, "kubernetes")
}

func isHostInWhitelist(host string, whitelist []string) bool {
	host = strings.ToLower(host)
	for _, allowed := range whitelist {
		allowed = strings.ToLower(allowed)
		if host == allowed {
			return true
		}
		if strings.HasPrefix(allowed, "*.") {
			domain := allowed[2:]
			if host == domain || strings.HasSuffix(host, "."+domain) {
				return true
			}
		}
	}
	return false
}
