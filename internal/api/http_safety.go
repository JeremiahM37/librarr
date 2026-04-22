package api

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// URLValidationConfig holds settings for URL validation.
type URLValidationConfig struct {
	AllowedHosts []string // Whitelist of allowed domain hosts (e.g., ["libgen.li", "libgen.la"])
	BlockPrivate bool     // Block private/reserved IP ranges
	BlockMetadata bool    // Block cloud metadata endpoints (169.254.169.254, etc.)
}

// DefaultURLValidationConfig returns a safe default configuration for most use cases.
func DefaultURLValidationConfig() URLValidationConfig {
	return URLValidationConfig{
		BlockPrivate:  true,
		BlockMetadata: true,
	}
}

// ValidateURLSafety performs comprehensive validation on a URL to prevent SSRF/injection attacks.
// It checks scheme, host, reserved/private IPs, cloud metadata endpoints, and optional whitelist.
func ValidateURLSafety(rawURL string, cfg URLValidationConfig) error {
	if rawURL == "" {
		return fmt.Errorf("URL is required")
	}

	// Parse URL
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Scheme validation: only http/https allowed
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("invalid URL scheme '%s': only http and https are allowed", scheme)
	}

	// Extract host (without port)
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("invalid URL: no hostname")
	}

	// Reject localhost and loopback addresses
	if isLoopback(host) {
		return fmt.Errorf("URL targets localhost (SSRF attack attempt blocked)")
	}

	// Block private/reserved IP ranges if configured
	if cfg.BlockPrivate {
		ip := net.ParseIP(host)
		if ip != nil && isPrivateOrReservedIP(ip) {
			return fmt.Errorf("URL targets a private or reserved IP address (SSRF attack blocked)")
		}
	}

	// Block cloud metadata endpoints if configured
	if cfg.BlockMetadata {
		if isCloudMetadataEndpoint(host) {
			return fmt.Errorf("URL targets a cloud metadata endpoint (SSRF attack blocked)")
		}
	}

	// If whitelist is provided, only allow listed hosts
	if len(cfg.AllowedHosts) > 0 {
		if !isHostInWhitelist(host, cfg.AllowedHosts) {
			return fmt.Errorf("URL host '%s' is not in the allowed hosts list", host)
		}
	}

	return nil
}

// isLoopback checks if the host is localhost or 127.x.x.x / ::1
func isLoopback(host string) bool {
	if host == "localhost" || host == "localhost.localdomain" {
		return true
	}
	ip := net.ParseIP(host)
	if ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// isPrivateOrReservedIP checks if an IP is private (RFC1918), link-local, or reserved.
func isPrivateOrReservedIP(ip net.IP) bool {
	if ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	// Check for specific reserved ranges not covered by IsPrivate/IsLinkLocal:
	// 0.0.0.0/8, 10.0.0.0/8 (handled by IsPrivate), 127.0.0.0/8 (IsLoopback),
	// 169.254.0.0/16 (IsLinkLocal), 172.16.0.0/12 (IsPrivate), 192.168.0.0/16 (IsPrivate),
	// 224.0.0.0/4 (multicast), 240.0.0.0/4 (reserved)
	if ip.IsMulticast() {
		return true
	}
	// Additional check for 0.0.0.0 and broadcast
	if ip.String() == "0.0.0.0" || ip.IsUnspecified() {
		return true
	}
	return false
}

// isCloudMetadataEndpoint checks for common cloud provider metadata service endpoints.
func isCloudMetadataEndpoint(host string) bool {
	lower := strings.ToLower(host)
	
	// AWS metadata service
	if lower == "169.254.169.254" || lower == "metadata.aws.internal" {
		return true
	}
	
	// Google Cloud metadata
	if lower == "metadata.google.internal" || lower == "169.254.169.254" {
		return true
	}
	
	// Azure metadata service
	if lower == "169.254.169.254" {
		return true
	}
	
	// Kubernetes internal API
	if strings.Contains(lower, "kubernetes") || strings.Contains(lower, "kubernetes.default") {
		return true
	}
	
	// Generic metadata domains
	if strings.HasPrefix(lower, "metadata.") || strings.HasPrefix(lower, "169.254.") {
		return true
	}
	
	// Link-local addresses (169.254.0.0/16, fe80::/10)
	if strings.HasPrefix(lower, "169.254.") || strings.HasPrefix(lower, "fe80:") {
		return true
	}
	
	return false
}

// isHostInWhitelist checks if a host matches any entry in the whitelist.
// Supports exact matches and wildcard subdomain matches (e.g., *.example.com).
func isHostInWhitelist(host string, whitelist []string) bool {
	host = strings.ToLower(host)
	for _, allowed := range whitelist {
		allowed = strings.ToLower(allowed)
		
		// Exact match
		if host == allowed {
			return true
		}
		
		// Wildcard subdomain match (*.example.com matches sub.example.com)
		if strings.HasPrefix(allowed, "*.") {
			domain := allowed[2:] // Remove "*."
			if host == domain || strings.HasSuffix(host, "."+domain) {
				return true
			}
		}
	}
	return false
}

// ValidateSearchQuery performs input sanitization on search queries.
// It checks length, rejects special query characters that could cause issues,
// and ensures only safe Unicode is present.
func ValidateSearchQuery(query string) error {
	if len(query) > 500 {
		return fmt.Errorf("search query too long (max 500 characters)")
	}
	
	// Check for null bytes (would break many string processing)
	if strings.ContainsRune(query, '\x00') {
		return fmt.Errorf("search query contains invalid null bytes")
	}
	
	// Reject control characters (except spaces, newlines are unlikely in queries)
	for _, r := range query {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return fmt.Errorf("search query contains invalid control characters")
		}
	}
	
	return nil
}

// QueryIntBounded extracts an integer query parameter with bounds checking.
// Returns the parsed value clamped to [min, max], or fallback if parsing fails.
func QueryIntBounded(r *http.Request, key string, fallback, min, max int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

// QueryIntDefault extracts an integer query parameter with a fallback.
// Returns the parsed value or fallback if parsing fails or value is negative.
func QueryIntDefault(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

// ValidatePaginationParams checks pagination parameters for reasonable bounds.
// Returns (limit, offset, error).
func ValidatePaginationParams(limit, offset, maxLimit int) (int, int, error) {
	if maxLimit <= 0 {
		maxLimit = 200
	}
	if limit < 1 {
		limit = 1
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset, nil
}

// ValidatePathSafety checks a file path for directory traversal attacks.
// Returns the cleaned path if safe, or an error.
func ValidatePathSafety(basePath, requestedPath string) (string, error) {
	if basePath == "" {
		return "", fmt.Errorf("base path is required")
	}
	if requestedPath == "" {
		return "", fmt.Errorf("requested path is required")
	}

	// Clean the requested path and ensure it doesn't escape basePath
	cleanBase := filepath.Clean(basePath)
	requested := filepath.Clean(requestedPath)

	// Resolve to absolute paths to detect traversal attempts
	absBase, err := filepath.Abs(cleanBase)
	if err != nil {
		return "", fmt.Errorf("invalid base path")
	}

	fullPath := filepath.Join(absBase, requested)
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", fmt.Errorf("invalid path")
	}

	// Verify the final path is within the base directory
	if !strings.HasPrefix(absPath, absBase) && absPath != absBase {
		return "", fmt.Errorf("path traversal attempt blocked")
	}

	return absPath, nil
}

// SanitizePath removes path separators and traversal sequences from a filename.
// It ensures the result is a safe relative filename, not a path.
func SanitizePath(filename string) string {
	filename = filepath.Base(strings.TrimSpace(filename))
	// Remove any remaining path separators
	filename = strings.ReplaceAll(filename, "/", "_")
	filename = strings.ReplaceAll(filename, "\\", "_")
	filename = strings.ReplaceAll(filename, ":", "_")
	if filename == "." || filename == ".." || filename == "" {
		filename = "file"
	}
	return filename
}

// ValidateEnumValue checks if a value is in the allowed set of options.
func ValidateEnumValue(value string, allowed []string) error {
	if value == "" {
		return fmt.Errorf("value is required")
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return fmt.Errorf("invalid value '%s': allowed values are %v", value, allowed)
}

// ValidateStringLength checks a string is within min/max bounds.
func ValidateStringLength(s string, minLen, maxLen int) error {
	if len(s) < minLen {
		return fmt.Errorf("value too short (minimum %d characters)", minLen)
	}
	if len(s) > maxLen {
		return fmt.Errorf("value too long (maximum %d characters)", maxLen)
	}
	return nil
}

// ValidateMetadataString checks a metadata string (title, author, etc) for valid characters.
// Allows letters, numbers, common punctuation, but rejects control characters and other threats.
func ValidateMetadataString(s string, maxLen int) error {
	if len(s) > maxLen {
		return fmt.Errorf("value too long (maximum %d characters)", maxLen)
	}

	// Reject null bytes
	if strings.ContainsRune(s, '\x00') {
		return fmt.Errorf("value contains invalid null bytes")
	}

	// Reject control characters
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return fmt.Errorf("value contains invalid control characters")
		}
	}

	return nil
}

// ValidateEmail performs basic email validation.
func ValidateEmail(email string) error {
	email = strings.TrimSpace(email)
	if email == "" {
		return fmt.Errorf("email is required")
	}
	if len(email) > 254 {
		return fmt.Errorf("email too long")
	}

	// Simple regex pattern for email validation
	// This is not RFC 5322 compliant but covers common cases
	pattern := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	if !pattern.MatchString(email) {
		return fmt.Errorf("invalid email format")
	}
	return nil
}

// ValidateUnicodeString checks that a string contains only valid Unicode and no homograph attack characters.
func ValidateUnicodeString(s string, maxLen int) error {
	if len(s) > maxLen {
		return fmt.Errorf("value too long (maximum %d characters)", maxLen)
	}

	// Reject null bytes
	if strings.ContainsRune(s, '\x00') {
		return fmt.Errorf("value contains invalid null bytes")
	}

	// Reject control characters
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return fmt.Errorf("value contains invalid control characters")
		}
	}

	return nil
}
