package netutil

import (
	"strings"
	"testing"
)

func TestValidateIntegrationURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"empty", "", true},
		{"invalid scheme", "ftp://example.com/file", true},
		{"localhost ok", "http://localhost:8080/", false},
		{"127.0.0.1 ok", "http://127.0.0.1/api", false},
		{"private 10.x ok", "http://10.0.0.1/", false},
		{"private 192.168 ok", "http://192.168.70.100:1111/", false},
		{"metadata host blocked", "http://metadata.google.internal/", true},
		{"metadata ip blocked", "http://169.254.169.254/", true},
		{"public https", "https://example.com/path", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIntegrationURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIntegrationURL(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestValidateOutboundURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"empty", "", true},
		{"invalid scheme", "ftp://example.com/file", true},
		{"localhost", "http://localhost:8080/", true},
		{"127.0.0.1", "http://127.0.0.1/api", true},
		{"private 10.x", "http://10.0.0.1/", true},
		{"private 192.168", "http://192.168.1.1/", true},
		{"link-local", "http://169.254.169.254/", true},
		{"metadata host", "http://metadata.google.internal/", true},
		{"public https", "https://example.com/path", false},
		{"public http", "http://prowlarr.example:9696/", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOutboundURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateOutboundURL(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestValidateSameOriginHTTPURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		origin  string
		wantErr bool
	}{
		{"same explicit port", "http://prowlarr.example:9696/download", "http://prowlarr.example:9696", false},
		{"same hostname case insensitive", "http://PROWLARR.example:9696/download", "http://prowlarr.EXAMPLE:9696", false},
		{"http default port equals explicit 80", "http://prowlarr.example/download", "http://prowlarr.example:80", false},
		{"https default port equals explicit 443", "https://prowlarr.example/download", "https://prowlarr.example:443", false},
		{"different hostname", "http://other.example:9696/download", "http://prowlarr.example:9696", true},
		{"different port", "http://prowlarr.example:9697/download", "http://prowlarr.example:9696", true},
		{"credentials rejected", "http://user:pass@prowlarr.example:9696/download", "http://prowlarr.example:9696", true},
		{"malformed port rejected", "http://prowlarr.example:bad/download", "http://prowlarr.example:9696", true},
		{"unsupported scheme rejected", "ftp://prowlarr.example/download", "http://prowlarr.example", true},
		{"missing host rejected", "http:///download", "http://prowlarr.example", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateSameOriginHTTPURL(tt.raw, tt.origin)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateSameOriginHTTPURL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeSensitiveText(t *testing.T) {
	input := `fetch http://user:pass@example.com/download?apikey=secret&token=abc&id=1 failed`
	got := SanitizeSensitiveText(input)
	if strings.Contains(got, "secret") || strings.Contains(got, "abc") || strings.Contains(got, "user:pass") {
		t.Fatalf("SanitizeSensitiveText leaked secret: %q", got)
	}
	if !strings.Contains(got, "id=1") {
		t.Fatalf("SanitizeSensitiveText removed non-secret query: %q", got)
	}
}
