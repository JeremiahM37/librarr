package api

import (
	"errors"
	"net"
	"net/http"
	"time"
)

// ErrTooManyRedirects is returned when HTTP redirect limit is exceeded.
var ErrTooManyRedirects = errors.New("too many redirects (max 5)")

// NewSecureHTTPClient creates an HTTP client with security hardening and timeouts.
// Useful for external API calls and downloads.
// Set requestTimeout to 0 to use the default (30 seconds).
// Set requestTimeout to negative values for large downloads (e.g., -5*time.Minute for 5 minute timeout).
func NewSecureHTTPClient(requestTimeout time.Duration) *http.Client {
	if requestTimeout == 0 {
		requestTimeout = 30 * time.Second
	}

	// Use absolute value for negative timeouts
	if requestTimeout < 0 {
		requestTimeout = -requestTimeout
	}

	return &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			// Connection timeouts
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,

			// Per-request timeouts
			ResponseHeaderTimeout: 10 * time.Second,
			IdleConnTimeout:       90 * time.Second,

			// Connection pooling
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			MaxConnsPerHost:     10,

			// Prevent SSRF attacks by limiting redirects
			// (typically handled at client level, but good to have)
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Limit redirects to 5
			if len(via) > 5 {
				return ErrTooManyRedirects
			}
			return nil
		},
	}
}
