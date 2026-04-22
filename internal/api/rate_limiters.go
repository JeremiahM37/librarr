package api

import (
	"fmt"
	"sync"
	"time"
)

// DownloadRateLimiter implements sliding window rate limiting for downloads.
// It tracks downloads per user/IP and enforces limits.
type DownloadRateLimiter struct {
	mu              sync.RWMutex
	limit           int           // Max downloads per window
	window          time.Duration // Time window (e.g., 1 minute)
	userDownloads   map[string][]*downloadEvent
	lastCleanupTime time.Time
}

// downloadEvent records a single download attempt with its timestamp.
type downloadEvent struct {
	timestamp time.Time
}

// NewDownloadRateLimiter creates a new rate limiter.
// Default: 5 downloads per minute per user.
func NewDownloadRateLimiter() *DownloadRateLimiter {
	limiter := &DownloadRateLimiter{
		limit:         5,
		window:        1 * time.Minute,
		userDownloads: make(map[string][]*downloadEvent),
		lastCleanupTime: time.Now(),
	}

	// Periodic cleanup of expired entries (every 5 minutes)
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			limiter.cleanup()
		}
	}()

	return limiter
}

// IsAllowed checks if a download is allowed for the given user.
// Returns (allowed, retryAfterSeconds).
func (d *DownloadRateLimiter) IsAllowed(userID string) (bool, int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	// Get or create events list for this user
	events, exists := d.userDownloads[userID]
	if !exists {
		events = make([]*downloadEvent, 0)
	}

	// Remove old events outside the window
	cutoff := now.Add(-d.window)
	validEvents := make([]*downloadEvent, 0)
	for _, e := range events {
		if e.timestamp.After(cutoff) {
			validEvents = append(validEvents, e)
		}
	}

	// Check if limit exceeded
	if len(validEvents) >= d.limit {
		// Calculate time until oldest event expires
		oldestTime := validEvents[0].timestamp
		retryAfter := int(d.window.Seconds()) - int(now.Sub(oldestTime).Seconds())
		if retryAfter < 1 {
			retryAfter = 1
		}
		return false, retryAfter
	}

	// Allow and record this download
	validEvents = append(validEvents, &downloadEvent{timestamp: now})
	d.userDownloads[userID] = validEvents

	return true, 0
}

// cleanup removes stale entries (not accessed for > 2 hours)
func (d *DownloadRateLimiter) cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	staleThreshold := 2 * time.Hour

	for userID, events := range d.userDownloads {
		// Keep only recent events
		recentEvents := make([]*downloadEvent, 0)
		for _, e := range events {
			if now.Sub(e.timestamp) < staleThreshold {
				recentEvents = append(recentEvents, e)
			}
		}

		if len(recentEvents) == 0 {
			delete(d.userDownloads, userID)
		} else {
			d.userDownloads[userID] = recentEvents
		}
	}

	d.lastCleanupTime = now
}

// TOTPRateLimiter implements rate limiting for TOTP verification attempts.
type TOTPRateLimiter struct {
	mu          sync.RWMutex
	limit       int           // Max attempts per window
	window      time.Duration // Time window
	attempts    map[string][]*totpAttempt
}

// totpAttempt records a TOTP verification attempt.
type totpAttempt struct {
	timestamp time.Time
	success   bool // Whether the attempt succeeded
}

// NewTOTPRateLimiter creates a new TOTP rate limiter.
// Default: 5 attempts per 5 minutes per user.
func NewTOTPRateLimiter() *TOTPRateLimiter {
	limiter := &TOTPRateLimiter{
		limit:    5,
		window:   5 * time.Minute,
		attempts: make(map[string][]*totpAttempt),
	}

	// Periodic cleanup
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			limiter.cleanup()
		}
	}()

	return limiter
}

// IsAllowed checks if a TOTP verification attempt is allowed.
// Returns (allowed, message).
func (t *TOTPRateLimiter) IsAllowed(userID string) (bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-t.window)

	// Get or create attempts list for this user
	attempts, exists := t.attempts[userID]
	if !exists {
		attempts = make([]*totpAttempt, 0)
	}

	// Remove old attempts outside the window
	validAttempts := make([]*totpAttempt, 0)
	for _, a := range attempts {
		if a.timestamp.After(cutoff) {
			validAttempts = append(validAttempts, a)
		}
	}

	// Check if limit exceeded
	if len(validAttempts) >= t.limit {
		return false, fmt.Sprintf("Too many TOTP verification attempts. Try again later.")
	}

	t.attempts[userID] = validAttempts

	return true, ""
}

// RecordAttempt records a TOTP verification attempt (successful or failed).
func (t *TOTPRateLimiter) RecordAttempt(userID string, success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	attempts, exists := t.attempts[userID]
	if !exists {
		attempts = make([]*totpAttempt, 0)
	}

	attempts = append(attempts, &totpAttempt{
		timestamp: time.Now(),
		success:   success,
	})

	t.attempts[userID] = attempts
}

// cleanup removes stale entries
func (t *TOTPRateLimiter) cleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	staleThreshold := 1 * time.Hour

	for userID, attempts := range t.attempts {
		recentAttempts := make([]*totpAttempt, 0)
		for _, a := range attempts {
			if now.Sub(a.timestamp) < staleThreshold {
				recentAttempts = append(recentAttempts, a)
			}
		}

		if len(recentAttempts) == 0 {
			delete(t.attempts, userID)
		} else {
			t.attempts[userID] = recentAttempts
		}
	}
}
