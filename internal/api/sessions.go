package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

const (
	ctxUserID   contextKey = "userID"
	ctxUserRole contextKey = "userRole"
	ctxUsername contextKey = "username"
)

// SessionData holds session metadata.
type SessionData struct {
	UserID   int64
	Username string
	Role     string
	Expiry   time.Time
}

// PendingTOTP holds a pending TOTP verification.
type PendingTOTP struct {
	UserID int64
	Expiry time.Time
}

// SessionStore manages session-based authentication with user tracking.
type SessionStore struct {
	mu          sync.RWMutex
	sessions    map[string]*SessionData
	pendingTOTP map[string]*PendingTOTP
}

// NewSessionStore creates a new session store.
func NewSessionStore() *SessionStore {
	s := &SessionStore{
		sessions:    make(map[string]*SessionData),
		pendingTOTP: make(map[string]*PendingTOTP),
	}

	// Periodically clean up expired sessions and pending TOTP tokens.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		for range ticker.C {
			now := time.Now()
			s.mu.Lock()
			for token, data := range s.sessions {
				if now.After(data.Expiry) {
					delete(s.sessions, token)
				}
			}
			for token, pending := range s.pendingTOTP {
				if now.After(pending.Expiry) {
					delete(s.pendingTOTP, token)
				}
			}
			s.mu.Unlock()
		}
	}()

	return s
}

// Create generates a new session token for a user, valid for 24 hours. It
// returns an error if the system CSPRNG fails, so callers fail closed rather
// than mint a predictable all-zero token.
func (s *SessionStore) Create(userID int64, username, role string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	token := hex.EncodeToString(b)

	s.mu.Lock()
	s.sessions[token] = &SessionData{
		UserID:   userID,
		Username: username,
		Role:     role,
		Expiry:   time.Now().Add(24 * time.Hour),
	}
	s.mu.Unlock()

	return token, nil
}

// CreatePendingTOTP creates a temporary token for TOTP verification (5 min
// expiry). Like Create, it fails closed if the CSPRNG errors.
func (s *SessionStore) CreatePendingTOTP(userID int64) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate pending TOTP token: %w", err)
	}
	token := hex.EncodeToString(b)

	s.mu.Lock()
	s.pendingTOTP[token] = &PendingTOTP{
		UserID: userID,
		Expiry: time.Now().Add(5 * time.Minute),
	}
	s.mu.Unlock()

	return token, nil
}

// ValidatePendingTOTP checks and consumes a pending TOTP token.
func (s *SessionStore) ValidatePendingTOTP(token string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending, ok := s.pendingTOTP[token]
	if !ok {
		return 0, false
	}
	delete(s.pendingTOTP, token)

	if time.Now().After(pending.Expiry) {
		return 0, false
	}
	return pending.UserID, true
}

// Get retrieves session data if the token is valid.
func (s *SessionStore) Get(token string) (*SessionData, bool) {
	s.mu.RLock()
	data, ok := s.sessions[token]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}
	if time.Now().After(data.Expiry) {
		s.mu.Lock()
		delete(s.sessions, token)
		s.mu.Unlock()
		return nil, false
	}
	return data, true
}

// Valid checks if a session token is valid and not expired (backward compat).
func (s *SessionStore) Valid(token string) bool {
	_, ok := s.Get(token)
	return ok
}

// Delete removes a session.
func (s *SessionStore) Delete(token string) {
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
}
