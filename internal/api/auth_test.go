package api

import (
	"testing"
	"time"
)

func TestSessionStore_CreateAndGet(t *testing.T) {
	store := NewSessionStore()

	token := store.Create(1, "testuser", "admin")
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	data, ok := store.Get(token)
	if !ok {
		t.Fatal("expected session to be valid")
	}
	if data.UserID != 1 {
		t.Errorf("expected user ID 1, got %d", data.UserID)
	}
	if data.Username != "testuser" {
		t.Errorf("expected username testuser, got %s", data.Username)
	}
	if data.Role != "admin" {
		t.Errorf("expected role admin, got %s", data.Role)
	}
}

func TestSessionStore_Valid(t *testing.T) {
	store := NewSessionStore()
	token := store.Create(1, "user", "admin")

	if !store.Valid(token) {
		t.Error("expected token to be valid")
	}

	if store.Valid("nonexistent-token") {
		t.Error("expected nonexistent token to be invalid")
	}
}

func TestSessionStore_Delete(t *testing.T) {
	store := NewSessionStore()
	token := store.Create(1, "user", "admin")

	store.Delete(token)
	if store.Valid(token) {
		t.Error("expected deleted token to be invalid")
	}
}

func TestSessionStore_Expiry(t *testing.T) {
	store := NewSessionStore()
	token := store.Create(1, "user", "admin")

	// Manually expire the session
	store.mu.Lock()
	store.sessions[token].Expiry = time.Now().Add(-1 * time.Hour)
	store.mu.Unlock()

	if store.Valid(token) {
		t.Error("expected expired token to be invalid")
	}

	// Should also be cleaned up from the store
	store.mu.RLock()
	_, exists := store.sessions[token]
	store.mu.RUnlock()
	if exists {
		t.Error("expected expired session to be deleted from store")
	}
}

func TestSessionStore_PendingTOTP(t *testing.T) {
	store := NewSessionStore()

	t.Run("create and validate", func(t *testing.T) {
		token := store.CreatePendingTOTP(42)
		if token == "" {
			t.Fatal("expected non-empty pending token")
		}

		userID, valid := store.ValidatePendingTOTP(token)
		if !valid {
			t.Error("expected pending TOTP to be valid")
		}
		if userID != 42 {
			t.Errorf("expected user ID 42, got %d", userID)
		}
	})

	t.Run("consumed after first use", func(t *testing.T) {
		token := store.CreatePendingTOTP(1)
		store.ValidatePendingTOTP(token)

		_, valid := store.ValidatePendingTOTP(token)
		if valid {
			t.Error("expected pending TOTP to be consumed after use")
		}
	})

	t.Run("expired pending TOTP", func(t *testing.T) {
		token := store.CreatePendingTOTP(1)

		store.mu.Lock()
		store.pendingTOTP[token].Expiry = time.Now().Add(-1 * time.Minute)
		store.mu.Unlock()

		_, valid := store.ValidatePendingTOTP(token)
		if valid {
			t.Error("expected expired pending TOTP to be invalid")
		}
	})

	t.Run("nonexistent token", func(t *testing.T) {
		_, valid := store.ValidatePendingTOTP("nonexistent")
		if valid {
			t.Error("expected nonexistent token to be invalid")
		}
	})
}

func TestSessionStore_UniqueTokens(t *testing.T) {
	store := NewSessionStore()
	tokens := make(map[string]bool)

	for i := 0; i < 100; i++ {
		token := store.Create(int64(i), "user", "admin")
		if tokens[token] {
			t.Fatalf("duplicate token generated: %s", token)
		}
		tokens[token] = true
	}
}

func TestHashPassword_And_CheckPassword(t *testing.T) {
	password := "mysecretpassword"
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hashPassword failed: %v", err)
	}

	if !checkPassword(password, hash) {
		t.Error("expected correct password to match")
	}

	if checkPassword("wrongpassword", hash) {
		t.Error("expected wrong password to not match")
	}

	if checkPassword("", hash) {
		t.Error("expected empty password to not match")
	}
}

func TestHashBackupCode(t *testing.T) {
	code := "12345678"
	hash1 := hashBackupCode(code)
	hash2 := hashBackupCode(code)

	if hash1 != hash2 {
		t.Error("expected same hash for same code")
	}

	hash3 := hashBackupCode("87654321")
	if hash1 == hash3 {
		t.Error("expected different hash for different code")
	}

	if len(hash1) != 64 {
		t.Errorf("expected SHA-256 hex length 64, got %d", len(hash1))
	}
}

func TestIsExempt(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/", true},
		{"/health", true},
		{"/api/health", true},
		{"/api/login", true},
		{"/api/login/totp", true},
		{"/api/register", true},
		{"/api/auth/status", true},
		{"/readyz", true},
		{"/torznab/api", true},
		{"/torznab/api?t=caps", true},
		{"/static/style.css", true},
		{"/opds", true},
		{"/opds/books", true},
		{"/metrics", true},
		{"/auth/oidc/callback", true},
		{"/api/search", false},
		{"/api/download", false},
		{"/api/library", false},
		{"/api/users", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := isExempt(tt.path)
			if result != tt.expected {
				t.Errorf("isExempt(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}
