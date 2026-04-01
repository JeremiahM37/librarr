package api

import (
	"testing"
)

func TestRateLimiter_RuleForPath(t *testing.T) {
	rl := NewRateLimiter(60, map[string]int{
		"login":    5,
		"search":   30,
		"download": 20,
		"api":      100,
		"default":  600,
	})

	tests := []struct {
		path     string
		expected string
	}{
		{"/api/login", "login"},
		{"/api/search", "search"},
		{"/api/search/ebooks", "search"},
		{"/api/download", "download"},
		{"/api/download/123", "download"},
		{"/api/library", "api"},
		{"/api/users", "api"},
		{"/some/other/path", "default"},
		{"/", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := rl.ruleForPath(tt.path)
			if result != tt.expected {
				t.Errorf("ruleForPath(%q) = %s, want %s", tt.path, result, tt.expected)
			}
		})
	}
}

func TestRateLimiter_Check_AllowsUnderLimit(t *testing.T) {
	rl := NewRateLimiter(60, map[string]int{
		"login":   3,
		"default": 100,
	})

	for i := 0; i < 3; i++ {
		allowed, _, _, _ := rl.Check("192.168.1.1", "/api/login")
		if !allowed {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
}

func TestRateLimiter_Check_BlocksOverLimit(t *testing.T) {
	rl := NewRateLimiter(60, map[string]int{
		"login":   3,
		"default": 100,
	})

	for i := 0; i < 3; i++ {
		rl.Check("192.168.1.1", "/api/login")
	}

	allowed, retryAfter, rule, limit := rl.Check("192.168.1.1", "/api/login")
	if allowed {
		t.Error("expected request to be blocked after exceeding limit")
	}
	if retryAfter <= 0 {
		t.Error("expected positive retry-after")
	}
	if rule != "login" {
		t.Errorf("expected rule login, got %s", rule)
	}
	if limit != 3 {
		t.Errorf("expected limit 3, got %d", limit)
	}
}

func TestRateLimiter_Check_SeparateIdentities(t *testing.T) {
	rl := NewRateLimiter(60, map[string]int{
		"login":   2,
		"default": 100,
	})

	// Exhaust limit for IP 1
	rl.Check("10.0.0.1", "/api/login")
	rl.Check("10.0.0.1", "/api/login")

	// IP 2 should still be allowed
	allowed, _, _, _ := rl.Check("10.0.0.2", "/api/login")
	if !allowed {
		t.Error("different IP should have its own bucket")
	}
}

func TestRateLimiter_Check_SeparateRules(t *testing.T) {
	rl := NewRateLimiter(60, map[string]int{
		"login":   2,
		"search":  100,
		"default": 100,
	})

	// Exhaust login limit
	rl.Check("10.0.0.1", "/api/login")
	rl.Check("10.0.0.1", "/api/login")

	// Search should still work for same IP
	allowed, _, _, _ := rl.Check("10.0.0.1", "/api/search")
	if !allowed {
		t.Error("different rule should have its own bucket")
	}
}

func TestRateLimiter_Check_DefaultLimit(t *testing.T) {
	rl := NewRateLimiter(60, map[string]int{})

	// No rules defined, should use fallback of 600
	allowed, _, _, limit := rl.Check("10.0.0.1", "/unknown/path")
	if !allowed {
		t.Error("expected request to be allowed with default limit")
	}
	if limit != 600 {
		t.Errorf("expected default limit 600, got %d", limit)
	}
}
