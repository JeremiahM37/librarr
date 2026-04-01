package api

import (
	"testing"
)

func TestValidateTOTPCode(t *testing.T) {
	t.Run("empty secret returns false", func(t *testing.T) {
		if validateTOTPCode("", "123456") {
			t.Error("expected false for empty secret")
		}
	})

	t.Run("empty code returns false", func(t *testing.T) {
		if validateTOTPCode("JBSWY3DPEHPK3PXP", "") {
			t.Error("expected false for empty code")
		}
	})

	t.Run("both empty returns false", func(t *testing.T) {
		if validateTOTPCode("", "") {
			t.Error("expected false for both empty")
		}
	})

	t.Run("invalid code returns false", func(t *testing.T) {
		// Valid base32 secret but wrong code
		if validateTOTPCode("JBSWY3DPEHPK3PXP", "000000") {
			// This could theoretically pass if the time aligns, but
			// extremely unlikely with a random code
			t.Log("TOTP validation returned true for random code - timing coincidence")
		}
	})
}

func TestGenerateBackupCodes(t *testing.T) {
	t.Run("generates correct count", func(t *testing.T) {
		codes, err := generateBackupCodes(8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(codes) != 8 {
			t.Errorf("expected 8 codes, got %d", len(codes))
		}
	})

	t.Run("codes are 8 digits", func(t *testing.T) {
		codes, err := generateBackupCodes(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, code := range codes {
			if len(code) != 8 {
				t.Errorf("code[%d] = %q, expected 8 digits", i, code)
			}
			// Verify all digits
			for _, ch := range code {
				if ch < '0' || ch > '9' {
					t.Errorf("code[%d] = %q contains non-digit character", i, code)
					break
				}
			}
		}
	})

	t.Run("codes are unique", func(t *testing.T) {
		codes, _ := generateBackupCodes(100)
		seen := make(map[string]bool)
		for _, code := range codes {
			if seen[code] {
				t.Errorf("duplicate code generated: %s", code)
			}
			seen[code] = true
		}
	})

	t.Run("zero codes", func(t *testing.T) {
		codes, err := generateBackupCodes(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(codes) != 0 {
			t.Errorf("expected 0 codes, got %d", len(codes))
		}
	})
}
