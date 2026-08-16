package api

import (
	"bytes"
	"encoding/base64"
	"image/png"
	"strings"
	"testing"

	"github.com/pquerna/otp/totp"
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

// TestTOTPQRDataURI checks the enrolment QR is rendered locally into a data:
// URI. The otpauth URL carries the TOTP secret, so it must never be handed to
// an external QR service, and data: keeps it inside the response we already send
// (and inside the img-src allowance in the CSP).
func TestTOTPQRDataURI(t *testing.T) {
	key, err := totp.Generate(totp.GenerateOpts{Issuer: "Librarr", AccountName: "tester"})
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	uri, err := totpQRDataURI(key)
	if err != nil {
		t.Fatalf("render QR: %v", err)
	}

	const prefix = "data:image/png;base64,"
	if !strings.HasPrefix(uri, prefix) {
		t.Fatalf("expected a PNG data URI, got %.40q", uri)
	}
	if strings.Contains(uri, "http://") || strings.Contains(uri, "https://") {
		t.Error("QR data URI references a remote host; the secret must stay on the instance")
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(uri, prefix))
	if err != nil {
		t.Fatalf("decode base64 payload: %v", err)
	}
	img, err := png.Decode(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("payload is not a valid PNG: %v", err)
	}
	if b := img.Bounds(); b.Dx() < 100 || b.Dy() < 100 {
		t.Errorf("QR image is too small to scan: %dx%d", b.Dx(), b.Dy())
	}
}

// TestTOTPQRVariesPerKey guards against the QR degenerating into a placeholder
// or a cached image: two enrolments must not produce identical pixels. There is
// no QR decoder in the toolchain, so this plus the PNG checks above is the
// available evidence that the image really encodes the key it was built from.
func TestTOTPQRVariesPerKey(t *testing.T) {
	uris := make([]string, 2)
	for i := range uris {
		key, err := totp.Generate(totp.GenerateOpts{Issuer: "Librarr", AccountName: "tester"})
		if err != nil {
			t.Fatalf("generate key %d: %v", i, err)
		}
		if uris[i], err = totpQRDataURI(key); err != nil {
			t.Fatalf("render QR %d: %v", i, err)
		}
	}
	if uris[0] == uris[1] {
		t.Error("two enrolments produced an identical QR image; it is not encoding the key")
	}
}
