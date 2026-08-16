package web

import (
	"regexp"
	"testing"
)

// The frontend refactor that externalized the JS dropped the TOTP settings
// markup while leaving the handlers, their i18n strings, and the backend routes
// in place — so 2FA could not be enabled from the UI at all, silently, because
// loadTOTPStatus early-returns when its container is missing. These tests pin
// the contract between index.html and app.js so that drift fails the build.

func appJS(t *testing.T) string {
	t.Helper()
	b, err := StaticFS.ReadFile("static/js/app.js")
	if err != nil {
		t.Fatalf("read app.js: %v", err)
	}
	return string(b)
}

// TestTOTPSettingsMarkupExists checks every element the TOTP settings handlers
// address is present in the shipped HTML.
func TestTOTPSettingsMarkupExists(t *testing.T) {
	html := string(IndexHTML)
	for _, id := range []string{
		"totp-settings",
		"totp-disabled-section",
		"totp-enabled-section",
		"totp-setup-section",
		"totp-disable-section",
		"totp-secret-display",
		"totp-otpauth-uri",
		"totp-qr-wrap",
		"totp-qr-img",
		"totp-backup-codes",
		"totp-verify-code",
		"totp-disable-code",
	} {
		if !regexp.MustCompile(`id="` + regexp.QuoteMeta(id) + `"`).MatchString(html) {
			t.Errorf("index.html is missing id=%q, which app.js addresses by ID", id)
		}
	}
}

// TestClickActionsAreRegistered checks every data-action in the static markup
// resolves to a handler in the CLICK_ACTIONS registry. A button wired to a
// missing action fails silently at runtime — the dispatcher returns early.
func TestClickActionsAreRegistered(t *testing.T) {
	js := appJS(t)

	start := regexp.MustCompile(`(?m)^const CLICK_ACTIONS = \{`).FindStringIndex(js)
	if start == nil {
		t.Fatal("could not locate the CLICK_ACTIONS registry in app.js")
	}
	end := regexp.MustCompile(`(?m)^\};`).FindStringIndex(js[start[1]:])
	if end == nil {
		t.Fatal("could not locate the end of the CLICK_ACTIONS registry")
	}
	registry := js[start[1] : start[1]+end[0]]

	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`data-action="([A-Za-z0-9_]+)"`).FindAllStringSubmatch(string(IndexHTML), -1) {
		action := m[1]
		if seen[action] {
			continue
		}
		seen[action] = true
		if !regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(action) + `\s*:`).MatchString(registry) {
			t.Errorf("index.html wires data-action=%q but CLICK_ACTIONS has no such handler", action)
		}
	}
	if len(seen) == 0 {
		t.Fatal("found no data-action attributes in index.html — the scan is broken")
	}
}

// TestNoThirdPartyTOTPQR guards the regression this PR fixes: rendering the
// enrolment QR through an external service sent the otpauth URL — which carries
// the TOTP secret — off-box, and broke the offline-by-default guarantee in
// embed.go.
func TestNoThirdPartyTOTPQR(t *testing.T) {
	if regexp.MustCompile(`api\.qrserver\.com|chart\.googleapis\.com/chart\?.*qr`).MatchString(appJS(t)) {
		t.Error("app.js builds a TOTP QR code through a third-party service; the secret must not leave the instance")
	}
}
