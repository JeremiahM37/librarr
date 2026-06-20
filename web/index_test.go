package web

import (
	"regexp"
	"strings"
	"testing"
)

// TestIntegrationSaveNotBrokenByUnguardedListener is a regression test for the
// "cannot save integration settings" bug (issue #71).
//
// The change-password card is only rendered for DB-backed accounts, so on
// env-credential / no-auth installs document.getElementById('change-password-form')
// is null. A top-level, UNGUARDED `.addEventListener` on that null threw at
// script-load time and aborted the rest of the inline <script>. Every later
// statement never ran, so `const` declarations further down the script —
// notably INTEGRATION_FIELDS — were left uninitialized in the temporal dead
// zone. saveIntegration() (a hoisted function still reachable via its onclick)
// then threw "Cannot access 'INTEGRATION_FIELDS' before initialization" and the
// Save button silently did nothing.
//
// The fix guards the registration with optional chaining
// (getElementById('change-password-form')?.addEventListener), making a missing
// element a harmless no-op so the rest of the script — including the const
// declarations the Save button depends on — runs to completion.
//
// This test fails against the pre-fix HTML (unguarded `)` immediately followed
// by `.addEventListener`) and passes once the guard is in place.
func TestIntegrationSaveNotBrokenByUnguardedListener(t *testing.T) {
	html := string(IndexHTML)

	// Guard against the test passing vacuously: the machinery the bug broke
	// must actually be present in the shipped UI.
	for _, needle := range []string{
		"const INTEGRATION_FIELDS", // the declaration left in the TDZ
		"function saveIntegration", // the handler the Save buttons call
		"change-password-form",     // the conditionally-rendered element
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("expected %q in the embedded web UI; the regression test is "+
				"no longer exercising the integration-save code path", needle)
		}
	}

	// The bug: getElementById('change-password-form') whose result is
	// dereferenced immediately with `.addEventListener`, i.e. ")." with nothing
	// in between. The fix interposes a "?" → ")?." which this pattern must NOT
	// match. Any whitespace/newlines around the closing paren are tolerated so
	// the test tracks intent rather than exact formatting.
	unguarded := regexp.MustCompile(`getElementById\(\s*['"]change-password-form['"]\s*\)\s*\.addEventListener`)
	if loc := unguarded.FindStringIndex(html); loc != nil {
		t.Errorf("change-password-form listener is registered without a null guard "+
			"at byte offset %d. On installs where that element is absent this throws "+
			"at the top level and aborts the inline script, leaving INTEGRATION_FIELDS "+
			"uninitialized so saveIntegration() fails. Use ?.addEventListener.", loc[0])
	}
}
