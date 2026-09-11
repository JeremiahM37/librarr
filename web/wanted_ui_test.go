package web

import (
	"strings"
	"testing"
)

// These routes are the data contracts used by the React Wanted and Settings
// components. E2E exercises their interactions; this catches a stale bundle
// being embedded without the feature entrypoints being compiled at all.
func TestWantedBundleContainsFeatureRoutes(t *testing.T) {
	b, err := StaticFS.ReadFile("static/react/librarr.js")
	if err != nil {
		t.Fatal(err)
	}
	bundle := string(b)
	for _, route := range []string{"/api/wishlist", "/api/scheduler/run"} {
		if !strings.Contains(bundle, route) {
			t.Errorf("React bundle does not include Wanted feature route %q", route)
		}
	}
}
