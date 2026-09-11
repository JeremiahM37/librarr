package web

import (
	"strings"
	"testing"
)

// The browser UI is a TypeScript/React production bundle. Keep this test at
// the serving boundary: Go must embed the entrypoint that index.html loads.
func TestReactBundleIsEmbedded(t *testing.T) {
	html := string(IndexHTML)
	if !strings.Contains(html, `id="root"`) {
		t.Fatal("index.html is missing the React root")
	}
	if !strings.Contains(html, `/static/react/librarr.js`) {
		t.Fatal("index.html does not load the React production entrypoint")
	}
	for _, name := range []string{"librarr.js", "librarr.css"} {
		if !strings.Contains(html, "/static/react/"+name) {
			t.Errorf("React entry does not load %s", name)
		}
		if data, err := StaticFS.ReadFile("static/react/" + name); err != nil || len(data) == 0 {
			t.Errorf("React asset %s is missing or empty: %v", name, err)
		}
	}
}

func TestReactEntryRetainsStrictCSPCompatibility(t *testing.T) {
	html := string(IndexHTML)
	if strings.Contains(html, "<script>") || strings.Contains(html, "onclick=") {
		t.Fatal("the React entry document contains inline executable code")
	}
}

// Enrollment/backup-code/disable behavior and zero remote secret requests are
// verified in e2e/test_react_totp.py, replacing legacy markup-ID inspection.
func TestReactEntryDoesNotLoadLegacyApplication(t *testing.T) {
	if strings.Contains(string(IndexHTML), `src="/static/js/app.js"`) {
		t.Fatal("legacy imperative app is loaded beside React")
	}
}
