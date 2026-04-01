package api

import (
	"strings"
	"testing"
)

func TestXmlEscape(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Hello", "Hello"},
		{"A & B", "A &amp; B"},
		{"<tag>", "&lt;tag&gt;"},
		{`"quoted"`, "&quot;quoted&quot;"},
		{"it's", "it&apos;s"},
		{"A & B < C > D \"E\" F'G", "A &amp; B &lt; C &gt; D &quot;E&quot; F&apos;G"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := xmlEscape(tt.input)
			if result != tt.expected {
				t.Errorf("xmlEscape(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestOpdsNow(t *testing.T) {
	result := opdsNow()
	if !strings.HasSuffix(result, "Z") {
		t.Errorf("expected UTC timestamp ending with Z, got %s", result)
	}
	if len(result) != 20 {
		t.Errorf("expected timestamp length 20, got %d: %s", len(result), result)
	}
}

func TestOpdsFeedOpen(t *testing.T) {
	result := opdsFeedOpen("test-id", "Test Feed", "navigation", "/opds/", 100, 1)

	if !strings.Contains(result, "urn:librarr:test-id") {
		t.Error("expected feed ID in output")
	}
	if !strings.Contains(result, "<title>Test Feed</title>") {
		t.Error("expected title in output")
	}
	if !strings.Contains(result, `<opensearch:totalResults>100</opensearch:totalResults>`) {
		t.Error("expected total results in output")
	}
	if !strings.Contains(result, `<opensearch:startIndex>1</opensearch:startIndex>`) {
		t.Error("expected start index 1 for page 1")
	}
}

func TestOpdsFeedOpen_Page2(t *testing.T) {
	result := opdsFeedOpen("id", "Feed", "acquisition", "/opds/books?page=2", 200, 2)

	expectedStartIndex := (2-1)*opdsPageSize + 1
	if !strings.Contains(result, "<opensearch:startIndex>51</opensearch:startIndex>") {
		t.Errorf("expected start index %d, output: %s", expectedStartIndex, result)
	}
}

func TestOpdsNavEntry(t *testing.T) {
	result := opdsNavEntry("lib", "My Library", "Browse books", "/opds/books", opdsAcqMIME)

	if !strings.Contains(result, "<title>My Library</title>") {
		t.Error("expected title in entry")
	}
	if !strings.Contains(result, "urn:librarr:lib") {
		t.Error("expected entry ID")
	}
	if !strings.Contains(result, `href="/opds/books"`) {
		t.Error("expected href in entry")
	}
}

func TestFormatMIMEs(t *testing.T) {
	tests := []struct {
		format   string
		expected string
	}{
		{"epub", "application/epub+zip"},
		{"pdf", "application/pdf"},
		{"mobi", "application/x-mobipocket-ebook"},
		{"mp3", "audio/mpeg"},
		{"m4b", "audio/mp4"},
		{"cbz", "application/x-cbz"},
		{"cbr", "application/x-cbr"},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			got := formatMIMEs[tt.format]
			if got != tt.expected {
				t.Errorf("formatMIMEs[%q] = %q, want %q", tt.format, got, tt.expected)
			}
		})
	}

	t.Run("unknown format", func(t *testing.T) {
		got := formatMIMEs["xyz"]
		if got != "" {
			t.Errorf("expected empty string for unknown format, got %q", got)
		}
	})
}
