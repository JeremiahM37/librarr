package search

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/sources/sourcestest"
)

type abbRoundTripper struct {
	body string
	req  *http.Request
}

func (r *abbRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.req = req.Clone(req.Context())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(r.body)),
		Request:    req,
	}, nil
}

func TestAudioBookBaySearchSetsBrowserHeaders(t *testing.T) {
	rt := &abbRoundTripper{
		body: `<html><body>
		<div class="post">
			<div class="postTitle"><h2><a href="/abss/the-martian-andy-weir/">The Martian - Andy Weir</a></h2></div>
			<div class="postInfo">Language: English <span style="margin-left:100px;">Keywords:</div>
		</div>
		</body></html>`,
	}
	reg, err := sourcestest.Registry()
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	cfg := &config.Config{
		UserAgent: "test",
		Sources:   reg,
	}
	cfg.Sources.AudioBookBay.Mirrors = []string{"audiobookbay.lu"}
	a := &AudioBookBay{cfg: cfg, client: &http.Client{Transport: rt}}

	results, err := a.searchDomain(context.Background(), "audiobookbay.lu", "The Martian")
	if err != nil {
		t.Fatalf("searchDomain returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if got := rt.req.Header.Get("User-Agent"); got != abbBrowserUserAgent {
		t.Fatalf("User-Agent = %q, want %q", got, abbBrowserUserAgent)
	}
	if got := rt.req.Header.Get("Accept-Language"); got != "en-US,en;q=0.9" {
		t.Fatalf("Accept-Language = %q, want en-US,en;q=0.9", got)
	}
	if got := rt.req.Header.Get("Upgrade-Insecure-Requests"); got != "1" {
		t.Fatalf("Upgrade-Insecure-Requests = %q, want 1", got)
	}
}

func TestResolveABBMagnetUsesBrowserHeaders(t *testing.T) {
	rt := &abbRoundTripper{
		body: `<html><body>
		<h1>The Martian</h1>
		<table>
			<tr><td>Info Hash:</td><td>0123456789ABCDEF0123456789ABCDEF01234567</td></tr>
			<tr><td>udp://tracker.example:1337/announce</td></tr>
		</table>
		</body></html>`,
	}
	client := &http.Client{Transport: rt}

	magnet, err := ResolveABBMagnet(context.Background(), client, "test", "/abss/the-martian-andy-weir/", []string{"audiobookbay.lu"}, []string{"udp://fallback.example:1337/announce"})
	if err != nil {
		t.Fatalf("ResolveABBMagnet returned error: %v", err)
	}
	if !strings.HasPrefix(magnet, "magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567") {
		t.Fatalf("unexpected magnet: %s", magnet)
	}
	if got := rt.req.Header.Get("User-Agent"); got != abbBrowserUserAgent {
		t.Fatalf("User-Agent = %q, want %q", got, abbBrowserUserAgent)
	}
	if got := rt.req.Header.Get("Accept-Encoding"); got != "identity" {
		t.Fatalf("Accept-Encoding = %q, want identity", got)
	}
}

func TestResolveABBMagnetFallsBackToLegacyInfoHashRegex(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		infoHash string
	}{
		{
			name:     "same line text",
			body:     `<html><body><h1>The Martian</h1><div>Info Hash: 89ABCDEF0123456789ABCDEF0123456789ABCDEF</div></body></html>`,
			infoHash: "89ABCDEF0123456789ABCDEF0123456789ABCDEF",
		},
		{
			name:     "same line td",
			body:     `<html><body><h1>The Martian</h1><table><tr><td colspan="2">Info Hash: legacy layout</td></tr><tr><td>FEDCBA9876543210FEDCBA9876543210FEDCBA98</td></tr></table></body></html>`,
			infoHash: "FEDCBA9876543210FEDCBA9876543210FEDCBA98",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &abbRoundTripper{body: tt.body}
			client := &http.Client{Transport: rt}

			magnet, err := ResolveABBMagnet(context.Background(), client, "test", "/abss/the-martian-andy-weir/", []string{"audiobookbay.lu"}, []string{"udp://fallback.example:1337/announce"})
			if err != nil {
				t.Fatalf("ResolveABBMagnet returned error: %v", err)
			}
			if !strings.HasPrefix(magnet, "magnet:?xt=urn:btih:"+tt.infoHash) {
				t.Fatalf("unexpected magnet: %s", magnet)
			}
			if !strings.Contains(magnet, "tr=udp%3A%2F%2Ffallback.example%3A1337%2Fannounce") {
				t.Fatalf("expected fallback tracker in magnet: %s", magnet)
			}
		})
	}
}

func TestExtractABBInfoHash(t *testing.T) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(`<html><body>
		<table>
			<tr>
				<td>Info Hash:</td>
				<td>0123456789ABCDEF0123456789ABCDEF01234567</td>
			</tr>
		</table>
	</body></html>`))
	if err != nil {
		t.Fatalf("parse HTML: %v", err)
	}

	got := extractABBInfoHash(doc, regexp.MustCompile(`(?i)\b[0-9a-f]{40}\b`))
	want := "0123456789ABCDEF0123456789ABCDEF01234567"
	if got != want {
		t.Fatalf("extractABBInfoHash = %q, want %q", got, want)
	}
}
