package search

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// WebNovel searches multiple web novel sites for light novels.
type WebNovel struct {
	cfg    *config.Config
	client *http.Client
}

func NewWebNovel(cfg *config.Config, client *http.Client) *WebNovel {
	return &WebNovel{cfg: cfg, client: client}
}

func (w *WebNovel) Name() string         { return "webnovel" }
func (w *WebNovel) Label() string        { return "Web Novels" }
func (w *WebNovel) Enabled() bool        { return w.cfg.WebNovelEnabled }
func (w *WebNovel) SearchTab() string    { return "main" }
func (w *WebNovel) DownloadType() string { return "direct" }

type webNovelResult struct {
	Title  string
	URL    string
	Site   string
	Author string
}

func (w *WebNovel) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	type searchFunc func(ctx context.Context, query string) []webNovelResult

	searchers := []searchFunc{
		w.searchAllNovelFull,
		w.searchReadNovelFull,
		w.searchNovelFull,
		w.searchFreeWebNovel,
		w.searchNovelBin,
		w.searchLightNovelPub,
		w.searchBoxNovel,
	}

	var mu sync.Mutex
	var allResults []webNovelResult
	var wg sync.WaitGroup

	for _, fn := range searchers {
		wg.Add(1)
		go func(f searchFunc) {
			defer wg.Done()
			res := f(ctx, query)
			mu.Lock()
			allResults = append(allResults, res...)
			mu.Unlock()
		}(fn)
	}
	wg.Wait()

	// Deduplicate by normalized title, keeping preferred sites.
	grouped := make(map[string]*webNovelResult)
	wnNormRe := regexp.MustCompile(`[^a-z0-9]`)
	for _, r := range allResults {
		r := r
		key := wnNormRe.ReplaceAllString(strings.ToLower(r.Title), "")
		if _, exists := grouped[key]; !exists {
			grouped[key] = &r
		} else {
			existing := grouped[key]
			if !strings.Contains(existing.Site, r.Site) {
				existing.Site += ", " + r.Site
			}
		}
	}

	// Filter by query relevance.
	qWords := extractWords(strings.ToLower(query))
	var results []models.SearchResult
	for _, r := range grouped {
		tWords := extractWords(strings.ToLower(r.Title))
		overlap := 0
		for w := range qWords {
			if tWords[w] {
				overlap++
			}
		}
		if len(qWords) > 0 && overlap == 0 {
			continue
		}

		sizeHuman := r.Site
		if sizeHuman == "" {
			sizeHuman = "Web Novel"
		}

		results = append(results, models.SearchResult{
			Source:    "webnovel",
			Title:     r.Title,
			Author:    r.Author,
			URL:       r.URL,
			SizeHuman: sizeHuman,
			MediaType: "ebook",
		})
	}

	return results, nil
}

func (w *WebNovel) doGet(ctx context.Context, urlStr string, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", w.cfg.UserAgent)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return w.client.Do(req)
}

func (w *WebNovel) searchFreeWebNovel(ctx context.Context, query string) []webNovelResult {
	var results []webNovelResult
	site := w.cfg.Sources.WebNovel("freewebnovel")
	if site == nil {
		return results
	}
	req, err := http.NewRequestWithContext(ctx, "GET", site.URL, nil)
	if err != nil {
		return results
	}
	q := req.URL.Query()
	q.Set("searchkey", query)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", w.cfg.UserAgent)

	resp, err := w.client.Do(req)
	if err != nil {
		slog.Debug("FreeWebNovel search failed", "error", err)
		return results
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return results
	}

	doc.Find("div.li-row").Each(func(_ int, s *goquery.Selection) {
		titleLink := s.Find("a.tit")
		title := strings.TrimSpace(titleLink.Text())
		href, _ := titleLink.Attr("href")
		author := strings.TrimSpace(s.Find("span.s1").Text())
		if title != "" {
			results = append(results, webNovelResult{
				Title: title, URL: href, Site: "FreeWebNovel", Author: author,
			})
		}
	})
	return results
}

var wnNovelTitleRe = regexp.MustCompile(`<h3[^>]*class="[^"]*truyen-title[^"]*"[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>`)
var wnNovelListRe = regexp.MustCompile(`<a\s+href="([^"]+)"[^>]*class="list-group-item"[^>]*title="([^"]+)"`)
var wnBoxNovelRe = regexp.MustCompile(`<div class="post-title">\s*<h3[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>`)

func (w *WebNovel) searchAllNovelFull(ctx context.Context, query string) []webNovelResult {
	site := w.cfg.Sources.WebNovel("allnovelfull")
	if site == nil {
		return nil
	}
	return w.searchRegexSite(ctx, site.URL, "keyword", query, site.BaseURL, site.Label, wnNovelTitleRe)
}

func (w *WebNovel) searchBoxNovel(ctx context.Context, query string) []webNovelResult {
	site := w.cfg.Sources.WebNovel("boxnovel")
	if site == nil {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, "GET", site.URL, nil)
	if err != nil {
		return nil
	}
	q := req.URL.Query()
	q.Set("s", query)
	q.Set("post_type", "wp-manga")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", w.cfg.UserAgent)

	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	body := wnReadBodyString(resp)
	return wnExtractRegexResults(body, wnBoxNovelRe, "", "BoxNovel")
}

func (w *WebNovel) searchNovelBin(ctx context.Context, query string) []webNovelResult {
	site := w.cfg.Sources.WebNovel("novelbin")
	if site == nil {
		return nil
	}
	resp, err := w.doGet(ctx, fmt.Sprintf("%s?keyword=%s", site.URL, query), nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	body := wnReadBodyString(resp)
	// Match links to novel-book pages on the configured base host. Accept either
	// http:// or https:// so a scheme mismatch in the served HTML doesn't drop
	// results (preserves pre-registry behavior).
	host := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(site.BaseURL, "https://"), "http://"), "/")
	re := regexp.MustCompile(`<a\s+href="(https?://` + regexp.QuoteMeta(host) + `/novel-book/[^"]+)"[^>]*title="([^"]+)"`)
	results := wnExtractRegexResults(body, re, "", site.Label)

	var filtered []webNovelResult
	seen := make(map[string]bool)
	for _, r := range results {
		if strings.Contains(r.URL, "/cchapter-") || strings.Contains(r.URL, "/chapter-") {
			continue
		}
		if seen[r.URL] {
			continue
		}
		seen[r.URL] = true
		filtered = append(filtered, r)
	}
	return filtered
}

func (w *WebNovel) searchNovelFull(ctx context.Context, query string) []webNovelResult {
	site := w.cfg.Sources.WebNovel("novelfull")
	if site == nil {
		return nil
	}
	return w.searchAjaxSite(ctx, site.URL, query, site.BaseURL, site.Label)
}

func (w *WebNovel) searchLightNovelPub(ctx context.Context, query string) []webNovelResult {
	site := w.cfg.Sources.WebNovel("lightnovelpub")
	if site == nil {
		return nil
	}
	resp, err := w.doGet(ctx, fmt.Sprintf("%s?inputContent=%s", site.URL, query),
		map[string]string{"X-Requested-With": "XMLHttpRequest"})
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 {
		return nil
	}

	body := wnReadBodyString(resp)

	// Try JSON parse first.
	type lnpResult struct {
		NovelName     string `json:"novelName"`
		NovelNameHref string `json:"novelNameHref"`
	}
	type lnpResponse struct {
		ResultList []lnpResult `json:"resultlist"`
	}

	var data lnpResponse
	if err := json.Unmarshal([]byte(body), &data); err == nil && len(data.ResultList) > 0 {
		var results []webNovelResult
		for _, item := range data.ResultList {
			if item.NovelName == "" || item.NovelNameHref == "" {
				continue
			}
			url := item.NovelNameHref
			if !strings.HasPrefix(url, "http") {
				url = site.BaseURL + url
			}
			results = append(results, webNovelResult{
				Title: strings.TrimSpace(item.NovelName),
				URL:   url,
				Site:  site.Label,
			})
		}
		return results
	}

	// Fallback to HTML regex.
	re := regexp.MustCompile(`<a\s+href="(/novel/[^"]+)"[^>]*>([^<]+)</a>`)
	return wnExtractRegexResults(body, re, site.BaseURL, site.Label)
}

func (w *WebNovel) searchReadNovelFull(ctx context.Context, query string) []webNovelResult {
	site := w.cfg.Sources.WebNovel("readnovelfull")
	if site == nil {
		return nil
	}
	return w.searchAjaxSite(ctx, site.URL, query, site.BaseURL, site.Label)
}

func (w *WebNovel) searchRegexSite(ctx context.Context, baseURL, paramName, query, baseHost, site string, re *regexp.Regexp) []webNovelResult {
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL, nil)
	if err != nil {
		return nil
	}
	q := req.URL.Query()
	q.Set(paramName, query)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", w.cfg.UserAgent)

	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	body := wnReadBodyString(resp)
	return wnExtractRegexResults(body, re, baseHost, site)
}

func (w *WebNovel) searchAjaxSite(ctx context.Context, ajaxURL, query, baseHost, site string) []webNovelResult {
	resp, err := w.doGet(ctx, fmt.Sprintf("%s?keyword=%s", ajaxURL, query),
		map[string]string{"X-Requested-With": "XMLHttpRequest"})
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}

	body := wnReadBodyString(resp)
	results := wnExtractRegexResults(body, wnNovelListRe, baseHost, site)

	var filtered []webNovelResult
	for _, r := range results {
		lower := strings.ToLower(r.Title)
		if strings.Contains(lower, "see more") || strings.Contains(r.URL, "search?") {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

func wnExtractRegexResults(body string, re *regexp.Regexp, baseHost, site string) []webNovelResult {
	matches := re.FindAllStringSubmatch(body, -1)
	var results []webNovelResult
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		url := m[1]
		title := strings.TrimSpace(m[2])
		if !strings.HasPrefix(url, "http") && baseHost != "" {
			url = baseHost + url
		}
		results = append(results, webNovelResult{
			Title: title, URL: url, Site: site,
		})
	}
	return results
}

func wnReadBodyString(resp *http.Response) string {
	body := make([]byte, 0, 64*1024)
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			body = append(body, buf[:n]...)
		}
		if err != nil {
			break
		}
		if len(body) > 2*1024*1024 {
			break
		}
	}
	return string(body)
}
