// Package zlibraryparse parses Z-Library search and book-detail
// responses (JSON and HTML fallbacks) into structured data.
package zlibraryparse

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
)

type Book struct {
	ID          int
	Hash        string
	Title       string
	Author      string
	Extension   string
	Filesize    int64
	Language    string
	Year        any
	Pages       int
	Cover       string
	Description string
	DL          string
}

func BooksFromJSON(body []byte) ([]Book, error) {
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, err
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("zlibrary response is not an object")
	}
	if !JSONTruthy(obj["success"], true) {
		return nil, fmt.Errorf("zlibrary request failed: %s", ErrorMessage(obj))
	}

	for _, candidate := range arrayCandidates(obj, []string{"books", "result", "results", "data", "items"}) {
		books := normalizeBooks(candidate)
		if len(books) > 0 {
			return books, nil
		}
	}
	return nil, nil
}

func DetailDownloadFromJSON(body []byte) (string, error) {
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return "", err
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return "", fmt.Errorf("zlibrary detail response is not an object")
	}
	if !JSONTruthy(obj["success"], true) {
		return "", fmt.Errorf("zlibrary detail failed: %s", ErrorMessage(obj))
	}

	for _, candidate := range objectCandidates(obj, []string{"book", "data", "response", "result", "item"}) {
		if dl := firstString(candidate, "dl", "downloadUrl", "download_url", "href", "url"); dl != "" {
			return dl, nil
		}
	}
	if dl := firstString(obj, "dl", "downloadUrl", "download_url", "href", "url"); dl != "" {
		return dl, nil
	}
	return "", nil
}

func FindDownloadLinkInHTML(baseURL string, body []byte) string {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)href=["']([^"']*/dl/[^"']+)["']`),
		regexp.MustCompile(`(?i)["']((?:https?://[^"']+)?/dl/[^"']+)["']`),
	}
	for _, pattern := range patterns {
		if match := pattern.FindSubmatch(body); len(match) > 1 {
			return AbsoluteURL(baseURL, htmlUnescape(string(match[1])))
		}
	}
	return ""
}

func JSONTruthy(v any, defaultValue bool) bool {
	if v == nil {
		return defaultValue
	}
	switch value := v.(type) {
	case bool:
		return value
	case float64:
		return value != 0
	case int:
		return value != 0
	case string:
		return value == "1" || strings.EqualFold(value, "true") || strings.EqualFold(value, "ok") || strings.EqualFold(value, "success")
	default:
		return defaultValue
	}
}

func ErrorMessage(obj map[string]any) string {
	for _, key := range []string{"error", "message", "msg", "reason"} {
		if value := stringValue(obj[key]); value != "" {
			return value
		}
	}
	if errorsValue, ok := obj["errors"].([]any); ok {
		var parts []string
		for _, item := range errorsValue {
			if value := stringValue(item); value != "" {
				parts = append(parts, value)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "; ")
		}
	}
	return "unknown error"
}

func AbsoluteURL(baseURL, path string) string {
	if path == "" || strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	base, err := url.Parse(strings.TrimRight(baseURL, "/") + "/")
	if err != nil {
		return path
	}
	ref, err := url.Parse(path)
	if err != nil {
		return path
	}
	return base.ResolveReference(ref).String()
}

func arrayCandidates(obj map[string]any, keys []string) [][]any {
	var candidates [][]any
	for _, key := range keys {
		if items, ok := obj[key].([]any); ok {
			candidates = append(candidates, items)
		}
		if nested, ok := obj[key].(map[string]any); ok {
			candidates = append(candidates, arrayCandidates(nested, keys)...)
		}
	}
	return candidates
}

func objectCandidates(obj map[string]any, keys []string) []map[string]any {
	var candidates []map[string]any
	for _, key := range keys {
		if nested, ok := obj[key].(map[string]any); ok {
			candidates = append(candidates, nested)
		}
	}
	return candidates
}

func normalizeBooks(items []any) []Book {
	books := make([]Book, 0, len(items))
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		books = append(books, Book{
			ID:          intValue(obj["id"]),
			Hash:        firstString(obj, "hash", "bookHash", "book_hash", "md5"),
			Title:       firstString(obj, "title", "name", "bookTitle", "book_title"),
			Author:      firstString(obj, "author", "authors", "authorName", "author_name"),
			Extension:   firstString(obj, "extension", "ext", "format", "fileType", "file_type"),
			Filesize:    int64Value(firstPresent(obj, "filesize", "fileSize", "file_size", "size", "sizeBytes", "size_bytes")),
			Language:    firstString(obj, "language", "lang"),
			Year:        firstPresent(obj, "year", "published", "publishYear", "publish_year"),
			Pages:       intValue(firstPresent(obj, "pages", "pageCount", "page_count")),
			Cover:       firstString(obj, "cover", "coverUrl", "cover_url", "image", "thumbnail"),
			Description: firstString(obj, "description", "desc", "summary"),
			DL:          firstString(obj, "dl", "downloadUrl", "download_url", "href", "url"),
		})
	}
	return books
}

func firstPresent(obj map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := obj[key]; ok {
			return value
		}
	}
	return nil
}

func firstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringValue(obj[key]); value != "" {
			return value
		}
	}
	return ""
}

func stringValue(v any) string {
	switch value := v.(type) {
	case string:
		return strings.TrimSpace(value)
	case []any:
		var parts []string
		for _, item := range value {
			if part := stringValue(item); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, ", ")
	case map[string]any:
		return firstString(value, "name", "title", "value")
	default:
		return ""
	}
}

func intValue(v any) int {
	n := int64Value(v)
	if n > int64(math.MaxInt) {
		return 0
	}
	return int(n)
}

func int64Value(v any) int64 {
	switch value := v.(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case json.Number:
		n, _ := value.Int64()
		return n
	case string:
		value = strings.TrimSpace(value)
		value = strings.ReplaceAll(value, ",", "")
		var n int64
		if _, err := fmt.Sscanf(value, "%d", &n); err == nil {
			return n
		}
	}
	return 0
}

func htmlUnescape(s string) string {
	replacer := strings.NewReplacer("&amp;", "&", "&#38;", "&")
	return replacer.Replace(s)
}
