package search

import (
	"regexp"
	"strings"

	"github.com/JeremiahM37/librarr/internal/models"
)

// ScoreBreakdown provides a detailed breakdown of a search result's confidence score.
type ScoreBreakdown struct {
	TitleMatch  float64 `json:"title_match"`  // 0-40 points
	AuthorMatch float64 `json:"author_match"` // 0-20 points
	FormatScore float64 `json:"format_score"` // 0-15 points (epub > pdf > other)
	SeederScore float64 `json:"seeder_score"` // 0-15 points
	SizeScore   float64 `json:"size_score"`   // 0-10 points (reasonable size range)
	Total       float64 `json:"total"`
	Confidence  string  `json:"confidence"` // "high", "medium", "low"
}

// ScoreResult scores a search result on a 0-100 confidence scale.
func ScoreResult(result models.SearchResult, query, author string) ScoreBreakdown {
	var sb ScoreBreakdown

	sb.TitleMatch = scoreTitleMatch(result.Title, query)
	sb.AuthorMatch = scoreAuthorMatch(result, author)
	sb.FormatScore = scoreFormat(result)
	sb.SeederScore = scoreSeeder(result)
	sb.SizeScore = scoreSize(result)

	sb.Total = sb.TitleMatch + sb.AuthorMatch + sb.FormatScore + sb.SeederScore + sb.SizeScore

	switch {
	case sb.Total >= 70:
		sb.Confidence = "high"
	case sb.Total >= 40:
		sb.Confidence = "medium"
	default:
		sb.Confidence = "low"
	}

	return sb
}

// scoreTitleMatch scores the title similarity (0-40).
func scoreTitleMatch(title, query string) float64 {
	if query == "" {
		return 20 // neutral
	}
	tLower := strings.ToLower(title)
	qLower := strings.ToLower(query)

	// Exact match.
	if tLower == qLower {
		return 40
	}

	// Full query is a substring.
	if strings.Contains(tLower, qLower) {
		return 35
	}

	// Word overlap scoring.
	qWords := extractContentWords(qLower)
	tWords := extractContentWords(tLower)
	if len(qWords) == 0 {
		return 20
	}

	overlap := 0
	for _, w := range qWords {
		for _, tw := range tWords {
			if w == tw {
				overlap++
				break
			}
		}
	}

	ratio := float64(overlap) / float64(len(qWords))
	return ratio * 40
}

// scoreAuthorMatch scores how well the author matches (0-20).
func scoreAuthorMatch(result models.SearchResult, author string) float64 {
	if author == "" {
		return 10 // neutral when no author specified
	}

	authorLower := strings.ToLower(author)

	// Check result author field.
	if result.Author != "" {
		resultAuthor := strings.ToLower(result.Author)
		if resultAuthor == authorLower {
			return 20
		}
		if strings.Contains(resultAuthor, authorLower) || strings.Contains(authorLower, resultAuthor) {
			return 16
		}
		// Last name match.
		authorParts := strings.Fields(authorLower)
		resultParts := strings.Fields(resultAuthor)
		if len(authorParts) > 0 && len(resultParts) > 0 {
			if authorParts[len(authorParts)-1] == resultParts[len(resultParts)-1] {
				return 14
			}
		}
	}

	// Check if author appears in title.
	titleLower := strings.ToLower(result.Title)
	if strings.Contains(titleLower, authorLower) {
		return 12
	}

	// Check partial author match in title.
	authorParts := strings.Fields(authorLower)
	for _, part := range authorParts {
		if len(part) > 2 && strings.Contains(titleLower, part) {
			return 8
		}
	}

	return 0
}

// scoreFormat scores the file format preference (0-15).
func scoreFormat(result models.SearchResult) float64 {
	format := strings.ToLower(result.Format)

	// Try to extract format from title if not set.
	if format == "" {
		format = extractFormatFromTitle(result.Title)
	}

	switch format {
	case "epub":
		return 15
	case "mobi", "azw3":
		return 12
	case "cbz", "cbr":
		return 10
	case "pdf":
		return 8
	default:
		// DDL and torrent sources without explicit format get a neutral score.
		if result.Source == "annas" || result.Source == "gutenberg" || result.Source == "standardebooks" {
			return 12 // these sources typically have good formats
		}
		return 5
	}
}

// scoreSeeder scores based on seeder count or download type (0-15).
func scoreSeeder(result models.SearchResult) float64 {
	// Direct download sources get a fixed score.
	isDDL := result.Source == "annas" || result.Source == "annas_manga" ||
		result.Source == "gutenberg" || result.Source == "openlibrary" ||
		result.Source == "standardebooks" || result.Source == "librivox" ||
		result.Source == "mangadex" || result.Source == "webnovel"
	if isDDL {
		return 12
	}

	// Usenet has no seeders by definition (Prowlarr omits the key, so
	// result.Seeders unmarshals to 0) - scoring it on the torrent scale
	// would rank every NZB as if it were a dead torrent. An indexer still
	// listing it is the usenet equivalent of a live, well-seeded torrent.
	if result.DownloadProtocol == "nzb" {
		return 15
	}

	switch {
	case result.Seeders >= 20:
		return 15
	case result.Seeders >= 5:
		return 10
	case result.Seeders >= 1:
		return 5
	default:
		return 0
	}
}

// scoreSize scores based on whether the file size is in a reasonable range (0-10).
func scoreSize(result models.SearchResult) float64 {
	size := result.Size
	if size == 0 {
		size = int64(parseSizeBytes(result.SizeHuman))
	}
	if size == 0 {
		return 5 // unknown size, neutral
	}

	mediaType := result.MediaType
	if mediaType == "" {
		mediaType = guessMediaType(result)
	}

	switch mediaType {
	case "audiobook":
		// Audiobooks: 50MB - 5GB is ideal.
		switch {
		case size >= 50e6 && size <= 5e9:
			return 10
		case size >= 20e6 && size <= 10e9:
			return 7
		default:
			return 3
		}
	case "manga":
		// Manga: 1MB - 500MB is ideal.
		switch {
		case size >= 1e6 && size <= 500e6:
			return 10
		case size >= 500e3 && size <= 1e9:
			return 7
		default:
			return 3
		}
	default:
		// Ebooks: 0.1MB - 50MB is ideal.
		switch {
		case size >= 100e3 && size <= 50e6:
			return 10
		case size >= 50e3 && size <= 200e6:
			return 7
		default:
			return 3
		}
	}
}

// extractContentWords splits a string into meaningful words (excluding stopwords).
func extractContentWords(s string) []string {
	matches := wordRe.FindAllString(s, -1)
	var words []string
	for _, w := range matches {
		w = strings.ToLower(w)
		if !stopwords[w] && len(w) > 1 {
			words = append(words, w)
		}
	}
	return words
}

var formatTokenRe = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])(epub|mobi|pdf|azw3|cbz|cbr|djvu|fb2|m4b|mp3|m4a|aac|flac|ogg|zip)(?:$|[^a-z0-9])`)

// extractFormatFromTitle finds a standalone file format token in a release title.
func extractFormatFromTitle(title string) string {
	match := formatTokenRe.FindStringSubmatch(title)
	if len(match) > 1 {
		return strings.ToLower(match[1])
	}
	return ""
}

// guessMediaType guesses the media type from the result source.
func guessMediaType(result models.SearchResult) string {
	switch result.Source {
	case "audiobook", "prowlarr_audiobooks":
		return "audiobook"
	case "prowlarr_manga", "nyaa_manga", "mangadex", "annas_manga":
		return "manga"
	default:
		return "ebook"
	}
}

// ScoreResults scores and sorts a slice of search results by score descending.
func ScoreResults(results []models.SearchResult, query, author string) []models.SearchResult {
	for i := range results {
		sb := ScoreResult(results[i], query, author)
		results[i].Score = sb.Total
		results[i].ScoreBreakdown = &models.ScoreBreakdown{
			TitleMatch:  sb.TitleMatch,
			AuthorMatch: sb.AuthorMatch,
			FormatScore: sb.FormatScore,
			SeederScore: sb.SeederScore,
			SizeScore:   sb.SizeScore,
			Total:       sb.Total,
			Confidence:  sb.Confidence,
		}
	}
	return results
}
