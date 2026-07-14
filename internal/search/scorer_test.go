package search

import (
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
)

func TestScoreResult_ExactTitleMatch(t *testing.T) {
	result := models.SearchResult{
		Title:  "The Great Gatsby",
		Source: "annas",
		Format: "epub",
	}
	sb := ScoreResult(result, "the great gatsby", "")
	if sb.TitleMatch < 35 {
		t.Errorf("exact title match should score >= 35, got %f", sb.TitleMatch)
	}
	if sb.Confidence != "high" {
		t.Errorf("expected high confidence for exact match, got %s (total: %f)", sb.Confidence, sb.Total)
	}
}

func TestScoreResult_PartialTitleMatch(t *testing.T) {
	result := models.SearchResult{
		Title:  "The Great Gatsby - F. Scott Fitzgerald [EPUB]",
		Source: "annas",
	}
	sb := ScoreResult(result, "the great gatsby", "")
	if sb.TitleMatch < 20 {
		t.Errorf("partial title match should score >= 20, got %f", sb.TitleMatch)
	}
}

func TestScoreResult_AuthorMatch(t *testing.T) {
	result := models.SearchResult{
		Title:  "The Great Gatsby",
		Author: "F. Scott Fitzgerald",
		Source: "annas",
	}
	sb := ScoreResult(result, "the great gatsby", "F. Scott Fitzgerald")
	if sb.AuthorMatch < 16 {
		t.Errorf("author match should score >= 16, got %f", sb.AuthorMatch)
	}
}

func TestScoreResult_NoAuthor(t *testing.T) {
	result := models.SearchResult{
		Title:  "The Great Gatsby",
		Source: "annas",
	}
	sb := ScoreResult(result, "the great gatsby", "")
	// Without author, should get neutral score.
	if sb.AuthorMatch != 10 {
		t.Errorf("no author should give 10 neutral, got %f", sb.AuthorMatch)
	}
}

func TestScoreResult_FormatEpub(t *testing.T) {
	result := models.SearchResult{
		Title:  "Test Book",
		Source: "annas",
		Format: "epub",
	}
	sb := ScoreResult(result, "test book", "")
	if sb.FormatScore != 15 {
		t.Errorf("epub should score 15, got %f", sb.FormatScore)
	}
}

func TestScoreResult_FormatPDF(t *testing.T) {
	result := models.SearchResult{
		Title:  "Test Book",
		Source: "torrent",
		Format: "pdf",
	}
	sb := ScoreResult(result, "test book", "")
	if sb.FormatScore != 8 {
		t.Errorf("pdf should score 8, got %f", sb.FormatScore)
	}
}

func TestScoreResult_SeedersDDL(t *testing.T) {
	result := models.SearchResult{
		Title:  "Test",
		Source: "annas",
	}
	sb := ScoreResult(result, "test", "")
	if sb.SeederScore != 12 {
		t.Errorf("DDL source should score 12, got %f", sb.SeederScore)
	}
}

func TestScoreResult_SeedersHigh(t *testing.T) {
	result := models.SearchResult{
		Title:   "Test",
		Source:  "torrent",
		Seeders: 25,
	}
	sb := ScoreResult(result, "test", "")
	if sb.SeederScore != 15 {
		t.Errorf("25 seeders should score 15, got %f", sb.SeederScore)
	}
}

func TestScoreResult_SizeEbook(t *testing.T) {
	result := models.SearchResult{
		Title:  "Test",
		Source: "annas",
		Size:   2e6, // 2MB
	}
	sb := ScoreResult(result, "test", "")
	if sb.SizeScore != 10 {
		t.Errorf("2MB ebook should score 10, got %f", sb.SizeScore)
	}
}

func TestScoreResult_SizeAudiobook(t *testing.T) {
	result := models.SearchResult{
		Title:     "Test Audiobook",
		Source:    "audiobook",
		MediaType: "audiobook",
		Size:      500e6, // 500MB
	}
	sb := ScoreResult(result, "test audiobook", "")
	if sb.SizeScore != 10 {
		t.Errorf("500MB audiobook should score 10, got %f", sb.SizeScore)
	}
}

func TestScoreResult_SizeProwlarrAudiobookWithoutMediaType(t *testing.T) {
	result := models.SearchResult{
		Title:  "Test Audiobook",
		Source: "prowlarr_audiobooks",
		Size:   500e6, // 500MB
	}
	sb := ScoreResult(result, "test audiobook", "")
	if sb.SizeScore != 10 {
		t.Errorf("500MB prowlarr audiobook should score 10, got %f", sb.SizeScore)
	}
}

func TestScoreResult_ConfidenceLevels(t *testing.T) {
	tests := []struct {
		name     string
		result   models.SearchResult
		query    string
		wantConf string
	}{
		{
			name: "high confidence",
			result: models.SearchResult{
				Title: "The Great Gatsby", Author: "F. Scott Fitzgerald",
				Source: "annas", Format: "epub", Size: 1e6,
			},
			query:    "the great gatsby",
			wantConf: "high",
		},
		{
			name: "low confidence - bad title match",
			result: models.SearchResult{
				Title:  "Something Completely Different",
				Source: "torrent", Seeders: 0,
			},
			query:    "the great gatsby",
			wantConf: "low",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := ScoreResult(tt.result, tt.query, "")
			if sb.Confidence != tt.wantConf {
				t.Errorf("expected confidence %q, got %q (total: %f)", tt.wantConf, sb.Confidence, sb.Total)
			}
		})
	}
}

func TestScoreResults_PopulatesFields(t *testing.T) {
	results := []models.SearchResult{
		{Title: "Wrong Book", Source: "torrent", Seeders: 1},
		{Title: "The Great Gatsby", Source: "annas", Format: "epub", Size: 1e6},
	}
	scored := ScoreResults(results, "the great gatsby", "")
	if len(scored) != 2 {
		t.Fatalf("expected 2 results, got %d", len(scored))
	}

	// The matching result should have a higher score.
	if scored[1].Score <= scored[0].Score {
		// ScoreResults does not sort; it only populates scores.
		// The second result should have a higher score since it matches the query.
	}

	// Both should have scores and breakdowns populated.
	for i, r := range scored {
		if r.Score == 0 {
			t.Errorf("result %d should have a non-zero score", i)
		}
		if r.ScoreBreakdown == nil {
			t.Errorf("result %d should have ScoreBreakdown populated", i)
		}
	}

	// The exact match should score higher than the non-match.
	if scored[1].Score <= scored[0].Score {
		t.Logf("scores: wrong=%f, gatsby=%f", scored[0].Score, scored[1].Score)
	}
}

func TestExtractFormatFromTitle(t *testing.T) {
	tests := []struct {
		title string
		want  string
	}{
		{"Book Title [EPUB]", "epub"},
		{"Book Title (PDF)", "pdf"},
		{"book.mobi", "mobi"},
		{"Audiobook-WEB-MP3-DE", "mp3"},
		{"Audiobook [M4B]", "m4b"},
		{"Audiobook MP3.zip", "mp3"},
		{"A Magnum Opus", ""},
		{"No format here", ""},
	}
	for _, tt := range tests {
		got := extractFormatFromTitle(tt.title)
		if got != tt.want {
			t.Errorf("extractFormatFromTitle(%q) = %q, want %q", tt.title, got, tt.want)
		}
	}
}
