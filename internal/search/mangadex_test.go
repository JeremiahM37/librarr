package search

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestMangaDex_Metadata(t *testing.T) {
	cfg := &config.Config{MangaDexEnabled: true}
	m := NewMangaDex(cfg, http.DefaultClient)

	if m.Name() != "mangadex" {
		t.Errorf("expected name mangadex, got %s", m.Name())
	}
	if m.SearchTab() != "manga" {
		t.Errorf("expected tab manga, got %s", m.SearchTab())
	}
	if m.DownloadType() != "direct" {
		t.Errorf("expected download type direct, got %s", m.DownloadType())
	}
}

func TestMangaDex_Enabled(t *testing.T) {
	cfg := &config.Config{MangaDexEnabled: false}
	m := NewMangaDex(cfg, http.DefaultClient)
	if m.Enabled() {
		t.Error("expected disabled")
	}

	cfg.MangaDexEnabled = true
	if !m.Enabled() {
		t.Error("expected enabled")
	}
}

func TestMangaDex_ResponseParsing(t *testing.T) {
	// Test JSON parsing logic without making HTTP calls
	response := mangadexResponse{
		Data: []mangadexManga{
			{
				ID: "uuid-1",
				Attributes: mangadexAttrs{
					Title:       map[string]string{"en": "One Piece"},
					LastChapter: "1000",
					Status:      "ongoing",
				},
				Relationships: []mangadexRelationship{
					{
						Type:       "author",
						ID:         "author-1",
						Attributes: map[string]interface{}{"name": "Oda Eiichiro"},
					},
					{
						Type:       "cover_art",
						ID:         "cover-1",
						Attributes: map[string]interface{}{"fileName": "cover.jpg"},
					},
				},
			},
			{
				ID: "uuid-2",
				Attributes: mangadexAttrs{
					Title:       map[string]string{"ja": "ナルト"},
					LastChapter: "",
					Status:      "completed",
				},
				Relationships: []mangadexRelationship{},
			},
			{
				ID: "uuid-3",
				Attributes: mangadexAttrs{
					Title:  map[string]string{},
					Status: "hiatus",
				},
			},
		},
	}

	// Verify serialization/deserialization
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var parsed mangadexResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(parsed.Data) != 3 {
		t.Fatalf("expected 3 manga, got %d", len(parsed.Data))
	}

	// Test English title preference
	title := parsed.Data[0].Attributes.Title["en"]
	if title != "One Piece" {
		t.Errorf("expected title One Piece, got %s", title)
	}

	// Test author extraction
	author := ""
	for _, rel := range parsed.Data[0].Relationships {
		if rel.Type == "author" && rel.Attributes != nil {
			if name, ok := rel.Attributes["name"].(string); ok {
				author = name
			}
		}
	}
	if author != "Oda Eiichiro" {
		t.Errorf("expected author Oda Eiichiro, got %s", author)
	}

	// Test Japanese fallback
	enTitle := parsed.Data[1].Attributes.Title["en"]
	if enTitle != "" {
		t.Errorf("expected no English title, got %s", enTitle)
	}
	jaTitle := parsed.Data[1].Attributes.Title["ja"]
	if jaTitle != "ナルト" {
		t.Errorf("expected Japanese title, got %s", jaTitle)
	}

	// Test empty title
	if len(parsed.Data[2].Attributes.Title) != 0 {
		t.Error("expected empty title map")
	}
}
