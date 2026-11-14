package api

import (
	"log/slog"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/links"
	"github.com/JeremiahM37/librarr/internal/models"
)

// libraryItemsJSON converts library items for the API and adds their links
// out to Kavita ("kavita_url") and Audiobookshelf ("abs_url"). An item with no
// cover of its own borrows the linked service's. Links whose service is no
// longer configured are left out until the next sync drops them. A failure to
// load links lists the items without them.
func (s *Server) libraryItemsJSON(items []models.LibraryItem) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(items))
	ids := make([]int64, 0, len(items))
	for _, item := range items {
		out = append(out, db.ItemToJSON(item))
		ids = append(ids, item.ID)
	}
	stored, err := s.db.LibraryLinks(ids)
	if err != nil {
		slog.Warn("library links unavailable", "error", err)
		return out
	}
	for i, item := range items {
		for _, link := range stored[item.ID] {
			var base, key string
			switch link.Service {
			case links.ServiceKavita:
				base, key = s.kavitaPublicURL(), "kavita_url"
			case links.ServiceABS:
				base, key = s.absPublicURL(), "abs_url"
			}
			if base == "" {
				continue
			}
			out[i][key] = base + link.URLPath
			if _, ok := out[i]["cover_url"]; !ok && link.CoverPath != "" {
				out[i]["cover_url"] = base + link.CoverPath
			}
		}
	}
	return out
}

// kavitaPublicURL is the Kavita base URL browsers use, or "" when Kavita is
// not configured.
func (s *Server) kavitaPublicURL() string {
	if !s.cfg.HasKavita() {
		return ""
	}
	if s.cfg.KavitaPublicURL != "" {
		return s.cfg.KavitaPublicURL
	}
	return s.cfg.KavitaURL
}

// absPublicURL is the Audiobookshelf base URL browsers use, or "" when
// Audiobookshelf is not configured.
func (s *Server) absPublicURL() string {
	if !s.cfg.HasAudiobookshelf() {
		return ""
	}
	if s.cfg.ABSPublicURL != "" {
		return s.cfg.ABSPublicURL
	}
	return s.cfg.ABSURL
}
