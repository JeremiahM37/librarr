package torznab

import "github.com/jamie75/librarr/internal/models"

// BuildCaps returns the Torznab capabilities XML structure.
func BuildCaps() *models.TorznabCaps {
	return &models.TorznabCaps{
		Server: models.TorznabServer{Title: "Librarr"},
		Limits: models.TorznabLimits{Max: 100, Default: 50},
		Searching: models.TorznabSearching{
			Search: models.TorznabSearchCap{
				Available:       "yes",
				SupportedParams: "q",
			},
			BookSearch: models.TorznabSearchCap{
				Available:       "yes",
				SupportedParams: "q,author,title",
			},
			AudioSearch: models.TorznabSearchCap{
				Available:       "yes",
				SupportedParams: "q",
			},
		},
		Categories: models.TorznabCategories{
			Categories: []models.TorznabCategory{
				{
					ID:   "7000",
					Name: "Books",
					Subs: []models.TorznabSubCategory{
						{ID: "7020", Name: "Books/Ebook"},
						{ID: "7030", Name: "Books/Comics"},
						{ID: "7040", Name: "Books/Magazines"},
						{ID: "7050", Name: "Books/Technical"},
					},
				},
				{
					ID:   "3000",
					Name: "Audio",
					Subs: []models.TorznabSubCategory{
						{ID: "3030", Name: "Audio/Audiobook"},
					},
				},
			},
		},
	}
}
