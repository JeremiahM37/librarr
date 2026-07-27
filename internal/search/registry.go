package search

import (
	"net/http"
	"sync"

	"github.com/jamie75/librarr/internal/config"
)

// SourceFactory is a function that creates a Searcher from config and HTTP client.
type SourceFactory func(cfg *config.Config, client *http.Client) Searcher

var (
	registryMu          sync.Mutex
	registeredFactories []SourceFactory
)

// RegisterSource registers a source factory. Called from init() functions in source files.
func RegisterSource(factory SourceFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registeredFactories = append(registeredFactories, factory)
}

// CreateRegisteredSources creates all auto-registered sources.
func CreateRegisteredSources(cfg *config.Config, client *http.Client) []Searcher {
	registryMu.Lock()
	defer registryMu.Unlock()

	var sources []Searcher
	for _, factory := range registeredFactories {
		sources = append(sources, factory(cfg, client))
	}
	return sources
}

// Register all built-in sources via init().
func init() {
	// Standard Ebooks
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewStandardEbooks(cfg, client)
	})
	// Librivox
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewLibrivox(cfg, client)
	})
	// MangaDex
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewMangaDex(cfg, client)
	})
	// Nyaa Manga
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewNyaaManga(cfg, client)
	})
	// Anna's Manga
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewAnnasManga(cfg, client)
	})
	// Web Novels
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewWebNovel(cfg, client)
	})
	// Flibusta (Russian book source)
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewFlibusta(cfg, client)
	})
	// Z-Library
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewZLibrary(cfg, client)
	})
	// ThePirateBay (Ebooks)
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewThePirateBay(cfg, client, "main")
	})
	// ThePirateBay (Audiobooks)
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewThePirateBay(cfg, client, "audiobook")
	})
	// BookTracker (Russian ebooks)
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewBookTracker(cfg, client, "main")
	})
	// BookTracker (Russian audiobooks)
	RegisterSource(func(cfg *config.Config, client *http.Client) Searcher {
		return NewBookTracker(cfg, client, "audiobook")
	})
}
