package search

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

type mockSearcher struct {
	name    string
	tab     string
	delay   time.Duration
	results []models.SearchResult
	err     error
}

func (m *mockSearcher) Name() string         { return m.name }
func (m *mockSearcher) Label() string        { return m.name }
func (m *mockSearcher) Enabled() bool        { return true }
func (m *mockSearcher) SearchTab() string    { return m.tab }
func (m *mockSearcher) DownloadType() string { return "direct" }
func (m *mockSearcher) Search(_ context.Context, query string) ([]models.SearchResult, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.err != nil {
		return nil, m.err
	}
	out := make([]models.SearchResult, len(m.results))
	copy(out, m.results)
	for i := range out {
		if out[i].Title == "" {
			out[i].Title = query
		}
	}
	return out, nil
}

func TestSearchWithAuthor_ConcurrentLoad(t *testing.T) {
	const numSources = 15
	const numRequests = 50

	var sources []Searcher
	for i := 0; i < numSources; i++ {
		sources = append(sources, &mockSearcher{
			name: fmt.Sprintf("mock_%d", i),
			tab:  "main",
			results: []models.SearchResult{{
				Source: "mock",
				Title:  "Concurrent Test Book",
				Format: "epub",
			}},
		})
	}

	cfg := &config.Config{}
	health := NewHealthTracker(3, 300)
	mgr := NewManager(cfg, sources, health)

	var wg sync.WaitGroup
	errCh := make(chan error, numRequests)
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			results, elapsed := mgr.SearchWithAuthor(context.Background(), "main", "concurrent test", "")
			if results == nil {
				errCh <- nil
				return
			}
			if elapsed < 0 {
				t.Errorf("request %d: negative elapsed", n)
			}
			errCh <- nil
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkSearchWithAuthor(b *testing.B) {
	var sources []Searcher
	for i := 0; i < 10; i++ {
		sources = append(sources, &mockSearcher{
			name: "bench",
			tab:  "main",
			results: []models.SearchResult{{
				Title: "Benchmark Book",
			}},
		})
	}
	cfg := &config.Config{}
	mgr := NewManager(cfg, sources, NewHealthTracker(3, 300))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.SearchWithAuthor(context.Background(), "main", "benchmark query", "author")
	}
}
