package api

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

// MetricsCollector tracks simple counters for Prometheus exposition.
type MetricsCollector struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		counters: make(map[string]*atomic.Int64),
	}
}

// Inc increments a counter.
func (m *MetricsCollector) Inc(name string, labels map[string]string) {
	key := metricsKey(name, labels)
	m.mu.Lock()
	c, ok := m.counters[key]
	if !ok {
		c = &atomic.Int64{}
		m.counters[key] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

func metricsKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	var parts []string
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf("%s=%q", k, v))
	}
	return fmt.Sprintf("%s{%s}", name, strings.Join(parts, ","))
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var lines []string

	// Job status counts.
	downloads := s.downloadMgr.GetDownloads()
	statusCounts := make(map[string]int)
	for _, d := range downloads {
		statusCounts[d.Status]++
	}
	lines = append(lines,
		"# HELP librarr_jobs_by_status Number of Librarr jobs by current status.",
		"# TYPE librarr_jobs_by_status gauge",
	)
	for status, count := range statusCounts {
		lines = append(lines, fmt.Sprintf("librarr_jobs_by_status{status=%q} %d", status, count))
	}

	// Library item counts.
	ebookCount, _ := s.library().CountLegacyItems(r.Context(), "ebook")
	audiobookCount, _ := s.library().CountLegacyItems(r.Context(), "audiobook")
	mangaCount, _ := s.library().CountLegacyItems(r.Context(), "manga")
	totalCount, _ := s.library().CountLegacyItems(r.Context(), "")
	activityCount, _ := s.db.CountActivity()

	lines = append(lines,
		"# HELP librarr_library_items_total Number of tracked library items.",
		"# TYPE librarr_library_items_total gauge",
		fmt.Sprintf("librarr_library_items_total %d", totalCount),
		fmt.Sprintf("librarr_library_ebooks_total %d", ebookCount),
		fmt.Sprintf("librarr_library_audiobooks_total %d", audiobookCount),
		fmt.Sprintf("librarr_library_manga_total %d", mangaCount),
		"# HELP librarr_activity_events_total Number of activity log events.",
		"# TYPE librarr_activity_events_total gauge",
		fmt.Sprintf("librarr_activity_events_total %d", activityCount),
	)

	// Source health.
	lines = append(lines,
		"# HELP librarr_source_health_score Source health score (0-100).",
		"# TYPE librarr_source_health_score gauge",
		"# HELP librarr_source_circuit_open Whether source search circuit is open (1=open).",
		"# TYPE librarr_source_circuit_open gauge",
	)
	for _, source := range s.searchMgr.GetSources() {
		meta := s.searchMgr.SourceMeta()
		for _, m := range meta {
			if m["name"] == source.Name() {
				if h, ok := m["health"].(map[string]interface{}); ok {
					score := 100.0
					if s, ok := h["score"].(float64); ok {
						score = s
					}
					isOpen := 0
					if o, ok := h["circuit_open"].(bool); ok && o {
						isOpen = 1
					}
					lines = append(lines,
						fmt.Sprintf("librarr_source_health_score{source=%q} %.1f", source.Name(), score),
						fmt.Sprintf("librarr_source_circuit_open{source=%q} %d", source.Name(), isOpen),
					)
				}
			}
		}
	}

	// Metrics from collector (counters).
	if s.metrics != nil {
		s.metrics.mu.Lock()
		for key, counter := range s.metrics.counters {
			lines = append(lines, fmt.Sprintf("%s %d", key, counter.Load()))
		}
		s.metrics.mu.Unlock()
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}
