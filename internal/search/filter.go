package search

import (
	"regexp"
	"sort"
	"strings"

	"github.com/JeremiahM37/librarr/internal/models"
)

// suspiciousKeywords are terms that indicate non-book content.
var suspiciousKeywords = []string{
	"exe", "msi", "keygen", "crack", "warez", "devcoursesweb",
	"trainer", "patch only", "activator", "serial", "nulled",
}

// IsSuspicious returns true if the title contains suspicious keywords.
func IsSuspicious(title string) bool {
	lower := strings.ToLower(title)
	for _, kw := range suspiciousKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

var normRe = regexp.MustCompile(`[^a-z0-9]`)

// normalizeForDedup returns the first 60 chars of a lowercased, stripped title.
func normalizeForDedup(title string) string {
	n := normRe.ReplaceAllString(strings.ToLower(title), "")
	if len(n) > 60 {
		n = n[:60]
	}
	return n
}

// FilterAndSortResults applies suspicious keyword filtering, seed count thresholds,
// size bounds, deduplication, and sorting to search results.
// titleRelevanceScore returns how well a title matches the query (higher = better).
// 3 = full query is substring, 2 = all query words present, 1 = partial match, 0 = minimal.
func titleRelevanceScore(title, query string) int {
	tLower := strings.ToLower(title)
	qLower := strings.ToLower(query)
	if strings.Contains(tLower, qLower) {
		return 3 // exact substring match
	}
	qWords := strings.Fields(qLower)
	matched := 0
	for _, w := range qWords {
		if len(w) > 2 && strings.Contains(tLower, w) {
			matched++
		}
	}
	if len(qWords) > 0 && matched == len(qWords) {
		return 2 // all words present
	}
	if len(qWords) > 0 && matched > len(qWords)/2 {
		return 1
	}
	return 0
}

func FilterAndSortResults(results []models.SearchResult, query string, minSize, maxSize int64) []models.SearchResult {
	var filtered []models.SearchResult
	seenTitles := make(map[string]int) // normalized title -> index in filtered

	for _, r := range results {
		// Suspicious keyword filter.
		if IsSuspicious(r.Title) {
			continue
		}

		// Prowlarr returns usenet results under a torrent source label on the
		// books and manga tabs, so the source string alone cannot decide which
		// filters apply. Keep the two questions separate: the source says which
		// bucket a result belongs to, the protocol says whether the checks that
		// are genuinely about torrents apply to it.
		isTorrentSource := r.Source == "torrent" || r.Source == "prowlarr_manga" || r.Source == "prowlarr_audiobooks" || r.Source == "nyaa_manga" ||
			r.Source == "tpb" || r.Source == "tpb_audiobook" ||
			r.Source == "booktracker" || r.Source == "booktracker_audiobook"
		isTorrent := isTorrentSource && r.DownloadProtocol != "nzb"
		isABB := r.Source == "audiobook"

		// Seed count threshold, torrents only (ABB may have 0 seeders with
		// valid magnets). Usenet has no seeders at all: Prowlarr omits the key,
		// r.Seeders unmarshals to 0, and applying this to an NZB silently drops
		// every usenet result the indexers returned.
		if isTorrent && r.Seeders < 1 {
			continue
		}

		// Size bounds. Despite the config names these are a sanity check on
		// what an indexer returned rather than a torrent property, so they hold
		// for usenet too - an NZB below MinTorrentSizeBytes is the same junk a
		// torrent that size would be.
		if isTorrentSource {
			size := r.Size
			if size == 0 {
				size = int64(parseSizeBytes(r.SizeHuman))
			}
			if size > 0 && (size < minSize || size > maxSize) {
				continue
			}
		}

		if isABB {
			// ABB may have 0 seeders with valid magnets from abb_url.
			if r.Seeders < 1 && r.AbbURL == "" {
				continue
			}
		}

		// Deduplication by first 60 chars of normalized title, keeping highest
		// seeders. Usenet results are deduplicated on the same terms: one
		// release carried by several indexers is the normal case there, and
		// they all arrive with 0 seeders, so the first copy seen wins.
		if isTorrentSource || isABB {
			norm := normalizeForDedup(r.Title)
			if idx, exists := seenTitles[norm]; exists {
				if r.Seeders > filtered[idx].Seeders {
					filtered[idx] = r
				}
				continue
			}
			seenTitles[norm] = len(filtered)
		}

		filtered = append(filtered, r)
	}

	// Sort: title relevance first, then source priority, then seeders, then size.
	sort.SliceStable(filtered, func(i, j int) bool {
		// Primary: title relevance (exact match > all words > partial)
		ri := titleRelevanceScore(filtered[i].Title, query)
		rj := titleRelevanceScore(filtered[j].Title, query)
		if ri != rj {
			return ri > rj
		}
		// Secondary: source priority
		pi := sourcePriority(filtered[i])
		pj := sourcePriority(filtered[j])
		if pi != pj {
			return pi < pj
		}
		// Tertiary: seeders descending.
		if filtered[i].Seeders != filtered[j].Seeders {
			return filtered[i].Seeders > filtered[j].Seeders
		}
		// Quaternary: size descending.
		si := filtered[i].Size
		if si == 0 {
			si = int64(parseSizeBytes(filtered[i].SizeHuman))
		}
		sj := filtered[j].Size
		if sj == 0 {
			sj = int64(parseSizeBytes(filtered[j].SizeHuman))
		}
		return si > sj
	})

	return filtered
}

func sourcePriority(r models.SearchResult) int {
	switch r.Source {
	case "annas", "annas_manga":
		return 0
	case "torrent", "audiobook", "prowlarr_manga", "prowlarr_audiobooks", "nyaa_manga":
		// Seeders stand in for "is this still retrievable", which is why a
		// zero-seeder torrent ranks below a live one. A usenet result carries
		// no seeders by definition, so reading its 0 the same way would rank
		// every NZB as if it were a dead torrent. An indexer still listing it
		// is the usenet equivalent of that signal.
		if r.Seeders > 0 || r.DownloadProtocol == "nzb" {
			return 1
		}
		return 2
	case "gutenberg", "openlibrary", "standardebooks", "librivox":
		return 3
	case "mangadex", "webnovel":
		return 2
	case "flibusta":
		return 1 // Direct download, popular for Russian books
	case "zlibrary":
		return 1 // Direct download, large catalog
	case "tpb", "tpb_audiobook":
		if r.Seeders > 0 {
			return 1
		}
		return 2
	case "booktracker", "booktracker_audiobook":
		return 1 // Russian book tracker, direct torrent download
	default:
		return 2
	}
}
