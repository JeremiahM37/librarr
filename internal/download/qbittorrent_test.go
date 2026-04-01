package download

import (
	"testing"
)

func TestMapTorrentStatus(t *testing.T) {
	tests := []struct {
		state    string
		expected string
	}{
		{"downloading", "downloading"},
		{"stalledDL", "downloading"},
		{"metaDL", "downloading"},
		{"forcedDL", "downloading"},
		{"pausedDL", "paused"},
		{"queuedDL", "queued"},
		{"uploading", "completed"},
		{"stalledUP", "completed"},
		{"pausedUP", "completed"},
		{"queuedUP", "completed"},
		{"stoppedUP", "completed"},
		{"checkingDL", "checking"},
		{"checkingUP", "checking"},
		{"error", "error"},
		{"missingFiles", "missingFiles"},
		{"unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			result := MapTorrentStatus(tt.state)
			if result != tt.expected {
				t.Errorf("MapTorrentStatus(%q) = %q, want %q", tt.state, result, tt.expected)
			}
		})
	}
}

func TestMapSABStatus(t *testing.T) {
	tests := []struct {
		status   string
		expected string
	}{
		{"Downloading", "downloading"},
		{"Paused", "paused"},
		{"Queued", "queued"},
		{"Completed", "completed"},
		{"downloading", "downloading"},
		{"SomeOtherStatus", "SomeOtherStatus"},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			result := mapSABStatus(tt.status)
			if result != tt.expected {
				t.Errorf("mapSABStatus(%q) = %q, want %q", tt.status, result, tt.expected)
			}
		})
	}
}

func TestValidTransitions(t *testing.T) {
	tests := []struct {
		from    string
		to      string
		allowed bool
	}{
		{"queued", "searching", true},
		{"queued", "downloading", true},
		{"queued", "error", true},
		{"queued", "completed", false},
		{"searching", "downloading", true},
		{"searching", "queued", true},
		{"downloading", "importing", true},
		{"downloading", "completed", true},
		{"downloading", "error", true},
		{"downloading", "retry_wait", true},
		{"downloading", "queued", false},
		{"importing", "completed", true},
		{"importing", "error", true},
		{"importing", "queued", false},
		{"retry_wait", "downloading", true},
		{"retry_wait", "searching", true},
		{"error", "queued", true},
		{"error", "dead_letter", true},
		{"error", "downloading", false},
		{"dead_letter", "queued", true},
		{"dead_letter", "downloading", false},
		{"completed", "queued", false},
	}

	for _, tt := range tests {
		t.Run(tt.from+"->"+tt.to, func(t *testing.T) {
			allowed, ok := validTransitions[tt.from]
			if !ok {
				t.Fatalf("no transitions defined for state %q", tt.from)
			}
			result := allowed[tt.to]
			if result != tt.allowed {
				t.Errorf("transition %s -> %s: got %v, want %v", tt.from, tt.to, result, tt.allowed)
			}
		})
	}
}
