package api

import (
	"testing"
	"time"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"zero", 0, "0m"},
		{"minutes", 5 * time.Minute, "5m"},
		{"hours_minutes", 2*time.Hour + 30*time.Minute, "2h 30m"},
		{"days_hours_minutes", 3*24*time.Hour + 5*time.Hour + 15*time.Minute, "3d 5h 15m"},
		{"one_day", 24 * time.Hour, "1d 0h 0m"},
		{"just_hours", 12 * time.Hour, "12h 0m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.d)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}

func TestVersionVars(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
	if GoVersion == "" {
		t.Error("GoVersion should not be empty")
	}
}
