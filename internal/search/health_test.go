package search

import (
	"testing"
	"time"
)

func TestHealthTracker_CanSearch(t *testing.T) {
	t.Run("new source is healthy", func(t *testing.T) {
		ht := NewHealthTracker(3, 300)
		if !ht.CanSearch("test_source") {
			t.Error("expected new source to be searchable")
		}
	})

	t.Run("circuit opens after threshold failures", func(t *testing.T) {
		ht := NewHealthTracker(3, 300)
		ht.RecordFailure("failing_source", "error 1", "search")
		ht.RecordFailure("failing_source", "error 2", "search")
		if !ht.CanSearch("failing_source") {
			t.Error("should still be searchable before threshold")
		}
		ht.RecordFailure("failing_source", "error 3", "search")
		if ht.CanSearch("failing_source") {
			t.Error("circuit should be open after 3 failures")
		}
	})

	t.Run("success resets streak", func(t *testing.T) {
		ht := NewHealthTracker(3, 300)
		ht.RecordFailure("source", "err", "search")
		ht.RecordFailure("source", "err", "search")
		ht.RecordSuccess("source", "search")
		ht.RecordFailure("source", "err", "search")
		ht.RecordFailure("source", "err", "search")
		// Only 2 in a row now, not 3
		if !ht.CanSearch("source") {
			t.Error("expected source to still be searchable after success reset")
		}
	})

	t.Run("success closes open circuit", func(t *testing.T) {
		ht := NewHealthTracker(3, 300)
		ht.RecordFailure("source", "err", "search")
		ht.RecordFailure("source", "err", "search")
		ht.RecordFailure("source", "err", "search")
		if ht.CanSearch("source") {
			t.Error("circuit should be open")
		}
		ht.RecordSuccess("source", "search")
		if !ht.CanSearch("source") {
			t.Error("success should have closed the circuit")
		}
	})
}

func TestHealthTracker_DefaultThresholds(t *testing.T) {
	// Test that invalid thresholds are corrected
	ht := NewHealthTracker(0, 0)
	if ht.threshold != 3 {
		t.Errorf("expected default threshold 3, got %d", ht.threshold)
	}
	if ht.openSec != 300 {
		t.Errorf("expected default openSec 300, got %d", ht.openSec)
	}
}

func TestHealthTracker_Snapshot(t *testing.T) {
	ht := NewHealthTracker(3, 300)
	ht.RecordSuccess("source_a", "search")
	ht.RecordFailure("source_b", "timeout", "download")

	snapshot := ht.Snapshot()

	if _, ok := snapshot["source_a"]; !ok {
		t.Error("expected source_a in snapshot")
	}
	if _, ok := snapshot["source_b"]; !ok {
		t.Error("expected source_b in snapshot")
	}

	srcA := snapshot["source_a"]
	if srcA["search_ok"].(int) != 1 {
		t.Errorf("expected search_ok=1 for source_a, got %v", srcA["search_ok"])
	}
	if srcA["circuit_open"].(bool) {
		t.Error("source_a circuit should be closed")
	}

	srcB := snapshot["source_b"]
	if srcB["download_fail"].(int) != 1 {
		t.Errorf("expected download_fail=1 for source_b, got %v", srcB["download_fail"])
	}
	if srcB["last_error"].(string) != "timeout" {
		t.Errorf("expected last_error='timeout', got %v", srcB["last_error"])
	}
}

func TestHealthTracker_Score(t *testing.T) {
	ht := NewHealthTracker(3, 300)

	// Record some successes and failures
	ht.RecordSuccess("source", "search")
	ht.RecordSuccess("source", "search")
	ht.RecordSuccess("source", "search")
	ht.RecordFailure("source", "err", "search")

	snapshot := ht.Snapshot()
	score := snapshot["source"]["score"].(float64)
	if score <= 0 || score >= 100 {
		t.Errorf("expected score between 0 and 100, got %f", score)
	}
}

func TestHealthTracker_LastErrorTruncation(t *testing.T) {
	ht := NewHealthTracker(3, 300)
	longError := make([]byte, 500)
	for i := range longError {
		longError[i] = 'x'
	}
	ht.RecordFailure("source", string(longError), "search")

	ht.mu.Lock()
	s := ht.data["source"]
	ht.mu.Unlock()

	if len(s.LastError) > 400 {
		t.Errorf("expected last_error truncated to 400, got %d", len(s.LastError))
	}
}

func TestHealthTracker_DownloadTracking(t *testing.T) {
	ht := NewHealthTracker(3, 300)
	ht.RecordSuccess("source", "download")
	ht.RecordFailure("source", "err", "download")

	snapshot := ht.Snapshot()
	src := snapshot["source"]
	if src["download_ok"].(int) != 1 {
		t.Errorf("expected download_ok=1, got %v", src["download_ok"])
	}
	if src["download_fail"].(int) != 1 {
		t.Errorf("expected download_fail=1, got %v", src["download_fail"])
	}
}

func TestHealthTracker_CircuitTimeout(t *testing.T) {
	// Use a very short timeout
	ht := NewHealthTracker(1, 1)
	ht.RecordFailure("source", "err", "search")
	if ht.CanSearch("source") {
		t.Error("circuit should be open immediately after failure")
	}
	// Wait for timeout
	time.Sleep(1100 * time.Millisecond)
	if !ht.CanSearch("source") {
		t.Error("circuit should have reopened after timeout")
	}
}
