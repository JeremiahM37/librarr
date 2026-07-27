package diagnostics

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDiagnoseProwlarrSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/system/status" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Api-Key"); got != "secret" {
			t.Fatalf("api key = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"version": "2.0.5"})
	}))
	defer server.Close()

	result := DiagnoseProwlarr(context.Background(), ProwlarrConfig{URL: server.URL, APIKey: "secret"})
	if !result.Success || result.Status != ResultConnected {
		t.Fatalf("result = %+v", result)
	}
	if !hasStep(result, "DNS Lookup", StatusSuccess) || !hasStep(result, "API Version", StatusSuccess) {
		t.Fatalf("steps = %+v", result.Steps)
	}
}

func TestDiagnoseProwlarrAuthenticationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	result := DiagnoseProwlarr(context.Background(), ProwlarrConfig{URL: server.URL, APIKey: "bad"})
	if result.Success || result.Status != ResultFailed {
		t.Fatalf("result = %+v", result)
	}
	step := findStep(result, "Authentication")
	if step == nil || step.Status != StatusFailed || !strings.Contains(step.Message, "401") || !strings.Contains(step.Suggestion, "API key") {
		t.Fatalf("auth step = %+v", step)
	}
}

func TestDiagnoseProwlarrMissingConfiguration(t *testing.T) {
	result := DiagnoseProwlarr(context.Background(), ProwlarrConfig{})
	if result.Success || len(result.Steps) != 1 {
		t.Fatalf("result = %+v", result)
	}
	if result.Steps[0].Name != "Configuration" || result.Steps[0].Status != StatusFailed {
		t.Fatalf("steps = %+v", result.Steps)
	}
}

func TestDiagnoseInvalidHostnameReportsDNSFailure(t *testing.T) {
	result := DiagnoseProwlarr(context.Background(), ProwlarrConfig{
		URL:     "http://librarr-diagnostics.invalid:9696",
		APIKey:  "secret",
		Timeout: 200 * time.Millisecond,
	})
	step := findStep(result, "DNS Lookup")
	if result.Success || step == nil || step.Status != StatusFailed || !strings.Contains(step.Message, "Unable to resolve hostname") {
		t.Fatalf("result = %+v dns=%+v", result, step)
	}
}

func TestDiagnoseWrongPortReportsTCPFailure(t *testing.T) {
	addr := unusedLocalAddress(t)
	result := DiagnoseProwlarr(context.Background(), ProwlarrConfig{
		URL:     "http://" + addr,
		APIKey:  "secret",
		Timeout: 200 * time.Millisecond,
	})
	step := findStep(result, "TCP Connection")
	if result.Success || step == nil || step.Status != StatusFailed {
		t.Fatalf("result = %+v tcp=%+v", result, step)
	}
}

func TestDiagnoseHTTPServerBehindHTTPSReportsTLSFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"version":"2.0.5"}`))
	}))
	defer server.Close()
	result := DiagnoseProwlarr(context.Background(), ProwlarrConfig{
		URL:     "https://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:  "secret",
		Timeout: 500 * time.Millisecond,
	})
	step := findStep(result, "HTTPS")
	if result.Success || step == nil || step.Status != StatusFailed {
		t.Fatalf("result = %+v tls=%+v", result, step)
	}
}

func TestDiagnoseQBittorrentSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("username") != "admin" || r.Form.Get("password") != "secret" {
				t.Fatalf("form = %+v", r.Form)
			}
			http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "sid"})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/app/version":
			if _, err := r.Cookie("QBT_SID"); err != nil {
				t.Fatal("missing qBittorrent session cookie")
			}
			_, _ = w.Write([]byte("v5.0.0"))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	result := DiagnoseQBittorrent(context.Background(), QBittorrentConfig{URL: server.URL, Username: "admin", Password: "secret"})
	if !result.Success || result.Status != ResultConnected {
		t.Fatalf("result = %+v", result)
	}
	if !hasStep(result, "Authentication", StatusSuccess) || !hasStep(result, "API Version", StatusSuccess) {
		t.Fatalf("steps = %+v", result.Steps)
	}
}

func TestDiagnoseQBittorrentAuthenticationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Fails.", http.StatusForbidden)
	}))
	defer server.Close()

	result := DiagnoseQBittorrent(context.Background(), QBittorrentConfig{URL: server.URL, Username: "admin", Password: "wrong"})
	if result.Success || result.Status != ResultFailed {
		t.Fatalf("result = %+v", result)
	}
	step := findStep(result, "Authentication")
	if step == nil || step.Status != StatusFailed || !strings.Contains(step.Suggestion, "username") {
		t.Fatalf("auth step = %+v", step)
	}
}

func hasStep(result Result, name string, status StepStatus) bool {
	step := findStep(result, name)
	return step != nil && step.Status == status
}

func findStep(result Result, name string) *Step {
	for i := range result.Steps {
		if result.Steps[i].Name == name {
			return &result.Steps[i]
		}
	}
	return nil
}

func unusedLocalAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
