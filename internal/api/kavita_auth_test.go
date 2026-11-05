package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

// kavitaAuthStub stands in for Kavita: it answers the login handshake with a
// JWT and rejects any other request that lacks the expected credentials.
type kavitaAuthStub struct {
	*httptest.Server
	mu     sync.Mutex
	logins int
	paths  []string
}

func newKavitaAuthStub(t *testing.T, wantHeader, wantValue string) *kavitaAuthStub {
	t.Helper()
	k := &kavitaAuthStub{}
	k.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if r.URL.Path == "/api/Account/login" {
			k.logins++
			io.WriteString(w, `{"token":"jwt-123"}`)
			return
		}
		k.paths = append(k.paths, r.URL.Path)
		if r.Header.Get(wantHeader) != wantValue {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/Series/all-v2", "/api/Library/libraries":
			io.WriteString(w, `[]`)
		case "/api/Search/search":
			io.WriteString(w, `{"series":[]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(k.Close)
	return k
}

func TestLibraryManga_KavitaAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		apiKey     string
		target     string
		wantLogins int
		wantHeader string
		wantValue  string
		wantPath   string
	}{
		{"auth key lists series without login", "kavita-key", "/api/library/manga", 0, "x-api-key", "kavita-key", "/api/Series/all-v2"},
		{"auth key searches without login", "kavita-key", "/api/library/manga?q=dune", 0, "x-api-key", "kavita-key", "/api/Search/search"},
		{"password lists series after login", "", "/api/library/manga", 1, "Authorization", "Bearer jwt-123", "/api/Series/all-v2"},
		{"password searches after login", "", "/api/library/manga?q=dune", 1, "Authorization", "Bearer jwt-123", "/api/Search/search"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newKavitaAuthStub(t, tc.wantHeader, tc.wantValue)
			s := &Server{cfg: &config.Config{KavitaURL: k.URL, KavitaUser: "u", KavitaPass: "p", KavitaAPIKey: tc.apiKey}}

			rr := httptest.NewRecorder()
			s.handleLibraryManga(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
			}
			k.mu.Lock()
			defer k.mu.Unlock()
			if k.logins != tc.wantLogins {
				t.Errorf("logins = %d, want %d", k.logins, tc.wantLogins)
			}
			if len(k.paths) != 1 || k.paths[0] != tc.wantPath {
				t.Errorf("requests = %v, want [%s]", k.paths, tc.wantPath)
			}
		})
	}
}

func TestTestKavita_Authentication(t *testing.T) {
	for _, tc := range []struct {
		name        string
		apiKey      string
		stubValue   string
		wantSuccess bool
		wantLogins  int
	}{
		{"valid auth key", "kavita-key", "kavita-key", true, 0},
		{"rejected auth key", "wrong-key", "kavita-key", false, 0},
		{"password login", "", "kavita-key", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newKavitaAuthStub(t, "x-api-key", tc.stubValue)
			s := &Server{cfg: &config.Config{KavitaURL: k.URL, KavitaUser: "u", KavitaPass: "p", KavitaAPIKey: tc.apiKey}}

			rr := httptest.NewRecorder()
			s.handleTestKavita(rr, httptest.NewRequest(http.MethodPost, "/api/test/kavita", nil))

			var resp struct {
				Success bool `json:"success"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode %q: %v", rr.Body.String(), err)
			}
			if resp.Success != tc.wantSuccess {
				t.Errorf("success = %v, want %v: %s", resp.Success, tc.wantSuccess, rr.Body.String())
			}
			k.mu.Lock()
			defer k.mu.Unlock()
			if k.logins != tc.wantLogins {
				t.Errorf("logins = %d, want %d", k.logins, tc.wantLogins)
			}
		})
	}
}

func TestGetSettings_MasksKavitaAPIKey(t *testing.T) {
	s, _ := settingsTestServer(t)
	s.cfg.KavitaAPIKey = "REAL_KAVITA_KEY"

	rr := httptest.NewRecorder()
	s.handleGetSettings(rr, httptest.NewRequest(http.MethodGet, "/api/settings", nil))

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := resp["kavita_api_key"]; got != maskedValue {
		t.Errorf("kavita_api_key should be masked, got %v", got)
	}
}
