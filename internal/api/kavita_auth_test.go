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
