package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/db"
)

// changePwTestSetup creates an in-memory DB, registers a user, and returns the
// handler + that user's ID. Mirrors the fixture style used by settings_test.go.
func changePwTestSetup(t *testing.T) (http.HandlerFunc, *db.DB, int64) {
	t.Helper()
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	hash, err := hashPassword("originalpw")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	id, err := database.CreateUser("alice", hash, "admin")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	return handleChangeOwnPassword(database), database, id
}

func postChangePw(t *testing.T, h http.HandlerFunc, userID int64, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/me/password", bytes.NewReader(b))
	if userID > 0 {
		req = req.WithContext(context.WithValue(req.Context(), ctxUserID, userID))
	}
	rr := httptest.NewRecorder()
	h(rr, req)
	return rr
}

// TestChangeOwnPassword_HappyPath — correct current pw + valid new pw updates.
func TestChangeOwnPassword_HappyPath(t *testing.T) {
	h, database, id := changePwTestSetup(t)

	rr := postChangePw(t, h, id, map[string]string{
		"current_password": "originalpw",
		"new_password":     "newpassword1",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Verify the new password actually works.
	user, _ := database.GetUserByUsername("alice")
	if !checkPassword("newpassword1", user.PasswordHash) {
		t.Error("new password does not verify after change")
	}
	if checkPassword("originalpw", user.PasswordHash) {
		t.Error("old password still works after change — hash was not updated")
	}
}

// TestChangeOwnPassword_WrongCurrent — must reject with 401 without updating.
func TestChangeOwnPassword_WrongCurrent(t *testing.T) {
	h, database, id := changePwTestSetup(t)

	rr := postChangePw(t, h, id, map[string]string{
		"current_password": "wrong-password",
		"new_password":     "newpassword1",
	})
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on wrong current pw, got %d: %s", rr.Code, rr.Body.String())
	}

	user, _ := database.GetUserByUsername("alice")
	if !checkPassword("originalpw", user.PasswordHash) {
		t.Error("password changed despite wrong current_password — security regression")
	}
}

// TestChangeOwnPassword_Unauthenticated — no userID in context → 401.
func TestChangeOwnPassword_Unauthenticated(t *testing.T) {
	h, _, _ := changePwTestSetup(t)

	rr := postChangePw(t, h, 0, map[string]string{
		"current_password": "originalpw",
		"new_password":     "newpassword1",
	})
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestChangeOwnPassword_TooShort — server-side guard against short new pw.
func TestChangeOwnPassword_TooShort(t *testing.T) {
	h, _, id := changePwTestSetup(t)

	rr := postChangePw(t, h, id, map[string]string{
		"current_password": "originalpw",
		"new_password":     "abc",
	})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on short pw, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestChangeOwnPassword_MissingFields — both fields required.
func TestChangeOwnPassword_MissingFields(t *testing.T) {
	h, _, id := changePwTestSetup(t)

	rr := postChangePw(t, h, id, map[string]string{
		"current_password": "originalpw",
	})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 when new_password missing, got %d", rr.Code)
	}

	rr = postChangePw(t, h, id, map[string]string{
		"new_password": "newpassword1",
	})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 when current_password missing, got %d", rr.Code)
	}
}
