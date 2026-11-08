package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/db"
	"golang.org/x/crypto/bcrypt"
)

func newResetTestDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

func passwordMatches(t *testing.T, database *db.DB, username, password string) bool {
	t.Helper()
	user, err := database.GetUserByUsername(username)
	if err != nil {
		t.Fatalf("load %s: %v", username, err)
	}
	return bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) == nil
}

func TestResetPassword_LocalAccount(t *testing.T) {
	database := newResetTestDB(t)
	// An admin created by the old SSO bootstrap: local, unknown password.
	if _, err := database.CreateUser("admin", "$2a$10$unknownunknownunknownunknownunknownunknownunknownunknown", "admin"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := resetPassword(database, "ADMIN", "new-password", false, &out); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !passwordMatches(t, database, "admin", "new-password") {
		t.Fatal("the new password must work")
	}
	if !strings.Contains(out.String(), "updated") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestResetPassword_SSOAccount(t *testing.T) {
	database := newResetTestDB(t)
	if _, err := database.CreateOIDCUser("alice", "sub-alice", "admin"); err != nil {
		t.Fatal(err)
	}

	if err := resetPassword(database, "alice", "new-password", false, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "--unlink-sso") {
		t.Fatalf("err = %v, want a pointer to --unlink-sso", err)
	}
	if u, _ := database.GetUserByUsername("alice"); !u.IsOIDC() {
		t.Fatal("a refused reset must not change the account")
	}

	if err := resetPassword(database, "alice", "new-password", true, &bytes.Buffer{}); err != nil {
		t.Fatalf("reset --unlink-sso: %v", err)
	}
	u, _ := database.GetUserByUsername("alice")
	if u.IsOIDC() || u.OIDCSubject != "" || u.Role != "admin" {
		t.Fatalf("got source=%q subject=%q role=%q", u.AuthSource, u.OIDCSubject, u.Role)
	}
	if !passwordMatches(t, database, "alice", "new-password") {
		t.Fatal("the new password must work")
	}
	if _, err := database.GetUserByOIDCSubject("sub-alice"); err == nil {
		t.Fatal("the subject must be unlinked")
	}
}

func TestResetPassword_Refusals(t *testing.T) {
	database := newResetTestDB(t)
	if _, err := database.CreateUser("bob", "x", "user"); err != nil {
		t.Fatal(err)
	}
	if err := resetPassword(database, "nobody", "new-password", false, &bytes.Buffer{}); err == nil {
		t.Fatal("expected an error for an unknown account")
	}
	if err := resetPassword(database, "bob", "short", false, &bytes.Buffer{}); err == nil {
		t.Fatal("expected an error for a short password")
	}
}

func TestRunResetPassword_ReadsStdin(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "librarr.db")
	t.Setenv("LIBRARR_DB_PATH", dbPath)
	database, err := db.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateUser("admin", "x", "admin"); err != nil {
		t.Fatal(err)
	}
	database.Close()

	stdin, err := os.CreateTemp(t.TempDir(), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	stdin.WriteString("from-stdin-pw\n")
	stdin.Seek(0, 0)

	var stdout, stderr bytes.Buffer
	if code := runResetPassword([]string{"admin"}, stdin, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d; stderr=%s", code, stderr.String())
	}
	database, _ = db.New(dbPath)
	defer database.Close()
	if !passwordMatches(t, database, "admin", "from-stdin-pw") {
		t.Fatal("the password from stdin must be set")
	}

	if code := runResetPassword(nil, stdin, &stdout, &stderr); code != 2 {
		t.Fatalf("missing username: exit %d, want 2", code)
	}
}
