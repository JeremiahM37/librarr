package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/JeremiahM37/librarr/internal/db"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"
)

const resetPasswordUsage = `Usage: librarr reset-password [--unlink-sso] <username>

Sets a new password for a Librarr account, for example an admin whose
password is unknown. Run it on the server, e.g.:

  docker exec -it librarr librarr reset-password admin

The new password is prompted for, or read from the first line of stdin when
stdin is not a terminal. It uses the same database as the server
(LIBRARR_DB_PATH), which can keep running.

  --unlink-sso   The account signs in with SSO: convert it back to a password
                 account. Do this when the identity provider is unavailable.
                 It can be linked to SSO again from Settings > Account.
`

// runResetPassword implements `librarr reset-password` and returns the exit
// code.
func runResetPassword(args []string, stdin *os.File, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("reset-password", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, resetPasswordUsage) }
	unlinkSSO := fs.Bool("unlink-sso", false, "convert an SSO account back to a password account")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	username := fs.Arg(0)

	password, err := readNewPassword(stdin, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "Error:", err)
		return 1
	}

	// Only the database is needed; config.Load would also fetch the sources
	// registry and read the settings file.
	dbPath := os.Getenv("LIBRARR_DB_PATH")
	if dbPath == "" {
		dbPath = "/data/librarr.db"
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	database, err := db.New(dbPath)
	if err != nil {
		fmt.Fprintln(stderr, "Error: open database:", err)
		return 1
	}
	defer database.Close()

	if err := resetPassword(database, username, password, *unlinkSSO, stdout); err != nil {
		fmt.Fprintln(stderr, "Error:", err)
		return 1
	}
	return 0
}

// resetPassword sets username's password. An SSO account is refused unless
// unlinkSSO is set, in which case it becomes a password account again.
func resetPassword(database *db.DB, username, password string, unlinkSSO bool, out io.Writer) error {
	if len(password) < 6 || len(password) > 72 {
		return errors.New("password must be 6-72 characters")
	}
	user, err := database.GetUserByUsername(username)
	if err != nil {
		return fmt.Errorf("no account named %q", username)
	}
	if user.IsOIDC() && !unlinkSSO {
		return fmt.Errorf("%q signs in with SSO; pass --unlink-sso to convert it back to a password account", user.Username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	if user.IsOIDC() {
		err = database.ConvertToLocal(user.ID, string(hash))
	} else {
		err = database.UpdateUserPassword(user.ID, string(hash))
	}
	if err != nil {
		return fmt.Errorf("update account: %w", err)
	}

	database.LogActivity(user.Username, "password_reset", user.Username, "Password reset from the command line")
	if user.IsOIDC() {
		fmt.Fprintf(out, "%s is now a password account with the new password. It no longer signs in with SSO.\n", user.Username)
	} else {
		fmt.Fprintf(out, "Password for %s updated.\n", user.Username)
	}
	if user.TOTPEnabled {
		fmt.Fprintln(out, "Two-factor authentication is still on for this account.")
	}
	return nil
}

// readNewPassword prompts twice without echo on a terminal, or reads the first
// line of stdin otherwise.
func readNewPassword(stdin *os.File, prompt io.Writer) (string, error) {
	fd := int(stdin.Fd())
	if !term.IsTerminal(fd) {
		line, err := bufio.NewReader(stdin).ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", fmt.Errorf("read password: %w", err)
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
	fmt.Fprint(prompt, "New password: ")
	first, err := term.ReadPassword(fd)
	fmt.Fprintln(prompt)
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	fmt.Fprint(prompt, "Confirm new password: ")
	second, err := term.ReadPassword(fd)
	fmt.Fprintln(prompt)
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	if string(first) != string(second) {
		return "", errors.New("passwords do not match")
	}
	return string(first), nil
}
