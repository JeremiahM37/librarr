package api

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image/png"
	"log/slog"
	"math/big"
	"net/http"
	"time"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
)

// totpQRDataURI renders the key's otpauth URL as a PNG QR code, encoded as a
// data: URI. otp/totp already depends on a barcode encoder, so this costs no
// new dependency and no request leaves the instance.
func totpQRDataURI(key *otp.Key) (string, error) {
	img, err := key.Image(220, 220)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return "", err
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// validateTOTPCode validates a TOTP code against a secret.
func validateTOTPCode(secret, code string) bool {
	if secret == "" || code == "" {
		return false
	}
	return totp.Validate(code, secret)
}

// generateBackupCodes generates n random 8-digit backup codes.
func generateBackupCodes(n int) ([]string, error) {
	codes := make([]string, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(100000000))
		if err != nil {
			return nil, err
		}
		codes[i] = fmt.Sprintf("%08d", num.Int64())
	}
	return codes, nil
}

// handleTOTPSetup handles POST /api/totp/setup — generate TOTP secret + QR + backup codes.
func handleTOTPSetup(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := getUserIDFromContext(r)
		if userID == 0 {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Not authenticated",
			})
			return
		}

		user, err := database.GetUser(userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "User not found",
			})
			return
		}

		if user.TOTPEnabled {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "TOTP is already enabled. Disable it first.",
			})
			return
		}

		// Generate a new TOTP key.
		key, err := totp.Generate(totp.GenerateOpts{
			Issuer:      "Librarr",
			AccountName: user.Username,
		})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to generate TOTP key",
			})
			return
		}

		// Store the secret (not yet enabled).
		if err := database.SetTOTPSecret(userID, key.Secret()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to save TOTP secret",
			})
			return
		}

		// Generate backup codes.
		backupCodes, err := generateBackupCodes(8)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to generate backup codes",
			})
			return
		}

		// Hash and store backup codes.
		var codeHashes []string
		for _, code := range backupCodes {
			codeHashes = append(codeHashes, hashBackupCode(code))
		}
		if err := database.SaveBackupCodes(userID, codeHashes); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to save backup codes",
			})
			return
		}

		resp := map[string]interface{}{
			"success":      true,
			"secret":       key.Secret(),
			"qr_url":       key.URL(),
			"backup_codes": backupCodes,
		}
		// Render the enrolment QR here rather than in the browser: the otpauth
		// URL carries the TOTP secret, so handing it to a third-party QR service
		// would disclose the seed (and break the offline-by-default UI). A data:
		// URI keeps it in the response we already send.
		if png, err := totpQRDataURI(key); err == nil {
			resp["qr_png"] = png
		} else {
			// Non-fatal: the UI falls back to the secret and otpauth URI, which
			// every authenticator app accepts for manual entry.
			slog.Warn("failed to render TOTP QR code", "error", err)
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// handleTOTPVerify handles POST /api/totp/verify — verify code and enable 2FA.
func handleTOTPVerify(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := getUserIDFromContext(r)
		if userID == 0 {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Not authenticated",
			})
			return
		}

		var req struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		user, err := database.GetUser(userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "User not found",
			})
			return
		}

		if user.TOTPSecret == "" {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "No TOTP secret configured. Call /api/totp/setup first.",
			})
			return
		}

		if !totp.Validate(req.Code, user.TOTPSecret) {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid TOTP code. Check your authenticator app.",
			})
			return
		}

		if err := database.EnableTOTP(userID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to enable TOTP",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Two-factor authentication enabled",
		})
	}
}

// handleTOTPDisable handles POST /api/totp/disable — disable 2FA (requires current code).
func handleTOTPDisable(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := getUserIDFromContext(r)
		if userID == 0 {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Not authenticated",
			})
			return
		}

		var req struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		user, err := database.GetUser(userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "User not found",
			})
			return
		}

		if !user.TOTPEnabled {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "TOTP is not enabled",
			})
			return
		}

		if !totp.Validate(req.Code, user.TOTPSecret) {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid TOTP code",
			})
			return
		}

		if err := database.DisableTOTP(userID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to disable TOTP",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Two-factor authentication disabled",
		})
	}
}

// handleTOTPStatus handles GET /api/totp/status — check if TOTP is enabled for current user.
func handleTOTPStatus(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := getUserIDFromContext(r)
		if userID == 0 {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"enabled": false,
			})
			return
		}

		user, err := database.GetUser(userID)
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"enabled": false,
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled":    user.TOTPEnabled,
			"created_at": user.CreatedAt.Format(time.RFC3339),
		})
	}
}
