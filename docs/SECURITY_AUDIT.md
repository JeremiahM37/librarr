# Librarr Security Audit

Audit date: 2026-06-03  
Last updated: 2026-06-03 (post-merge to `main`)  
Scope: Amateur-God/librarr — security review stack merged to `main`  
Method: Static code review of HTTP handlers, auth flows, file I/O, and outbound HTTP clients (AI-assisted review; human-verified locally).

Findings are grouped by severity. **Fixed** indicates a code change is present on `main`.

---

## Critical

### C1: SSRF via direct download URL

| | |
|---|---|
| **Files** | `internal/api/download.go`, `internal/download/direct.go` |
| **Scenario** | Authenticated user POSTs `{"url":"http://169.254.169.254/..."}` to `/api/download`; server fetches internal metadata or other private endpoints. |
| **Fix** | **Fixed** — `netutil.ValidateOutboundURL` applied before `StartDirectDownload`. |

### C2: Arbitrary filesystem read via manual import

| | |
|---|---|
| **Files** | `internal/api/manualimport.go` |
| **Scenario** | User supplies `/etc` or other paths to `/api/import/scan` or `/api/import/files`. |
| **Fix** | **Fixed** — paths must resolve under configured library/incoming roots (`validateAllowedPath`). |

---

## High

### H1: SSRF via webhook test/create

| | |
|---|---|
| **Files** | `internal/api/webhooks.go`, `internal/webhook/webhook.go` |
| **Scenario** | Admin triggers outbound POST to internal services via webhook test or malicious webhook URL. |
| **Fix** | **Fixed** — URL validation on create and test; generic error on test failure. |

### H2: Incomplete SSRF guard on connection tests

| | |
|---|---|
| **Files** | `internal/api/settings.go` |
| **Scenario** | Prowlarr connection test accepted `localhost` / private IPs via prefix-only blocklist. |
| **Fix** | **Fixed** — connection tests use `ValidateIntegrationURL` (allows LAN/localhost for homelab); strict `ValidateOutboundURL` kept for user-supplied download URLs. |

### H3: OPDS library exposure without auth

| | |
|---|---|
| **Files** | `internal/api/auth.go`, `internal/api/opds.go` |
| **Scenario** | When session auth is enabled, `/opds/books` and `/opds/download/{id}` were reachable without login. |
| **Fix** | **Fixed** — OPDS routes now require OPDS-compatible HTTP Basic authentication against existing enabled Librarr users. API-key access remains available for scripted clients. |

### H4: Session cookie missing Secure flag

| | |
|---|---|
| **Files** | `internal/api/auth.go`, `internal/api/oidc.go`, `internal/api/cookies.go` |
| **Scenario** | Session cookie sent over HTTP on TLS-terminated deployments without `Secure`. |
| **Fix** | **Fixed** — `sessionCookie()` sets `Secure` when TLS or `X-Forwarded-Proto: https`. |

### H5: OIDC email_verified not enforced

| | |
|---|---|
| **Files** | `internal/api/oidc.go`, `internal/config/config.go` |
| **Scenario** | Unverified email from IdP used for account creation/login. |
| **Fix** | **Fixed** — reject when email present and `email_verified == false` (configurable via `OIDC_REQUIRE_EMAIL_VERIFIED`, default `true`). |

### H6: Error messages leak upstream details

| | |
|---|---|
| **Files** | `internal/api/download.go`, `internal/api/backup.go`, `internal/api/health.go` (+ callers) |
| **Scenario** | Raw `err.Error()` returned to client may include filesystem paths or upstream URLs. |
| **Fix** | **Fixed** — generic client messages via `writeError()`; details logged server-side. |

### H7: Backup API returns internal filesystem path

| | |
|---|---|
| **Files** | `internal/api/backup.go` |
| **Scenario** | `POST /api/backup/create` JSON included absolute `path` to zip on disk. |
| **Fix** | **Fixed** — path removed from response. |

### H8: Admin routes 403 when auth disabled

| | |
|---|---|
| **Files** | `internal/api/auth.go` |
| **Scenario** | Open homelab installs with no users, legacy auth, or API key configured could not save settings — `requireAdmin` returned 403 because no role was set in context. |
| **Fix** | **Fixed** — auth middleware grants admin context when auth is fully disabled. |

---

## Medium

### M1: API key via query parameter

| | |
|---|---|
| **Files** | `internal/api/auth.go` |
| **Scenario** | `?apikey=` may appear in access logs, Referer, browser history. |
| **Fix** | **Partial** — audit documents risk; warning logged when query param used. Prefer `X-Api-Key` header. |

### M2: Login response includes token in JSON body

| | |
|---|---|
| **Files** | `internal/api/auth.go` |
| **Scenario** | Token in body increases XSS exfil surface if UI ever renders untrusted content. |
| **Fix** | **Documented** — cookie is primary mechanism; body token retained for API clients. |

### M3: Torznab API key optional

| | |
|---|---|
| **Files** | `internal/torznab/handler.go` |
| **Scenario** | When `TORZNAB_API_KEY` unset, Torznab endpoint is unauthenticated. |
| **Fix** | **Documented** — set `TORZNAB_API_KEY` in production. |

### M4: File upload extension-only validation

| | |
|---|---|
| **Files** | `internal/api/upload.go`, `internal/download/direct.go` |
| **Scenario** | Malicious file with allowed extension but wrong content. |
| **Fix** | **Fixed** — upload handler verifies magic bytes via `download.DetectFileExtension` and rejects extension/content mismatches. |

### M5: TOTP brute force

| | |
|---|---|
| **Files** | `internal/api/totp.go`, `internal/api/auth.go`, `internal/api/ratelimit.go` |
| **Scenario** | No dedicated rate limit on `/api/login/totp`. |
| **Fix** | **Partial** — `/api/login/totp` falls under the general `api` rate-limit bucket (300/min per IP), not the tighter `login` bucket (20/min). A dedicated TOTP limit remains a follow-up. |

### M6: Settings credentials at rest

| | |
|---|---|
| **Files** | `settings.json` |
| **Scenario** | API keys/passwords stored plaintext (file mode `0600`). |
| **Fix** | **Documented** — acceptable for single-user homelab; encryption at rest out of scope. |

### M7: LIBRARR_SOURCES_URL SSRF if env compromised

| | |
|---|---|
| **Files** | `internal/sources/load.go` |
| **Scenario** | Attacker with env access points registry fetch at internal URL. |
| **Fix** | **Documented** — admin-controlled env; validate if URL becomes user-settable. |

---

## Low / Informational

### L1: OPDS reader feature depth

OPDS now requires authentication and supports catalog browsing/downloads. Reading
progress sync, annotations, highlights, and OPDS 2.0 remain future feature work.

### L2: OIDC sub not stored for account linking

No DB column for OIDC `sub` yet; username collision possible across IdP changes. Document for future migration.

### L3: DNS rebinding

`ValidateOutboundURL` resolves at validation time; time-of-check/time-of-use gap remains. Mitigation: custom `http.Transport` with dial-time IP check (future hardening).

### L4: Webhook delivery goroutines unbounded

| | |
|---|---|
| **Files** | `internal/webhook/webhook.go` |
| **Scenario** | Unbounded goroutines under high webhook volume. |
| **Fix** | **Partial** — delivery capped at 10 concurrent sends via semaphore; full worker pool still a follow-up. |

### L5: Logging

No passwords/API keys found in slog calls. OIDC errors log generic messages.

### L6: Homelab integration test URLs

Connection tests use `ValidateIntegrationURL`, which allows private IPs and localhost (typical homelab: `http://192.168.x.x:port`). Cloud metadata endpoints remain blocked. User-supplied download URLs still use strict SSRF validation.

---

## Positive controls observed

- bcrypt password hashing with reasonable length limits
- Constant-time API key comparison (`subtle.ConstantTimeCompare`)
- Sensitive settings masked on GET (`settings.go`)
- Request body size limits (1MB JSON, 500MB multipart upload)
- OPDS download path confinement with symlink resolution
- SQLite WAL mode + busy timeout
- CI: `go test -race`, govulncheck, staticcheck, fuzz tests, integration tests, CodeQL workflow

---

## Merged follow-up work (now on `main`)

The review stack added the following beyond the initial security pass:

| Area | Changes |
|------|---------|
| **Code quality** | `writeError()` helper, search query truncation, MD5 validation, upload magic-byte checks, download-manager and DB race fixes, webhook concurrency cap, session `rand.Read` error handling |
| **Testing** | Download pipeline test, search/API fuzz tests, concurrent search stress test, integration test (`-tags=integration`), CI workflow updates |
| **Auth** | Admin context granted when auth is fully disabled (settings save fix) |

### Remaining follow-ups

- Dedicated rate limit for `/api/login/totp`
- Full webhook worker pool (semaphore cap is in place)
- Dial-time IP validation to close DNS rebinding gap
- OPDS Basic Auth for protected catalogs
