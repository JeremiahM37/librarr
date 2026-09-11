# Librarr React + TypeScript migration

Source is `src/`; Vite embeds generated `../static/react/librarr.{js,css}` via
Go's existing static filesystem. The server/backend contracts are retained.
The old `static/js/app.js` is a reference asset and is not loaded by index.html.

## Implemented

- Account bootstrap/no-auth local-admin mode, API-key storage, OIDC, invitation
  registration, login/logout, TOTP challenge and full enrollment/backup-code/
  disable controls. Own-password change and administrator users/invite CRUD.
- Streaming search with JSON fallback, canceled-search race protection, actual
  three category routes, metadata/cover/ownership cards, sorting, all source
  download fields (including Usenet routing), conflict-to-force transition,
  asynchronous download job status and manual Anna fallback link.
- Library category browsing/filter/grouping/pagination/readers/covers/deletion;
  Downloads real status/progress/detail/retry/cancel/clear/polling.
- Wanted monitoring/profiles/new item profile/manual search/dry-run outcomes,
  scheduler settings and all quality-profile formats/order/cutoff/size fields.
  Author interval/auto-add/baseline/new-work controls.
- All nine integration credential groups, connection checks on supported
  services, import/remove-torrent/foreign-language options, configuration/sources.
- Existing English/Russian catalog and language switch, keyboard search shortcut.

## Verification contracts

The former Go tests inspected legacy string markup and global click registries.
React handlers are compiled with TypeScript; equivalent user behavior is exercised
by `e2e/test_wanted.py`, `test_react_totp.py`, `test_react_account_settings.py`,
`test_react_search_contracts.py`, and the original real-download/import journeys.
Tests that formerly called `state.renderedResults`/`renderBookCard` now exercise
actual rendered buttons and intercepted transport. No production compatibility
DOM/global renderer was introduced for tests.

Current source suite `.verify.yaml` builds/types React, tests Go and runs all real
Chromium journeys against temporary services. Production remains unmodified until
parent deploys and runs live service/read-browser checks. No commit/push requested.
