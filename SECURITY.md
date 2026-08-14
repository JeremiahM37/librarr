# Security Policy

## Supported versions

Security fixes land on `main` and ship in the next release. Only the latest
release is supported — please upgrade before reporting an issue against an
older tag.

## Reporting a vulnerability

Please report privately, not in a public issue:

- **Preferred:** [Report a vulnerability](https://github.com/JeremiahM37/librarr/security/advisories/new)
  via GitHub's private vulnerability reporting.
- Alternatively, open a GitHub issue titled "Security contact request" with no
  technical detail and we'll arrange a private channel.

A useful report includes the affected version or commit, the affected code
path, reproduction steps or a PoC, and the impact you believe it has.

### What to expect

- **Acknowledgement** within a few days.
- **Assessment** and a fix plan once the report is confirmed.
- **Disclosure** through a GitHub Security Advisory once a fix is released,
  coordinated with you on timing.

Librarr is a self-hosted, volunteer-maintained project — there is no bug
bounty, but valid reports are credited.

## Credit

Reporters are credited in the published GitHub Security Advisory, in the
release notes, and with a `Reported-by:` trailer on the fixing commit — unless
you'd rather stay anonymous. Tell us how you'd like to be named (real name,
handle, affiliation) when you report.

## Threat model

Librarr is designed to run on a private network or behind a reverse proxy with
authentication. Note in particular:

- **Do not expose an unauthenticated instance to the internet.** With no users
  in the database, no `AUTH_USERNAME`/`AUTH_PASSWORD`, and no `API_KEY`, the
  instance treats every caller as an administrator so that first-run setup
  works. Configure authentication before exposing the service.
- The first account created via `/api/register` becomes the administrator.
  Create it immediately after deployment.
- `LIBRARR_INSECURE_ALLOW_PRIVATE_URLS=1` disables the SSRF guard on outbound
  download URLs. Only set it when you intentionally download from a LAN mirror.

Issues that require an already-privileged position (for example a
misconfiguration that grants an attacker admin credentials) are still worth
reporting — we just weigh them against this model.
