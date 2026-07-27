# Security Policy

## Supported versions

Librarr 2.0 is under active development. Security fixes are currently applied
to the active `main` branch and the current Librarr 2.0 development branch.

Older dogfood or beta images should be upgraded promptly when a security fix is
released.

## Reporting a vulnerability

Please do not open a public GitHub issue for an active vulnerability.

If GitHub private vulnerability reporting is available for this repository, use
that workflow. If it is not available, contact the maintainer privately through
the repository owner's GitHub profile or another already-published project
contact method.

Include as much of the following as possible:

- Librarr version, Docker tag, or commit SHA.
- Deployment method and relevant configuration.
- A clear description of the vulnerability.
- Reproduction steps or proof of concept.
- Impact and affected components.
- Whether the issue requires authentication.
- Sanitized logs, requests, or screenshots.

Do not include real passwords, API keys, session cookies, private tracker URLs,
or personal library contents in reports.

## Response expectations

This is a hobby open-source project, so response times may vary. The maintainer
will make a best effort to acknowledge credible reports, investigate impact, and
coordinate a fix before public disclosure.

## Security scope

In scope:

- authentication and authorization flaws
- session handling problems
- path traversal or unsafe file download behavior
- SSRF or unsafe outbound request behavior
- secret leakage in logs, APIs, or UI
- unsafe Docker/default deployment behavior
- dependency vulnerabilities with practical impact

Out of scope:

- vulnerabilities requiring full host or container admin access
- issues in third-party services such as qBittorrent, Prowlarr, or reverse
  proxies unless Librarr materially worsens the risk
- denial-of-service reports without practical exploit detail
- reports based only on outdated dependencies without a reachable vulnerable
  code path

## Self-hosting guidance

- Put Librarr behind HTTPS when accessed outside your LAN.
- Use a trusted reverse proxy and configure trusted proxy headers carefully.
- Treat API keys, download-client credentials, OAuth/OIDC secrets, and session
  cookies as sensitive.
- Keep database and settings volumes private and backed up.
- Avoid sharing logs until secrets and private URLs have been removed.
- Prefer private GHCR images or authenticated pulls until public release
  settings are intentionally configured.
