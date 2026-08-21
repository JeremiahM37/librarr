# Contributing to Librarr

Thanks for your interest in contributing! Here's how to get started.

## Quick Start

```bash
git clone https://github.com/JeremiahM37/librarr.git
cd librarr
go build -o librarr ./cmd/librarr/
go test ./...
```

## Good First Issues

Check the [good first issue](https://github.com/JeremiahM37/librarr/labels/good%20first%20issue) label for beginner-friendly tasks.

## How to Contribute

1. Fork the repo
2. Create a branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests (`go test ./...`)
5. Commit and push
6. Open a Pull Request

## Test Suites

| Suite | Command | Needs |
|---|---|---|
| Unit + race | `go test ./... -race` | nothing |
| Integration | `go test -tags=integration ./internal/integration/...` | nothing |
| Browser end-to-end | `pip install -r e2e/requirements.txt && playwright install chromium`, then `LIBRARR_E2E_BIN=./librarr pytest e2e/` | Chromium |
| Import modes vs. a real torrent client | `LIBRARR_BIN=./librarr python3 e2e/manual_qbittorrent_check.py --spawn` | Docker |

The last one is not part of CI. It seeds real torrents in a disposable
qBittorrent and force-rechecks them, which is the only way to prove that an
imported torrent is still seedable and that deleting a torrent's files leaves
the library copy intact. Run it when you touch the import pipeline.

## Code Style

- Standard Go formatting (`gofmt`)
- Table-driven tests with `t.Run()`
- Mock HTTP calls with `httptest.NewServer`
- No external test frameworks — just `testing`

## Adding a Search Source

1. Create `internal/search/yoursource.go` implementing the `Source` interface
2. Add it to the source registry in `internal/search/searcher.go`
3. Add config vars in `internal/config/config.go`
4. Write tests in `internal/search/yoursource_test.go`

## Areas That Need Help

- **Security audit** — review HTTP clients for SSRF/injection (#4)
- **New search sources** — more book/audiobook/manga sources
- **UI improvements** — the web UI is functional but minimal
- **Documentation** — guides for specific setups (Readarr integration, Calibre-Web, etc.)

## License

By contributing, you agree that your contributions will be licensed under GPL-3.0.
