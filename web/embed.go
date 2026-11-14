// Package web embeds the static web UI assets served by the API.
package web

import "embed"

// Build the frontend into static/react before building the binary:
// go generate ./web (or make build).
//
//go:generate npm ci --prefix ui
//go:generate npm run build --prefix ui

// StaticFS holds the frontend assets (CSS, JS, vendored Tailwind + Inter font)
// served under /static/. Everything is embedded so the binary stays fully
// self-contained and the UI works with no internet access.
//
// static/react is the frontend build output (npm run build --prefix web/ui)
// and is not committed.
//
//go:embed static
var StaticFS embed.FS

// IndexHTML is the page served for the UI, written by the frontend build. A
// binary built without running the frontend build serves NotBuiltHTML instead.
var IndexHTML = indexHTML()

// NotBuiltHTML is served when the binary was built without the frontend.
const NotBuiltHTML = `<!doctype html>
<html lang="en">
<head><meta charset="UTF-8"><title>Librarr</title></head>
<body>
  <p>The Librarr web UI was not built into this binary. Run
  <code>npm ci --prefix web/ui &amp;&amp; npm run build --prefix web/ui</code>
  before <code>go build</code>.</p>
</body>
</html>
`

func indexHTML() []byte {
	if b, err := StaticFS.ReadFile("static/react/index.html"); err == nil {
		return b
	}
	return []byte(NotBuiltHTML)
}
