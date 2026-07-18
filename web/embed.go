// Package web embeds the static web UI assets served by the API.
package web

import "embed"

//go:embed index.html
var IndexHTML []byte

// StaticFS holds the frontend assets (CSS, JS, vendored Tailwind + Inter font)
// served under /static/. Everything is embedded so the binary stays fully
// self-contained and the UI works with no internet access.
//
//go:embed static
var StaticFS embed.FS
