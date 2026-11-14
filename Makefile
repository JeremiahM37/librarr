.PHONY: build ui test clean

# Build the web UI, then the binary that embeds it.
build: ui
	go build -o librarr ./cmd/librarr/

# Build the web UI into web/static/react (see the go:generate lines in web/embed.go).
ui:
	go generate ./web

test: ui
	go test ./...

clean:
	rm -rf librarr web/static/react
