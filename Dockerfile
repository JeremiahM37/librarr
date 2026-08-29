FROM golang:1.25-alpine AS builder

WORKDIR /build

# Cache dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Build the binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /librarr ./cmd/librarr/

# --- Runtime image ---
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata su-exec && \
    adduser -D -u 1000 librarr

COPY --from=builder /librarr /usr/local/bin/librarr
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# No USER directive here on purpose - entrypoint.sh starts as root so it can
# fix up ownership on whatever gets mounted at /data and /books/*, then
# drops to PUID:PGID (default 1000:1000, matching the adduser above) via
# su-exec before ever executing the librarr binary itself.
EXPOSE 5050

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
