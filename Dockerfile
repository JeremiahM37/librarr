FROM node:20.19-alpine AS ui-builder

WORKDIR /build/web/ui
COPY web/ui/package.json web/ui/package-lock.json ./
RUN npm ci
COPY web/ui/ ./
COPY web/index.html ../index.html
RUN npm run build

FROM golang:1.25-alpine AS builder

WORKDIR /build

# Cache dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Build the binary.
COPY . .
COPY --from=ui-builder /build/web/static/react ./web/static/react
COPY --from=ui-builder /build/web/index.html ./web/index.html
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /librarr ./cmd/librarr/

# --- Runtime image ---
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 librarr

COPY --from=builder /librarr /usr/local/bin/librarr

USER librarr
EXPOSE 5050

ENTRYPOINT ["/usr/local/bin/librarr"]
