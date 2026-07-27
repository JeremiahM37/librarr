FROM golang:1.25-alpine AS builder

WORKDIR /build

# Cache dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Build the binary.
COPY . .
ARG VERSION=development
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
ARG CHANNEL=development
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X github.com/jamie75/librarr/internal/api.Version=${VERSION} -X github.com/jamie75/librarr/internal/api.Commit=${COMMIT} -X github.com/jamie75/librarr/internal/api.BuildTime=${BUILD_TIME} -X github.com/jamie75/librarr/internal/api.Channel=${CHANNEL}" \
    -o /librarr ./cmd/librarr/

# --- Runtime image ---
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 librarr

COPY --from=builder /librarr /usr/local/bin/librarr

USER librarr
EXPOSE 5050

ENTRYPOINT ["/usr/local/bin/librarr"]
