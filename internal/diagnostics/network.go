package diagnostics

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/netutil"
)

const defaultTimeout = 5 * time.Second

type endpoint struct {
	Raw    string
	URL    *url.URL
	Host   string
	Port   string
	Origin string
}

func prepareEndpoint(raw string) (*endpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("URL is required")
	}
	if err := netutil.ValidateIntegrationURL(raw); err != nil {
		return nil, err
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return nil, fmt.Errorf("URL host is required")
	}
	port := parsed.Port()
	if port == "" {
		switch parsed.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return nil, fmt.Errorf("unsupported URL scheme")
		}
	}
	origin := parsed.Scheme + "://" + parsed.Host
	return &endpoint{Raw: raw, URL: parsed, Host: host, Port: port, Origin: strings.TrimRight(origin, "/")}, nil
}

func lookupDNS(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

func tcpConnect(ctx context.Context, host, port string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	return conn.Close()
}

func tlsHandshake(ctx context.Context, host, port string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(&dialer, "tcp", net.JoinHostPort(host, port), &tls.Config{
		ServerName: host,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		return err
	}
	return conn.Close()
}

func httpClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

func statusText(code int) string {
	return fmt.Sprintf("HTTP %d %s", code, http.StatusText(code))
}
