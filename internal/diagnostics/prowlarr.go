package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type ProwlarrConfig struct {
	URL     string
	APIKey  string
	Timeout time.Duration
}

func DiagnoseProwlarr(ctx context.Context, cfg ProwlarrConfig) Result {
	r := newRunner("prowlarr")
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	if step := timedStep("Configuration", func() (string, string, error) {
		if strings.TrimSpace(cfg.URL) == "" {
			return "Prowlarr URL is missing.", "Set the Prowlarr URL in Settings.", fmt.Errorf("missing Prowlarr URL")
		}
		if strings.TrimSpace(cfg.APIKey) == "" {
			return "Prowlarr API key is missing.", "Copy the API key from Prowlarr Settings.", fmt.Errorf("missing Prowlarr API key")
		}
		return "URL and API key are configured.", "", nil
	}); step.Status == StatusFailed {
		r.add(step)
		return r.finish()
	} else {
		r.add(step)
	}

	var ep *endpoint
	if step := timedStep("URL Valid", func() (string, string, error) {
		var err error
		ep, err = prepareEndpoint(cfg.URL)
		if err != nil {
			return "Prowlarr URL is invalid.", "Use a full http:// or https:// URL that Librarr can reach.", err
		}
		return ep.Origin, "", nil
	}); step.Status == StatusFailed {
		r.add(step)
		return r.finish()
	} else {
		r.add(step)
	}

	if step := timedStep("DNS Lookup", func() (string, string, error) {
		addrs, err := lookupDNS(ctx, ep.Host)
		if err != nil {
			return "Unable to resolve hostname.", "Verify Docker network, hostname, or DNS configuration.", err
		}
		return fmt.Sprintf("Resolved %d address(es).", len(addrs)), "", nil
	}); step.Status == StatusFailed {
		r.add(step)
		return r.finish()
	} else {
		r.add(step)
	}

	if step := timedStep("TCP Connection", func() (string, string, error) {
		if err := tcpConnect(ctx, ep.Host, ep.Port, timeout); err != nil {
			return fmt.Sprintf("Connection timed out or failed on port %s.", ep.Port), "Verify host, firewall, routing, container status, or Docker network.", err
		}
		return "TCP connection succeeded.", "", nil
	}); step.Status == StatusFailed {
		r.add(step)
		return r.finish()
	} else {
		r.add(step)
	}

	if ep.URL.Scheme == "https" {
		if step := timedStep("HTTPS", func() (string, string, error) {
			if err := tlsHandshake(ctx, ep.Host, ep.Port, timeout); err != nil {
				return "Certificate validation failed.", "Verify HTTPS configuration or certificate trust.", err
			}
			return "TLS handshake succeeded.", "", nil
		}); step.Status == StatusFailed {
			r.add(step)
			return r.finish()
		} else {
			r.add(step)
		}
	} else {
		r.add(Step{Name: "HTTPS", Status: StatusSkipped, Message: "HTTP endpoint; TLS was not used."})
	}

	client := httpClient(timeout)
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.Origin+"/api/v1/system/status", nil)
	if err != nil {
		r.add(Step{Name: "HTTP Request", Status: StatusFailed, Message: "Could not build Prowlarr request.", Suggestion: "Verify the configured URL."})
		return r.finish()
	}
	req.Header.Set("X-Api-Key", cfg.APIKey)
	resp, err := client.Do(req)
	duration := time.Since(start).Milliseconds()
	if err != nil {
		r.add(Step{Name: "HTTP Request", Status: StatusFailed, DurationMS: duration, Message: "Prowlarr HTTP request failed.", Suggestion: "Verify reverse proxy, URL path, and service availability."})
		return r.finish()
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	r.add(Step{Name: "HTTP Request", Status: StatusSuccess, DurationMS: duration, Message: statusText(resp.StatusCode)})

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		r.add(Step{Name: "Authentication", Status: StatusFailed, Message: statusText(resp.StatusCode), Suggestion: "Verify API key."})
		return r.finish()
	}
	r.add(Step{Name: "Authentication", Status: StatusSuccess, Message: "API key accepted."})

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.add(Step{Name: "API Validation", Status: StatusFailed, Message: statusText(resp.StatusCode), Suggestion: "Verify Prowlarr API path and reverse proxy routing."})
		return r.finish()
	}
	if readErr != nil {
		r.add(Step{Name: "API Validation", Status: StatusFailed, Message: "Could not read Prowlarr response.", Suggestion: "Retry the diagnostic or check Prowlarr logs."})
		return r.finish()
	}
	var status struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		r.add(Step{Name: "API Validation", Status: StatusFailed, Message: "Prowlarr returned invalid JSON.", Suggestion: "Verify the URL points to Prowlarr, not a login page or reverse-proxy error."})
		return r.finish()
	}
	r.add(Step{Name: "API Validation", Status: StatusSuccess, Message: "Prowlarr API returned JSON."})
	if strings.TrimSpace(status.Version) == "" {
		r.add(Step{Name: "API Version", Status: StatusWarning, Message: "Prowlarr did not report a version.", Suggestion: "Verify Prowlarr version endpoint behavior."})
	} else {
		r.add(Step{Name: "API Version", Status: StatusSuccess, Message: strings.TrimSpace(status.Version)})
	}
	r.add(Step{Name: "Latency", Status: StatusSuccess, DurationMS: duration, Message: fmt.Sprintf("%d ms", duration)})
	return r.finish()
}
