package diagnostics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type QBittorrentConfig struct {
	URL      string
	Username string
	Password string
	Timeout  time.Duration
}

func DiagnoseQBittorrent(ctx context.Context, cfg QBittorrentConfig) Result {
	r := newRunner("qbittorrent")
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	if step := timedStep("Configuration", func() (string, string, error) {
		if strings.TrimSpace(cfg.URL) == "" {
			return "qBittorrent URL is missing.", "Set the qBittorrent Web UI URL in Settings.", fmt.Errorf("missing qBittorrent URL")
		}
		if strings.TrimSpace(cfg.Username) == "" {
			return "qBittorrent username is empty.", "Set a qBittorrent username if Web UI authentication is enabled.", nil
		}
		return "URL and credentials are configured.", "", nil
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
			return "qBittorrent URL is invalid.", "Use a full http:// or https:// URL that Librarr can reach.", err
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
	form := url.Values{"username": {cfg.Username}, "password": {cfg.Password}}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.Origin+"/api/v2/auth/login", strings.NewReader(form.Encode()))
	if err != nil {
		r.add(Step{Name: "HTTP Request", Status: StatusFailed, Message: "Could not build qBittorrent login request.", Suggestion: "Verify the configured URL."})
		return r.finish()
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	duration := time.Since(start).Milliseconds()
	if err != nil {
		r.add(Step{Name: "HTTP Request", Status: StatusFailed, DurationMS: duration, Message: "qBittorrent HTTP request failed.", Suggestion: "Verify reverse proxy, URL path, and service availability."})
		return r.finish()
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	r.add(Step{Name: "HTTP Request", Status: StatusSuccess, DurationMS: duration, Message: statusText(resp.StatusCode)})

	bodyText := strings.TrimSpace(string(body))
	hasSession := false
	for _, cookie := range resp.Cookies() {
		if strings.HasPrefix(cookie.Name, "QBT_SID") {
			hasSession = true
			break
		}
	}
	if readErr != nil {
		r.add(Step{Name: "Authentication", Status: StatusFailed, Message: "Could not read qBittorrent login response.", Suggestion: "Retry the diagnostic or check qBittorrent logs."})
		return r.finish()
	}
	authOK := resp.StatusCode >= 200 && resp.StatusCode < 300 && (bodyText == "Ok." || hasSession || resp.StatusCode == http.StatusNoContent)
	if !authOK {
		message := statusText(resp.StatusCode)
		if strings.Contains(strings.ToLower(bodyText), "banned") {
			message = "qBittorrent reports this client is temporarily banned."
		} else if bodyText != "" {
			message = bodyText
		}
		r.add(Step{Name: "Authentication", Status: StatusFailed, Message: message, Suggestion: "Verify qBittorrent username, password, Web UI access rules, and ban status."})
		return r.finish()
	}
	r.add(Step{Name: "Authentication", Status: StatusSuccess, Message: "Login accepted."})

	versionReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.Origin+"/api/v2/app/version", nil)
	if err != nil {
		r.add(Step{Name: "API Validation", Status: StatusFailed, Message: "Could not build qBittorrent version request.", Suggestion: "Verify the configured URL."})
		return r.finish()
	}
	for _, cookie := range resp.Cookies() {
		versionReq.AddCookie(cookie)
	}
	versionStart := time.Now()
	versionResp, err := client.Do(versionReq)
	versionDuration := time.Since(versionStart).Milliseconds()
	if err != nil {
		r.add(Step{Name: "API Validation", Status: StatusFailed, DurationMS: versionDuration, Message: "qBittorrent version request failed.", Suggestion: "Verify Web API access."})
		return r.finish()
	}
	defer versionResp.Body.Close()
	versionBody, readErr := io.ReadAll(io.LimitReader(versionResp.Body, 64<<10))
	if versionResp.StatusCode < 200 || versionResp.StatusCode >= 300 {
		r.add(Step{Name: "API Validation", Status: StatusFailed, DurationMS: versionDuration, Message: statusText(versionResp.StatusCode), Suggestion: "Verify qBittorrent Web API permissions."})
		return r.finish()
	}
	if readErr != nil {
		r.add(Step{Name: "API Validation", Status: StatusFailed, DurationMS: versionDuration, Message: "Could not read qBittorrent version response.", Suggestion: "Retry the diagnostic or check qBittorrent logs."})
		return r.finish()
	}
	r.add(Step{Name: "API Validation", Status: StatusSuccess, DurationMS: versionDuration, Message: "qBittorrent Web API responded."})
	version := strings.TrimSpace(string(versionBody))
	if version == "" {
		r.add(Step{Name: "API Version", Status: StatusWarning, Message: "qBittorrent did not report a version.", Suggestion: "Verify qBittorrent Web API version endpoint."})
	} else {
		r.add(Step{Name: "API Version", Status: StatusSuccess, Message: version})
	}
	totalHTTP := duration + versionDuration
	r.add(Step{Name: "Latency", Status: StatusSuccess, DurationMS: totalHTTP, Message: fmt.Sprintf("%d ms", totalHTTP)})
	return r.finish()
}
