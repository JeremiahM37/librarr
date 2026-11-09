package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	coverMatchTTL         = 30 * 24 * time.Hour
	coverMissTTL          = 24 * time.Hour
	coverDefaultLimit     = 3
	coverLookupTimeout    = 15 * time.Second
	coverSearchLimit      = "20"
	coverSearchFields     = "title,author_name,cover_i"
	coverMaxResponseBytes = 2 << 20 // 2 MiB
	coverDefaultBaseURL   = "https://covers.openlibrary.org"
	coverCoolDownDefault  = time.Minute
	coverCoolDownMax      = time.Hour
	// Open Library allows one request a second, or three when the User-Agent
	// carries a contact address.
	coverInterval           = time.Second
	coverIdentifiedInterval = time.Second / 3
)

// ErrCoverCoolingDown means Open Library asked Librarr to back off, so a lookup
// was not answered. It says nothing about the book and is never cached.
var ErrCoverCoolingDown = errors.New("open library cover lookups are cooling down")

// CoverCache stores cover lookups between runs. A found entry with an empty URL
// is a cached miss.
type CoverCache interface {
	GetCoverLookup(key string) (url string, found bool, fetchedAt time.Time, err error)
	PutCoverLookup(key, url string, fetchedAt time.Time) error
}

// CoverResolverOptions configures a CoverResolver. Cache is required.
type CoverResolverOptions struct {
	Cache         CoverCache
	HTTPClient    *http.Client
	SearchURL     func() string // full search.json URL, read per lookup
	CoverBaseURL  func() string // e.g. https://covers.openlibrary.org
	MaxConcurrent int           // 0 means 3
	Now           func() time.Time
	// Contact is an email or phone number for Open Library to reach the
	// operator. When set it is added to the User-Agent and allows a faster pace.
	Contact string
	// MinInterval is the least time between two searches; 0 picks the pace
	// Open Library allows.
	MinInterval time.Duration
}

// CoverResolver turns titles and authors into Open Library cover URLs. Callers
// asking for the same book share one request, only a limited number of
// requests run at a time, and searches are spaced out to Open Library's limit.
type CoverResolver struct {
	cache        CoverCache
	httpClient   *http.Client
	searchURL    func() string
	coverBaseURL func() string
	now          func() time.Time
	slots        chan struct{}
	userAgent    string
	interval     time.Duration

	// turn is held by the next search to go out. Blocked senders on a channel
	// are served in order, so searches leave in the order they asked.
	turn     chan struct{}
	lastSent time.Time // guarded by turn

	mu        sync.Mutex
	inflight  map[string]*coverLookup
	notBefore time.Time // no request is sent before this time
}

// coverLookup is one in-flight request and the callers waiting on it. url and
// err are written before done closes and read only after. waiters is guarded by
// the resolver's mutex.
type coverLookup struct {
	done    chan struct{}
	waiters int
	cancel  context.CancelFunc
	url     string
	err     error
}

// NewCoverResolver creates a resolver.
func NewCoverResolver(o CoverResolverOptions) *CoverResolver {
	r := &CoverResolver{
		cache:        o.Cache,
		httpClient:   o.HTTPClient,
		searchURL:    o.SearchURL,
		coverBaseURL: o.CoverBaseURL,
		now:          o.Now,
		inflight:     make(map[string]*coverLookup),
		userAgent:    olUserAgent,
		interval:     o.MinInterval,
		turn:         make(chan struct{}, 1),
	}
	if o.Contact != "" {
		r.userAgent = strings.TrimSuffix(olUserAgent, ")") + "; " + o.Contact + ")"
	}
	if r.interval <= 0 {
		r.interval = coverInterval
		if o.Contact != "" {
			r.interval = coverIdentifiedInterval
		}
	}
	if r.httpClient == nil {
		r.httpClient = &http.Client{Timeout: coverLookupTimeout}
	}
	if r.searchURL == nil {
		r.searchURL = func() string { return olSearchAPI }
	}
	if r.coverBaseURL == nil {
		r.coverBaseURL = func() string { return coverDefaultBaseURL }
	}
	if r.now == nil {
		r.now = time.Now
	}
	limit := o.MaxConcurrent
	if limit <= 0 {
		limit = coverDefaultLimit
	}
	r.slots = make(chan struct{}, limit)
	return r
}

// Resolve returns the cover URL for a title and author. It returns "" with a nil
// error for a miss. Failed lookups return an error and are not cached. While
// cooling down, anything not in the cache returns ErrCoverCoolingDown.
//
// Cancelling ctx releases only this caller; the request itself stops once no
// caller is left waiting on it.
func (r *CoverResolver) Resolve(ctx context.Context, kind CoverKind, title, author string) (string, error) {
	if kind != CoverKindBook {
		return "", nil
	}
	key := CoverKey(kind, title, author)
	readings := coverReadings(title, author)
	if key == "" || len(readings) == 0 {
		return "", nil
	}

	url, found, err := r.cached(key)
	if err != nil {
		return "", err
	}
	if found {
		return url, nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r.coolingDown() {
		return "", ErrCoverCoolingDown
	}

	e := r.join(key, coverQueryText(title, author), readings)
	select {
	case <-e.done:
		return e.url, e.err
	case <-ctx.Done():
		r.leave(key, e)
		return "", ctx.Err()
	}
}

// cached reads a lookup that has not yet expired from the cache.
func (r *CoverResolver) cached(key string) (url string, found bool, err error) {
	url, found, fetchedAt, err := r.cache.GetCoverLookup(key)
	if err != nil {
		return "", false, fmt.Errorf("cover cache: %w", err)
	}
	if !found {
		return "", false, nil
	}
	ttl := coverMissTTL
	if url != "" {
		ttl = coverMatchTTL
	}
	if r.now().Sub(fetchedAt) > ttl {
		return "", false, nil
	}
	return url, true, nil
}

// join adds the caller to the key's in-flight lookup, starting one if needed.
// The lookup's context is detached from the caller's, because others may join.
func (r *CoverResolver) join(key, query string, readings []coverReading) *coverLookup {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.inflight[key]; ok {
		e.waiters++
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), coverLookupTimeout)
	e := &coverLookup{done: make(chan struct{}), waiters: 1, cancel: cancel}
	r.inflight[key] = e
	go r.run(ctx, key, query, readings, e)
	return e
}

// leave removes a caller from a lookup. The last one out cancels it and takes it
// out of the map, so that a later caller starts afresh instead of joining a
// lookup that is already dying.
func (r *CoverResolver) leave(key string, e *coverLookup) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.waiters--
	if e.waiters == 0 {
		e.cancel()
		r.forget(key, e)
	}
}

// forget drops a lookup from the map unless a newer one has taken its place. The
// caller holds the mutex.
func (r *CoverResolver) forget(key string, e *coverLookup) {
	if r.inflight[key] == e {
		delete(r.inflight, key)
	}
}

// coolingDown reports whether requests are being held back.
func (r *CoverResolver) coolingDown() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.now().Before(r.notBefore)
}

// coolDown holds requests back for d. Responses in flight together can finish in
// any order, so a shorter wait never replaces a longer one.
func (r *CoverResolver) coolDown(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if until := r.now().Add(d); until.After(r.notBefore) {
		r.notBefore = until
	}
}

// coverRetryAfter reads a Retry-After header given in seconds. Anything else,
// including the date form and zero, gets the default wait: the server has just
// said it is overloaded, so retrying at once is never the polite reading.
func coverRetryAfter(header string) time.Duration {
	secs, err := strconv.ParseUint(strings.TrimSpace(header), 10, 64)
	switch {
	case errors.Is(err, strconv.ErrRange):
		return coverCoolDownMax
	case err != nil || secs == 0:
		return coverCoolDownDefault
	case secs >= uint64(coverCoolDownMax/time.Second):
		return coverCoolDownMax
	}
	return time.Duration(secs) * time.Second
}

func (r *CoverResolver) run(ctx context.Context, key, query string, readings []coverReading, e *coverLookup) {
	defer e.cancel()
	e.url, e.err = r.lookup(ctx, key, query, readings)
	r.mu.Lock()
	r.forget(key, e)
	r.mu.Unlock()
	close(e.done)
}

// lookup waits for a request slot, then searches and caches the result.
func (r *CoverResolver) lookup(ctx context.Context, key, query string, readings []coverReading) (string, error) {
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// An earlier lookup may have finished after this caller checked the cache.
	url, found, err := r.cached(key)
	if err != nil {
		return "", err
	}
	if found {
		return url, nil
	}
	if err := r.waitTurn(ctx); err != nil {
		return "", err
	}
	// The cool-down may have started while this lookup waited.
	if r.coolingDown() {
		return "", ErrCoverCoolingDown
	}

	docs, err := r.search(ctx, query)
	if err != nil {
		return "", fmt.Errorf("open library cover search: %w", err)
	}
	if doc := matchCoverDoc(readings, docs); doc != nil {
		url = fmt.Sprintf("%s/b/id/%d-M.jpg", strings.TrimRight(r.coverBaseURL(), "/"), doc.CoverI)
	}
	if err := r.cache.PutCoverLookup(key, url, r.now()); err != nil {
		slog.Warn("cover cache write failed", "key", key, "error", err)
	}
	return url, nil
}

// waitTurn blocks until this search may go out: after every search that asked
// earlier, and at least the interval after the previous one. A caller that gives
// up while waiting leaves without using a turn.
func (r *CoverResolver) waitTurn(ctx context.Context) error {
	select {
	case r.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.turn }()
	if wait := time.Until(r.lastSent.Add(r.interval)); wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.lastSent = time.Now()
	return nil
}

// search runs one Open Library query. A response that is too large or is not a
// search result is an error, so that it is never cached as a miss.
func (r *CoverResolver) search(ctx context.Context, query string) ([]olSearchDoc, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", r.searchURL(), nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("q", query)
	q.Set("fields", coverSearchFields)
	q.Set("limit", coverSearchLimit)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", r.userAgent)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		wait := coverRetryAfter(resp.Header.Get("Retry-After"))
		r.coolDown(wait)
		slog.Warn("open library is limiting cover lookups", "status", resp.StatusCode, "retry_in", wait)
		return nil, fmt.Errorf("HTTP %d: %w", resp.StatusCode, ErrCoverCoolingDown)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, coverMaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > coverMaxResponseBytes {
		return nil, errors.New("response too large")
	}
	var data struct {
		Docs *[]olSearchDoc `json:"docs"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if data.Docs == nil {
		return nil, errors.New("response has no docs")
	}
	return *data.Docs, nil
}
