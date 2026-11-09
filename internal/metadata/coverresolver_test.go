package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	duneBody        = `{"docs":[{"key":"/works/OL1W","title":"Dune","author_name":["Frank Herbert"],"cover_i":42}]}`
	testCoverURL    = "https://covers.test/b/id/42-M.jpg"
	emptyDocsBody   = `{"docs":[]}`
	testWaitLimit   = 5 * time.Second
	coverCacheDay   = 24 * time.Hour
	coverCacheMonth = 30 * coverCacheDay
)

// memCoverCache is an in-memory CoverCache.
type memCoverCache struct {
	mu         sync.Mutex
	entries    map[string]memCoverEntry
	puts       int
	getErr     error
	putErr     error
	staleReads int // reads that report a miss whatever is stored
}

type memCoverEntry struct {
	url string
	at  time.Time
}

func (c *memCoverCache) GetCoverLookup(key string) (string, bool, time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return "", false, time.Time{}, c.getErr
	}
	if c.staleReads > 0 {
		c.staleReads--
		return "", false, time.Time{}, nil
	}
	e, ok := c.entries[key]
	return e.url, ok, e.at, nil
}

func (c *memCoverCache) PutCoverLookup(key, url string, fetchedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.putErr != nil {
		return c.putErr
	}
	c.puts++
	c.entries[key] = memCoverEntry{url, fetchedAt}
	return nil
}

func (c *memCoverCache) putCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts
}

// testClock is a clock tests advance by hand.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// coverStub is an Open Library stand-in that counts requests, together with a
// resolver pointed at it.
type coverStub struct {
	*CoverResolver
	requests atomic.Int64
	cache    *memCoverCache
	clock    *testClock

	mu   sync.Mutex
	reqs []*http.Request
}

func (s *coverStub) count() int64 { return s.requests.Load() }

func (s *coverStub) lastRequest(t *testing.T) *http.Request {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reqs) == 0 {
		t.Fatal("no request reached the stub")
	}
	return s.reqs[len(s.reqs)-1]
}

func newCoverStub(t *testing.T, handler http.HandlerFunc) *coverStub {
	t.Helper()
	s := &coverStub{
		cache: &memCoverCache{entries: map[string]memCoverEntry{}},
		clock: &testClock{t: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		s.mu.Lock()
		s.reqs = append(s.reqs, r)
		s.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	s.CoverResolver = NewCoverResolver(CoverResolverOptions{
		Cache:        s.cache,
		HTTPClient:   srv.Client(),
		SearchURL:    func() string { return srv.URL + "/search.json" },
		CoverBaseURL: func() string { return "https://covers.test" },
		Now:          s.clock.now,
	})
	return s
}

func respondWith(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, body) }
}

// gate holds stub requests until released. A test must also release it at
// cleanup, registered after the stub's server so that it runs first (cleanups
// are last-in-first-out): closing the server waits for requests to finish.
type gate struct {
	c    chan struct{}
	once sync.Once
}

func newGate() *gate { return &gate{c: make(chan struct{})} }

func (g *gate) release() { g.once.Do(func() { close(g.c) }) }

// blockedHandler reports each arrival, then answers once the gate opens. A
// request whose client went away gets no body.
func blockedHandler(arrived chan<- struct{}, g *gate, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-g.c:
			io.WriteString(w, body)
		case <-r.Context().Done():
		}
	}
}

func newBlockedStub(t *testing.T, body string) (*coverStub, *gate, chan struct{}) {
	t.Helper()
	arrived := make(chan struct{}, 64)
	g := newGate()
	s := newCoverStub(t, blockedHandler(arrived, g, body))
	t.Cleanup(g.release)
	return s, g, arrived
}

type resolveResult struct {
	url string
	err error
}

func resolveAsync(ctx context.Context, r *CoverResolver, title, author string) <-chan resolveResult {
	ch := make(chan resolveResult, 1)
	go func() {
		u, err := r.Resolve(ctx, CoverKindBook, title, author)
		ch <- resolveResult{u, err}
	}()
	return ch
}

func awaitResult(t *testing.T, ch <-chan resolveResult) resolveResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(testWaitLimit):
		t.Fatal("Resolve did not return")
		return resolveResult{}
	}
}

func awaitArrivals(t *testing.T, arrived <-chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-arrived:
		case <-time.After(testWaitLimit):
			t.Fatalf("saw %d of %d requests", i, n)
		}
	}
}

// awaitLookup waits for the key's in-flight lookup to have exactly the given
// number of waiters. Joining a lookup leaves nothing a channel could report, so
// this one condition is polled.
func awaitLookup(t *testing.T, r *CoverResolver, key string, waiters int) *coverLookup {
	t.Helper()
	deadline := time.Now().Add(testWaitLimit)
	for {
		r.mu.Lock()
		e := r.inflight[key]
		got := 0
		if e != nil {
			got = e.waiters
		}
		r.mu.Unlock()
		if e != nil && got == waiters {
			return e
		}
		if time.Now().After(deadline) {
			t.Fatalf("lookup %q has %d waiters, want %d", key, got, waiters)
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitDone(t *testing.T, e *coverLookup) {
	t.Helper()
	select {
	case <-e.done:
	case <-time.After(testWaitLimit):
		t.Fatal("lookup goroutine did not finish")
	}
}

func resolveDune(t *testing.T, r *CoverResolver) string {
	t.Helper()
	got, err := r.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return got
}

func duneKey() string { return CoverKey(CoverKindBook, "Dune", "Frank Herbert") }

func TestResolveReturnsCoverURL(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))

	if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
		t.Errorf("Resolve = %q, want %q", got, testCoverURL)
	}
	if n := s.count(); n != 1 {
		t.Errorf("made %d requests, want 1 (no works lookup)", n)
	}

	req := s.lastRequest(t)
	if req.URL.Path != "/search.json" {
		t.Errorf("path = %q", req.URL.Path)
	}
	q := req.URL.Query()
	if got := q.Get("q"); !strings.Contains(got, "Dune") || !strings.Contains(got, "Frank Herbert") {
		t.Errorf("q = %q, want title and author", got)
	}
	if got := q.Get("limit"); got != "20" {
		t.Errorf("limit = %q, want 20", got)
	}
	fields := strings.Split(q.Get("fields"), ",")
	for _, want := range []string{"title", "cover_i", "author_name"} {
		found := false
		for _, f := range fields {
			found = found || f == want
		}
		if !found {
			t.Errorf("fields = %v, missing %s", fields, want)
		}
	}
	if got := req.Header.Get("User-Agent"); got != olUserAgent {
		t.Errorf("User-Agent = %q", got)
	}
}

func TestResolveCachesMatch(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))

	resolveDune(t, s.CoverResolver)
	s.clock.advance(coverCacheMonth - time.Second)
	if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
		t.Errorf("cached Resolve = %q", got)
	}
	if n := s.count(); n != 1 {
		t.Errorf("within 30 days: %d requests, want 1", n)
	}

	s.clock.advance(2 * time.Second) // now 30 days + 1 second
	if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
		t.Errorf("refreshed Resolve = %q", got)
	}
	if n := s.count(); n != 2 {
		t.Errorf("after 30 days: %d requests, want 2", n)
	}
}

func TestResolveCachesMiss(t *testing.T) {
	s := newCoverStub(t, respondWith(emptyDocsBody))

	if got := resolveDune(t, s.CoverResolver); got != "" {
		t.Errorf("Resolve = %q, want a miss", got)
	}
	s.clock.advance(coverCacheDay - time.Second)
	if got := resolveDune(t, s.CoverResolver); got != "" {
		t.Errorf("cached Resolve = %q, want a miss", got)
	}
	if n := s.count(); n != 1 {
		t.Errorf("within 24 hours: %d requests, want 1", n)
	}

	s.clock.advance(2 * time.Second) // now 24 hours + 1 second
	if got := resolveDune(t, s.CoverResolver); got != "" {
		t.Errorf("refreshed Resolve = %q, want a miss", got)
	}
	if n := s.count(); n != 2 {
		t.Errorf("after 24 hours: %d requests, want 2", n)
	}
}

func TestResolveRejectsWrongBook(t *testing.T) {
	s := newCoverStub(t, respondWith(`{"docs":[{"title":"Dune Messiah","author_name":["Frank Herbert"],"cover_i":7}]}`))

	if got := resolveDune(t, s.CoverResolver); got != "" {
		t.Errorf("Resolve = %q, want a miss for a different book", got)
	}
	if n := s.cache.putCount(); n != 1 {
		t.Errorf("cached %d lookups, want the miss", n)
	}
}

func TestResolveSharesCacheAcrossNameOrder(t *testing.T) {
	s := newCoverStub(t, respondWith(`{"docs":[{"title":"The Left Hand of Darkness","author_name":["Ursula K. Le Guin"],"cover_i":42}]}`))
	ctx := context.Background()

	first, err := s.Resolve(ctx, CoverKindBook, "Ursula K. Le Guin - The Left Hand of Darkness", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Resolve(ctx, CoverKindBook, "The Left Hand of Darkness - Ursula K. Le Guin", "")
	if err != nil {
		t.Fatal(err)
	}
	if first != testCoverURL || second != first {
		t.Errorf("got %q then %q, want %q twice", first, second, testCoverURL)
	}
	if n := s.count(); n != 1 {
		t.Errorf("made %d requests, want 1", n)
	}
}

func TestResolveDoesNotCacheFailures(t *testing.T) {
	// Trailing whitespace keeps the oversized body valid JSON, so only the size
	// cap can reject it.
	oversized := duneBody + strings.Repeat(" ", 2<<20)
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"server error", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "broken", http.StatusInternalServerError)
		}},
		{"oversized", respondWith(oversized)},
		{"malformed", respondWith(`{"docs":[`)},
		{"not a search response", respondWith(`{"error":"rate limited"}`)},
		{"null", respondWith(`null`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var healthy atomic.Bool
			s := newCoverStub(t, func(w http.ResponseWriter, r *http.Request) {
				if healthy.Load() {
					io.WriteString(w, duneBody)
					return
				}
				tt.handler(w, r)
			})

			got, err := s.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert")
			if err == nil {
				t.Errorf("Resolve = %q, want an error", got)
			}
			if n := s.cache.putCount(); n != 0 {
				t.Errorf("wrote %d cache entries, want 0", n)
			}

			healthy.Store(true)
			if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
				t.Errorf("next Resolve = %q", got)
			}
			if n := s.count(); n != 2 {
				t.Errorf("made %d requests, want 2", n)
			}
		})
	}
}

func TestResolveAcceptsLargestResponse(t *testing.T) {
	body := duneBody + strings.Repeat(" ", (2<<20)-len(duneBody))
	s := newCoverStub(t, respondWith(body))

	if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
		t.Errorf("Resolve = %q", got)
	}
}

func TestResolveSkipsManga(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))

	got, err := s.Resolve(context.Background(), CoverKindManga, "Dune", "Frank Herbert")
	if got != "" || err != nil {
		t.Errorf("Resolve = %q, %v", got, err)
	}
	if n := s.count(); n != 0 {
		t.Errorf("made %d requests, want 0", n)
	}
	if n := s.cache.putCount(); n != 0 {
		t.Errorf("wrote %d cache entries, want 0", n)
	}
}

func TestResolveEmptyKeySkipsRequest(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))

	for _, author := range []string{"", "Frank Herbert"} { // an author alone names no book
		got, err := s.Resolve(context.Background(), CoverKindBook, "[EPUB]", author)
		if got != "" || err != nil {
			t.Errorf("author %q: Resolve = %q, %v", author, got, err)
		}
	}
	if n := s.count(); n != 0 {
		t.Errorf("made %d requests, want 0", n)
	}
	if n := s.cache.putCount(); n != 0 {
		t.Errorf("wrote %d cache entries, want 0", n)
	}
}

func TestResolveLimitsConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int64
	arrived := make(chan struct{}, 64)
	g := newGate()
	s := newCoverStub(t, func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		arrived <- struct{}{}
		select {
		case <-g.c:
			// Before answering: the answer frees a slot for the next request.
			inFlight.Add(-1)
			io.WriteString(w, emptyDocsBody)
		case <-r.Context().Done():
			inFlight.Add(-1)
		}
	})
	t.Cleanup(g.release)

	var results []<-chan resolveResult
	for i := 0; i < 10; i++ {
		results = append(results, resolveAsync(context.Background(), s.CoverResolver, fmt.Sprintf("Volume %d", 100+i), "Some Author"))
	}
	awaitArrivals(t, arrived, 3)

	// Every lookup has started before the gate opens, so an unlimited resolver
	// would have the chance to exceed the limit. The pause can only hide a
	// failure, never cause one.
	deadline := time.Now().Add(testWaitLimit)
	for {
		s.CoverResolver.mu.Lock()
		started := len(s.CoverResolver.inflight)
		s.CoverResolver.mu.Unlock()
		if started == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of 10 lookups started", started)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if p := peak.Load(); p != 3 {
		t.Errorf("before release, peak in-flight = %d, want 3", p)
	}

	g.release()
	for _, ch := range results {
		if res := awaitResult(t, ch); res.err != nil {
			t.Errorf("Resolve: %v", res.err)
		}
	}
	if p := peak.Load(); p != 3 {
		t.Errorf("peak in-flight = %d, want 3", p)
	}
	if n := s.count(); n != 10 {
		t.Errorf("made %d requests, want 10", n)
	}
}

func TestResolveSharesInFlight(t *testing.T) {
	s, g, arrived := newBlockedStub(t, duneBody)

	var results []<-chan resolveResult
	for i := 0; i < 5; i++ {
		results = append(results, resolveAsync(context.Background(), s.CoverResolver, "Dune", "Frank Herbert"))
	}
	awaitArrivals(t, arrived, 1)
	awaitLookup(t, s.CoverResolver, duneKey(), 5)
	g.release()

	for _, ch := range results {
		res := awaitResult(t, ch)
		if res.err != nil || res.url != testCoverURL {
			t.Errorf("Resolve = %q, %v", res.url, res.err)
		}
	}
	if n := s.count(); n != 1 {
		t.Errorf("made %d requests, want 1", n)
	}
}

func TestResolveCancelledWhileQueued(t *testing.T) {
	s, g, arrived := newBlockedStub(t, emptyDocsBody)

	var holders []<-chan resolveResult
	for i := 0; i < 3; i++ {
		holders = append(holders, resolveAsync(context.Background(), s.CoverResolver, fmt.Sprintf("Volume %d", 100+i), "Some Author"))
	}
	awaitArrivals(t, arrived, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const queuedTitle = "Queued Title"
	queued := resolveAsync(ctx, s.CoverResolver, queuedTitle, "")
	e := awaitLookup(t, s.CoverResolver, CoverKey(CoverKindBook, queuedTitle, ""), 1)

	cancel()
	if res := awaitResult(t, queued); !errors.Is(res.err, context.Canceled) {
		t.Errorf("Resolve error = %v, want context.Canceled", res.err)
	}
	// The slots are still full, so finishing here means it gave up in the queue.
	awaitDone(t, e)

	g.release()
	for _, ch := range holders {
		if res := awaitResult(t, ch); res.err != nil {
			t.Errorf("holder: %v", res.err)
		}
	}
	if n := s.count(); n != 3 {
		t.Errorf("made %d requests, want 3: the cancelled lookup reached the stub", n)
	}
	if n := s.cache.putCount(); n != 3 {
		t.Errorf("wrote %d cache entries, want 3", n)
	}
}

func TestResolveCancelKeepsOtherWaiter(t *testing.T) {
	s, g, arrived := newBlockedStub(t, duneBody)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaving := resolveAsync(ctx, s.CoverResolver, "Dune", "Frank Herbert")
	staying := resolveAsync(context.Background(), s.CoverResolver, "Dune", "Frank Herbert")
	awaitArrivals(t, arrived, 1)
	awaitLookup(t, s.CoverResolver, duneKey(), 2)

	cancel()
	if res := awaitResult(t, leaving); !errors.Is(res.err, context.Canceled) {
		t.Errorf("cancelled caller: %v, want context.Canceled", res.err)
	}
	g.release()

	res := awaitResult(t, staying)
	if res.err != nil || res.url != testCoverURL {
		t.Errorf("remaining caller: %q, %v", res.url, res.err)
	}
	if n := s.count(); n != 1 {
		t.Errorf("made %d requests, want 1", n)
	}
}

func TestResolveLastCancelStopsLookup(t *testing.T) {
	aborted := make(chan struct{})
	arrived := make(chan struct{}, 1)
	var once sync.Once
	s := newCoverStub(t, func(w http.ResponseWriter, r *http.Request) {
		first := false
		once.Do(func() { first = true })
		if !first {
			io.WriteString(w, duneBody)
			return
		}
		arrived <- struct{}{}
		<-r.Context().Done()
		close(aborted)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := resolveAsync(ctx, s.CoverResolver, "Dune", "Frank Herbert")
	awaitArrivals(t, arrived, 1)
	e := awaitLookup(t, s.CoverResolver, duneKey(), 1)

	cancel()
	if got := awaitResult(t, res); !errors.Is(got.err, context.Canceled) {
		t.Errorf("Resolve error = %v, want context.Canceled", got.err)
	}
	select {
	case <-aborted:
	case <-time.After(testWaitLimit):
		t.Fatal("the request outlived its last caller")
	}
	awaitDone(t, e)

	s.CoverResolver.mu.Lock()
	remaining := len(s.CoverResolver.inflight)
	s.CoverResolver.mu.Unlock()
	if remaining != 0 {
		t.Errorf("%d lookups left in the map, want 0", remaining)
	}
	if n := s.cache.putCount(); n != 0 {
		t.Errorf("wrote %d cache entries for a cancelled lookup, want 0", n)
	}
	if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
		t.Errorf("next Resolve = %q", got)
	}
}

// stallingTransport holds each round trip on its own gate, ignoring the request
// context, so a cancelled lookup can be kept alive as long as a test needs.
type stallingTransport struct {
	gates   []*gate
	arrived chan struct{}
	trips   atomic.Int64
}

func (s *stallingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	n := s.trips.Add(1)
	s.arrived <- struct{}{}
	<-s.gates[n-1].c
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(duneBody)),
		Request:    req,
	}, nil
}

// A caller arriving just after the last one left must not join the dying lookup
// and inherit its cancellation, and that lookup's cleanup must not remove its
// replacement.
func TestResolveAfterLastCancelStartsFresh(t *testing.T) {
	stall := &stallingTransport{
		gates:   []*gate{newGate(), newGate()},
		arrived: make(chan struct{}, 2),
	}
	for _, g := range stall.gates {
		t.Cleanup(g.release)
	}
	r := NewCoverResolver(CoverResolverOptions{
		Cache:        &memCoverCache{entries: map[string]memCoverEntry{}},
		HTTPClient:   &http.Client{Transport: stall},
		SearchURL:    func() string { return "http://openlibrary.test/search.json" },
		CoverBaseURL: func() string { return "https://covers.test" },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	abandoned := resolveAsync(ctx, r, "Dune", "Frank Herbert")
	awaitArrivals(t, stall.arrived, 1)
	dying := awaitLookup(t, r, duneKey(), 1)
	cancel()
	if res := awaitResult(t, abandoned); !errors.Is(res.err, context.Canceled) {
		t.Fatalf("abandoned caller: %v, want context.Canceled", res.err)
	}

	second := resolveAsync(context.Background(), r, "Dune", "Frank Herbert")
	awaitArrivals(t, stall.arrived, 1) // its own request, not the dying lookup's
	replacement := awaitLookup(t, r, duneKey(), 1)
	if replacement == dying {
		t.Fatal("the new caller joined the cancelled lookup")
	}

	stall.gates[0].release()
	awaitDone(t, dying)

	third := resolveAsync(context.Background(), r, "Dune", "Frank Herbert")
	if got := awaitLookup(t, r, duneKey(), 2); got != replacement {
		t.Error("the third caller did not join the live lookup")
	}
	stall.gates[1].release()

	for _, ch := range []<-chan resolveResult{second, third} {
		res := awaitResult(t, ch)
		if res.err != nil || res.url != testCoverURL {
			t.Errorf("Resolve = %q, %v", res.url, res.err)
		}
	}
	if n := stall.trips.Load(); n != 2 {
		t.Errorf("made %d round trips, want 2", n)
	}
}

// A lookup can finish between a caller's cache check and it joining the map; the
// caller's new lookup must find that result rather than ask again.
func TestResolveRechecksCacheBeforeFetching(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))
	s.cache.entries[duneKey()] = memCoverEntry{"https://covers.test/stored.jpg", s.clock.now()}
	s.cache.staleReads = 1

	if got := resolveDune(t, s.CoverResolver); got != "https://covers.test/stored.jpg" {
		t.Errorf("Resolve = %q", got)
	}
	if n := s.count(); n != 0 {
		t.Errorf("made %d requests, want 0", n)
	}
}

func TestResolveCacheReadFailureSkipsRequest(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))
	wantErr := errors.New("disk on fire")
	s.cache.getErr = wantErr

	got, err := s.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert")
	if !errors.Is(err, wantErr) || got != "" {
		t.Errorf("Resolve = %q, %v, want %v", got, err, wantErr)
	}
	if n := s.count(); n != 0 {
		t.Errorf("made %d requests, want 0", n)
	}
}

func TestResolveCacheWriteFailureStillReturnsURL(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	s := newCoverStub(t, respondWith(duneBody))
	s.cache.putErr = errors.New("disk on fire")

	if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
		t.Errorf("Resolve = %q", got)
	}
}

func TestResolveAlreadyCancelledSkipsRequest(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Resolve(ctx, CoverKindBook, "Dune", "Frank Herbert"); !errors.Is(err, context.Canceled) {
		t.Errorf("Resolve error = %v, want context.Canceled", err)
	}
	if n := s.count(); n != 0 {
		t.Errorf("made %d requests, want 0", n)
	}
}

func TestResolveOptionDefaults(t *testing.T) {
	srv := httptest.NewServer(respondWith(duneBody))
	t.Cleanup(srv.Close)

	var searchURLReads atomic.Int64
	r := NewCoverResolver(CoverResolverOptions{
		Cache: &memCoverCache{entries: map[string]memCoverEntry{}},
		SearchURL: func() string {
			searchURLReads.Add(1)
			return srv.URL + "/search.json"
		},
		CoverBaseURL: func() string { return "https://covers.test/" },
	})

	if cap(r.slots) != 3 {
		t.Errorf("default concurrency = %d, want 3", cap(r.slots))
	}
	if got := resolveDune(t, r); got != testCoverURL {
		t.Errorf("Resolve = %q, want one slash after the base", got)
	}
	if n := searchURLReads.Load(); n != 1 {
		t.Errorf("SearchURL read %d times, want once per lookup", n)
	}
}

// statusThenBody answers with status and an optional Retry-After until healthy is
// set, and with duneBody afterwards.
func statusThenBody(status int, retryAfter string, healthy *atomic.Bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if healthy.Load() {
			io.WriteString(w, duneBody)
			return
		}
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		http.Error(w, "busy", status)
	}
}

func TestResolveCoolsDownAfterRateLimit(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		retryAfter string
		want       time.Duration
	}{
		{"429 with Retry-After", http.StatusTooManyRequests, "120", 120 * time.Second},
		{"429 without Retry-After", http.StatusTooManyRequests, "", 60 * time.Second},
		{"503 with Retry-After", http.StatusServiceUnavailable, "120", 120 * time.Second},
		{"503 without Retry-After", http.StatusServiceUnavailable, "", 60 * time.Second},
		{"Retry-After above the cap", http.StatusTooManyRequests, "86400", time.Hour},
		{"Retry-After too large to parse", http.StatusTooManyRequests, "99999999999999999999999", time.Hour},
		{"Retry-After as a date", http.StatusTooManyRequests, "Wed, 21 Oct 2026 07:28:00 GMT", 60 * time.Second},
		{"Retry-After as words", http.StatusTooManyRequests, "soon", 60 * time.Second},
		{"Retry-After negative", http.StatusTooManyRequests, "-5", 60 * time.Second},
		{"Retry-After zero", http.StatusTooManyRequests, "0", 60 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var healthy atomic.Bool
			s := newCoverStub(t, statusThenBody(tt.status, tt.retryAfter, &healthy))
			resolve := func() error {
				_, err := s.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert")
				return err
			}

			// The answer that starts the cool-down is reported the same way, so
			// the job does not mark an item Open Library declined to look at.
			if err := resolve(); !errors.Is(err, ErrCoverCoolingDown) {
				t.Fatalf("first Resolve: %v, want ErrCoverCoolingDown", err)
			}
			healthy.Store(true)

			if err := resolve(); !errors.Is(err, ErrCoverCoolingDown) {
				t.Fatalf("second Resolve: %v, want ErrCoverCoolingDown", err)
			}
			s.clock.advance(tt.want - time.Second)
			if err := resolve(); !errors.Is(err, ErrCoverCoolingDown) {
				t.Fatalf("Resolve a second early: %v, want ErrCoverCoolingDown", err)
			}
			if n := s.count(); n != 1 {
				t.Fatalf("made %d requests during the cool-down, want 1", n)
			}

			s.clock.advance(time.Second)
			if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
				t.Errorf("Resolve after the cool-down = %q", got)
			}
			if n := s.count(); n != 2 {
				t.Errorf("made %d requests, want 2", n)
			}
		})
	}
}

func TestResolveOtherStatusesDoNotCoolDown(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var healthy atomic.Bool
			s := newCoverStub(t, statusThenBody(status, "120", &healthy))

			_, err := s.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert")
			if err == nil || errors.Is(err, ErrCoverCoolingDown) {
				t.Fatalf("Resolve: %v, want a plain failure", err)
			}
			healthy.Store(true)
			if got := resolveDune(t, s.CoverResolver); got != testCoverURL {
				t.Errorf("next Resolve = %q", got)
			}
			if n := s.count(); n != 2 {
				t.Errorf("made %d requests, want 2", n)
			}
		})
	}
}

func TestResolveServesCacheHitsDuringCoolDown(t *testing.T) {
	var healthy atomic.Bool
	s := newCoverStub(t, statusThenBody(http.StatusTooManyRequests, "", &healthy))
	key := CoverKey(CoverKindBook, "Neuromancer", "William Gibson")
	if err := s.cache.PutCoverLookup(key, "https://covers.test/b/id/7-M.jpg", s.clock.now()); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert"); !errors.Is(err, ErrCoverCoolingDown) {
		t.Fatalf("Resolve: %v", err)
	}
	got, err := s.Resolve(context.Background(), CoverKindBook, "Neuromancer", "William Gibson")
	if err != nil || got != "https://covers.test/b/id/7-M.jpg" {
		t.Errorf("cache hit during cool-down = %q, %v", got, err)
	}
	if n := s.count(); n != 1 {
		t.Errorf("made %d requests, want 1", n)
	}
}

func TestResolveCoolDownMissWritesNothing(t *testing.T) {
	var healthy atomic.Bool
	s := newCoverStub(t, statusThenBody(http.StatusTooManyRequests, "", &healthy))

	for i := 0; i < 2; i++ {
		if _, err := s.Resolve(context.Background(), CoverKindBook, "Dune", "Frank Herbert"); !errors.Is(err, ErrCoverCoolingDown) {
			t.Fatalf("Resolve %d: %v", i, err)
		}
	}
	if n := s.cache.putCount(); n != 0 {
		t.Errorf("wrote %d cache entries, want 0", n)
	}
}

// Lookups already waiting for a slot when the cool-down starts must not send.
func TestResolveQueuedLookupHonoursCoolDown(t *testing.T) {
	arrived := make(chan struct{}, 64)
	g := newGate()
	s := newCoverStub(t, func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-g.c:
			http.Error(w, "busy", http.StatusTooManyRequests)
		case <-r.Context().Done():
		}
	})
	t.Cleanup(g.release)

	var results []<-chan resolveResult
	for i := 0; i < coverDefaultLimit; i++ {
		results = append(results, resolveAsync(context.Background(), s.CoverResolver, fmt.Sprintf("Volume %d", 100+i), "Some Author"))
	}
	awaitArrivals(t, arrived, coverDefaultLimit)

	queued := resolveAsync(context.Background(), s.CoverResolver, "Dune", "Frank Herbert")
	awaitLookup(t, s.CoverResolver, duneKey(), 1)

	g.release()
	for _, ch := range results {
		if res := awaitResult(t, ch); !errors.Is(res.err, ErrCoverCoolingDown) {
			t.Errorf("Resolve: %v", res.err)
		}
	}
	if res := awaitResult(t, queued); !errors.Is(res.err, ErrCoverCoolingDown) {
		t.Errorf("queued Resolve: %v, want ErrCoverCoolingDown", res.err)
	}
	if n := s.count(); n != coverDefaultLimit {
		t.Errorf("made %d requests, want %d", n, coverDefaultLimit)
	}
}

// Responses in flight together can finish in any order; the longest wait wins.
func TestCoolDownIsNotShortened(t *testing.T) {
	s := newCoverStub(t, respondWith(duneBody))
	s.coolDown(time.Hour)
	s.coolDown(time.Minute)
	s.clock.advance(time.Minute)
	if !s.coolingDown() {
		t.Error("a shorter cool-down replaced a longer one")
	}
	s.clock.advance(59 * time.Minute)
	if s.coolingDown() {
		t.Error("still cooling down after the longest wait")
	}
}
