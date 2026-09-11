"""Stateful browser coverage for the React Library and Downloads tabs."""


def _sign_in(app, page):
    page.goto(app["base"], wait_until="networkidle")
    if page.locator("#app").is_visible():
        return
    if page.locator("#register-form").is_visible():
        page.fill("#register-username", "react-admin")
        page.fill("#register-password", "correct horse battery staple")
        page.locator("#register-form button[type=submit]").click()
    else:
        page.fill("#login-username", "react-admin")
        page.fill("#login-password", "correct horse battery staple")
        page.locator("#login-form button[type=submit]").click()
    page.wait_for_selector("#app")


def test_library_categories_grouping_filter_pagination_links_and_cover_fallback(app, page):
    _sign_in(app, page)
    seen = []

    def library(route, request):
        seen.append(request.url)
        page_number = 2 if "page=2" in request.url else 1
        route.fulfill(json={"items": [
            {"id": "e1", "title": f"Earthsea One P{page_number}", "author": "Ursula Le Guin", "series": "Earthsea #1", "format": "epub", "size": 1048576, "cover_url": "https://127.0.0.1:1/broken.jpg"},
            {"id": "e2", "title": "Earthsea Two", "author": "Ursula Le Guin", "series": "Earthsea Book 2", "file_path": "/books/two.pdf"},
            {"id": "e3", "title": "Standalone", "author": "Writer"},
        ], "page": page_number, "pages": 3})

    page.route("**/api/library?*", library)
    page.route("**/api/library/audiobooks?*", lambda route: route.fulfill(json={"items":[{"id":"a1","title":"Dune Audio","author":"Frank Herbert","duration_hours":21.25,"num_files":12,"abs_url":"https://abs.example/item/a1"}],"pages":1}))
    page.route("**/api/library/manga?*", lambda route: route.fulfill(json={"items":[{"id":"m1","name":"Akira","pages":382,"library":"Manga","kavita_url":"https://kavita.example/series/1"}],"pages":1}))
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    page.wait_for_selector('[data-series-group="Earthsea"]')
    assert page.locator('[data-series-group="Earthsea"] header').inner_text() == "Earthsea\n2 items"
    assert page.locator("#library-results").get_by_text("Other").is_visible()
    page.wait_for_selector("[data-cover-fallback]")
    assert page.locator("[data-cover-fallback]").first.inner_text() == "EO"

    page.locator("#library-pagination").get_by_text("2", exact=True).click()
    page.wait_for_function("() => document.querySelector('#library-results')?.textContent.includes('P2')")
    page.fill("#library-search", "left hand")
    page.wait_for_timeout(500)
    assert any("page=1" in url and "q=left+hand" in url or "q=left%20hand" in url for url in seen)

    page.locator('[data-library-tab="audiobooks"]').click()
    page.wait_for_selector('a[href="https://abs.example/item/a1"]')
    assert "21.3h" in page.locator("#library-results").inner_text()
    assert "12 files" in page.locator("#library-results").inner_text()
    page.locator('[data-library-tab="manga"]').click()
    page.wait_for_selector('a[href="https://kavita.example/series/1"]')
    assert "382 pages" in page.locator("#library-results").inner_text()


def test_download_cards_progress_retry_cancel_clear_refresh_and_poll(app, page):
    _sign_in(app, page)
    gets = []
    actions = []
    jobs = [
        {"job_id":"active-1","title":"Active Book","source":"Fixture","status":"downloading","progress":42.25,"size":"8 MB","speed":"2 MB/s","detail":"fetching"},
        {"job_id":"retry-1","title":"Failed Book","status":"dead_letter","error":"HTTP 504","retry_count":2,"max_retries":2},
        {"hash":"abc123","title":"Torrent Book","status":"queued"},
        {"job_id":"done-1","title":"Done Book","status":"completed"},
    ]
    def downloads(route, request):
        gets.append(request.url)
        route.fulfill(json={"downloads":jobs})
    page.route("**/api/downloads", downloads)
    page.route("**/api/downloads/jobs/retry-1/retry", lambda route, request: (actions.append((request.method, request.url)), route.fulfill(json={"success":True})))
    page.route("**/api/downloads/torrent/abc123", lambda route, request: (actions.append((request.method, request.url)), route.fulfill(json={"success":True})))
    page.route("**/api/downloads/clear", lambda route, request: (actions.append((request.method, request.url)), route.fulfill(json={"success":True})))
    page.on("dialog", lambda dialog: dialog.accept())
    page.locator('[data-action="switchTab"][data-arg="downloads"]').click()
    page.wait_for_selector('[data-download-id="active-1"]')
    active = page.locator('[data-download-id="active-1"]')
    assert active.locator('[role="progressbar"]').get_attribute("aria-valuenow") == "42.25"
    assert "42.3%" in active.inner_text()
    assert "2 MB/s" in active.inner_text()
    assert page.locator("#dl-badge").inner_text() == "2"
    failed = page.locator('[data-download-id="retry-1"]')
    assert "Attempt 3/3" in failed.inner_text()
    failed.locator('[data-action="retryDownload"]').click()
    page.wait_for_function("() => document.querySelector('#toast-container')?.textContent.includes('Retrying download')")
    page.locator('[data-download-id="abc123"] [data-action="cancelDownload"]').click()
    page.locator("#downloads-clear-btn").click()
    page.locator("#downloads-refresh-btn").click()
    page.wait_for_timeout(5200)
    assert len(gets) >= 5
    assert any(method == "POST" and "/jobs/retry-1/retry" in url for method,url in actions)
    assert any(method == "DELETE" and "/torrent/abc123" in url for method,url in actions)
    assert any(method == "POST" and url.endswith("/api/downloads/clear") for method,url in actions)
