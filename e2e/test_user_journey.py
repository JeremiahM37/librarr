"""End-to-end user-journey tests for the librarr web UI.

Runs the real binary + real Chromium against a stubbed Gutenberg source (see
conftest.py) — no external network. Covers the full scope of a user:

  boot → strict CSP → tab navigation → search → sort → download → import →
  library → wishlist CRUD → cover fallback → language toggle

Every test also implicitly asserts a zero-JS-error journey (ui fixture).
"""
import json
import time

import pytest


def active_tab(page):
    return page.evaluate("() => document.querySelector('.tab-content.active')?.id")


def api(page, path):
    return page.evaluate(f"() => fetch('{path}').then(r => r.json())")


# ── Boot, security posture, assets ─────────────────────────────────────────

def test_boot_serves_ui_with_strict_csp(ui):
    page = ui["page"]
    resp = page.request.get(ui["base"] + "/")
    assert resp.status == 200
    csp = resp.headers.get("content-security-policy", "")
    assert "script-src 'self'" in csp, f"strict CSP missing: {csp}"

    html = resp.text()
    for attr in ("onclick=", "onerror=", "onchange=", "onsubmit="):
        assert attr not in html, f"inline handler {attr} leaked back into index.html"
    assert "<script>" not in html, "inline script block leaked back into index.html"

    for asset in ("/static/js/app.js", "/static/css/app.css",
                  "/static/js/vendor/tailwind.js", "/static/fonts/inter-latin.woff2"):
        r = page.request.get(ui["base"] + asset)
        assert r.status == 200, f"{asset} -> {r.status}"


def test_tailwind_and_font_render_offline(ui):
    page = ui["page"]
    bg = page.evaluate(
        "() => getComputedStyle(document.querySelector('[class*=bg-slate]')).backgroundColor")
    assert bg not in ("", "rgba(0, 0, 0, 0)"), "Tailwind runtime produced no styles"
    font = page.evaluate("() => getComputedStyle(document.body).fontFamily")
    assert "Inter" in font


# ── Navigation via event delegation ─────────────────────────────────────────

def test_all_tabs_switch(ui):
    page = ui["page"]
    for tab in ("library", "downloads", "wishlist", "settings", "search"):
        page.click(f'[data-action="switchTab"][data-arg="{tab}"]')
        page.wait_for_timeout(200)
        assert active_tab(page) == f"tab-{tab}"


# ── Search / sort / the data-idx contract ───────────────────────────────────

@pytest.fixture()
def searched(ui):
    page = ui["page"]
    page.fill("#search-input", "test adventure")
    page.press("#search-input", "Enter")
    page.wait_for_selector('[data-action="startDownload"]', timeout=30000)
    return ui


def test_search_renders_stub_results(searched):
    page = searched["page"]
    cards = page.locator('[data-action="startDownload"]').count()
    assert cards == 3, f"expected the 3 stub books, got {cards} cards"
    body = page.inner_text("#tab-search")
    assert "Test Adventure" in body


def test_data_idx_maps_to_rendered_result_in_every_sort_mode(searched):
    """The download button carries data-idx into the *rendered* (sorted) list.
    A mismatch here would download the wrong book — the most dangerous
    possible regression of the inline-handler removal."""
    page = searched["page"]
    for mode in ("size", "seeders", "relevance"):
        page.click(f'[data-action="setSortMode"][data-arg="{mode}"]')
        page.wait_for_timeout(300)
        mismatches = page.evaluate("""() =>
            [...document.querySelectorAll('[data-action="startDownload"]')].map(btn => {
                const card = btn.closest('.book-card');
                const shown = card?.querySelector('h3')?.textContent.trim();
                const mapped = state.renderedResults[+btn.dataset.idx]?.title;
                return shown === mapped ? null : `${shown} != ${mapped}`;
            }).filter(Boolean)""")
        assert mismatches == [], f"sort={mode}: {mismatches}"


def test_retry_wait_shows_progress_in_search_and_downloads(searched):
    page = searched["page"]
    retry_text = page.evaluate("""() => {
        const result = state.renderedResults[0];
        const key = getDownloadKey(result);
        state.trackedDownloadJobs.set('retry-probe', {
            key, title: result.title, source: 'annas', url: result.url || ''
        });
        state.downloadJobs = [{
            job_id: 'retry-probe', title: result.title, source: 'annas',
            status: 'retry_wait', detail: 'Retry 1/2 scheduled',
            retry_count: 1, max_retries: 2, error: 'download HTTP 504'
        }];
        renderSearchResults();
        renderDownloadList();
        return {
            button: document.querySelector('[data-action="startDownload"]')?.innerText,
            downloads: document.querySelector('#downloads-list')?.innerText,
        };
    }""")
    assert "Retry 1/2 scheduled" in retry_text["button"]
    assert "Retry 1/2 scheduled" in retry_text["downloads"]
    assert "Attempt 2/3" in retry_text["downloads"]

    page.evaluate("""() => {
        state.trackedDownloadJobs.delete('retry-probe');
        state.downloadJobs = [];
        renderSearchResults();
        renderDownloadList();
    }""")


def test_download_completes_and_lands_in_library(searched):
    page = searched["page"]
    # Download the first rendered card; remember which book it claims to be.
    title = page.evaluate("""() => {
        const btn = document.querySelector('[data-action="startDownload"]');
        return state.renderedResults[+btn.dataset.idx].title;
    }""")
    page.click('[data-action="startDownload"]')

    # Poll the API until the job finishes (direct download from the stub).
    deadline = time.time() + 60
    last = None
    while time.time() < deadline:
        jobs = api(page, "/api/downloads")
        rows = jobs.get("downloads") or []
        ours = [j for j in rows if j.get("title") == title]
        if ours:
            last = ours[0]
            if last.get("status") == "completed":
                break
            assert last.get("status") not in ("error", "dead_letter"), \
                f"download failed: {json.dumps(last)}"
        time.sleep(2)  # gentle poll — 1/s trips the API rate limiter
    assert last and last.get("status") == "completed", f"job never completed: {last}"

    # The epub must exist on disk under the library dir.
    files = list(searched["books_dir"].rglob("*.epub"))
    assert files, "no .epub imported into the library directory"

    # And the Downloads tab renders the job row.
    page.click('[data-action="switchTab"][data-arg="downloads"]')
    page.click('[data-action="refreshDownloads"]')
    page.wait_for_timeout(500)
    assert title in page.inner_text("#tab-downloads")


# ── Wishlist CRUD through delegated row buttons ─────────────────────────────

def test_wishlist_add_search_delete(ui):
    page = ui["page"]
    page.click('[data-action="switchTab"][data-arg="wishlist"]')
    page.click('[data-action="showWishlistForm"]')
    page.wait_for_timeout(200)
    first_input = page.evaluate(
        "() => [...document.querySelectorAll('#wishlist-form input')]"
        ".find(i => i.type !== 'checkbox')?.id")
    assert first_input, "wishlist form has no text input"
    page.fill(f"#{first_input}", "The Hobbit")
    page.click('[data-action="addWishlistItem"]')
    page.wait_for_selector('[data-action="deleteWishlistItem"]', timeout=5000)

    # Row's search button jumps to the search tab with the query.
    page.click('[data-action="searchWishlistItem"]')
    page.wait_for_timeout(400)
    assert active_tab(page) == "tab-search"

    # Delete removes the row.
    page.click('[data-action="switchTab"][data-arg="wishlist"]')
    page.wait_for_timeout(200)
    page.click('[data-action="deleteWishlistItem"]')
    page.wait_for_timeout(500)
    assert page.locator('[data-action="deleteWishlistItem"]').count() == 0


# ── Cover fallback via capture-phase error delegation ───────────────────────

def test_broken_cover_falls_back_to_placeholder(ui):
    page = ui["page"]
    ok = page.evaluate("""() => new Promise(res => {
        const img = document.createElement('img');
        img.dataset.phTitle = 'Fallback Probe';
        img.dataset.phIdx = '0';
        img.src = '/static/definitely-missing.png';
        document.body.appendChild(img);
        setTimeout(() => res(!!document.querySelector('.cover-placeholder')), 800);
    })""")
    assert ok, "broken cover did not swap to the gradient placeholder"


# ── i18n toggle (re-renders DOM incl. converted anchor templates) ───────────

def test_language_toggle_rerenders(ui):
    page = ui["page"]
    before = page.evaluate("() => document.body.innerText.slice(0, 300)")
    page.click('[data-action="toggleLanguage"]')
    page.wait_for_timeout(400)
    after = page.evaluate("() => document.body.innerText.slice(0, 300)")
    assert before != after, "language toggle changed nothing"
    page.click('[data-action="toggleLanguage"]')  # restore EN for later tests
    page.wait_for_timeout(300)


# ── Edition metadata on result cards (issue #94) ────────────────────────────

def test_language_badge_comes_from_real_source_metadata(searched):
    """The stub declares languages:["en"] on every book, so the language badge
    must reach the card through the real Gutenberg parser and API response —
    not just through the renderer."""
    page = searched["page"]
    badges = page.locator(".result-language")
    assert badges.count() == 3, f"expected a language badge per card, got {badges.count()}"
    assert badges.first.inner_text().strip().lower() == "en"


def _render_card(page, result):
    """Render one result through the shipped renderer and return its badge row."""
    page.evaluate(
        "r => { document.getElementById('search-results').innerHTML = renderBookCard(r, 0); }",
        result,
    )
    return page


def test_edition_metadata_badges_render(ui):
    """An Anna's Archive hit now carries language, year and publisher; all three
    have to reach the card, because they are what tells near-identical rows
    apart."""
    page = _render_card(ui["page"], {
        "source": "annas",
        "title": "The Mark of Athena",
        "author": "Rick Riordan",
        "size_human": "1.2MB",
        "format": "epub",
        "language": "en",
        "year": "2012",
        "publisher": "Hyperion Book CH",
        "md5": "3e8184fac9f9d2413af8260dbf240ac9",
    })
    assert page.locator(".result-language").inner_text().strip().lower() == "en"
    assert page.locator(".result-year").inner_text().strip() == "2012"
    assert page.locator(".result-publisher").inner_text().strip() == "Hyperion Book CH"
    # The publisher is truncated by CSS, so the full imprint lives in the tooltip.
    assert page.locator(".result-publisher").get_attribute("title") == "Hyperion Book CH"
    assert page.locator(".result-copies").count() == 0, "single copy must not show a count"


def test_missing_metadata_renders_no_empty_badges(ui):
    """Sources that report none of this must not leak 'undefined' into the row."""
    page = _render_card(ui["page"], {
        "source": "prowlarr",
        "title": "Some Torrent Release",
        "size_human": "700MB",
        "seeders": 12,
    })
    for cls in (".result-language", ".result-year", ".result-publisher", ".result-copies"):
        assert page.locator(cls).count() == 0, f"{cls} rendered without data"
    assert "undefined" not in page.inner_text(".book-card")


def test_copies_badge_reports_collapsed_duplicates(ui):
    page = _render_card(ui["page"], {
        "source": "annas",
        "title": "The Mark of Athena",
        "size_human": "1.3MB",
        "format": "epub",
        "copies": 3,
        "md5": "aaa",
    })
    badge = page.locator(".result-copies")
    assert badge.count() == 1
    assert "3" in badge.inner_text()
    assert "3" in badge.get_attribute("title")


def test_metadata_badges_escape_hostile_values(ui):
    """These fields are scraped from a third-party page, so they are untrusted
    input on a strict-CSP page. They must be escaped, not executed."""
    page = _render_card(ui["page"], {
        "source": "annas",
        "title": "Injection Probe",
        "publisher": '<img src=x onerror="window.__pwned=1">',
        "year": '"><script>window.__pwned=1</script>',
        "language": "en",
        "md5": "bbb",
    })
    page.wait_for_timeout(300)
    assert page.evaluate("() => window.__pwned") is None, "metadata badge executed injected markup"
    assert page.locator("#search-results img").count() == 0
    assert "<img" in page.locator(".result-publisher").inner_text()


def test_quotes_in_metadata_do_not_truncate_tooltips(ui):
    """Escaping via textContent leaves quotes intact, which would close a
    title="..." attribute early and silently drop the rest of the tooltip."""
    publisher = 'The "Best" Books Ltd.'
    page = _render_card(ui["page"], {
        "source": "annas",
        "title": 'A "Quoted" Title',
        "publisher": publisher,
        "md5": "ccc",
    })
    assert page.locator(".result-publisher").get_attribute("title") == publisher
    assert page.locator(".book-card h3").get_attribute("title") == 'A "Quoted" Title'


# ── Kavita scan on ebook import (issue #98) ────────────────────────────────

def test_ebook_download_triggers_kavita_scan(searched):
    """A user downloads a book; Kavita must be told to scan without anyone
    touching its UI. Before the fix only manga imports asked Kavita to look,
    so a downloaded ebook sat on disk invisible until a manual scan.

    The stub Kavita mirrors the real API's 400 for a libraryId-less
    /api/Library/scan, so a scan the real server would reject cannot pass here.
    """
    page = searched["page"]
    scans = searched["kavita_scans"]
    before = len(scans)

    # Download a book the earlier journey test did not take.
    title = page.evaluate("""() => {
        const btns = [...document.querySelectorAll('[data-action="startDownload"]')];
        const btn = btns[btns.length - 1];
        btn.scrollIntoView();
        return state.renderedResults[+btn.dataset.idx].title;
    }""")
    page.locator('[data-action="startDownload"]').last.click()

    deadline = time.time() + 60
    last = None
    while time.time() < deadline:
        rows = (api(page, "/api/downloads") or {}).get("downloads") or []
        ours = [j for j in rows if j.get("title") == title]
        if ours:
            last = ours[0]
            if last.get("status") == "completed":
                break
            assert last.get("status") not in ("error", "dead_letter"), \
                f"download failed: {json.dumps(last)}"
        time.sleep(2)  # gentle poll — 1/s trips the API rate limiter
    assert last and last.get("status") == "completed", f"job never completed: {last}"

    # The import fires the scan asynchronously, just after the job flips to
    # completed — give it a moment to land.
    scan_deadline = time.time() + 20
    while time.time() < scan_deadline and len(scans) == before:
        time.sleep(0.5)

    new_scans = scans[before:]
    assert new_scans, "ebook import triggered no Kavita scan at all (issue #98)"
    # No library ID configured -> scan every library. The bare /api/Library/scan
    # is the call Kavita answers with 400, and must never be what we send.
    assert all(s == "/api/Library/scan-all" for s in new_scans), \
        f"unexpected Kavita scan calls: {new_scans}"


def test_kavita_library_ids_save_from_settings_ui(ui):
    """The optional per-library scan targets are settable without editing env
    vars: type an ID, hit Kavita's Save, and it survives a reload."""
    page = ui["page"]
    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_selector("#setting-kavita_ebook_library_id")

    page.fill("#setting-kavita_ebook_library_id", "4")
    page.fill("#setting-kavita_manga_library_id", "9")
    page.click('[data-action="saveIntegration"][data-arg="kavita"]')
    page.wait_for_timeout(500)

    saved = api(page, "/api/settings")
    assert saved.get("kavita_ebook_library_id") == "4"
    assert saved.get("kavita_manga_library_id") == "9"

    page.reload(wait_until="networkidle")
    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_timeout(500)
    assert page.input_value("#setting-kavita_ebook_library_id") == "4"
    assert page.input_value("#setting-kavita_manga_library_id") == "9"


# ── Import mode / seeding (issue #59) ──────────────────────────────────────

def _wait_for_download(page, title, timeout=60):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        rows = api(page, "/api/downloads").get("downloads") or []
        ours = [j for j in rows if j.get("title") == title]
        if ours:
            last = ours[0]
            if last.get("status") == "completed":
                return last
            assert last.get("status") not in ("error", "dead_letter"), \
                f"download failed: {json.dumps(last)}"
        time.sleep(2)  # gentle poll — 1/s trips the API rate limiter
    raise AssertionError(f"job never completed: {last}")


def test_import_mode_saves_from_settings_ui(ui):
    """Keeping a torrent seedable needs its payload left in place, which is what
    the import-mode select controls. It must round-trip UI → API → reload."""
    page = ui["page"]
    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_selector("#setting-import_mode")
    assert page.input_value("#setting-import_mode") == "move", \
        "default import mode should be move"

    page.select_option("#setting-import_mode", "hardlink")
    page.wait_for_timeout(500)
    try:
        assert api(page, "/api/settings").get("import_mode") == "hardlink"
        assert api(page, "/api/config").get("import_mode") == "hardlink"

        page.reload(wait_until="networkidle")
        page.click('[data-action="switchTab"][data-arg="settings"]')
        page.wait_for_timeout(500)
        assert page.input_value("#setting-import_mode") == "hardlink", \
            "saved import mode did not survive a reload"
    finally:
        page.select_option("#setting-import_mode", "move")
        page.wait_for_timeout(500)


def test_direct_download_moves_even_in_hardlink_mode(ui):
    """A direct HTTP download is librarr's own file, not a seedable payload.
    Hardlink mode must not strand a duplicate of it in the incoming directory."""
    page = ui["page"]
    incoming = ui["data"] / "incoming"

    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_selector("#setting-import_mode")
    page.select_option("#setting-import_mode", "hardlink")
    page.wait_for_timeout(500)
    try:
        page.click('[data-action="switchTab"][data-arg="search"]')
        page.fill("#search-input", "test adventure")
        page.press("#search-input", "Enter")
        page.wait_for_selector('[data-action="startDownload"]', timeout=30000)

        # Pick the last card so this is a book the earlier download test did
        # not already import.
        title = page.evaluate("""() => {
            const btns = [...document.querySelectorAll('[data-action="startDownload"]')];
            const btn = btns[btns.length - 1];
            return state.renderedResults[+btn.dataset.idx].title;
        }""")
        page.evaluate("""() => {
            const btns = [...document.querySelectorAll('[data-action="startDownload"]')];
            btns[btns.length - 1].click();
        }""")
        _wait_for_download(page, title)

        assert list(ui["books_dir"].rglob("*.epub")), "nothing imported into the library"
        leftovers = [p for p in incoming.rglob("*") if p.is_file()]
        assert not leftovers, \
            f"hardlink mode stranded librarr's own download in incoming: {leftovers}"
    finally:
        page.click('[data-action="switchTab"][data-arg="settings"]')
        page.wait_for_timeout(200)
        page.select_option("#setting-import_mode", "move")
        page.wait_for_timeout(500)
