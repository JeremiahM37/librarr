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
