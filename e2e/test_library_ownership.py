"""End-to-end coverage for "already in library" detection (issue #96).

Runs the real binary + real Chromium against the stubbed Gutenberg source (see
conftest.py). The journey is the one from the issue, in reverse: download a
book for real, then go back to search and confirm the app now knows you own it
— in the API response, on the card, and in the server's refusal to fetch it a
second time.
"""
import time

import pytest

OWNED_TITLE = "Adventure of the Test Case"


def _search(page, query):
    page.click('[data-action="switchTab"][data-arg="search"]')
    page.fill("#search-input", query)
    page.press("#search-input", "Enter")
    page.wait_for_selector('[data-action="startDownload"]', timeout=30000)


def _result_by_title(page, title):
    return page.evaluate(
        "t => (state.renderedResults || []).find(r => r.title === t) || null", title)


def _library_titles(page):
    data = page.evaluate("() => fetch('/api/library?limit=200').then(r => r.json())")
    items = data.get("items") or data.get("library") or []
    return [i.get("title", "") for i in items]


@pytest.fixture()
def owned(ui):
    """Downloads OWNED_TITLE for real and yields once it is in the library.

    The app fixture is session-scoped, so after the first test this is a
    library lookup that finds the book already there.
    """
    page = ui["page"]
    _search(page, OWNED_TITLE)

    if OWNED_TITLE not in _library_titles(page):
        idx = page.evaluate(
            "t => (state.renderedResults || []).findIndex(r => r.title === t)", OWNED_TITLE)
        assert idx >= 0, f"{OWNED_TITLE!r} not among the stub results"
        page.click(f'[data-action="startDownload"][data-idx="{idx}"]')

        deadline = time.time() + 90
        while time.time() < deadline:
            if OWNED_TITLE in _library_titles(page):
                break
            time.sleep(2)  # gentle poll — 1/s trips the API rate limiter
        else:
            pytest.fail(f"{OWNED_TITLE!r} never landed in the library")

    return ui


def test_search_reports_ownership_in_the_api_response(owned):
    """The fields the issue asked for, on the real /api/search response."""
    page = owned["page"]
    data = page.evaluate(
        "q => fetch('/api/search?q=' + encodeURIComponent(q)).then(r => r.json())",
        OWNED_TITLE)
    results = data.get("results") or []
    assert results, "search returned nothing"

    owned_results = [r for r in results if r["title"] == OWNED_TITLE]
    assert owned_results, f"{OWNED_TITLE!r} missing from search results"
    for r in owned_results:
        assert r.get("in_library") is True, f"owned book not flagged: {r}"
        assert r.get("library_item_id"), f"no library_item_id on an owned result: {r}"

    for r in results:
        # The flag must always be present, so a client can tell "not owned"
        # from "this build does not report ownership".
        assert "in_library" in r, f"in_library missing from result: {r}"
        if r["title"] != OWNED_TITLE:
            assert r["in_library"] is False, f"unowned book flagged as owned: {r}"
            assert "library_item_id" not in r


def test_card_shows_the_in_library_badge_and_download_anyway(owned):
    page = owned["page"]
    _search(page, OWNED_TITLE)

    card = page.locator(".book-card", has=page.locator(f'h3[title="{OWNED_TITLE}"]'))
    assert card.count() == 1, f"expected one card for {OWNED_TITLE!r}, got {card.count()}"
    assert card.locator(".result-in-library").count() == 1, "owned card has no badge"
    assert "In Library" in card.locator(".result-in-library").inner_text()
    assert OWNED_TITLE in card.locator(".result-in-library").get_attribute("title")
    assert "anyway" in card.locator('[data-action="startDownload"]').inner_text().lower()

    # Books the user does not own keep the plain Download button and no badge.
    others = page.locator(".book-card").count() - 1
    assert page.locator(".result-in-library").count() == 1, \
        f"badge leaked onto {others} unowned cards"


def test_download_of_an_owned_book_is_refused_by_the_server(owned):
    """The core of the issue: the API itself must stop the duplicate, not the
    UI. Posted directly, exactly as a script or third-party client would."""
    page = owned["page"]
    result = _result_by_title(page, OWNED_TITLE)
    assert result, "owned result missing from the rendered list"

    resp = page.evaluate(
        """r => fetch('/api/download', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                title: r.title, author: r.author || '', source: r.source,
                download_url: r.download_url || r.url || '',
            }),
        }).then(async res => ({status: res.status, body: await res.json()}))""",
        result)

    assert resp["status"] == 409, f"duplicate download was accepted: {resp}"
    body = resp["body"]
    assert body["success"] is False
    assert body["in_library"] is True
    assert body["code"] == "already_in_library"
    assert body["library_item_id"] > 0
    assert body["library_title"] == OWNED_TITLE

    # And nothing was queued behind that refusal.
    jobs = page.evaluate("() => fetch('/api/downloads').then(r => r.json())")
    running = [j for j in (jobs.get("downloads") or [])
               if j.get("title") == OWNED_TITLE and j.get("status") not in ("completed",)]
    assert not running, f"the refused download still started a job: {running}"


def test_author_prefixed_result_title_still_matches(owned):
    """Sources publish "Author - Title"; the library row holds only the title.
    This is the normalization case from the issue, checked against the real
    matcher through the real endpoint."""
    page = owned["page"]
    resp = page.evaluate(
        """t => fetch('/api/download', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                title: 'Carol Coder - ' + t + ' (Illustrated Edition)',
                author: 'Carol Coder', source: 'annas',
                download_url: 'http://127.0.0.1:1/nope.epub',
            }),
        }).then(async res => ({status: res.status, body: await res.json()}))""",
        OWNED_TITLE)
    assert resp["status"] == 409, f"decorated title slipped past the check: {resp}"
    assert resp["body"]["library_title"] == OWNED_TITLE


def test_download_anyway_overrides_the_check(owned):
    """The block must not be a dead end — a second edition is a real want."""
    page = owned["page"]
    result = _result_by_title(page, OWNED_TITLE)

    resp = page.evaluate(
        """r => fetch('/api/download', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                title: r.title, author: r.author || '', source: r.source,
                download_url: r.download_url || r.url || '', force: true,
            }),
        }).then(async res => ({status: res.status, body: await res.json()}))""",
        result)

    assert resp["status"] != 409, f"force was ignored: {resp}"
    assert resp["body"].get("code") != "already_in_library"

    # The UI's owned button is what sends that flag.
    _search(page, OWNED_TITLE)
    sends_force = page.evaluate(
        "() => (state.renderedResults || []).some(r => r.in_library === true)")
    assert sends_force, "no rendered result is flagged, so nothing would send force"


def test_unowned_book_downloads_without_a_prompt(owned):
    """Ownership detection must not get in the way of a normal grab."""
    page = owned["page"]
    _search(page, "test adventure")
    unowned = page.evaluate(
        "t => (state.renderedResults || []).find(r => !r.in_library && r.title !== t) || null",
        OWNED_TITLE)
    assert unowned, "no unowned stub book left to check"

    resp = page.evaluate(
        """r => fetch('/api/download', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                title: r.title, author: r.author || '', source: r.source,
                download_url: r.download_url || r.url || '',
            }),
        }).then(async res => ({status: res.status, body: await res.json()}))""",
        unowned)
    assert resp["status"] != 409, f"an unowned book was blocked: {resp}"
