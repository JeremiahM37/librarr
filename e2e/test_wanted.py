"""End-to-end tests for the wanted list, quality profiles and author monitoring.

Real binary + real Chromium against the stub sources in conftest.py. The
centrepiece is the *arr upgrade loop run for real:

  add wanted item → scheduler pass grabs a PDF (state: upgrade wanted)
  → stub starts serving EPUB → next pass grabs it, the PDF is retired
  → state: satisfied, exactly one file on disk.

Every test also asserts a zero-JS-error journey (ui fixture).
"""
import json
import time
import urllib.request

import pytest

LADDER_TITLE = "Format Ladder"


def api(page, path, method="GET", body=None):
    payload = "null" if body is None else json.dumps(json.dumps(body))
    return page.evaluate(
        f"() => fetch('{path}', {{method: '{method}', headers: {{'Content-Type': 'application/json'}},"
        f" body: {payload} === null ? undefined : {payload}}}).then(r => r.json())"
    )


def set_ladder(stub, fmt):
    urllib.request.urlopen(urllib.request.Request(f"{stub}/admin/ladder?fmt={fmt}", method="POST"), timeout=5)


def ol_add(stub, title, author="Stub Author"):
    from urllib.parse import quote_plus
    urllib.request.urlopen(
        urllib.request.Request(f"{stub}/admin/ol?add={quote_plus(title)}&author={quote_plus(author)}", method="POST"),
        timeout=5)


def goto_wanted(page):
    page.click('[data-action="switchTab"][data-arg="wishlist"]')
    page.wait_for_timeout(400)


def wanted_rows(page):
    return api(page, "/api/wishlist")["items"]


def wait_for_no_active_jobs(page, timeout=60):
    """Poll until every download job is terminal (the stub is local, so this
    is quick); fail loudly on an error job."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = api(page, "/api/downloads").get("downloads") or []
        active = [j for j in rows if j.get("status") in ("queued", "searching", "downloading", "importing", "retry_wait")]
        bad = [j for j in rows if j.get("status") in ("error", "dead_letter")]
        assert not bad, f"download failed: {json.dumps(bad)}"
        if not active:
            return rows
        time.sleep(1)
    raise AssertionError("downloads still active after %ss" % timeout)


def run_pass(page):
    """One synchronous scheduler pass through the UI button on the wanted tab."""
    page.click('#wanted-search-all')
    # The button awaits /api/scheduler/run?wait=1 and then reloads the list.
    page.wait_for_timeout(800)
    return wait_for_no_active_jobs(page)


def row_state(page, title):
    page.wait_for_timeout(300)
    for it in wanted_rows(page):
        if it["title"] == title:
            return it
    return None


def library_files(books_dir, stem):
    return sorted(p.name for p in books_dir.rglob("*") if p.is_file() and stem.lower() in p.name.lower())


# ── The upgrade loop ────────────────────────────────────────────────────────

def test_wanted_item_is_grabbed_then_upgraded_pdf_to_epub(ui):
    page, stub, books = ui["page"], ui["stub"], ui["books_dir"]
    set_ladder(stub, "pdf")

    # Add through the form; the profile select defaults to the built-in.
    goto_wanted(page)
    page.click('[data-action="showWishlistForm"]')
    page.fill("#wl-title", LADDER_TITLE)
    page.fill("#wl-author", "Dana Ranker")
    page.click('[data-action="addWishlistItem"]')
    page.wait_for_selector(f'[data-wanted-id] h4:has-text("{LADDER_TITLE}")', timeout=5000)
    badge = page.locator(f'[data-wanted-id]:has(h4:has-text("{LADDER_TITLE}")) [data-wanted-state]')
    assert badge.get_attribute("data-wanted-state") == "missing"

    # Pass 1: the only release is (secretly) a PDF. It is grabbed because the
    # item has no file at all, and recorded with its real format.
    run_pass(page)
    goto_wanted(page)
    item = row_state(page, LADDER_TITLE)
    assert item["current_format"] == "pdf", item
    assert item["state"] == "upgrade", item  # PDF is below the EPUB cutoff
    assert item["cutoff_met"] is False
    assert library_files(books, "format ladder") == ["Format Ladder.pdf"], library_files(books, "format ladder")
    badge = page.locator(f'[data-wanted-id]:has(h4:has-text("{LADDER_TITLE}")) [data-wanted-state]')
    assert badge.get_attribute("data-wanted-state") == "upgrade"
    assert "PDF on disk" in page.inner_text(f'[data-wanted-id="{item["id"]}"]')

    # Pass 2 with the stub still on PDF. The source *claims* EPUB, so the
    # release is grabbed as an upgrade — and delivers the same PDF. The import
    # is rejected (nothing improved), the release is blocklisted, and the
    # library is unchanged. Pass 2b proves the blocklist holds: no new grab.
    run_pass(page)
    item = row_state(page, LADDER_TITLE)
    assert item["state"] == "upgrade" and item["current_format"] == "pdf", item
    assert "blocklisted" in item["last_result"], item["last_result"]
    assert library_files(books, "format ladder") == ["Format Ladder.pdf"]
    blocked = api(page, "/api/blocklist")
    entries = blocked if isinstance(blocked, list) else (blocked.get("entries") or blocked.get("items") or [])
    assert any("rel=pdf" in (e.get("download_url") or "") for e in entries), entries
    jobs_before = len(api(page, "/api/downloads").get("downloads") or [])
    run_pass(page)
    item = row_state(page, LADDER_TITLE)
    assert "blocklisted" in item["last_result"] and "none acceptable" in item["last_result"], item["last_result"]
    assert len(api(page, "/api/downloads").get("downloads") or []) == jobs_before

    # Pass 3: the stub now serves an EPUB → upgrade, PDF retired.
    set_ladder(stub, "epub")
    run_pass(page)
    goto_wanted(page)
    item = row_state(page, LADDER_TITLE)
    assert item["current_format"] == "epub", item
    assert item["state"] == "satisfied" and item["cutoff_met"] is True, item
    assert library_files(books, "format ladder") == ["Format Ladder.epub"], library_files(books, "format ladder")
    lib = api(page, "/api/library")
    ladder_rows = [b for b in (lib.get("books") or lib.get("items") or []) if b.get("title") == LADDER_TITLE]
    assert len(ladder_rows) == 1, ladder_rows
    assert ladder_rows[0]["file_format"] == "epub"

    # Pass 4: satisfied items are skipped without even searching.
    jobs_before = len(api(page, "/api/downloads").get("downloads") or [])
    stats = api(page, "/api/scheduler/run?wait=1", method="POST")["stats"]
    assert stats["skipped"] >= 1 and stats["grabbed"] == 0, stats
    assert len(api(page, "/api/downloads").get("downloads") or []) == jobs_before

    # The activity feed recorded the upgrade.
    activity = api(page, "/api/activity")
    events = activity.get("events") or activity.get("activity") or activity.get("items") or []
    assert any(e.get("event_type") == "wanted_upgraded" for e in events), events[:5]


def test_explain_button_renders_every_candidate_without_grabbing(ui):
    page = ui["page"]
    api(page, "/api/wishlist", "POST", {"title": "Test Adventure", "author": "Alice Author"})
    goto_wanted(page)
    row = page.locator('[data-wanted-id]:has(h4:has-text("Test Adventure"))')
    jobs_before = len(api(page, "/api/downloads").get("downloads") or [])
    row.locator('[data-action="explainWanted"]').click()
    page.wait_for_selector('[data-wanted-decisions]', timeout=30000)
    table = row.locator('[data-wanted-decisions]')
    assert int(table.get_attribute("data-wanted-decisions")) >= 1
    text = row.inner_text()
    assert "EPUB" in text and "[dry run]" in text
    # A dry run never starts a download.
    assert len(api(page, "/api/downloads").get("downloads") or []) == jobs_before
    item = row_state(page, "Test Adventure")
    assert item.get("active_job_id", "") == ""
    api(page, f"/api/wishlist/{item['id']}", "DELETE")


def test_monitor_toggle_and_profile_select_persist(ui):
    page = ui["page"]
    wid = api(page, "/api/wishlist", "POST", {"title": "Toggle Me"})["id"]
    goto_wanted(page)
    row = page.locator(f'[data-wanted-id="{wid}"]')
    row.locator('[data-action-change="toggleWantedMonitored"]').uncheck()
    page.wait_for_timeout(500)
    assert row.locator('[data-wanted-state]').get_attribute("data-wanted-state") == "unmonitored"
    assert row_state(page, "Toggle Me")["monitored"] is False

    # A custom profile shows up in the row's select and sticks.
    created = api(page, "/api/quality-profiles", "POST",
                  {"name": "E2E PDF-first", "media_type": "ebook", "format_ranking": ["pdf", "epub"], "cutoff_format": "pdf"})
    goto_wanted(page)
    row = page.locator(f'[data-wanted-id="{wid}"]')
    row.locator('[data-action-change="setWantedProfile"]').select_option(str(created["id"]))
    page.wait_for_timeout(500)
    item = row_state(page, "Toggle Me")
    assert item["quality_profile_id"] == created["id"] and item["profile_name"] == "E2E PDF-first"

    # Deleting the profile drops the item back to the default.
    api(page, f"/api/quality-profiles/{created['id']}", "DELETE")
    assert row_state(page, "Toggle Me")["quality_profile_id"] == 0
    api(page, f"/api/wishlist/{wid}", "DELETE")


# ── Settings: quality profile editor ────────────────────────────────────────

def test_quality_profile_editor_roundtrip(ui):
    page = ui["page"]
    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_selector('[data-qp]', timeout=5000)
    cards = page.locator('[data-qp]')
    assert cards.count() >= 3
    # Built-ins carry the badge and no Delete button.
    builtin = page.locator('[data-qp]:has-text("Built-in")').first
    assert builtin.locator('[data-action="qpDelete"]').count() == 0

    # New ebook profile: name it, drop PDF, demote EPUB below AZW3, save.
    page.click('[data-action="qpNew"][data-arg="ebook"]')
    new_card = page.locator('[data-qp^="new-"]')
    assert new_card.count() == 1
    new_card.locator('input[data-field="name"]').fill("E2E Kindle first")
    new_card.locator('input[data-field="name"]').dispatch_event("change")
    # The default draft ranks the first two known formats (epub, azw3).
    new_card.locator('[data-action-change="qpToggleFormat"][data-format="mobi"]').check()
    page.wait_for_timeout(200)
    new_card = page.locator('[data-qp^="new-"]')
    new_card.locator('[data-action="qpMove"][data-format="azw3"][data-dir="-1"]').click()
    page.wait_for_timeout(200)
    new_card = page.locator('[data-qp^="new-"]')
    ranking = new_card.locator('[data-qp-row]').evaluate_all("els => els.map(e => e.dataset.qpRow)")
    assert ranking == ["azw3", "epub", "mobi"], ranking
    new_card.locator('select[data-field="cutoff_format"]').select_option("epub")
    new_card.locator('[data-action="qpSave"]').click()
    page.wait_for_timeout(800)

    profiles = api(page, "/api/quality-profiles")
    mine = [p for p in profiles if p["name"] == "E2E Kindle first"]
    assert len(mine) == 1, profiles
    assert mine[0]["format_ranking"] == ["azw3", "epub", "mobi"] and mine[0]["cutoff_format"] == "epub"
    assert mine[0]["builtin"] is False and mine[0]["media_type"] == "ebook"

    # Saving an invalid edit is refused with the server's reason, not silently.
    card = page.locator(f'[data-qp="{mine[0]["id"]}"]')
    for fmt in ("azw3", "epub", "mobi"):
        card.locator(f'[data-action-change="qpToggleFormat"][data-format="{fmt}"]').uncheck()
        page.wait_for_timeout(150)
        card = page.locator(f'[data-qp="{mine[0]["id"]}"]')
    card.locator('[data-action="qpSave"]').click()
    page.wait_for_timeout(500)
    assert "at least one format" in page.inner_text("#toast-container")
    still = [p for p in api(page, "/api/quality-profiles") if p["id"] == mine[0]["id"]]
    assert still[0]["format_ranking"] == ["azw3", "epub", "mobi"]

    # Delete through the UI (confirm dialog accepted).
    page.once("dialog", lambda d: d.accept())
    card.locator('[data-action="qpDelete"]').click()
    page.wait_for_timeout(800)
    assert not [p for p in api(page, "/api/quality-profiles") if p["name"] == "E2E Kindle first"]


def test_scheduler_settings_save_and_reload(ui):
    page = ui["page"]
    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_selector('#setting-scheduler_interval_hours', timeout=5000)
    page.wait_for_timeout(500)
    assert page.is_checked('#setting-auto_upgrade_enabled')  # from env in conftest

    page.fill('#setting-scheduler_interval_hours', '6')
    page.fill('#setting-scheduler_min_score', '55')
    page.uncheck('#setting-auto_upgrade_enabled')
    page.click('[data-action="saveWantedSettings"]')
    page.wait_for_timeout(600)
    settings = api(page, "/api/settings")
    assert settings["scheduler_interval_hours"] == 6
    assert settings["scheduler_min_score"] == 55
    assert settings["auto_upgrade_enabled"] is False
    saved = json.loads((ui["data"] / "settings.json").read_text())
    assert saved["scheduler_min_score"] == 55 and saved["auto_upgrade_enabled"] is False

    # With upgrades off the wanted tab warns, once there is something listed.
    wid = api(page, "/api/wishlist", "POST", {"title": "Banner Check"})["id"]
    goto_wanted(page)
    assert page.is_visible('#wanted-upgrades-off')

    # Restore for the other tests.
    api(page, "/api/settings", "POST", {"auto_upgrade_enabled": True, "scheduler_min_score": 70, "scheduler_interval_hours": 24})
    api(page, f"/api/wishlist/{wid}", "DELETE")
    goto_wanted(page)
    assert not page.is_visible('#wanted-upgrades-off')


# ── Settings: author monitoring acts ────────────────────────────────────────

def test_author_follow_baseline_then_new_work_is_wanted(ui):
    page, stub = ui["page"], ui["stub"]
    urllib.request.urlopen(urllib.request.Request(f"{stub}/admin/ol?add=", method="POST"), timeout=5)  # reset
    ol_add(stub, "First Novel")
    ol_add(stub, "Second Novel")

    page.click('[data-action="switchTab"][data-arg="settings"]')
    page.wait_for_selector('#author-name', timeout=5000)
    page.fill('#author-name', 'Stub Author')
    page.click('[data-action="addAuthor"]')
    page.wait_for_selector('[data-author-id]', timeout=5000)
    row = page.locator('[data-author-id]').first
    assert "not checked yet" in row.inner_text()

    # First check: baseline, nothing wanted.
    row.locator('[data-action="checkAuthor"]').click()
    page.wait_for_timeout(800)
    assert "baseline recorded" in page.inner_text("#toast-container")
    row = page.locator('[data-author-id]').first
    assert "2 works known" in row.inner_text()
    assert not [w for w in wanted_rows(page) if w["author"] == "Stub Author"]

    # A new work appears → added to the wanted list, monitored, attributed.
    ol_add(stub, "Third Novel")
    row.locator('[data-action="checkAuthor"]').click()
    page.wait_for_timeout(800)
    added = [w for w in wanted_rows(page) if w["title"] == "Third Novel"]
    assert len(added) == 1, wanted_rows(page)
    assert added[0]["author"] == "Stub Author" and added[0]["monitored"] is True
    assert added[0]["source"].startswith("author:")
    assert "3 works known" in page.locator('[data-author-id]').first.inner_text()

    # Reissues (same work keys) do not fire again.
    row.locator('[data-action="checkAuthor"]').click()
    page.wait_for_timeout(800)
    assert len([w for w in wanted_rows(page) if w["title"] == "Third Novel"]) == 1

    # Auto-add off → notify only.
    row.locator('[data-action-change="authorAutoAdd"]').uncheck()
    page.wait_for_timeout(400)
    ol_add(stub, "Fourth Novel")
    row.locator('[data-action="checkAuthor"]').click()
    page.wait_for_timeout(800)
    assert not [w for w in wanted_rows(page) if w["title"] == "Fourth Novel"]
    assert "1 new" in page.inner_text("#toast-container")

    # Unfollow cleans up.
    page.once("dialog", lambda d: d.accept())
    row.locator('[data-action="deleteAuthor"]').click()
    page.wait_for_timeout(500)
    assert page.locator('[data-author-id]').count() == 0
    api(page, f"/api/wishlist/{added[0]['id']}", "DELETE")
