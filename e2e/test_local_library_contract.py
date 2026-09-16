"""Real DB -> Go API -> React contracts, with no library-response mocks."""
import sqlite3
import zipfile
from pathlib import Path

import pytest
from playwright.sync_api import expect
from conftest import app as app_fixture
from conftest import TINY_IMG


@pytest.fixture()
def local_app(stub_server, librarr_binary, tmp_path_factory):
    # Disable Kavita; ABS is unconfigured in the fixture environment too.
    yield from app_fixture.__wrapped__(
        stub_server, {"url": "", "scan_calls": []}, librarr_binary, tmp_path_factory
    )


@pytest.mark.parametrize("category,media,extension", [
    ("ebooks", "ebook", "epub"),
    ("audiobooks", "audiobook", "m4b"),
    ("manga", "manga", "cbz"),
])
def test_local_metadata_filter_and_pagination(local_app, page, category, media, extension):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    with sqlite3.connect(local_app["data"] / "librarr.db") as conn:
        conn.executemany(
            "INSERT INTO library_items (title,author,media_type,file_format,file_size,added_at) VALUES (?,?,?,?,?,?)",
            [(f"Local {media} {i:03}", "Fixture Artist", media, extension, 2097152, i)
             for i in range(101)],
        )
        conn.execute("INSERT INTO library_items (title,media_type) VALUES (?,?)",
                     ("Wrong category", "manga" if media != "manga" else "ebook"))
    page.goto(local_app["base"], wait_until="networkidle")
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    page.locator(f'[data-library-tab="{category}"]').click()
    card = page.locator("#library-results article").filter(has=page.get_by_role("heading", name=f"Local {media} 100", exact=True))
    expect(card).to_be_visible()
    expect(card).to_contain_text("Fixture Artist")
    expect(card).to_contain_text(extension)
    expect(card).to_contain_text("2.0 MB")
    expect(card.locator("[data-cover-fallback]")).to_have_text("L" + media[0].upper())
    expect(card.locator("a")).to_have_count(0)
    expect(page.locator("#library-results")).not_to_contain_text("Unknown")
    expect(page.locator("#library-results")).not_to_contain_text("Wrong category")
    page.locator("#library-pagination").get_by_role("button", name="3" if category == "ebooks" else "2", exact=True).click()
    expect(page.get_by_role("heading", name=f"Local {media} 000", exact=True)).to_be_visible()
    expect(page.get_by_role("heading", name=f"Local {media} 100", exact=True)).to_have_count(0)
    # The initial search debounce must not reset a quick page change.
    page.wait_for_timeout(500)
    expect(page.get_by_role("heading", name=f"Local {media} 000", exact=True)).to_be_visible()
    # Search must filter the whole library, not just the loaded page.
    page.fill("#library-search", f"Local {media} 100")
    expect(page.locator("#library-results article")).to_have_count(1)
    expect(card).to_be_visible()
    expect(page.locator("#library-pagination")).to_have_count(0)
    page.fill("#library-search", "no matching title")
    expect(page.locator("#library-empty")).to_be_visible()
    page.fill("#library-search", "fixture artist")
    expect(page.locator("#library-pagination")).to_be_visible()
    assert errors == []


@pytest.mark.parametrize("category,media,route,page_size", [
    ("ebooks", "ebook", "book", 50),
    ("audiobooks", "audiobook", "audiobook", 100),
    ("manga", "manga", "manga", 100),
])
@pytest.mark.parametrize("mobile", [False, True])
def test_local_library_remove_cancel_failure_last_page_and_reload(
    local_app, page, category, media, route, page_size, mobile
):
    if mobile:
        page.set_viewport_size({"width": 390, "height": 844})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    payload = local_app["data"] / "keep.cbz"
    payload.write_bytes(b"library removal must preserve the file")
    with sqlite3.connect(local_app["data"] / "librarr.db") as conn:
        target = conn.execute(
            "INSERT INTO library_items (title,media_type,file_path,added_at) VALUES (?,?,?,?)",
            ("Remove this volume", media, str(payload), 0),
        ).lastrowid
        conn.executemany(
            "INSERT INTO library_items (title,media_type,added_at) VALUES (?,?,?)",
            [(f"Keep volume {index:03}", media, index + 1) for index in range(page_size)],
        )
    page.goto(local_app["base"], wait_until="networkidle")
    if mobile:
        page.get_by_role("button", name="Navigation menu").click()
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    page.locator(f'[data-library-tab="{category}"]').click()
    page.locator("#library-pagination").get_by_role("button", name="2", exact=True).click()
    card = page.locator("#library-results article").filter(has_text="Remove this volume")
    expect(card).to_be_visible()
    remove = card.locator('[data-action="deleteLibraryItem"]')
    expect(remove).to_have_accessible_name("Remove from library")
    bounds = remove.bounding_box()
    assert bounds and bounds["width"] >= 44 and bounds["height"] >= 44
    deletes = []
    page.on("request", lambda request: deletes.append(request.url) if request.method == "DELETE" else None)
    page.once("dialog", lambda dialog: dialog.dismiss())
    remove.click()
    expect(card).to_be_visible()
    assert deletes == []
    delete_url = f"**/api/library/{route}/{target}*"
    page.route(delete_url, lambda request: request.fulfill(status=500, json={"error": "Fixture delete failed"}))
    page.once("dialog", lambda dialog: dialog.accept())
    remove.click()
    expect(page.locator("#toast-container")).to_contain_text("Fixture delete failed")
    expect(card).to_be_visible()
    page.unroute(delete_url)
    page.once("dialog", lambda dialog: dialog.accept())
    remove.click()
    expect(card).to_have_count(0)
    expect(page.locator("#library-results article")).to_have_count(page_size)
    expect(page.locator("#library-pagination")).to_have_count(0)
    page.reload(wait_until="networkidle")
    if mobile:
        page.get_by_role("button", name="Navigation menu").click()
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    page.locator(f'[data-library-tab="{category}"]').click()
    expect(page.locator("#library-results article")).to_have_count(page_size)
    expect(page.locator("#library-results")).not_to_contain_text("Remove this volume")
    with sqlite3.connect(local_app["data"] / "librarr.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM library_items WHERE id=?", (target,)).fetchone()[0] == 0
    assert payload.read_bytes() == b"library removal must preserve the file"
    assert errors == []


@pytest.mark.parametrize("category,media", [
    ("ebooks", "ebook"), ("audiobooks", "audiobook"), ("manga", "manga"),
])
def test_remove_only_filtered_item_keeps_other_records(local_app, page, category, media):
    with sqlite3.connect(local_app["data"] / "librarr.db") as conn:
        conn.executemany("INSERT INTO library_items (title,media_type) VALUES (?,?)",
                         [("Delete needle", media), ("Keep unrelated", media)])
    page.goto(local_app["base"], wait_until="networkidle")
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    page.locator(f'[data-library-tab="{category}"]').click()
    page.fill("#library-search", "Delete needle")
    expect(page.locator("#library-results article")).to_have_count(1)
    page.once("dialog", lambda dialog: dialog.accept())
    page.get_by_role("button", name="Remove from library").click()
    expect(page.locator("#library-empty")).to_be_visible()
    expect(page.locator("#library-search")).to_have_value("Delete needle")
    page.fill("#library-search", "")
    expect(page.locator("#library-results article")).to_have_count(1)
    expect(page.locator("#library-results")).to_contain_text("Keep unrelated")


def test_manga_import_to_library_and_remove(local_app, page):
    source = local_app["data"] / "incoming" / "Regression Volume.cbz"
    source.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("001.png", TINY_IMG)
    response = page.request.post(local_app["base"] + "/api/import/files", data={
        "files": [{"path": str(source), "title": "Regression Volume", "author": "Manga Artist"}],
    })
    assert response.ok
    assert response.json()["imported"] == 1, response.text()
    items = page.request.get(local_app["base"] + "/api/library/manga").json()["items"]
    assert len(items) == 1
    item = items[0]
    assert item["media_type"] == "manga" and item["file_format"] == "cbz"
    imported = Path(item["file_path"])
    assert imported.is_file()
    original = imported.read_bytes()
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(local_app["base"], wait_until="networkidle")
    page.get_by_role("button", name="Navigation menu").click()
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    page.locator('[data-library-tab="manga"]').click()
    card = page.locator("#library-results article")
    expect(card).to_contain_text("Regression Volume")
    expect(card).to_contain_text("Manga Artist")
    expect(card).to_contain_text("cbz")
    page.screenshot(path=str(local_app["data"] / "manga-remove-mobile.png"))
    page.once("dialog", lambda dialog: dialog.accept())
    card.get_by_role("button", name="Remove from library").click()
    expect(page.locator("#library-empty")).to_be_visible()
    assert page.request.get(local_app["base"] + "/api/library/manga").json()["total"] == 0
    assert imported.read_bytes() == original
