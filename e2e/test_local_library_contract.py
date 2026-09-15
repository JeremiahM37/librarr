"""Real DB -> Go API -> React contracts, with no library-response mocks."""
import sqlite3

import pytest
from playwright.sync_api import expect
from conftest import app as app_fixture


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
