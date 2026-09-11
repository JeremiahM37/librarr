"""Browser coverage for React Wanted decisions and author-check feedback."""


def _register(app, page):
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


def test_wanted_summary_and_both_search_outcome_tables(app, page):
    _register(app, page)
    item = {
        "id": 41,
        "title": "The Dispossessed",
        "author": "Ursula K. Le Guin",
        "media_type": "ebook",
        "state": "missing",
        "monitored": True,
    }

    def wishlist(route):
        route.fulfill(
            json={
                "items": [item],
                "counts": {"missing": 1, "satisfied": 2},
                "upgrades_enabled": True,
            }
        )

    requests = []

    def search(route, request):
        payload = request.post_data_json
        requests.append(payload)
        dry_run = payload["dry_run"]
        route.fulfill(
            json={
                "item": item,
                "outcome": {
                    "action": "matched" if dry_run else "grabbed",
                    "reason": "best acceptable release",
                    "candidate": "Release A (EPUB, Fixture, score 91)",
                    "decisions": [
                        {
                            "accepted": True,
                            "upgrade": not dry_run,
                            "format": "epub",
                            "score": 91.4,
                            "title": "Release A",
                            "source": "Fixture",
                            "reason": "highest ranked format",
                        },
                        {
                            "accepted": False,
                            "format": "pdf",
                            "score": 62.2,
                            "title": "Release B",
                            "source": "Fixture",
                            "reason": "below minimum score",
                        },
                    ],
                },
            }
        )

    page.route("**/api/wishlist", wishlist)
    page.route("**/api/wishlist/41/search", search)
    page.locator('[data-action="switchTab"][data-arg="wishlist"]').click()
    page.wait_for_selector('[data-wanted-id="41"]')
    assert page.locator("#wanted-summary").inner_text() == "1 missing · 2 satisfied"

    card = page.locator('[data-wanted-id="41"]')
    card.locator('[data-action="explainWanted"]').click()
    page.wait_for_selector('[data-wanted-decisions="2"]')
    table = card.locator("[data-wanted-outcome]")
    assert "[dry run]" in table.inner_text()
    assert "EPUB" in table.inner_text()
    assert "91" in table.inner_text()
    assert "Release B Fixture" in table.inner_text()
    assert "below minimum score" in table.inner_text()

    card.locator('[data-action="searchWantedNow"]').click()
    page.wait_for_function(
        "() => document.querySelector('[data-wanted-outcome]')?.textContent.includes('grabbed')"
    )
    assert "[dry run]" not in table.inner_text()
    assert "↑" in table.inner_text()
    assert requests == [{"dry_run": True}, {"dry_run": False}]


def test_author_manual_check_reports_baseline_and_new_work(app, page):
    _register(app, page)
    author = {
        "id": 7,
        "name": "Octavia Butler",
        "check_interval_days": 7,
        "auto_add": True,
        "seen_works": 3,
    }
    checks = iter(
        [
            {"author": "Octavia Butler", "baseline": True, "seen": 3},
            {
                "author": "Octavia Butler",
                "new": ["Parable of the Talents"],
                "added": 1,
            },
        ]
    )
    page.route("**/api/authors", lambda route: route.fulfill(json={"authors": [author]}))
    page.route(
        "**/api/authors/7/check",
        lambda route: route.fulfill(json={"result": next(checks)}),
    )
    page.locator('[data-action="switchTab"][data-arg="settings"]').click()
    page.wait_for_selector('[data-author-id="7"]')
    button = page.locator('[data-author-id="7"] [data-action="checkAuthor"]')

    button.click()
    page.wait_for_function(
        "() => document.querySelector('#toast-container')?.textContent.includes('baseline recorded')"
    )
    assert "Checked Octavia Butler: baseline recorded (3 works), new releases will be picked up from now on" in page.locator("#toast-container").inner_text()

    button.click()
    page.wait_for_function(
        "() => document.querySelector('#toast-container')?.textContent.includes('1 added to wanted')"
    )
    assert "Checked Octavia Butler: 1 new, 1 added to wanted" in page.locator("#toast-container").inner_text()
