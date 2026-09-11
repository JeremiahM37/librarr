"""React quality-profile size inputs preserve the byte-valued API contract."""


def test_profile_size_fields_post_bytes(app, page):
    page.goto(app["base"], wait_until="networkidle")
    page.locator('[data-action="switchTab"][data-arg="settings"]').click()
    page.wait_for_selector('[data-qp]')

    card = page.locator('[data-qp]').filter(has=page.locator('input[data-field="preferred_size_min"]')).first
    card.locator('input[data-field="preferred_size_min"]').fill("12")
    card.locator('input[data-field="preferred_size_max"]').fill("48")
    card.locator('[data-action="qpSave"]').click()
    page.wait_for_timeout(300)

    profile_id = card.get_attribute("data-qp")
    profiles = page.evaluate("() => fetch('/api/quality-profiles').then(r => r.json())")
    saved = next(p for p in profiles if str(p["id"]) == profile_id)
    assert saved["preferred_size_min"] == 12 * 1048576
    assert saved["preferred_size_max"] == 48 * 1048576
