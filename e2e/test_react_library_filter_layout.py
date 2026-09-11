"""Library category filters must look and behave like separate buttons."""
import pytest
from playwright.sync_api import expect
from test_react_library_downloads import _sign_in


@pytest.mark.parametrize('width', [320, 390, 1440])
def test_library_category_buttons_are_separate_and_switch_results(app, page, width):
    page.set_viewport_size({'width': width, 'height': 900})
    _sign_in(app, page)
    results = {'ebooks': 'Ebook fixture', 'audiobooks': 'Audiobook fixture', 'manga': 'Manga fixture'}
    def response(title):
        return lambda route: route.fulfill(json={'items': [{'id': title, 'title': title}], 'pages': 1})
    for category, title in results.items():
        endpoint = '/api/library' + ('' if category == 'ebooks' else '/' + category)
        page.route('**' + endpoint + '?*', response(title))
    if width < 640:
        page.get_by_role('button', name='Navigation menu').click()
    page.locator('[data-action="switchTab"][data-arg="library"]').click()
    buttons = page.locator('[data-library-tab]')
    expect(buttons).to_have_count(3)
    boxes = []
    for category in results:
        button = page.locator(f'[data-library-tab="{category}"]')
        expect(button).to_be_visible()
        box = button.bounding_box()
        assert round(box['height'], 2) >= 44
        assert box['x'] >= 0 and box['x'] + box['width'] <= width
        style = button.evaluate('e => { const s=getComputedStyle(e); return {padding:parseFloat(s.paddingLeft),border:s.borderStyle}; }')
        assert style['padding'] >= 8, 'Category labels must have button padding, not run together'
        assert style['border'] != 'none', 'Category buttons need a visible boundary'
        assert button.evaluate('e => {const r=e.getBoundingClientRect();return e.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));}'), 'Filter is covered by another control'
        boxes.append(box)
    for left, right in zip(boxes, boxes[1:]):
        if abs(left['y'] - right['y']) < 2:
            assert right['x'] - left['x'] - left['width'] >= 7
    search_box = page.locator('#library-search').bounding_box()
    assert all(search_box['y'] >= box['y']+box['height'] or search_box['x'] >= box['x']+box['width'] for box in boxes)
    for category, title in results.items():
        selected = page.locator(f'[data-library-tab="{category}"]')
        selected.click()
        expect(selected).to_have_attribute('aria-pressed', 'true')
        expect(page.locator('#library-results')).to_contain_text(title)
        for other, other_title in results.items():
            if other != category:
                inactive = page.locator(f'[data-library-tab="{other}"]')
                expect(inactive).to_have_attribute('aria-pressed', 'false')
                assert selected.evaluate('e => getComputedStyle(e).backgroundColor') != inactive.evaluate('e => getComputedStyle(e).backgroundColor')
                expect(page.locator('#library-results')).not_to_contain_text(other_title)
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
