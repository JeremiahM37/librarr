"""Streaming search and download contracts through the actual React UI."""
import json
from playwright.sync_api import expect


def test_stream_race_manga_download_fields_and_ownership(app,page):
    late=[]
    errors=[]
    page.on('pageerror',lambda e:errors.append(str(e)))
    result={'title':'Contract Probe','author':'Example','source':'prowlarr','source_id':'fixture-source',
            'download_url':'https://example.invalid/proxy/nzb','abb_url':'https://example.invalid/abb',
            'md5':'fixture-md5','info_hash':'fixture-hash','magnet':'magnet:?xt=fixture',
            'media_type':'manga','download_protocol':'usenet','language':'ja','year':2024,'format':'cbz'}
    requests=[]
    def search(route):
        requests.append(route.request.url)
        if 'q=late' in route.request.url:
            late.append(route)
            return
        body='event: results\r\ndata: '+json.dumps({'results':[result]})+'\r\n\r\nevent: complete\r\ndata: '+json.dumps({'results':[result]})+'\r\n\r\n'
        route.fulfill(content_type='text/event-stream',body=body)
    page.route('**/api/search**',search)
    page.goto(app['base'],wait_until='networkidle')
    page.fill('#search-input','late')
    page.press('#search-input','Enter')
    page.wait_for_timeout(100)
    page.fill('#search-input','current')
    page.press('#search-input','Enter')
    expect(page.locator('.book-card h3')).to_have_text('Contract Probe')
    for route in late:
        route.fulfill(content_type='text/event-stream',body='event: complete\ndata: {"results":[{"title":"STALE","source":"fixture"}]}\n\n')
    page.wait_for_timeout(100)
    expect(page.locator('.book-card h3')).to_have_text('Contract Probe')
    page.locator('[data-action="switchSearchTab"][data-arg="manga"]').click()
    expect(page.locator('#search-spinner')).to_have_count(0)
    assert any('/api/search/manga/stream?' in url for url in requests)
    assert not any('/mangas' in url for url in requests)
    writes=[]
    def download(route):
        writes.append(route.request.post_data_json)
        if len(writes)==1:
            route.fulfill(status=409,json={'success':False,'in_library':True,'library_title':'Owned Probe','library_item_id':17})
        else:
            route.fulfill(json={'success':True,'job_id':'fixture-job'})
    page.route('**/api/download',download)
    page.route('**/api/downloads',lambda route:route.fulfill(json={'downloads':[{'job_id':'fixture-job','title':'Contract Probe','status':'retry_wait','detail':'Retry 1/2 scheduled','retry_count':1,'max_retries':2}]}))
    page.locator('[data-action="startDownload"]').click()
    expect(page.locator('.result-in-library')).to_have_text('In Library')
    expect(page.locator('[data-action="startDownload"]')).to_have_text('Download anyway')
    assert writes[0]['force'] is False
    page.locator('[data-action="startDownload"]').click()
    expect(page.locator('[data-action="startDownload"]')).to_have_text('Retry 1/2 scheduled')
    assert writes[1]['force'] is True
    for key in ('download_url','abb_url','source_id','md5','info_hash','magnet','media_type','download_protocol'):
        assert writes[1][key]==result[key]
    assert not errors


def test_mobile_search_and_settings_keep_controls_on_screen(app,page):
    for width in (320,390,1440):
        page.set_viewport_size({'width':width,'height':900})
        page.goto(app['base'],wait_until='networkidle')
        page.fill('#search-input','test adventure')
        page.press('#search-input','Enter')
        page.wait_for_selector('.book-card')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        if width<=640: page.get_by_role("button",name="Navigation menu").click()
        page.locator('[data-action="switchTab"][data-arg="settings"]').click()
        page.wait_for_selector('#quality-profiles-list [data-qp]')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert page.locator('#setting-scheduler_min_score').is_visible()
        if width==390: page.screenshot(path='/tmp/librarr-react-settings.png',full_page=True)
