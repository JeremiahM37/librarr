"""Streaming search and download contracts through the actual React UI."""
import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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



PNG='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=='


def cover_row(i,**extra):
    return {'title':f'Cover Probe {i}','author':f'Author {i}','source':'prowlarr','source_id':f'cover-{i}',
            'download_url':f'https://example.invalid/dl/{i}',**extra}


def sse(results,event='results'):
    return f'event: {event}\ndata: '+json.dumps({'results':results})+'\n\n'


class Lookups:
    """Records /api/covers/lookup bodies; optionally holds them until released."""
    def __init__(self,page,hold=False,status=200):
        self.bodies=[]
        self.held=[]
        self.hold=hold
        self.status=status
        page.route('**/api/covers/lookup**',self.handle)

    def handle(self,route):
        body=route.request.post_data_json
        self.bodies.append(body)
        if self.hold:
            self.held.append((route,body))
            return
        self.answer(route,body)

    def answer(self,route,body):
        if self.status!=200:
            route.fulfill(status=self.status,json={'error':'boom'})
            return
        route.fulfill(json={'covers':{item['id']:PNG for item in body['items']}})

    def release(self):
        held,self.held=self.held,[]
        for route,body in held: self.answer(route,body)

    def titles(self,n):
        return [item['title'] for item in self.bodies[n]['items']]


class Stream:
    """A local SSE server whose events the test pushes one at a time, so a
    results event can arrive after a lookup has already completed."""
    def __init__(self,page):
        self.queues=[]
        stream=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*a): pass
            def do_GET(self):
                q=queue.Queue()
                stream.queues.append(q)
                self.send_response(200)
                self.send_header('Content-Type','text/event-stream')
                self.send_header('Access-Control-Allow-Origin','*')
                self.end_headers()
                while True:
                    chunk=q.get()
                    if chunk is None: return
                    self.wfile.write(chunk.encode()); self.wfile.flush()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=True
        threading.Thread(target=self.server.serve_forever,daemon=True).start()
        self.url=f'http://127.0.0.1:{self.server.server_address[1]}/stream'
        page.route('**/api/search**',lambda route:route.continue_(url=self.url))

    def wait_open(self,n=1):
        deadline=time.time()+5
        while len(self.queues)<n and time.time()<deadline: time.sleep(0.02)
        assert len(self.queues)>=n

    def send(self,results,event='results',conn=-1):
        self.queues[conn].put(sse(results,event))

    def close(self):
        for q in self.queues: q.put(None)
        self.server.shutdown()


def fulfilled_search(page,results):
    page.route('**/api/search**',lambda route:route.fulfill(content_type='text/event-stream',body=sse(results)+sse(results,'complete')))


def wait_until(page,cond,timeout=5):
    """Poll while pumping Playwright's event loop (time.sleep would not run route handlers)."""
    deadline=time.time()+timeout
    while not cond() and time.time()<deadline: page.wait_for_timeout(50)


def search_for(page,q='probe'):
    page.fill('#search-input',q)
    page.press('#search-input','Enter')


def test_results_render_before_covers_and_fill_in(app,page):
    lookups=Lookups(page,hold=True)
    fulfilled_search(page,[cover_row(1)])
    page.goto(app['base'],wait_until='networkidle')
    search_for(page)
    card=page.locator('.book-card')
    expect(card.locator('h3')).to_be_visible()
    expect(card.locator('.cover-placeholder')).to_be_visible()
    expect(card.locator('img')).to_have_count(0)
    page.wait_for_timeout(600)
    assert len(lookups.held)==1
    lookups.release()
    expect(card.locator('img')).to_have_attribute('src',PNG)


def test_looked_up_cover_survives_stream_update(app,page):
    lookups=Lookups(page)
    stream=Stream(page)
    try:
        page.goto(app['base'],wait_until='networkidle')
        search_for(page)
        stream.wait_open()
        stream.send([cover_row(1)])
        expect(page.locator('.book-card img')).to_have_attribute('src',PNG)
        stream.send([cover_row(1)],'complete')
        page.wait_for_timeout(700)
        expect(page.locator('.book-card img')).to_have_attribute('src',PNG)
        assert len(lookups.bodies)==1
    finally:
        stream.close()


def test_new_results_in_stream_are_looked_up_once(app,page):
    lookups=Lookups(page)
    stream=Stream(page)
    try:
        page.goto(app['base'],wait_until='networkidle')
        search_for(page)
        stream.wait_open()
        stream.send([cover_row(1)])
        expect(page.locator('.book-card img')).to_have_count(1)
        stream.send([cover_row(1),cover_row(2)],'complete')
        expect(page.locator('.book-card img')).to_have_count(2)
        page.wait_for_timeout(700)
        assert len(lookups.bodies)==2
        assert lookups.titles(0)==['Cover Probe 1']
        assert lookups.titles(1)==['Cover Probe 2']
    finally:
        stream.close()


def test_source_cover_wins_and_is_not_looked_up(app,page):
    lookups=Lookups(page)
    fulfilled_search(page,[cover_row(1,cover_url=PNG),cover_row(2)])
    page.goto(app['base'],wait_until='networkidle')
    search_for(page)
    expect(page.locator('.book-card img')).to_have_count(2)
    assert len(lookups.bodies)==1
    assert lookups.titles(0)==['Cover Probe 2']


STALE_URL=PNG+'#stale'
FRESH_URL=PNG+'#fresh'


def same_row_searches(page):
    """Every search returns one row, so both searches' covers share a keyOf."""
    fulfilled_search(page,[cover_row(1)])
    routes=[]
    page.route('**/api/covers/lookup**',lambda route:routes.append(route))
    return routes


def answer_lookup(route,url):
    route.fulfill(json={'covers':{item['id']:url for item in route.request.post_data_json['items']}})


def test_new_search_cancels_cover_requests(app,page):
    routes=same_row_searches(page)
    aborted=[]
    page.on('requestfailed',lambda request:aborted.append(request) if '/api/covers/lookup' in request.url else None)
    card=page.locator('.book-card')
    page.goto(app['base'],wait_until='networkidle')
    search_for(page,'first')
    wait_until(page,lambda:len(routes)==1)
    assert len(routes)==1
    search_for(page,'second')
    # The first lookup holds the queue until it is aborted, so a second one
    # arriving at all shows the abort worked.
    wait_until(page,lambda:len(routes)==2)
    assert len(routes)==2
    assert aborted==[routes[0].request]
    answer_lookup(routes[1],FRESH_URL)
    expect(card.locator('img')).to_have_attribute('src',FRESH_URL)
    # The browser has dropped this request, so the answer must change nothing.
    answer_lookup(routes[0],STALE_URL)
    page.wait_for_timeout(300)
    expect(card.locator('img')).to_have_attribute('src',FRESH_URL)
    expect(card).to_have_count(1)


def test_stale_cover_answer_is_ignored_without_an_abort(app,page):
    """The abort can lose the race with an answer already on its way. Fetch is
    made to ignore it here, which leaves only the generation check."""
    page.add_init_script("""
      const realFetch=window.fetch;
      window.fetch=(url,init={})=>{
        if(String(url).includes('/api/covers/lookup')){const {signal,...rest}=init;init=rest}
        return realFetch(url,init);
      };
    """)
    routes=same_row_searches(page)
    card=page.locator('.book-card')
    page.goto(app['base'],wait_until='networkidle')
    search_for(page,'first')
    wait_until(page,lambda:len(routes)==1)
    assert len(routes)==1
    search_for(page,'second')
    expect(card.locator('.cover-placeholder')).to_be_visible()
    page.wait_for_timeout(500)
    answer_lookup(routes[0],STALE_URL)
    # Once answered, the first lookup frees the queue for the new search's own.
    wait_until(page,lambda:len(routes)==2)
    assert len(routes)==2
    expect(card.locator('img')).to_have_count(0)
    answer_lookup(routes[1],FRESH_URL)
    expect(card.locator('img')).to_have_attribute('src',FRESH_URL)


def test_manga_tab_sends_no_lookups(app,page):
    lookups=Lookups(page)
    fulfilled_search(page,[cover_row(1,media_type='manga')])
    page.goto(app['base'],wait_until='networkidle')
    page.locator('[data-action="switchSearchTab"][data-arg="manga"]').click()
    search_for(page)
    expect(page.locator('.book-card h3')).to_have_text('Cover Probe 1')
    page.wait_for_timeout(800)
    assert lookups.bodies==[]
    expect(page.locator('.book-card .cover-placeholder')).to_be_visible()


def test_lookup_failure_keeps_placeholder(app,page):
    errors=[]
    page.on('pageerror',lambda e:errors.append(str(e)))
    lookups=Lookups(page,status=500)
    fulfilled_search(page,[cover_row(1)])
    page.goto(app['base'],wait_until='networkidle')
    search_for(page)
    expect(page.locator('.book-card h3')).to_be_visible()
    page.wait_for_timeout(800)
    assert len(lookups.bodies)==1
    expect(page.locator('.book-card .cover-placeholder')).to_be_visible()
    expect(page.locator('.book-card h3')).to_be_visible()
    expect(page.locator('[role="alert"]')).to_have_count(0)
    assert not errors


def test_late_url_replaces_failed_image(app,page):
    page.route('https://example.invalid/bad.png',lambda route:route.abort())
    stream=Stream(page)
    try:
        # the lookup answers with a URL that cannot load
        seen=[]
        def bad(route):
            body=route.request.post_data_json
            seen.append(body)
            route.fulfill(json={'covers':{item['id']:'https://example.invalid/bad.png' for item in body['items']}})
        page.route('**/api/covers/lookup**',bad)
        page.goto(app['base'],wait_until='networkidle')
        search_for(page)
        stream.wait_open()
        stream.send([cover_row(1)])
        wait_until(page,lambda:seen)
        assert seen
        page.wait_for_timeout(300)
        expect(page.locator('.book-card .cover-placeholder')).to_be_visible()
        stream.send([cover_row(1,cover_url=PNG)],'complete')
        expect(page.locator('.book-card img')).to_have_attribute('src',PNG)
    finally:
        stream.close()


def test_large_result_set_batches_lookups(app,page):
    lookups=Lookups(page,hold=True)
    fulfilled_search(page,[cover_row(i) for i in range(13)])
    page.goto(app['base'],wait_until='networkidle')
    search_for(page)
    expect(page.locator('.book-card')).to_have_count(13)
    page.wait_for_timeout(800)
    # Small batches, sent one at a time from the top, so the first covers
    # arrive while Open Library is still being paced for the rest.
    assert [len(b['items']) for b in lookups.bodies]==[5]
    assert lookups.titles(0)==[f'Cover Probe {i}' for i in range(5)]
    lookups.release()
    wait_until(page,lambda:len(lookups.bodies)>=2)
    page.wait_for_timeout(500)
    assert [len(b['items']) for b in lookups.bodies]==[5,5]
    lookups.release()
    wait_until(page,lambda:len(lookups.bodies)>=3)
    assert [len(b['items']) for b in lookups.bodies]==[5,5,3]
    lookups.release()
    expect(page.locator('.book-card img')).to_have_count(13)


def test_lookups_follow_the_displayed_order(app,page):
    lookups=Lookups(page)
    fulfilled_search(page,[cover_row(0)])
    page.goto(app['base'],wait_until='networkidle')
    search_for(page)
    expect(page.locator('.book-card img')).to_have_count(1)
    page.click('[data-action="setSortMode"][data-arg="seeders"]')
    page.unroute('**/api/search**')
    fulfilled_search(page,[cover_row(i,seeders=i) for i in range(1,8)])
    search_for(page,'other')
    wait_until(page,lambda:len(lookups.bodies)>=3)
    assert lookups.titles(1)==[f'Cover Probe {i}' for i in (7,6,5,4,3)]
    assert lookups.titles(2)==['Cover Probe 2','Cover Probe 1']


def test_overlong_title_does_not_block_other_lookups(app,page):
    lookups=Lookups(page)
    long_title='\u00e9'*151  # 302 bytes but only 151 characters
    rows=[cover_row(1),cover_row(2),cover_row(3)]
    rows[1]['title']=long_title
    fulfilled_search(page,rows)
    page.goto(app['base'],wait_until='networkidle')
    search_for(page)
    expect(page.locator('.book-card')).to_have_count(3)
    expect(page.locator('.book-card img')).to_have_count(2)
    assert len(lookups.bodies)==1
    assert lookups.titles(0)==['Cover Probe 1','Cover Probe 3']
    expect(page.locator('.book-card',has_text=long_title).locator('.cover-placeholder')).to_be_visible()
