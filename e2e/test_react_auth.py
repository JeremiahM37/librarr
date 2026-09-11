"""Browser-level auth coverage for the React UI.

Uses an isolated service: it creates the fixture's first administrator,
then verifies logout, rejected credentials, and session restoration by login.
"""


def test_userless_instance_remains_open_then_logout_and_login(isolated_auth_app, page):
    app = isolated_auth_app
    page.goto(app["base"], wait_until="networkidle")
    assert page.locator("#app").is_visible()
    assert page.evaluate("""() => fetch('/api/register', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({username:'react-admin', password:'correct horse battery staple'})}).then(r => r.json()).then(r => r.success)""")
    page.reload(wait_until="networkidle")
    assert page.locator("#header-username").inner_text() == "react-admin"

    page.locator('[data-action="doLogout"]').click()
    page.wait_for_selector("#login-form")
    page.fill("#login-username", "react-admin")
    page.fill("#login-password", "wrong password")
    page.locator("#login-form button[type=submit]").click()
    page.wait_for_timeout(250)
    assert "Invalid credentials" in page.locator("#toast-container").inner_text()

    page.fill("#login-password", "correct horse battery staple")
    page.locator("#login-form button[type=submit]").click()
    page.wait_for_selector("#app")
    assert page.locator("#header-role").inner_text() == "admin"


def test_failed_boot_recovers_and_totp_challenge_uses_pending_token(app,page):
    state={'available':False,'authenticated':False}
    writes=[]
    def status(route):
        if not state['available']:
            route.fulfill(status=500,json={'error':'Fixture unavailable'})
        else:
            route.fulfill(json={'authenticated':state['authenticated'],'has_users':True,'oidc_enabled':True,'oidc_provider_name':'Example SSO','username':'reader','role':'user'})
    def login(route):
        writes.append(route.request.post_data_json)
        route.fulfill(json={'success':True,'needs_totp':True,'session_pending':'fixture-pending'})
    def totp(route):
        writes.append(route.request.post_data_json)
        state['authenticated']=True
        route.fulfill(json={'success':True,'backup_code_used':True})
    page.route('**/api/auth/status',status)
    page.route('**/api/login',login)
    page.route('**/api/login/totp',totp)
    page.goto(app['base'],wait_until='networkidle')
    page.get_by_role('alert').filter(has_text='Fixture unavailable').wait_for()
    state['available']=True
    page.get_by_role('button',name='Retry',exact=True).click()
    page.locator('#login-form').wait_for()
    assert page.locator('#oidc-login-link').get_attribute('href')=='/auth/oidc/login'
    page.locator('[data-action="showRegisterForm"]').click()
    assert page.locator('#register-invite-code').is_visible()
    page.locator('[data-action="showLoginForm"]').click()
    page.fill('#login-username','reader')
    page.fill('#login-password','fixture-password')
    page.locator('#login-form button[type=submit]').click()
    page.fill('#totp-code','backup-fixture')
    page.locator('#totp-form button[type=submit]').click()
    page.locator('#app').wait_for()
    assert writes[-1]=={'session_pending':'fixture-pending','code':'backup-fixture'}


def test_saved_api_key_uses_protected_config_identity(app,page):
    page.add_init_script('localStorage.setItem("librarr_apikey","fixture-key")')
    page.route('**/api/auth/status?*',lambda route:route.fulfill(json={'authenticated':False,'has_users':True,'oidc_enabled':False}))
    checked=[]
    def config(route):
        checked.append(route.request.url)
        route.fulfill(json={'current_user':'api','current_role':'admin'})
    page.route('**/api/config?*',config)
    page.goto(app['base'],wait_until='networkidle')
    page.locator('#app').wait_for()
    assert page.locator('#header-username').inner_text()=='api'
    assert page.locator('#header-role').inner_text()=='admin'
    assert checked and 'apikey=fixture-key' in checked[0]
