"""React security controls: fixture transport, no shared account mutations."""
import json
from urllib.parse import urlsplit


def test_totp_enrollment_retry_cancel_and_disable(app, page):
    origin = app['base']
    requests, writes, errors = [], [], []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.on('request', lambda request: requests.append(request.url))
    state = {'enabled': False, 'reject': True}
    secret = 'JBSWY3DPEHPK3PXP'
    uri = f'otpauth://totp/Librarr:fixture?secret={secret}&issuer=Librarr'
    png = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=='

    def respond(route):
        path = urlsplit(route.request.url).path
        if path == '/api/auth/status':
            data = {'authenticated': True, 'has_users': True, 'user_id': 7,
                    'username': 'fixture', 'role': 'user', 'oidc_enabled': False}
        elif path == '/api/totp/status':
            data = {'enabled': state['enabled']}
        elif path == '/api/totp/setup':
            writes.append((path, None))
            data = {'success': True, 'secret': secret, 'qr_url': uri,
                    'qr_png': png, 'backup_codes': ['fixture-a', 'fixture-b']}
        elif path in ('/api/totp/verify', '/api/totp/disable'):
            writes.append((path, route.request.post_data_json))
            if state['reject']:
                state['reject'] = False
                data = {'success': False, 'error': 'Invalid authentication code'}
            else:
                state['enabled'] = path.endswith('/verify')
                data = {'success': True}
        else:
            route.fallback()
            return
        route.fulfill(content_type='application/json', body=json.dumps(data))

    page.route(f'{origin}/api/**', respond)
    page.goto(origin, wait_until='networkidle')
    page.locator('[data-action="switchTab"][data-arg="settings"]').click()
    page.get_by_role('button', name='Enable 2FA', exact=True).click()
    assert page.locator('#totp-qr-img').get_attribute('src') == png
    assert page.locator('#totp-secret-display').inner_text() == secret
    assert page.locator('#totp-otpauth-uri').inner_text() == uri
    assert 'fixture-b' in page.locator('#totp-backup-codes').inner_text()
    page.locator('#totp-verify-code').fill('000000')
    page.get_by_role('button', name='Verify & Enable', exact=True).click()
    page.get_by_role('alert').filter(has_text='Invalid authentication code').wait_for()
    assert page.locator('#totp-secret-display').is_visible()
    page.locator('#totp-verify-code').fill('123456')
    page.get_by_role('button', name='Verify & Enable', exact=True).click()
    page.get_by_role('button', name='Disable 2FA', exact=True).wait_for()
    assert page.locator('#totp-secret-display').count() == 0
    page.get_by_role('button', name='Disable 2FA', exact=True).click()
    page.get_by_role('button', name='Cancel', exact=True).click()
    assert len(writes) == 3
    page.get_by_role('button', name='Disable 2FA', exact=True).click()
    page.locator('#totp-disable-code').fill('654321')
    page.get_by_role('button', name='Disable two-factor authentication', exact=True).click()
    page.get_by_role('button', name='Enable 2FA', exact=True).wait_for()
    assert writes[-1] == ('/api/totp/disable', {'code': '654321'})
    assert not any(secret in url or 'otpauth' in url for url in requests)
    assert not errors
