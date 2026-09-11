"""Exercise account administration without mutating the shared test database."""
import json
from urllib.parse import urlsplit
from playwright.sync_api import expect


def test_account_password_users_and_invites(app, page):
    state = {'users': [{'id': 7, 'username': 'fixture', 'role': 'admin', 'totp_enabled': True}],
             'invites': [], 'reject_password': True}
    writes, errors = [], []
    page.on('pageerror', lambda error: errors.append(str(error)))

    def respond(route):
        path, method = urlsplit(route.request.url).path, route.request.method
        body = route.request.post_data_json if route.request.post_data else None
        if method != 'GET':
            writes.append((method, path, body))
        if path == '/api/auth/status':
            result = {'authenticated': True, 'has_users': True, 'user_id': 7,
                      'username': 'fixture', 'role': 'admin', 'oidc_enabled': False}
        elif path == '/api/me/password':
            result = {'success': not state['reject_password'], 'error': 'Current password is incorrect'}
            state['reject_password'] = False
        elif path == '/api/users':
            result = {'success': True, 'users': state['users']}
        elif path == '/api/register':
            state['users'].append({'id': 8, 'username': body['username'], 'role': 'user', 'totp_enabled': False})
            result = {'success': True}
        elif path == '/api/users/8':
            if method == 'PATCH':
                state['users'][1]['role'] = body['role']
            else:
                state['users'] = state['users'][:1]
            result = {'success': True}
        elif path == '/api/invites':
            if method == 'POST':
                state['invites'].append({'id': 4, 'code': 'fixture-invite', 'role': body['role'],
                                         'max_uses': body['max_uses'], 'uses': 0, 'expires_at': 1900000000})
            result = {'success': True, 'invites': state['invites']}
        elif path == '/api/invites/4':
            state['invites'] = []
            result = {'success': True}
        elif path == '/api/quality-profiles':
            result = []
        elif path == '/api/quality-profiles/formats':
            result = {'formats': {}}
        elif path == '/api/authors':
            result = {'authors': []}
        elif path == '/api/settings':
            result = {}
        elif path == '/api/totp/status':
            result = {'enabled': True}
        else:
            route.fallback()
            return
        route.fulfill(content_type='application/json', body=json.dumps(result))

    page.route(f"{app['base']}/api/**", respond)
    page.goto(app['base'], wait_until='networkidle')
    page.locator('[data-action="switchTab"][data-arg="settings"]').click()
    page.locator('#cp-current').fill('old-password')
    page.locator('#cp-new').fill('new-password')
    page.locator('#cp-confirm').fill('different-password')
    page.locator('#change-password-form button').click()
    expect(page.locator('#cp-error')).to_have_text('New password and confirmation do not match')
    assert not writes
    page.locator('#cp-confirm').fill('new-password')
    page.locator('#change-password-form button').click()
    expect(page.locator('#cp-error')).to_have_text('Current password is incorrect')
    expect(page.locator('#cp-new')).to_have_value('new-password')
    page.locator('#change-password-form button').click()
    expect(page.locator('#cp-new')).to_have_value('')
    assert writes[-1] == ('POST', '/api/me/password', {'current_password': 'old-password', 'new_password': 'new-password'})
    page.locator('#new-user-name').fill('reader <b>literal</b>')
    page.locator('#new-user-pass').fill('reader-password')
    page.get_by_role('button', name='Add user', exact=True).click()
    role = page.get_by_role('combobox', name='Role for reader <b>literal</b>')
    role.wait_for()
    assert page.locator('#users-list b').count() == 0
    role.select_option('admin')
    expect(role).to_have_value('admin')
    page.locator('#invite-max-uses').fill('3')
    page.locator('#invite-expires-days').fill('2')
    page.get_by_role('button', name='Generate invite', exact=True).click()
    expect(page.locator('#invite-codes-list')).to_contain_text('fixture-invite')
    assert ('POST', '/api/invites', {'role': 'user', 'max_uses': 3, 'expires_in': 172800}) in writes
    dismiss = lambda dialog: dialog.dismiss()
    page.on('dialog', dismiss)
    page.get_by_role('button', name='Revoke', exact=True).click()
    assert not any(method == 'DELETE' for method, _, _ in writes)
    page.remove_listener('dialog', dismiss)
    page.on('dialog', lambda dialog: dialog.accept())
    page.get_by_role('button', name='Revoke', exact=True).click()
    expect(page.locator('#invite-codes-list')).to_contain_text('No invite codes yet.')
    page.get_by_role('button', name='Delete reader <b>literal</b>', exact=True).click()
    expect(role).to_have_count(0)
    assert not errors
