"""Admin User/Group CRUD contracts against disposable real HTTP fixtures.

Each implementation completes a lifecycle before the next creates the same
unique name. Only the created object's top-level ID is normalized. Explicit
date_joined fixtures avoid normalizing otherwise observable user fields.
"""
import copy
import hashlib
import json
import secrets
import time
from urllib.parse import urlsplit
import uuid


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def normalized_created(value, object_id):
    result = copy.deepcopy(value)
    if isinstance(result, dict) and result.get('id') == object_id:
        result['id'] = '<created-object-id>'
    return result


def safe_fixture(baseline, candidate, accounts):
    if baseline.rstrip('/') == candidate.rstrip('/') or accounts['admin']['id'] == accounts['viewer']['id']:
        return False
    for endpoint in (baseline, candidate):
        parsed = urlsplit(endpoint)
        if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
            return False
    return all(isinstance(accounts[name]['id'], int) and not isinstance(accounts[name]['id'], bool)
               and accounts[name]['id'] > 0 and accounts[name]['username'].startswith('rust-probe-')
               and accounts[name]['access'] for name in ('admin', 'viewer'))


def run(http, baseline, candidate, accounts, record):
    try:
        safe = safe_fixture(baseline, candidate, accounts)
    except (KeyError, TypeError, ValueError):
        safe = False
    if not safe:
        record('accounts-crud:fixture-safety', False, {'safe_fixture': False})
        return
    admin, viewer = accounts['admin'], accounts['viewer']
    suffix = uuid.uuid4().hex[:12]
    username = 'rust-probe-crud-' + suffix
    group_name = 'rust-probe-crud-group-' + suffix
    profile_name = 'rust-probe-crud-profile-' + suffix
    password = secrets.token_urlsafe(24)
    changed_password = secrets.token_urlsafe(24)
    profile_id = None

    def invoke(base, path, body=None, token=admin['access'], method=None):
        return http(base + path, body=body, token=token, method=method)

    def capture(name, callback):
        try:
            return callback()
        except Exception as error:
            record('accounts-crud:' + name, False, {'error_type': type(error).__name__})
            return None

    def emit(name, old, new, old_id=None, new_id=None, expected=None):
        old_body = normalized_created(old[1], old_id) if old_id is not None else old[1]
        new_body = normalized_created(new[1], new_id) if new_id is not None else new[1]
        passed = old[0] == new[0] and digest(old_body) == digest(new_body)
        if expected is not None:
            passed = passed and old[0] == expected
        record('accounts-crud:' + name, passed,
               {'python_status': old[0], 'rust_status': new[0],
                'python_body_sha256': digest(old_body), 'rust_body_sha256': digest(new_body),
                'normalization': 'created-root-id-only' if old_id is not None else 'none'})

    def validate_created(response, expected_name, field):
        if (response[0] != 201 or not isinstance(response[1], dict)
                or response[1].get(field) != expected_name
                or not isinstance(response[1].get('id'), int)
                or isinstance(response[1].get('id'), bool) or response[1]['id'] < 1):
            raise AssertionError('Creation did not return the expected disposable object')
        object_id = response[1]['id']
        if field == 'username' and object_id in (admin['id'], viewer['id']):
            raise AssertionError('Creation returned a protected fixture ID')
        path = '/api/accounts/' + ('users/' if field == 'username' else 'groups/') + str(object_id) + '/'
        existing = invoke(baseline, path)
        if existing[0] != 200 or existing[1].get(field) != expected_name:
            raise AssertionError('Reference API did not confirm the created disposable object')
        return object_id

    def cleanup_object(path, field, allowed_prefix):
        existing = invoke(baseline, path)
        if existing[0] == 404:
            return
        if existing[0] != 200 or not existing[1].get(field, '').startswith(allowed_prefix):
            raise AssertionError('Refusing to delete an unverified fixture object')
        deleted = invoke(baseline, path, method='DELETE')
        if deleted[0] != 204:
            raise AssertionError('Reference cleanup did not remove the temporary object')

    def setup_profile():
        result = invoke(baseline, '/api/channels/profiles/',
                        {'name': profile_name, 'start_empty': True}, method='POST')
        if result[0] != 201 or result[1].get('name') != profile_name:
            raise AssertionError('Reference channel-profile fixture setup failed')
        return result[1]['id']

    profile_id = capture('profile-fixture', setup_profile)
    permissions = capture('permission-fixture', lambda: invoke(baseline, '/api/accounts/permissions/'))
    if profile_id is None or permissions is None or permissions[0] != 200 or not permissions[1]:
        record('accounts-crud:required-fixtures', False, {'fixtures_available': False})
        if profile_id is not None:
            capture('profile-cleanup',lambda:cleanup_object('/api/channels/profiles/'+str(profile_id)+'/',
                                                          'name',profile_name))
        return
    permission_id = permissions[1][0]['id']

    def user_lifecycle(label, base):
        results = {}
        object_id = None
        path = None
        payload = {'username': username, 'password': password, 'email': 'rust-probe@example.test',
                   'first_name': ' Created ', 'last_name': ' Fixture ', 'user_level': 1,
                   'stream_limit': 0, 'is_staff': False, 'is_superuser': False,
                   'last_login': None, 'date_joined': '2026-11-09T00:00:00Z',
                   'custom_properties': {'first': 'delete-later', 'xc_password': 'probe-safe'},
                   'avatar_config': {'style': 'square', 'colors': ['#123456']},
                   'channel_profiles': [profile_id], 'api_key': 'ignored-readonly-input',
                   'is_active': False, 'id': 999999}
        try:
            created = invoke(base, '/api/accounts/users/', payload, method='POST')
            results['create'] = created
            object_id = validate_created(created, username, 'username')
            path = '/api/accounts/users/' + str(object_id) + '/'
            record('accounts-crud:user-shape:' + label,
                   'password' not in created[1] and created[1].get('api_key') is None
                   and created[1].get('channel_profiles') == [profile_id],
                   {'status': created[0], 'password_omitted': 'password' not in created[1]})
            results['duplicate-username'] = invoke(base, '/api/accounts/users/', payload, method='POST')
            results['read'] = invoke(base, path)
            results['viewer-denied-update'] = invoke(base, path, {'user_level': 10},
                                                    token=viewer['access'], method='PATCH')
            results['viewer-denied-delete'] = invoke(base, path, token=viewer['access'], method='DELETE')
            results['patch'] = invoke(base, path,
                                     {'username': username + '-patched', 'email': 'updated@example.test',
                                      'password': changed_password, 'avatar_config': None,
                                      'custom_properties': {'first': None, 'second': 2,
                                                            'navOrder': ['guide', 'channels']}}, method='PATCH')
            results['put'] = invoke(base, path,
                                   {'username': username + '-put', 'channel_profiles': [],
                                    'custom_properties': {'second': None, 'third': 'present'}}, method='PUT')
            results['missing-put-username'] = invoke(base, path, {'email': 'valid@example.test'}, method='PUT')
            results['bad-profile-type'] = invoke(base, path, {'channel_profiles': [True]}, method='PATCH')
            results['missing-profile'] = invoke(base, path, {'channel_profiles': [-999999]}, method='PATCH')
            results['bad-credentials-property'] = invoke(base, path,
                                                        {'custom_properties': {'xc_password': 'bad space'}}, method='PATCH')
            results['bad-profile-allowlist'] = invoke(base, path,
                                                     {'custom_properties': {'allowed_m3u_profile_ids': [True]}}, method='PATCH')
            results['relink-before-delete'] = invoke(base, path, {'channel_profiles': [profile_id]}, method='PATCH')
            # Seed a real dependent row via source API; candidate deletion must
            # reproduce Django's collector rather than relying on SQL CASCADE.
            dependent = invoke(baseline, path + 'app-data/org.rust.usercrud/',
                               {'probe': 'cascade'}, method='PUT')
            record('accounts-crud:user-dependent-fixture:' + label, dependent[0] == 200,
                   {'status': dependent[0]})
            if label == 'rust':
                # All field comparisons have completed before login mutates
                # last_login. Tokens/passwords never leave the HTTP callback.
                # Source and candidate now share the same real Redis login
                # budget. Let foundation's attempt expire before these three
                # independent credential checks; never reset/bypass auth state.
                for _ in range(2):time.sleep(31)
                for peer_label, peer_base in [('python', baseline), ('rust', candidate)]:
                    login = invoke(peer_base, '/api/accounts/token/',
                                   {'username': username + '-put', 'password': changed_password},
                                   token=None, method='POST')
                    record('accounts-crud:rust-password-accepted:' + peer_label,
                           login[0] == 200 and set(login[1]) == {'access', 'refresh'},
                           {'status': login[0], 'shared_budget_isolation_wait_seconds':62})
                wrong = invoke(candidate, '/api/accounts/token/',
                               {'username': username + '-put', 'password': password}, token=None, method='POST')
                record('accounts-crud:old-password-rejected', wrong[0] == 401, {'status': wrong[0]})
            results['delete'] = invoke(base, path, method='DELETE')
            results['read-deleted'] = invoke(base, path)
            results['delete-missing'] = invoke(base, path, method='DELETE')
            return object_id, results
        finally:
            if path is not None:
                capture('user-cleanup:' + label,
                        lambda: cleanup_object(path, 'username', username))

    def group_lifecycle(label, base):
        results = {}
        object_id = None
        path = None
        payload = {'name': group_name, 'permissions': [permission_id]}
        try:
            created = invoke(base, '/api/accounts/groups/', payload, method='POST')
            results['create'] = created
            object_id = validate_created(created, group_name, 'name')
            path = '/api/accounts/groups/' + str(object_id) + '/'
            results['duplicate-name'] = invoke(base, '/api/accounts/groups/', payload, method='POST')
            results['read'] = invoke(base, path)
            results['viewer-denied-update'] = invoke(base, path, {'name': group_name + '-forbidden'},
                                                    token=viewer['access'], method='PATCH')
            results['viewer-denied-delete'] = invoke(base, path, token=viewer['access'], method='DELETE')
            results['patch'] = invoke(base, path, {'name': group_name + '-patched',
                                                  'permissions': [permission_id, permission_id]}, method='PATCH')
            results['put'] = invoke(base, path, {'name': group_name + '-put', 'permissions': []}, method='PUT')
            results['missing-put-permissions'] = invoke(base, path, {'name': group_name}, method='PUT')
            results['bad-permission-type'] = invoke(base, path, {'permissions': [True]}, method='PATCH')
            results['missing-permission'] = invoke(base, path, {'permissions': [-999999]}, method='PATCH')
            results['delete'] = invoke(base, path, method='DELETE')
            results['read-deleted'] = invoke(base, path)
            results['delete-missing'] = invoke(base, path, method='DELETE')
            return object_id, results
        finally:
            if path is not None:
                capture('group-cleanup:' + label, lambda: cleanup_object(path, 'name', group_name))

    try:
        for kind, callback in [('user', user_lifecycle), ('group', group_lifecycle)]:
            python = capture(kind + '-lifecycle:python', lambda: callback('python', baseline))
            rust = capture(kind + '-lifecycle:rust', lambda: callback('rust', candidate))
            if python is None or rust is None:
                continue
            old_id, old_results = python
            new_id, new_results = rust
            for step in sorted(set(old_results) | set(new_results)):
                if step not in old_results or step not in new_results:
                    record('accounts-crud:' + kind + ':' + step, False, {'missing_result': True})
                    continue
                expected = (201 if step == 'create' else 204 if step == 'delete'
                            else 404 if step in ('read-deleted', 'delete-missing')
                            else 403 if step.startswith('viewer-denied-')
                            else 400 if step.startswith(('duplicate-', 'bad-', 'missing-')) else 200)
                emit(kind + ':' + step, old_results[step], new_results[step],
                     old_id, new_id, expected)
        for kind, path, payload in [('user', '/api/accounts/users/',
                                    {'username': username, 'password': password}),
                                   ('group', '/api/accounts/groups/',
                                    {'name': group_name, 'permissions': []})]:
            for role, token, expected in [('viewer', viewer['access'], 403), ('anonymous', None, 401)]:
                old = invoke(baseline, path, payload, token=token, method='POST')
                new = invoke(candidate, path, payload, token=token, method='POST')
                emit(kind + '-create-denied:' + role, old, new, expected=expected)
    finally:
        def cleanup_profile():
            path = '/api/channels/profiles/' + str(profile_id) + '/'
            response = invoke(baseline, path)
            if response[0] == 404:
                return
            if response[0] != 200 or response[1].get('name') != profile_name:
                raise AssertionError('Refusing to delete an unverified profile fixture')
            if invoke(baseline, path, method='DELETE')[0] != 204:
                raise AssertionError('Profile fixture cleanup failed')
        capture('profile-cleanup', cleanup_profile)
