"""Real HTTP account mutation probes for disposable rust-probe-* fixtures only.

Random keys are normalized for comparisons and never emitted to the recorder.
Baseline/candidate share a fixture DB: reset idempotent test state before each
implementation so baseline mutations cannot mask a candidate no-op.
"""
import copy
import hashlib
import json
import importlib.util
from pathlib import Path
import re
from urllib.parse import urlsplit


class UnsafeFixture(ValueError):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(',', ':')).encode()).hexdigest()


def normalized_key_response(value):
    result = copy.deepcopy(value)
    if isinstance(result, dict):
        if 'key' in result:
            result['key'] = '<random-api-key>'
        if isinstance(result.get('user'), dict) and 'api_key' in result['user']:
            result['user']['api_key'] = '<random-api-key>'
    return result


def evidence(old, new, normalize=lambda value: value):
    return {'python_status': old[0], 'rust_status': new[0],
            'python_body_sha256': digest(normalize(old[1])),
            'rust_body_sha256': digest(normalize(new[1]))}


def validate_fixture(baseline, candidate, accounts):
    if baseline.rstrip('/') == candidate.rstrip('/'):
        raise UnsafeFixture('Reference and candidate must be distinct fixture endpoints')
    for url in (baseline, candidate):
        parsed = urlsplit(url)
        if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
            raise UnsafeFixture('Only loopback HTTP fixture endpoints are allowed')
    for name in ('admin', 'viewer'):
        account = accounts[name]
        if (not isinstance(account['id'], int) or isinstance(account['id'], bool)
                or account['id'] < 1 or not account['username'].startswith('rust-probe-')
                or not account['access']):
            raise UnsafeFixture('Expected disposable probe accounts')
    if accounts['admin']['id'] == accounts['viewer']['id']:
        raise UnsafeFixture('Admin and viewer must be separate fixture users')


def run(http, baseline, candidate, accounts, record):
    """Call root's extended HTTP helper and append redacted contract evidence.

    http(url, body=None, token=None, method=None, headers=None) -> (status, JSON)
    record(name, passed, details) stores only status/scalar/hash diagnostics.
    All mutation requests target supplied disposable users. Returns no secrets.
    """
    try:
        validate_fixture(baseline, candidate, accounts)
    except (KeyError, TypeError, ValueError) as error:
        record('accounts:fixture-safety', False, {'error_type': type(error).__name__})
        return
    admin, viewer = accounts['admin'], accounts['viewer']
    me_path = '/api/accounts/users/me/'
    keys_path = '/api/accounts/api-keys/'
    target_path = '/api/accounts/users/' + str(viewer['id']) + '/'

    def invoke(base, path, body=None, *, token=None, method=None, headers=None):
        return http(base + path, body=body, token=token, method=method, headers=headers)

    def capture(name, callback):
        try:
            return callback()
        except Exception as error:
            # Exception strings may contain credentials or response payloads.
            record('accounts:' + name, False, {'error_type': type(error).__name__})
            return None

    snapshot = capture('fixture-user-read', lambda: invoke(baseline, me_path, token=viewer['access']))
    if (snapshot is None or snapshot[0] != 200 or snapshot[1].get('id') != viewer['id']
            or snapshot[1].get('username') != viewer['username']):
        record('accounts:fixture-user-read', False, {'status': snapshot[0] if snapshot else None})
        return
    original = snapshot[1]
    initial_properties = {'probe_remove': 'delete-me', 'xc_password': 'probe-xc-credential',
                          'allowed_networks': {}, 'catchup_enabled': False,
                          'vod_movies_enabled': False, 'vod_series_enabled': False,
                          'dvr_access': 'none', 'allowed_m3u_profile_ids': []}

    def reset(properties, first_name):
        current = invoke(baseline, me_path, token=viewer['access'])
        if current[0] != 200:
            raise AssertionError('Fixture owner became inaccessible')
        existing = current[1].get('custom_properties') or {}
        changes = {key: None for key in existing if key not in properties}
        changes.update(copy.deepcopy(properties))
        response = invoke(baseline, target_path,
                          {'custom_properties': changes, 'first_name': first_name,
                           **{name: original[name] for name in
                              ('username', 'user_level', 'is_staff', 'is_superuser', 'stream_limit')}},
                          token=admin['access'], method='PATCH')
        if response[0] != 200:
            raise AssertionError('Reference fixture reset failed')

    def pair(name, path, body=None, *, token=None, method=None, normalize=lambda value: value,
             before=None, expected=None):
        if before:
            before()
        old = invoke(baseline, path, body, token=token, method=method)
        if before:
            before()
        new = invoke(candidate, path, body, token=token, method=method)
        passed = old[0] == new[0] and digest(normalize(old[1])) == digest(normalize(new[1]))
        if expected is not None:
            passed = passed and old[0] == expected
        record('accounts:' + name, passed, evidence(old, new, normalize))
        return old, new

    try:
        def firstname_only():
            old,new=pair('firstname-only-preserves-null',me_path,{'first_name':' rust-probe-owner '},
                         token=viewer['access'],method='PATCH',expected=200)
            record('accounts:omitted-custom-properties-preserved',
                   old[0]==new[0]==200 and old[1].get('custom_properties')==original.get('custom_properties')
                   and new[1].get('custom_properties')==original.get('custom_properties'),
                   {'python_status':old[0],'rust_status':new[0],
                    'original_was_null':original.get('custom_properties') is None})
        capture('firstname-only',firstname_only)
        def protected_patch():
            payload = {'user_level': 10, 'is_superuser': True, 'is_staff': True,
                       'username': 'not-a-real-admin', 'stream_limit': 999,
                       'first_name': ' rust-probe-owner ',
                       'custom_properties': {'probe_theme': 'dark', 'probe_remove': None,
                                             'xc_password': 'forged', 'allowed_networks': {'UI': '0.0.0.0/0'},
                                             'catchup_enabled': True, 'vod_movies_enabled': True,
                                             'vod_series_enabled': True, 'dvr_access': 'manage',
                                             'allowed_m3u_profile_ids': [999999]}}
            results = pair('owner-patch-body', me_path, payload, token=viewer['access'],
                           method='PATCH', before=lambda: reset(initial_properties, ''), expected=200)
            new = results[1]
            safe = (new[0] == 200 and new[1]['id'] == viewer['id']
                    and new[1]['username'] == viewer['username'] and new[1]['user_level'] == 0
                    and not new[1]['is_staff'] and not new[1]['is_superuser']
                    and new[1]['stream_limit'] == 0 and 'password' not in new[1]
                    and new[1]['custom_properties'].get('probe_theme') == 'dark'
                    and 'probe_remove' not in new[1]['custom_properties']
                    and all(new[1]['custom_properties'].get(key) == value
                            for key, value in initial_properties.items() if key != 'probe_remove'))
            record('accounts:owner-privilege-protection-and-null-delete', safe,
                   {'rust_status': new[0]})

        capture('owner-patch', protected_patch)
        for name, body in [
            ('null-properties-merge', {'custom_properties': None}),
            ('invalid-nav', {'custom_properties': {'hiddenNav': ['not-a-nav-item']}}),
            ('invalid-properties-type', {'custom_properties': []}),
            ('invalid-email', {'email': 'not-an-email'}),
            ('oversized-name', {'first_name': 'x' * 151}),
        ]:
            capture(name, lambda name=name, body=body: pair(
                name, me_path, body, token=viewer['access'], method='PATCH',
                before=lambda: reset(initial_properties, ''),
                expected=200 if name == 'null-properties-merge' else 400))
    finally:
        capture('restore-owner-fixture', lambda: reset(original.get('custom_properties') or {},
                                                       original['first_name']))

    def key_lifecycle():
        responses = []
        for label, base in [('python', baseline), ('rust', candidate)]:
            generated = invoke(base, keys_path + 'generate/', {}, token=viewer['access'], method='POST')
            key = generated[1].get('key') if generated[0] == 201 else None
            shape = (isinstance(key, str) and bool(re.fullmatch(r'[A-Za-z0-9_-]{54}', key))
                     and generated[1].get('user', {}).get('api_key') == key
                     and 'password' not in generated[1].get('user', {}))
            record('accounts:key-generation-shape:' + label, shape, {'status': generated[0]})
            responses.append(generated)
            if not shape:
                continue
            for peer_label, peer_base in [('python', baseline), ('rust', candidate)]:
                authenticated = invoke(peer_base, me_path, headers={'X-API-Key': key})
                record('accounts:key-cross-auth:' + label + ':' + peer_label,
                       authenticated[0] == 200 and authenticated[1].get('id') == viewer['id'],
                       {'status': authenticated[0]})
            revoked = invoke(base, keys_path + 'revoke/', {}, token=viewer['access'], method='POST')
            record('accounts:key-revoke:' + label, revoked[0] == 200 and digest(revoked[1]) == digest({'success': True}),
                   {'status': revoked[0], 'body_sha256': digest(revoked[1])})
            for peer_label, peer_base in [('python', baseline), ('rust', candidate)]:
                rejected = invoke(peer_base, me_path, headers={'X-API-Key': key})
                record('accounts:revoked-key-rejected:' + label + ':' + peer_label,
                       rejected[0] == 401, {'status': rejected[0]})
        if len(responses) == 2:
            old, new = responses
            record('accounts:key-generation-response-parity',
                   old[0] == new[0] == 201 and digest(normalized_key_response(old[1])) == digest(normalized_key_response(new[1])),
                   evidence(old, new, normalized_key_response))

    capture('key-lifecycle', key_lifecycle)

    def denied_targets():
        before = invoke(baseline, keys_path, token=admin['access'])
        for action in ('generate', 'revoke'):
            for name, body, token, expected in [
                ('other-user', {'user_id': admin['id']}, viewer['access'], 403),
                ('explicit-self', {'user_id': viewer['id']}, viewer['access'], 403),
                ('malformed-user-id', {'user_id': 'not-a-number'}, admin['access'], 404),
                ('anonymous', {}, None, 401),
            ]:
                pair('key-' + action + '-' + name, keys_path + action + '/', body,
                     token=token, method='POST', expected=expected)
        after = invoke(baseline, keys_path, token=admin['access'])
        record('accounts:denied-key-actions-preserve-other-user', before == after,
               {'before_sha256': digest(before), 'after_sha256': digest(after)})

    capture('denied-key-targets', denied_targets)


def run_crud(http, baseline, candidate, accounts, record):
    """Load the independent admin CRUD lifecycle suite without sys.path changes."""
    spec=importlib.util.spec_from_file_location('dispatcharr_account_crud_probes',
                                               Path(__file__).with_name('crud_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_app_data(http, baseline, candidate, accounts, record):
    """Load the independent app-data quota/ownership lifecycle suite."""
    spec=importlib.util.spec_from_file_location('dispatcharr_app_data_probes',
                                               Path(__file__).with_name('app_data_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_core(http, baseline, candidate, accounts, record):
    """Load source-vs-candidate core settings/profile read contracts."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_core_read_probes',
                                               Path(__file__).with_name('core_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)
    spec=importlib.util.spec_from_file_location('dispatcharr_startup_probes',Path(__file__).with_name('startup_probes.py'))
    startup=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(startup)
    startup.run(http,baseline,candidate,accounts,record)


def run_profiles(http, baseline, candidate, accounts, record):
    """Load transactional channel-profile CRUD and duplicate oracles."""
    validate_fixture(baseline,candidate,accounts)
    validate_fixture(baseline,candidate,dict(accounts,viewer=accounts['standard']))
    spec=importlib.util.spec_from_file_location('dispatcharr_profile_probes',
                                               Path(__file__).with_name('profile_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_memberships(http, baseline, candidate, accounts, record):
    """Load owner single/admin bulk profile-membership mutation contracts."""
    validate_fixture(baseline,candidate,accounts)
    validate_fixture(baseline,candidate,dict(accounts,viewer=accounts['standard']))
    spec=importlib.util.spec_from_file_location('dispatcharr_membership_probes',
                                               Path(__file__).with_name('membership_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_channel_reads(http, baseline, candidate, accounts, record):
    """Load real channel serialization/filter/access/page contracts."""
    validate_fixture(baseline,candidate,accounts)
    validate_fixture(baseline,candidate,dict(accounts,viewer=accounts['standard']))
    spec=importlib.util.spec_from_file_location('dispatcharr_channel_read_probes',
                                               Path(__file__).with_name('channel_read_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_channel_crud(http, baseline, candidate, accounts, record):
    """Load transactional channel scalar/override/stream/profile CRUD oracles."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_channel_crud_probes',
                                               Path(__file__).with_name('channel_crud_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_groups(http, baseline, candidate, accounts, record):
    """Load manual/provider channel-group CRUD and ownership contracts."""
    validate_fixture(baseline,candidate,accounts)
    validate_fixture(baseline,candidate,dict(accounts,viewer=accounts['standard']))
    spec=importlib.util.spec_from_file_location('dispatcharr_group_probes',
                                               Path(__file__).with_name('group_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_channel_stops(http, baseline, candidate, accounts, record, seed):
    """Load real native media active/inactive stop-before-delete contracts."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_channel_stop_probes',
                                               Path(__file__).with_name('channel_stop_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record,seed)


def run_group_cleanup(http, baseline, candidate, accounts, record):
    """Load isolated global-cleanup/FK effects with unrelated rows protected."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_group_cleanup_probes',
                                               Path(__file__).with_name('group_cleanup_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_streams(http, baseline, candidate, accounts, record):
    """Load source stream read/pagination/provider-lock/CRUD/rollup contracts."""
    validate_fixture(baseline,candidate,accounts)
    validate_fixture(baseline,candidate,dict(accounts,viewer=accounts['standard']))
    spec=importlib.util.spec_from_file_location('dispatcharr_stream_probes',
                                               Path(__file__).with_name('stream_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_proxy_auth(http,baseline,candidate,accounts,record):
    """Requires root runner's separately selected trusted-proxy fixture."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_proxy_auth_probes',
                                               Path(__file__).with_name('proxy_auth_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_provider_reads(http,baseline,candidate,accounts,record):
    """Original provider/EPG management read contracts, no import writes."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_provider_read_probes',
                                               Path(__file__).with_name('provider_read_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)


def run_server_group_crud(http,baseline,candidate,accounts,record,fixture):
    """No-job ServerGroup writes plus original collector SET_NULL semantics."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_server_group_probes',
                                               Path(__file__).with_name('server_group_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record,fixture)


def run_filter_crud(http,baseline,candidate,accounts,record):
    """Scoped M3UFilter writes, preserving source serializer regex behavior."""
    validate_fixture(baseline,candidate,accounts)
    spec=importlib.util.spec_from_file_location('dispatcharr_filter_crud_probes',
                                               Path(__file__).with_name('filter_crud_probes.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run(http,baseline,candidate,accounts,record)
