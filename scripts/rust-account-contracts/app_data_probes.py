"""Owner/admin app-data storage contracts and quota contention against real APIs."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from urllib.parse import urlsplit

MAX_VALUE = 128 * 1024


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(',', ':')).encode()).hexdigest()


def safe_fixture(baseline, candidate, accounts):
    if baseline.rstrip('/') == candidate.rstrip('/') or accounts['admin']['id'] == accounts['viewer']['id']:
        return False
    for endpoint in (baseline, candidate):
        url = urlsplit(endpoint)
        if url.scheme != 'http' or url.hostname not in ('127.0.0.1', 'localhost', '::1'):
            return False
    for role in ('admin', 'viewer'):
        account = accounts[role]
        if (not isinstance(account['id'], int) or isinstance(account['id'], bool)
                or account['id'] < 1 or not account['username'].startswith('rust-probe-')
                or not account['access']):
            return False
    return True


def run(http, baseline, candidate, accounts, record):
    try:
        safe = safe_fixture(baseline, candidate, accounts)
    except (KeyError, TypeError, ValueError):
        safe = False
    if not safe:
        record('app-data:fixture-safety', False, {'safe_fixture': False})
        return
    admin, viewer = accounts['admin'], accounts['viewer']
    collection = '/api/accounts/users/me/app-data/'
    numeric = '/api/accounts/users/' + str(viewer['id']) + '/app-data/'

    def invoke(base, path, body=None, token=viewer['access'], method=None):
        return http(base + path, body=body, token=token, method=method)

    def capture(name, callback):
        try:
            return callback()
        except Exception as error:
            record('app-data:' + name, False, {'error_type': type(error).__name__})
            return None

    def clear_reference():
        cleared = invoke(baseline, collection, method='DELETE')
        if cleared[0] != 204:
            raise AssertionError('Reference fixture app-data cleanup failed')

    def lifecycle(label, base):
        clear_reference()
        results = {}
        try:
            results['initial-empty-list'] = invoke(base, collection)
            results['missing-key'] = invoke(base, collection + 'org.rust.missing/')
            results['invalid-key'] = invoke(base, collection + 'short/')
            value = {'greeting': 'é🔑', 'number': 1e-6, 'zero': -0.0,
                     'large_integer': 10**30 + 1, 'nested': {'array': [True, None]}}
            results['put-valid-json'] = invoke(base, collection + 'org.rust.settings/', value, method='PUT')
            results['get-valid-json'] = invoke(base, collection + 'org.rust.settings/')
            results['compact-byte-summary'] = invoke(base, collection)
            results['owner-numeric-alias'] = invoke(base, numeric + 'org.rust.settings/')
            results['admin-other-owner-read'] = invoke(base, numeric + 'org.rust.settings/', token=admin['access'])
            results['anonymous-denied'] = invoke(base, collection, token=None)
            results['other-owner-denied'] = invoke(base, '/api/accounts/users/' + str(admin['id']) + '/app-data/')
            results['unknown-target-admin'] = invoke(base, '/api/accounts/users/999999999/app-data/', token=admin['access'])
            results['unknown-target-nonadmin'] = invoke(base, '/api/accounts/users/999999999/app-data/')
            results['reject-array'] = invoke(base, collection + 'org.rust.invalid/', [], method='PUT')
            results['reject-nul-value'] = invoke(base, collection + 'org.rust.invalid/', {'x': '\0'}, method='PUT')
            results['reject-nul-key'] = invoke(base, collection + 'org.rust.invalid/', {'bad\0key': 1}, method='PUT')
            nested = {}
            for _ in range(8):
                nested = {'nested': nested}
            results['reject-depth'] = invoke(base, collection + 'org.rust.invalid/', nested, method='PUT')
            results['reject-value-size'] = invoke(base, collection + 'org.rust.invalid/',
                                                 {'x': 'a' * (MAX_VALUE - 7)}, method='PUT')
            results['reject-body-size'] = invoke(base, collection + 'org.rust.invalid/',
                                                {'x': 'a' * (MAX_VALUE + 1024)}, method='PUT')
            results['delete-key'] = invoke(base, collection + 'org.rust.settings/', method='DELETE')
            results['delete-key-idempotent'] = invoke(base, collection + 'org.rust.settings/', method='DELETE')
            results['after-delete-missing'] = invoke(base, collection + 'org.rust.settings/')
            # Python equality regards bool/int/float values as equal. Source
            # therefore retains the first stored representation and byte_size.
            results['put-bool'] = invoke(base, collection + 'org.rust.numeric/', {'x': True}, method='PUT')
            results['put-equal-number'] = invoke(base, collection + 'org.rust.numeric/', {'x': 1.0}, method='PUT')
            results['numeric-idempotent-get'] = invoke(base, collection + 'org.rust.numeric/')
            results['numeric-idempotent-size'] = invoke(base, collection)
            results['clear-before-total-quota'] = invoke(base, collection, method='DELETE')
            maximum = {'x': 'a' * (MAX_VALUE - 8)}
            fill_status = []
            for index in range(8):
                fill_status.append(invoke(base, collection + f'org.rust.quota{index:02}/',
                                          maximum, method='PUT')[0])
            record('app-data:fill-total-quota:' + label, fill_status == [200] * 8,
                   {'successful_writes': fill_status.count(200), 'attempted_writes': len(fill_status)})
            results['total-quota-summary'] = invoke(base, collection)
            results['reject-total-quota'] = invoke(base, collection + 'org.rust.extra/', {}, method='PUT')
            results['replace-at-total-quota'] = invoke(base, collection + 'org.rust.quota00/', {}, method='PUT')
            results['insert-after-reducing-total'] = invoke(base, collection + 'org.rust.extra/', {}, method='PUT')
            results['clear-before-app-count'] = invoke(base, collection, method='DELETE')
            statuses = [invoke(base, collection + f'org.rust.apps{index:02}/', {}, method='PUT')[0]
                        for index in range(25)]
            record('app-data:fill-app-count:' + label, statuses == [200] * 25,
                   {'successful_writes': statuses.count(200), 'attempted_writes': len(statuses)})
            results['app-count-summary'] = invoke(base, collection)
            results['reject-app-count'] = invoke(base, collection + 'org.rust.apps25/', {}, method='PUT')
            results['replace-at-app-count'] = invoke(base, collection + 'org.rust.apps00/', {'x': 1}, method='PUT')
            results['delete-before-app-insert'] = invoke(base, collection + 'org.rust.apps01/', method='DELETE')
            results['insert-after-app-delete'] = invoke(base, collection + 'org.rust.apps25/', {}, method='PUT')
            results['final-clear'] = invoke(base, collection, method='DELETE')
            results['final-empty-list'] = invoke(base, collection)
            # Run contended quota inserts independently for each implementation.
            # Which key wins is scheduling-dependent, so compare status counts
            # and quota totals rather than normalize away arbitrary key data.
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(invoke, base, collection + f'org.rust.concurrent{index:02}/',
                                           {}, method='PUT') for index in range(26)]
                concurrent = [future.result() for future in futures]
            counts = {str(status): sum(row[0] == status for row in concurrent)
                      for status in sorted({row[0] for row in concurrent})}
            summary = invoke(base, collection)
            valid = (counts == {'200': 25, '409': 1} and summary[0] == 200
                     and len(summary[1].get('apps', [])) == 25 and summary[1].get('total_bytes') == 50)
            record('app-data:concurrent-quota:' + label, valid,
                   {'accepted': counts.get('200', 0), 'rejected': counts.get('409', 0),
                    'stored_apps': len(summary[1].get('apps', [])),
                    'total_bytes': summary[1].get('total_bytes'),
                    'normalization': 'concurrent-key-winners-not-compared'})
            results['post-contention-clear'] = invoke(base, collection, method='DELETE')
            results['post-contention-empty'] = invoke(base, collection)
            return results
        finally:
            capture('fixture-cleanup:' + label, clear_reference)

    old = capture('lifecycle:python', lambda: lifecycle('python', baseline))
    new = capture('lifecycle:rust', lambda: lifecycle('rust', candidate))
    if old is None or new is None:
        return
    expected = {
        'missing-key': 404, 'invalid-key': 400, 'anonymous-denied': 401,
        'other-owner-denied': 403, 'unknown-target-admin': 404, 'unknown-target-nonadmin': 403,
        'reject-array': 400, 'reject-nul-value': 400, 'reject-nul-key': 400, 'reject-depth': 400,
        'reject-value-size': 413, 'reject-body-size': 413, 'after-delete-missing': 404,
        'reject-total-quota': 413, 'reject-app-count': 409,
    }
    for name in sorted(set(old) | set(new)):
        if name not in old or name not in new:
            record('app-data:' + name, False, {'missing_result': True})
            continue
        required = expected.get(name, 204 if name.startswith(('delete-', 'clear-', 'final-clear',
                                                             'post-contention-clear')) else 200)
        record('app-data:' + name, old[name][0] == new[name][0] == required and digest(old[name][1]) == digest(new[name][1]),
               {'python_status': old[name][0], 'rust_status': new[name][0],
                'python_body_sha256': digest(old[name][1]), 'rust_body_sha256': digest(new[name][1])})
