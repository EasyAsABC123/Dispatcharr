import copy
import json
import unittest
from urllib.parse import urlsplit

import probes


ACCOUNTS = {
    'admin': {'id': 1, 'username': 'rust-probe-admin', 'access': 'secret-admin-access'},
    'viewer': {'id': 2, 'username': 'rust-probe-viewer', 'access': 'secret-viewer-access'},
}


class FakeHTTP:
    def __init__(self, candidate_noop=False, candidate_exception=False):
        self.users = {
            1: dict(id=1, username='rust-probe-admin', first_name='', user_level=10,
                    is_staff=True, is_superuser=True, stream_limit=0,
                    custom_properties={}, api_key='secret-admin-key'),
            2: dict(id=2, username='rust-probe-viewer', first_name='', user_level=0,
                    is_staff=False, is_superuser=False, stream_limit=0,
                    custom_properties={}, api_key='secret-viewer-key'),
        }
        self.calls = []
        self.candidate_noop = candidate_noop
        self.candidate_exception = candidate_exception

    def __call__(self, url, body=None, token=None, method=None, headers=None):
        result = self.respond(url, body, token, method, headers)
        return copy.deepcopy(result)

    def respond(self, url, body, token, method, headers):
        self.calls.append((url, method))
        parsed = urlsplit(url)
        candidate = parsed.port == 2
        actor = None
        for role, account in ACCOUNTS.items():
            if token == account['access']:
                actor = self.users[account['id']]
        key = (headers or {}).get('X-API-Key')
        if key:
            actor = next((user for user in self.users.values() if user['api_key'] == key), None)
            if actor is None:
                return 401, {'detail': 'Invalid API key'}
        if actor is None:
            return 401, {'detail': 'Authentication credentials were not provided.'}
        path = parsed.path
        if path == '/api/accounts/users/me/':
            if method == 'PATCH':
                if candidate and self.candidate_exception:
                    raise RuntimeError('secret-viewer-access secret-viewer-key')
                if candidate and self.candidate_noop:
                    return 200, actor
                if body.get('email') == 'not-an-email':
                    return 400, {'email': ['Enter a valid email address.']}
                if len(body.get('first_name', '')) > 150:
                    return 400, {'first_name': ['Ensure this field has no more than 150 characters.']}
                properties = body.get('custom_properties')
                if properties is not None and not isinstance(properties, dict):
                    return 400, {'custom_properties': ['custom_properties must be a dictionary']}
                if isinstance(properties, dict) and properties.get('hiddenNav') == ['not-a-nav-item']:
                    return 400, {'custom_properties': ["'not-a-nav-item' is not a valid navigation item ID"]}
                for key, value in (properties or {}).items():
                    if key in ('xc_password', 'allowed_networks', 'catchup_enabled',
                               'vod_movies_enabled', 'vod_series_enabled', 'dvr_access',
                               'allowed_m3u_profile_ids'):
                        continue
                    if value is None:
                        actor['custom_properties'].pop(key, None)
                    else:
                        actor['custom_properties'][key] = value
                if 'first_name' in body:
                    actor['first_name'] = body['first_name'].strip()
            return 200, actor
        if path == '/api/accounts/users/2/' and method == 'PATCH':
            if actor['user_level'] < 10:
                return 403, {'detail': 'Not allowed'}
            user = self.users[2]
            if user['custom_properties'] is None:
                user['custom_properties'] = {}
            for key, value in body.get('custom_properties', {}).items():
                if value is None:
                    user['custom_properties'].pop(key, None)
                else:
                    user['custom_properties'][key] = value
            if 'first_name' in body:
                user['first_name'] = body['first_name']
            for field in ('username', 'user_level', 'is_staff', 'is_superuser', 'stream_limit'):
                if field in body:
                    user[field] = body[field]
            return 200, user
        if path == '/api/accounts/api-keys/':
            return 200, {'key': actor['api_key']}
        if path.startswith('/api/accounts/api-keys/'):
            generate = path.endswith('/generate/')
            target = actor
            if body.get('user_id'):
                if actor['user_level'] < 10:
                    return 403, {'detail': ('Not allowed to create keys for other users.' if generate
                                            else 'Not allowed to revoke keys for other users.')}
                try:
                    target = self.users[int(body['user_id'])]
                except (ValueError, KeyError):
                    return 404, {'detail': 'User not found.'}
            if generate:
                key = ('B' if candidate else 'A') * 54
                target['api_key'] = key
                return 201, {'key': key, 'user': target}
            target['api_key'] = None
            return 200, {'success': True}
        raise AssertionError('Unexpected fake request')


class ProbeTests(unittest.TestCase):
    def run_suite(self, fake, accounts=None):
        records = []
        probes.run(fake, 'http://127.0.0.1:1', 'http://127.0.0.1:2', accounts or ACCOUNTS,
                   lambda name, passed, details: records.append((name, passed, details)))
        return records

    def test_successful_suite_preserves_admin_and_restores_owner_metadata(self):
        fake = FakeHTTP()
        records = self.run_suite(fake)
        self.assertGreater(len(records), 20)
        self.assertTrue(all(row[1] for row in records), records)
        self.assertEqual(fake.users[1]['api_key'], 'secret-admin-key')
        self.assertEqual(fake.users[2]['custom_properties'], {})
        self.assertEqual(fake.users[2]['first_name'], '')
        self.assertEqual(fake.users[2]['user_level'], 0)
        self.assertIsNone(fake.users[2]['api_key'])

    def test_baseline_mutation_cannot_mask_candidate_noop(self):
        records = self.run_suite(FakeHTTP(candidate_noop=True))
        result = next(row for row in records if row[0] == 'accounts:owner-patch-body')
        self.assertFalse(result[1])
        protection = next(row for row in records
                          if row[0] == 'accounts:owner-privilege-protection-and-null-delete')
        self.assertFalse(protection[1])

    def test_legacy_null_is_checked_before_explicit_property_writes_normalize_it(self):
        fake = FakeHTTP()
        fake.users[2]['custom_properties'] = None
        records = self.run_suite(fake)
        self.assertTrue(all(row[1] for row in records), records)
        checked = next(row for row in records
                       if row[0] == 'accounts:omitted-custom-properties-preserved')
        self.assertTrue(checked[2]['original_was_null'])
        self.assertEqual(fake.users[2]['custom_properties'], {})

    def test_recorded_evidence_never_contains_fixture_secrets_or_exception_text(self):
        records = self.run_suite(FakeHTTP(candidate_exception=True))
        rendered = json.dumps(records)
        for secret in ('secret-admin-access', 'secret-viewer-access', 'secret-admin-key',
                       'secret-viewer-key', 'probe-xc-credential', 'A' * 54, 'B' * 54):
            self.assertNotIn(secret, rendered)
        self.assertTrue(any(not row[1] and row[2].get('error_type') == 'RuntimeError'
                            for row in records))

    def test_unsafe_fixture_is_rejected_without_any_requests(self):
        fake = FakeHTTP()
        accounts = copy.deepcopy(ACCOUNTS)
        accounts['viewer']['username'] = 'actual-production-viewer'
        records = self.run_suite(fake, accounts)
        self.assertEqual(fake.calls, [])
        self.assertFalse(records[0][1])
        with self.assertRaises(probes.UnsafeFixture):
            probes.validate_fixture('https://production.example', 'http://127.0.0.1:2', ACCOUNTS)

    def test_only_random_key_fields_are_normalized_without_mutating_input(self):
        value = {'key': 'secret', 'user': {'api_key': 'secret', 'username': 'unchanged',
                                         'custom_properties': {'key': 'keep-me'}}}
        before = copy.deepcopy(value)
        normalized = probes.normalized_key_response(value)
        self.assertEqual(value, before)
        self.assertEqual(normalized['user']['username'], 'unchanged')
        self.assertEqual(normalized['user']['custom_properties']['key'], 'keep-me')
        self.assertEqual(normalized['key'], '<random-api-key>')

    def test_canonical_comparison_distinguishes_boolean_integer_and_float_types(self):
        self.assertNotEqual(probes.digest({'x':True}),probes.digest({'x':1}))
        self.assertNotEqual(probes.digest({'x':1}),probes.digest({'x':1.0}))


if __name__ == '__main__':
    unittest.main()
