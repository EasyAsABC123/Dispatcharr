"""Public-only corruption controls; synthetic parser fixtures are not Source evidence."""
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import contract


class Contracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.spec = contract.load()
        names = list(self.spec['fixture_code_sha256']) + [
            'scripts/rust-contracts/provider_gets/contracts.json',
            'scripts/rust-contracts/case-bindings.json',
            'scripts/rust-contracts/datasets/image44-scoped-cases.json']
        for name in names:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(contract.ROOT / name, p)

    def reject_spec(self, mutate):
        p = self.root / 'scripts/rust-contracts/provider_gets/contracts.json'
        value = json.loads(p.read_text())
        mutate(value)
        p.write_text(json.dumps(value))
        with self.assertRaises((AssertionError, KeyError, StopIteration)):
            contract.load(self.root)

    def synthetic(self):
        records = [{} for _ in range(150)]
        for item in self.spec['contracts']:
            for case in item['cases']:
                headers = copy.deepcopy(case['static_selected_headers'])
                body = '[]' if case['status'] == 200 else '{}'
                if not headers['Content-Type'].startswith('application/json'):
                    body = '<html>fixture</html>'
                headers['Content-Length'] = str(len(body.encode()))
                response = {'status': case['status'], 'headers': headers, 'body_text': body,
                            'body_bytes': len(body.encode()), 'body_sha256': contract.sha(body.encode())}
                if headers['Content-Type'].startswith('application/json'):
                    response['json'] = json.loads(body)
                records[case['source_index']] = {k: case[k] for k in ['path', 'role', 'method', 'label', 'accept']}
                records[case['source_index']].update(expected=response, observation='actual Original TCP')
        capture = dict(passed=True, mode='http', records=records, owned_metadata_restored=True,
                       fullmicro18table_before={str(i): 'h' for i in range(18)},
                       fullmicro18table_after={str(i): 'h' for i in range(18)}, provider_info_or_refresh_executed=False)
        run = dict(provider_action_cases=150, passed=True, resources_retained=False,
                   cleanup='owned Compose project removed', provider_trap_attempts=0,
                   provider_refresh_rows_unchanged=True)
        return capture, run

    def bad_actual(self, mutate):
        capture, run = self.synthetic()
        mutate(capture, run)
        with self.assertRaises((AssertionError, ValueError)):
            contract.assert_capture(self.spec, capture, run)

    def test_public_fixture_load(self):
        self.assertEqual(len(contract.load(self.root)['contracts']), 3)

    def test_wrong_contract_id(self):
        self.reject_spec(lambda s: s['contracts'][0].update(contract_id='invented:GET'))

    def test_undeclared_HEAD_not_invented(self):
        self.reject_spec(lambda s: s['contracts'][0].update(method='HEAD'))

    def test_wrong_alias(self):
        self.reject_spec(lambda s: s['contracts'][0].update(pattern_exact='api/vod/movies/providers'))

    def test_wrong_role_fixture(self):
        self.reject_spec(lambda s: s['contracts'][0]['setup_fixture']['role_specs']['basic'].update(user_level=10))

    def test_wrong_owner(self):
        self.reject_spec(lambda s: s['contracts'][0].update(owning_milestone='M4'))

    def test_wrong_fixture_command(self):
        self.reject_spec(lambda s: s['contracts'][0]['setup_fixture']['argv'].append('--native-provider-actions'))

    def test_wrong_case_status(self):
        self.reject_spec(lambda s: s['contracts'][0]['cases'][0].update(status=418))

    def test_wrong_historical_tie_authority(self):
        self.reject_spec(lambda s: s['contracts'][0]['cases'][0].update(historical_response_authority='initial_not_tie'))

    def test_no_endpoint_promotion(self):
        self.reject_spec(lambda s: s['contracts'][0].update(fully_verified=True))

    def test_fixture_hash_required(self):
        p = self.root / 'scripts/rust-vod-api-contracts/source.py'
        p.write_bytes(p.read_bytes() + b'\n')
        with self.assertRaisesRegex(AssertionError, 'fixture code'):
            contract.load(self.root)

    def test_synthetic_capture_parser_only(self):
        capture, run = self.synthetic()
        self.assertEqual(contract.assert_capture(self.spec, capture, run)['GET_assertions'], 78)

    def test_actual_wrong_role(self):
        self.bad_actual(lambda c, r: c['records'][0].update(role='anonymous'))

    def test_actual_wrong_status(self):
        self.bad_actual(lambda c, r: c['records'][0]['expected'].update(status=503))

    def test_actual_wrong_body_hash(self):
        self.bad_actual(lambda c, r: c['records'][0]['expected'].update(body_sha256='0' * 64))

    def test_actual_wrong_length(self):
        self.bad_actual(lambda c, r: c['records'][0]['expected']['headers'].update({'Content-Length': '999'}))

    def test_actual_failed_cleanup(self):
        self.bad_actual(lambda c, r: r.update(resources_retained=True))

    def test_actual_table_mutation(self):
        self.bad_actual(lambda c, r: c['fullmicro18table_after'].update({'0': 'changed'}))

    def test_actual_unexpected_refresh(self):
        self.bad_actual(lambda c, r: r.update(provider_trap_attempts=1))


if __name__ == '__main__':
    unittest.main()
