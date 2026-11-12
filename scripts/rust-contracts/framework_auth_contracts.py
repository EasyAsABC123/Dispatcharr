"""Validate exact pinned Original framework-test assignments; no Django imports.
Original Dispatcharr/dependency authors retain application/algorithm credit.
Historical per-test success is separate from new execution and Native acceptance.
"""
import ast
import hashlib
import json
import re
from pathlib import Path

EXPECTED_FILES = [{'original_path': 'apps/accounts/tests/test_accounts.py', 'path': 'scripts/rust-contracts/framework-auth/original/test_accounts.py', 'sha256': 'abb5476c7a08e28c195bea4ee6359d591db6d611f019daeb718e4090b6643ab0'}, {'original_path': 'apps/accounts/tests/test_login_rate_limit_inbox.py', 'path': 'scripts/rust-contracts/framework-auth/original/test_login_rate_limit_inbox.py', 'sha256': 'f651a3cc1ffc6c84f930d80eb0af0d54dd5dc78b734ae243a8a2ca40b7747550'}]

EXPECTED_AUTHORITIES = [{'path': 'scripts/rust-contracts/framework-auth/full-suite-reference.json', 'sha256': '494e9945c39e94c2c05301efed9bba3ba89b5da3b60eb55142f2c701729091c6'}, {'path': 'scripts/rust-contracts/framework-auth/observations.json', 'sha256': 'ab827e9ea2256527ea1c373ba0db59289052ca628fbe71f00c4f87be1b2c2b45'}]

EXPECTED_IDENTITIES = {'route-1486c7d150ac1a2ebe13:METHOD_UNRESOLVED': {'pattern_exact': 'api/accounts/initialize-superuser/', 'source_handler': 'apps.accounts.api_views.initialize_superuser', 'method': None, 'source_action': None, 'owning_milestone': 'M5'}, 'route-f7c174fdedb4d5f479b2:METHOD_UNRESOLVED': {'pattern_exact': 'api/accounts/token/', 'source_handler': 'apps.accounts.api_views.TokenObtainPairView', 'method': None, 'source_action': None, 'owning_milestone': 'M5'}}

EXPECTED_TESTS = {'route-1486c7d150ac1a2ebe13:METHOD_UNRESOLVED': ['apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_true_when_superuser_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_true_when_admin_level_user_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_false_when_no_admin_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_false_when_no_users_exist', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_create_superuser_when_none_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_cannot_create_superuser_when_admin_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_blocked_from_public_ip_by_default', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_get_reports_setup_not_allowed_for_public_ip', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_allowed_from_private_lan', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_allowed_for_env_override_ip', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_blocked_when_env_ip_does_not_match', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_spoofed_x_real_ip_ignored_without_trusted_proxy', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_x_real_ip_used_when_peer_is_trusted_proxy', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_allowed_for_ipv4_mapped_private_address', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_invalid_setup_allowed_ip_env_denies_post'], 'route-f7c174fdedb4d5f479b2:METHOD_UNRESOLVED': ['apps.accounts.tests.test_login_rate_limit_inbox.LoginRateLimitInboxTests.test_jwt_trip_creates_event_and_inbox_once', 'apps.accounts.tests.test_login_rate_limit_inbox.LoginRateLimitInboxTests.test_repeat_429_does_not_parse_username', 'apps.accounts.tests.test_login_rate_limit_inbox.LoginRateLimitInboxTests.test_recorder_error_still_returns_429']}

EXPECTED_ASSERTIONS = ['Execute the exact named unmodified Original test methods with their original setUp, helper and teardown code', 'Retain every selected unittest/mock assertion expression and its source AST identity; status, SQL/cache and mock effects have only their original test scope', 'Require the exact successful per-test log blocks and the independently hash-bound full3362 Original suite; no method declaration resolution or TCP response equality is inferred', 'Anonymous APIClient request role only; complete roles, renderers, wire behavior and Native implementation remain unverified']

EXPECTED_FIXTURE = {'framework-auth18': {'id': 'framework-auth18', 'baseline_revision': '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430', 'baseline_image': 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f', 'roles': {'anonymous': None}, 'setup': ['Archive pinned Original revision with the existing baseline runner; no working application edits', 'Use only a new unique disposable Compose project, local PostgreSQL/Redis and original CI bootstrap', 'Run the exact eighteen selected Original test labels; preserve their unmodified fixtures, mocks and APIClient transport', 'Retain tests.log, run.json and dependency records; inspect process exit and each selected test result', 'Remove only the owned project/container; no production database or live provider'], 'execution': {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_true_when_superuser_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_true_when_admin_level_user_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_false_when_no_admin_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_returns_false_when_no_users_exist', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_create_superuser_when_none_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_cannot_create_superuser_when_admin_exists', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_blocked_from_public_ip_by_default', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_get_reports_setup_not_allowed_for_public_ip', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_allowed_from_private_lan', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_allowed_for_env_override_ip', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_blocked_when_env_ip_does_not_match', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_spoofed_x_real_ip_ignored_without_trusted_proxy', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_x_real_ip_used_when_peer_is_trusted_proxy', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_post_allowed_for_ipv4_mapped_private_address', 'apps.accounts.tests.test_accounts.InitializeSuperuserTests.test_invalid_setup_allowed_ip_env_denies_post', 'apps.accounts.tests.test_login_rate_limit_inbox.LoginRateLimitInboxTests.test_jwt_trip_creates_event_and_inbox_once', 'apps.accounts.tests.test_login_rate_limit_inbox.LoginRateLimitInboxTests.test_repeat_429_does_not_parse_username', 'apps.accounts.tests.test_login_rate_limit_inbox.LoginRateLimitInboxTests.test_recorder_error_still_returns_429'], 'fresh_live_replay_qualified_this_task': False}, 'ownership': 'Original TestCase transactional database/cache fixtures in the existing disposable baseline environment; no production credentials or paid upstream', 'test_mocks_are_preserved': True}}

def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def validate(root, value, bindings=None):
    root = Path(root)
    assert value['format_version'] == 1
    assert value['binding_kind'] == 'pinned_original_framework_tests'
    assert value['original_test_files'] == EXPECTED_FILES, 'pinned Source file identity'
    assert value['authorities'] == EXPECTED_AUTHORITIES, 'pinned historical authority'
    assert value['fixtures'] == EXPECTED_FIXTURE, 'exact owned Source fixture'
    assert value['operation_count'] == 2 and value['historical_case_count'] == 18
    assert type(value['fresh_Source_case_count']) is int and value['fresh_Source_case_count'] == 0
    assert type(value['fully_verified_operations']) is int and value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
    loaded = []
    for reference in EXPECTED_AUTHORITIES:
        raw = (root / reference['path']).read_bytes()
        assert sha(raw) == reference['sha256'], 'historical authority bytes'
        loaded.append(json.loads(raw))
    summary, observation = loaded
    assert summary['exit_code'] == 0 and summary['tests_passed'] == 3362
    assert summary['scope'] == 'unchanged-upstream-full-python-test-suite'
    assert observation['full_log_sha256'] == summary['artifact_sha256']['tests.log']
    assert observation['selected_test_cases'] == len(observation['cases']) == 18
    original = {}
    for reference in EXPECTED_FILES:
        raw = (root / reference['path']).read_bytes()
        assert sha(raw) == reference['sha256'], 'unmodified Original tests'
        original[reference['original_path']] = ast.parse(raw)
    assert len(value['contracts']) == 2
    seen, all_cases = set(), []
    for contract in value['contracts']:
        identifier = contract['contract_id']
        assert identifier in EXPECTED_IDENTITIES and identifier not in seen, 'exact unique operation'
        seen.add(identifier)
        assert {k:contract[k]for k in EXPECTED_IDENTITIES[identifier]} == EXPECTED_IDENTITIES[identifier], 'stable identity and unresolved methods'
        assert contract['fixture_id'] == 'framework-auth18'
        assert contract['observed_roles'] == ['anonymous'] and contract['source_role_specs'] == {'anonymous':None}, 'Source test role only'
        assert contract['fully_verified'] is False
        expected_methods = ['GET','POST'] if identifier.startswith('route-1486') else ['POST']
        assert contract['observed_request_methods'] == expected_methods, 'observations are not declarations'
        assert contract['assertions'] == EXPECTED_ASSERTIONS and len(contract['remaining']) == 4
        assert [c['test_id']for c in contract['cases']] == EXPECTED_TESTS[identifier], 'exact selected test methods'
        assert contract['result'] == {'state':'historical_Original_framework_tests_passed_not_new_execution','historical_test_cases':len(contract['cases']),'new_Source_cases_this_task':0,'historical_authority':'scripts/rust-baseline/verified-full-baseline.json'}, 'historical result scope'
        for case in contract['cases']:
            assert case['source_path'] in original
            assert case['role'] == 'anonymous' and case['historical_result'] == 'ok'
            assert case['transport'] == 'Original Django APIClient; no TCP transcript claim'
            cls = next(n for n in original[case['source_path']].body if isinstance(n,ast.ClassDef) and n.name == case['class'])
            method = next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name == case['method'])
            assert case['test_id'] == case['source_path'][:-3].replace('/','.')+'.'+cls.name+'.'+method.name
            assert sha(ast.dump(method,include_attributes=False).encode()) == case['source_ast_sha256'], 'exact test AST'
            assertions = [ast.unparse(n)for n in ast.walk(method)if isinstance(n,ast.Call)and isinstance(n.func,ast.Attribute)and n.func.attr.startswith('assert')]
            assert assertions and case['assertion_expressions'] == assertions, 'unmodified executable assertions'
            block = case['framework_log_block']
            assert block.startswith(method.name+' ('+case['test_id']+')')
            assert re.search(r'(?:\.\.\. |\n)ok\s*\Z',block), 'actual per-test successful terminal'
            assert sha(block.encode()) == case['framework_log_block_sha256']
            all_cases.append(case)
    assert seen == set(EXPECTED_IDENTITIES)
    assert all_cases == observation['cases'], 'exact immutable historical observations'
    return {'passed':True,'operations':2,'historical_Original_tests':18,'fresh_executions':0,'fully_verified':0}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    value = json.loads((args.root/'scripts/rust-contracts/framework-auth/bindings.json').read_text())
    print(json.dumps(validate(args.root,value)))


if __name__ == '__main__':
    main()
