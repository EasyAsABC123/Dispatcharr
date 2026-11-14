"""Bind seven ordinary VOD operations to four unchanged Original test workflows.

Original Dispatcharr authors retain application and algorithm credit. This is
retained Source evidence, not new framework execution or Native acceptance.
"""
import argparse
import ast
import hashlib
import json
import re
from pathlib import Path

from framework_auth_contracts import ast_fingerprint

BASE = 'scripts/rust-contracts/vod-access'
KIND = 'pinned_original_vod_access_tests'
REV = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ORIGINAL = 'apps/vod/tests/test_vod_rest_access_flag.py'
ORIGINAL_SHA = '1852928f82c6c194626f1401854cc87386e627407692a2082996a37a4af8926e'
OBS_SHA = '59db633ba20196f43a8ef96901474f2af59c1c137d2921ce9a575b758612a4b8'
TESTS = [
    'test_absent_flags_keep_rest_access',
    'test_movies_flag_hides_movies_but_not_series',
    'test_series_flag_hides_series_and_episodes',
    'test_categories_are_filtered_by_kind',
]
# Exact runtime identities; format aliases, category detail and direct queryset
# tests are deliberately excluded. Indices name whole workflows, not assertions
# individually attributed to each endpoint.
OPERATIONS = [
    ('route-5f82ae2734b96921aeca:GET', 'movies', 'Movie', 'list', [0, 1, 2]),
    ('route-a459995c757709dbe718:GET', 'movies', 'Movie', 'retrieve', [1]),
    ('route-789db48af44156314ebf:GET', 'episodes', 'Episode', 'list', [0, 2]),
    ('route-cd82f96ce33a40b0f29d:GET', 'episodes', 'Episode', 'retrieve', [2]),
    ('route-e872c1f90698f4c60d0b:GET', 'series', 'Series', 'list', [0, 1, 2]),
    ('route-49cb72fffcf3e6da6900:GET', 'series', 'Series', 'retrieve', [2]),
    ('route-326e44c9aeb369a38d45:GET', 'categories', 'VODCategory', 'list', [3]),
]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def authority(root, raw_log=None):
    root = Path(root)
    raw = (root / BASE / 'observations.json').read_bytes()
    assert sha(raw) == OBS_SHA, 'ExactHistoricalObservations'
    obs = json.loads(raw)
    original = (root / BASE / 'original' / ORIGINAL).read_bytes()
    assert sha(original) == obs['original_sha256'] == ORIGINAL_SHA, 'PinnedOriginalTests'
    ref_raw = (root / 'scripts/rust-contracts/framework-auth/full-suite-reference.json').read_bytes()
    assert sha(ref_raw) == obs['full_suite_reference_sha256'], 'ExactFullSuiteReference'
    ref = json.loads(ref_raw)
    assert ref['source_commit'] == REV and ref['exit_code'] == 0 and ref['tests_passed'] == 3362
    assert obs['full_suite_log_sha256'] == ref['artifact_sha256']['tests.log']
    cls = next(n for n in ast.parse(original).body
               if isinstance(n, ast.ClassDef) and n.name == 'VodRestAccessFlagTests')
    assert ast_fingerprint(cls) == obs['class_AST_sha256'], 'WholeFixtureHelpersAndTests'
    assert [r['method'] for r in obs['observations']] == TESTS, 'ExactSelectedMethods'
    for row in obs['observations']:
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == row['method'])
        assert ast_fingerprint(method) == row['method_ast_sha256'], 'ExactTestAST'
        assert row['test_id'] == ORIGINAL[:-3].replace('/', '.') + '.' + cls.name + '.' + method.name
        assertions = [ast.unparse(n) for n in ast.walk(method)
                      if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                      and n.func.attr.startswith('assert')]
        assert assertions and assertions == row['assertion_expressions'], 'WholeWorkflowAssertions'
        block = row['framework_log_block']
        assert block.startswith(method.name + ' (' + row['test_id'] + ')')
        assert re.search(r'(?:\.\.\. |\n)ok\s*\Z', block), 'ActualNamedSuccess'
        assert sha(block.encode()) == row['framework_log_block_sha256']
    if raw_log is not None:
        raw = Path(raw_log).read_bytes()
        assert sha(raw) == obs['full_suite_log_sha256'], 'ActualRawFullSuiteHash'
        for row in obs['observations']:
            assert raw.count(row['framework_log_block'].encode()) == 1, 'ExactRawNamedSuccess'
    return obs


def build(root):
    obs = authority(root)
    fixture = 'Original-VOD-access-flags4'
    roles = {'forced_standard': {
        'source_expression': 'User.UserLevel.STANDARD',
        'transport': 'Django APIClient with force_authenticate',
        'authentication_backend_tested': False,
        'properties': 'Absent, movie-disabled or series-disabled as set by each exact workflow',
    }}
    contracts = []
    for identifier, resource, view, action, indices in OPERATIONS:
        selected = [obs['observations'][i] for i in indices]
        pattern = 'api/vod/^' + resource + ('/$' if action == 'list' else '/(?P<pk>[^/.]+)/$')
        contracts.append({
            'contract_id': identifier, 'pattern_exact': pattern,
            'source_handler': 'apps.vod.api_views.' + view + 'ViewSet',
            'method': 'GET', 'source_action': action, 'owning_milestone': 'M5',
            'fixture_id': fixture, 'observed_roles': list(roles), 'source_role_specs': roles,
            'source_test_methods': [r['test_id'] for r in selected],
            'assertions': [
                'Execute the exact named unchanged Original workflows with their original setUp, _user and _results helpers',
                'Preserve whole-workflow assertions and actual historical success blocks; helper response-status assertions remain in the full class',
                'List visibility, empty lists and disabled detail 404 have only their exact fixture scope; no unobserved enabled-detail result is inferred',
                'Forced STANDARD authentication does not test JWT/session backends; aliases, complete permissions, TCP and Native parity remain unverified',
            ],
            'result': {'state': 'selected_named_Original_tests_passed_in_retained_full3362_suite',
                       'historical_workflows': len(selected), 'new_Source_cases_this_task': 0,
                       'new_Source_runs': 0, 'Native_or_TCP_acceptance': False,
                       'complete_role_matrix': False},
            'fully_verified': False,
        })
    return {
        'format_version': 1, 'binding_kind': KIND, 'Original_revision': REV,
        'credit': 'Original Dispatcharr and dependency authors retain all application, algorithm and licensing credit.',
        'scope': 'Seven partial ordinary Source GET assignments from four historical workflows; no new execution or acceptance',
        'operation_count': 7, 'fully_verified_added': 0, 'Native_coverage_increased': False,
        'fixtures': {fixture: {
            'id': fixture, 'baseline_revision': REV,
            'baseline_image': 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f',
            'roles': roles,
            'setup': ['Use the pinned unchanged Original revision and supported disposable baseline runner',
                      'Preserve actual M3U account, both categories, movie/series/episode and relation fixtures, UUIDs and force-authenticated STANDARD users',
                      'Run the four exact labels, retain actual results and remove only the owned fixture'],
            'execution': {'entrypoint': 'scripts/rust-baseline/run.py',
                          'argv': ['--output', '<fresh-empty-output>', *[r['test_id'] for r in obs['observations']]],
                          'fresh_live_replay_qualified_this_task': False},
            'observations': BASE + '/observations.json',
            'excluded': ['Direct anonymous get_queryset test is not HTTP permission evidence',
                         'Unified all and movie-image workflows have separate existing assignments',
                         'Format aliases and category retrieve are unobserved'],
        }}, 'contracts': contracts, 'non_HTTP_contracts': [],
    }


def validate(root, value, bindings=None):
    assert value == build(root), 'ExactVODAccessContractAndScope'
    return {'passed': True, 'operations': 7, 'Original_named_successes': 4,
            'new_Source_runs': 0, 'fully_verified_added': 0}


def dependencies():
    return [BASE + '/bindings.json', BASE + '/observations.json', BASE + '/original/' + ORIGINAL,
            'scripts/rust-contracts/framework-auth/full-suite-reference.json']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--raw-log', type=Path)
    parser.add_argument('--write-bindings', action='store_true')
    args = parser.parse_args()
    authority(args.repo, args.raw_log)
    path = args.repo / BASE / 'bindings.json'
    if args.write_bindings:
        path.write_text(json.dumps(build(args.repo), indent=2) + '\n')
    print(json.dumps(validate(args.repo, json.loads(path.read_text())), indent=2))
