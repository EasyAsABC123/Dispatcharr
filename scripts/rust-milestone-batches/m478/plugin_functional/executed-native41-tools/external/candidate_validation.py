"""M4/M7/M8 owner adapter for Root's ONE global milestone scheduler.
Exports existing cohort commands. Never launches a process, Docker stack or sampler.
Root owns dispatch/polling/global3/quiet; this adapter checks bindings and results.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
WORKING_FIXTURES = REPO / 'artifacts/rust/m4-m7-m8-wide-batch-1'
STATES = {'passed', 'failed', 'blocked', 'not_run'}


def need(value, label):
    if not value:
        raise ValueError(label)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load():
    matrix = json.loads((HERE / 'matrix.json').read_text())
    nodes = {row['id']: row for row in matrix['nodes']}
    external = {row['id']: row for row in matrix['shared_external_nodes']}
    need(len(nodes) == len(matrix['nodes']) and not set(nodes) & set(external), 'unique global IDs')
    visiting, visited = set(), set()
    def visit(identifier):
        if identifier in external or identifier in visited:
            return
        need(identifier in nodes and identifier not in visiting, 'known acyclic dependency: ' + identifier)
        visiting.add(identifier)
        for dep in nodes[identifier]['requires']:
            visit(dep)
        visiting.remove(identifier)
        visited.add(identifier)
    for identifier in nodes:
        visit(identifier)
    return matrix, nodes, external


def bindings(path):
    value = json.loads(Path(path).read_text()) if path else {'candidate': {}, 'nodes': {}}
    need(type(value) is dict and type(value.get('nodes')) is dict and type(value.get('candidate')) is dict, 'typed Root bindings')
    for identifier, row in value['nodes'].items():
        need(type(identifier) is str and type(row) is dict, 'typed node binding')
        for name, expected in row.get('input_files', {}).items():
            need(type(expected) is str and re.fullmatch('[a-f0-9]{64}', expected), 'SHA256 input binding')
            target = Path(name)
            need(target.is_absolute() and target.is_file() and not target.is_symlink() and digest(target) == expected, 'exact node input: ' + name)
        if 'argv' in row:
            need(type(row['argv']) is list and row['argv'] and all(type(v) is str and '\0' not in v for v in row['argv']), 'literal argv/no shell')
        need(not set(row.get('environment', {})) & {'HOME', 'CODEX_HOME'}, 'do not repurpose HOME/CODEX_HOME')
    return value


def candidate_ready(candidate):
    for key, length in [('git', 40), ('source_map_sha256', 64), ('build_manifest_sha256', 64)]:
        if type(candidate.get(key)) is not str or re.fullmatch('[a-f0-9]{' + str(length) + '}', candidate[key]) is None:
            return False
    if type(candidate.get('image_id')) is not str or re.fullmatch('sha256:[a-f0-9]{64}', candidate['image_id']) is None:
        return False
    for field, expected in [('source_map_file', 'source_map_sha256'), ('build_manifest_file', 'build_manifest_sha256')]:
        path = Path(candidate.get(field, ''))
        if not path.is_file() or path.is_symlink() or digest(path) != candidate[expected]:
            return False
    try:
        build = json.loads(Path(candidate['build_manifest_file']).read_text())
        mapping = json.loads(Path(candidate['source_map_file']).read_text())
        if candidate.get('source_map_format', 'path_to_sha256') == 'files_path_to_sha256':
            mapping = mapping['files']
        elif candidate.get('source_map_format', 'path_to_sha256') != 'path_to_sha256':
            return False
        if type(mapping) is not dict or not mapping or not all(type(k) is str and type(v) is str and re.fullmatch('[a-f0-9]{64}', v) for k, v in mapping.items()):
            return False
        expected_rust = {k: v for k, v in mapping.items() if k.startswith('rust/')}
        app = build['source_sha256']
        actual_rust = {k: v for k, v in app.items() if k.startswith('rust/')}
        if not expected_rust or expected_rust != actual_rust:
            return False
        if build['passed'] is not True or type(build['exit_code']) is not int or build['exit_code'] != 0 or build['git_revision'] != candidate['git'] or build['image_id'] != candidate['image_id']:
            return False
        archive = Path(candidate.get('source_archive_file', ''))
        if not archive.is_file() or archive.is_symlink() or digest(archive) != candidate.get('source_archive_sha256') or candidate.get('source_archive_sha256') != build['source_archive_sha256']:
            return False
        if app.get('LICENSE') != '8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef' or 'ATTRIBUTION.md' not in app or not any(k.startswith('frontend/') for k in app):
            return False
    except (KeyError, TypeError, ValueError, OSError):
        return False
    return True


def blockers(node, bound):
    row = bound['nodes'].get(node['id'])
    reasons = []
    if not row or 'argv' not in row or 'evaluator_argv' not in row:
        reasons.append('Exact existing cohort controller/evaluator binding is missing')
    if node['id'].startswith(('native/', 'Main/', 'performance/')) and not candidate_ready(bound['candidate']):
        reasons.append('One current Root release source-map/archive/build/image binding is missing; old image62/component proofs do not substitute')
    if node['id'].startswith('source/') and (not row or not row.get('new_unmeasured_context_definition')):
        reasons.append('Only NEW Source context is allowed; literal unmeasured definition/bodies/roles need binding, passed Source is reused')
    if node['implementation_status'] == 'known_missing':
        reasons.append(node['known_missing'])
    if node['implementation_status'] in ('literal_cases_needed', 'authority_crosswalk_needed'):
        reasons.append('Complete remaining-case Source crosswalk/literal definition before this Source node dispatch')
    return reasons


def state(matrix, bound, receipts=None):
    receipts = receipts or {}
    output = {}
    for node in matrix['nodes']:
        identifier = node['id']
        receipt = receipts.get(identifier)
        if receipt is None:
            reasons = blockers(node, bound)
            output[identifier] = {'status': 'blocked' if reasons else 'not_run', 'reasons': reasons, 'actual_calls': None, 'execution_performed': False}
            continue
        # A failed actual controller/capture stays failed even if a separate later
        # observation qualification is useful. Never relabel a raw exit.
        actual = receipt.get('execution', {})
        cleanup = receipt.get('cleanup', {})
        qualified = receipt.get('qualification', {})
        semantic = actual.get('actual_wait_exit') == actual.get('actual_poll_exit') == 0 and type(actual.get('actual_wait_exit')) is int and type(actual.get('actual_poll_exit')) is int and actual.get('stdout_EOF') is True and actual.get('stderr_EOF') is True and actual.get('physical_settled') is True and qualified.get('evaluator_exit') == 0 and type(qualified.get('evaluator_exit')) is int and qualified.get('semantic_passed') is True and cleanup.get('independent_exit') == 0 and type(cleanup.get('independent_exit')) is int and cleanup.get('exact_owned_absence') is True and cleanup.get('PID_namespace_checked') is True
        if node['id'].startswith(('native/', 'Main/', 'performance/')):
            semantic = semantic and candidate_ready(bound['candidate']) and receipt.get('candidate') == bound['candidate']
        if node['id'].startswith('Main/'):
            semantic = semantic and qualified.get('entry_point_scope') == 'production-Main'
        if node['declared_calls'] is not None:
            semantic = semantic and type(receipt.get('actual_calls')) is int and receipt['actual_calls'] == node['declared_calls']
        evidence = receipt.get('evidence', [])
        semantic = semantic and bool(evidence)
        for item in evidence:
            path = Path(item.get('path', ''))
            semantic = semantic and path.is_file() and not path.is_symlink() and digest(path) == item.get('sha256')
        diagnostic = qualified.get('diagnostic_only') is True or bound['nodes'].get(identifier, {}).get('safe_missing_capability_probe') is True
        label = ('blocked' if semantic else 'failed') if diagnostic else ('passed' if semantic else 'failed')
        output[identifier] = {'status': label, 'actual_calls': receipt.get('actual_calls'), 'execution_performed': True, 'actual_exit': actual.get('actual_wait_exit'), 'capture_status': receipt.get('capture_status'), 'qualification': qualified, 'cleanup': cleanup, 'raw_receipt': receipt, 'full_required_cohort_still_unexecuted': diagnostic}

    # Independent cells survive failures; dependency failures block only descendants.
    changed = True
    while changed:
        changed = False
        for node in matrix['nodes']:
            result = output[node['id']]
            if result['execution_performed'] or result['status'] != 'not_run':
                continue
            bad = [dep for dep in node['requires'] if dep in output and output[dep]['status'] in ('failed', 'blocked')]
            if bad:
                result.update(status='blocked', reasons=['Dependency not accepted: ' + value for value in bad])
                changed = True
    return output


def dispatch_spec(identifier, matrix, bound):
    node = next((n for n in matrix['nodes'] if n['id'] == identifier), None)
    need(node is not None, 'this owner node ID')
    row = bound['nodes'].get(identifier, {})
    reasons = blockers(node, bound)
    # Missing implementation can still be tested by an explicitly bound safe
    # production capability probe. That probe's unsupported result is a failure.
    if row.get('safe_missing_capability_probe') is True:
        reasons = [reason for reason in reasons if reason != node.get('known_missing')]
    need(not reasons, 'BLOCKED: ' + '; '.join(reasons))
    need(type(row.get('evaluator_argv')) is list and row['evaluator_argv'] and all(type(v) is str for v in row['evaluator_argv']), 'literal evaluator argv')
    return {'id': identifier, 'owner': matrix['owner'], 'argv': row['argv'], 'cwd': row.get('cwd', str(REPO)), 'environment': row.get('environment', {}), 'evaluator_argv': row['evaluator_argv'], 'requires': node['requires'], 'exclusive_resources': node['exclusive_resources'], 'global_ordinary_slots': 1, 'global_limit_owned_by': 'Root global batch scheduler, NOT per owner', 'heavy_exclusive': 'GLOBAL_HEAVY_QUIET' in node['exclusive_resources'], 'normal_seconds': row.get('normal_seconds', 1200), 'cleanup_seconds': row.get('cleanup_seconds', 90), 'output_bytes': row.get('output_bytes', 67108864), 'no_automatic_retry': True, 'cleanup_unknown_holds_resources': True, 'unsupported_is_failed_not_passed': True, 'acceptance_scope': 'safe missing-capability diagnostic ONLY; cannot accept full required cohort' if row.get('safe_missing_capability_probe') is True else 'full declared cohort'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('export', 'readiness', 'dispatch-spec', 'evaluate'))
    parser.add_argument('--bindings')
    parser.add_argument('--node')
    parser.add_argument('--receipts')
    parser.add_argument('--output')
    args = parser.parse_args()
    matrix, nodes, external = load()
    bound = bindings(args.bindings)
    status = 0
    if args.action == 'export':
        result = {'matrix': matrix, 'runner_api': {'dispatch_spec': ['python', '-B', str(HERE / 'batch.py'), 'dispatch-spec', '--bindings', 'ROOT_BINDINGS', '--node', 'NODE_ID'], 'readiness': ['python', '-B', str(HERE / 'batch.py'), 'readiness', '--bindings', 'ROOT_BINDINGS'], 'evaluate': ['python', '-B', str(HERE / 'batch.py'), 'evaluate', '--bindings', 'ROOT_BINDINGS', '--receipts', 'ROOT_ACTUAL_RECEIPTS'], 'processes_started_here': 0, 'Root_global_dispatch_only': True}}
    elif args.action == 'dispatch-spec':
        try:
            result = dispatch_spec(args.node, matrix, bound)
        except ValueError as error:
            result = {'id': args.node, 'status': 'blocked', 'reason': str(error), 'execution_performed': False}
            status = 2
    else:
        receipts = json.loads(Path(args.receipts).read_text()) if args.receipts else {}
        results = state(matrix, bound, receipts)
        counts = {label: sum(v['status'] == label for v in results.values()) for label in STATES}
        # External shared nodes also need exact-current accepted Root receipts.
        external_complete = all(receipts.get(key, {}).get('current_candidate_accepted') is True and receipts[key].get('candidate') == bound['candidate'] and candidate_ready(bound['candidate']) for key in external)
        accepted = counts['passed'] == len(nodes) and external_complete
        result = {'matrix_valid': True, 'milestones_accepted': accepted, 'execution_performed_by_this_adapter': False, 'counts': counts, 'nodes': results, 'shared_external_pending': [key for key in external if receipts.get(key, {}).get('current_candidate_accepted') is not True or receipts[key].get('candidate') != bound['candidate']], 'Source_applications_or_Docker_started': 0}
        status = 0 if accepted else 2
    raw = json.dumps(result, indent=2, allow_nan=False) + '\n'
    if args.output:
        Path(args.output).write_text(raw)
    else:
        print(raw, end='')
    return status


if __name__ == '__main__':
    raise SystemExit(main())
