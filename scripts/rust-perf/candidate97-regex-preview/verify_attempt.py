"""Verify actual optimized Candidate97 regex timing against the fixed Candidate94 protocol."""
import argparse
from hashlib import file_digest, sha256
import importlib.util
import json
from pathlib import Path
from statistics import median
import tarfile


ARTIFACTS = Path(__file__).resolve().parent.parent
PREVIOUS = Path(__file__).resolve().parents[1] / 'candidate94-regex-preview' / 'verify_attempt.py'
C94_REDUCER = '3e96f20233afb6da9ccdbd6643e8b502748b31030c1b9136ee6fd34cedadc4c0'
REVISION = 'fdebfb97615f9c4a8e81f29b1dacebfca91f6950'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = '4c76b4bd506089e7ad01fae7565b8ece9ae9fa44689ff21718f96ccffa7cb389'
OPTIMIZED_IMPORTS = {
    'rust/crates/store/src/stream_regex_preview.rs': '0b80036409cdfc90f2e4697f85a5aaef03b6dd23710d661a91a37e674b64fee5',
    'rust/runtime/regex_bridge/dispatcharr_native/stream_preview.py': '5a498e3e25b09b881a7d58fa259867476313853a9fcf5c86a59b96564ee045f5',
    'rust/runtime/regex_bridge/dispatcharr_native/regex_only.py': '53d3284c3b8bf4058f92910e3a2e61cd85253b412ec8e69617601016b44d31f5',
}
FUNCTIONAL_SEAL = 'c0940746abe6c7fec563a7085dec3b24c89fccc7d80afa05ecd19cfa55276c68'
RUNNER_SEAL = '0e2e28461ce494c060e2f4930c59b87e8b5e413d0f68f16ceeee3b4022c02618'
REQUEST_MANIFEST = 'd344bb6e450ffcda06ef9644d5cddf711d41e05892f83acebf7d3e9190fdbe73'
PLAN = 'a4e0371c6a80711bbf522f3cd855e089aa4705a993e62071ceada4120b60ca4b'


def digest(path):
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


if digest(PREVIOUS) != C94_REDUCER:
    raise ValueError('Qualified independent reducer changed')
spec = importlib.util.spec_from_file_location('verified_previous_regex_reducer', PREVIOUS)
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)
load, require, approx = previous.load, previous.require, previous.approx


def canonical(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def optimized_archive(path):
    require(digest(path) == ARCHIVE, 'exact frozen Candidate97 archive')
    with tarfile.open(path) as archive:
        for name, expected in OPTIMIZED_IMPORTS.items():
            member = archive.getmember(name)
            require(member.isfile() and 0 < member.size <= 65536, 'regular bounded archived IPC inputs')
            with archive.extractfile(member) as stream:
                require(sha256(stream.read()).hexdigest() == expected, 'exact archived optimized IPC input')
    return OPTIMIZED_IMPORTS.copy()


def build(path):
    value = load(path)
    require(value['git_revision'] == REVISION and value['format_version'] == 1
            and value['passed'] is True and value['exit_code'] == 0, 'exact successful Candidate97 immutable build')
    image = value['image_id']
    require(image.startswith('sha256:') and len(image) == 71 and all(c in '0123456789abcdef' for c in image[7:]),
            'exact immutable image content ID')
    require(value['source_archive'] == 'source.tar' and value['source_archive_sha256'] == ARCHIVE,
            'exact archived build input')
    require(canonical(value['source_sha256']) == value['source_tree_sha256']
            and all(value['source_sha256'].get(name) == expected for name, expected in OPTIMIZED_IMPORTS.items()),
            'exact source map and all optimized transport hashes')
    optimized_archive(path.parent / value['source_archive'])
    return value


def functional(path, build_path, image, tree, runtime):
    directory = path.parent
    receipt = load(path)
    binding = load(directory / 'binding.json')
    exit_row = load(directory / 'execution.json')
    teardown = load(directory / 'independent-cleanup.json')
    report = load(directory / 'evidence/integration.json')
    freeze_path = runtime / 'freeze.json'
    require(digest(freeze_path) == binding['frozen_inputs_sha256'] == FUNCTIONAL_SEAL, 'exact successor functional input seal')
    freeze = load(freeze_path)
    require(freeze['original_revision'] == SOURCE, 'exact Original fixture base')
    require(receipt['passed'] is True and receipt['stream_regex_ui_scope_passed'] is True
            and receipt['scope'] == 'paired_stream_regex_stock_react' and receipt['candidate_revision'] == REVISION
            and receipt['image_id'] == image and receipt['source_revision'] == SOURCE and receipt['tracked_runner_actual_exit'] == 0,
            'passing physical successor functional receipt')
    require(binding['candidate_revision'] == REVISION and binding['image_id'] == image
            and binding['build_sha256'] == digest(build_path) and binding['source_archive_sha256'] == ARCHIVE
            and report['candidate_image_binding'] == {
                'image_id': image, 'source_tree_sha256': tree, 'manifest_verified': True}
            and report['build_manifest_sha256'] == digest(build_path) and report['source_baseline'] == SOURCE
            and report['baseline_archive_sha256'] == freeze['copied_inputs']['original-source.tar'],
            'exact admitted Candidate97 project/source/installed image')
    require(len({entry['project'] for entry in (receipt, binding, teardown, report)}) == 1,
            'one uniquely owned functional project')
    require(exit_row['actual_exit'] == 0 and exit_row['stdout_EOF'] and exit_row['stderr_EOF']
            and exit_row['process_group_settled'] and not exit_row['remaining_group_members'], 'actual functional runner settlement')
    roles = teardown['owned_container_generations']
    require(teardown['passed'] and not any(teardown['inventories'].values()) and len(roles) == 4
            and {row['service'] for row in roles} == {'baseline', 'origin', 'candidate', 'data-init'}
            and all(row['actual_exit_before_cleanup'] == 0 and row['container_absent'] and row['host_generation_absent'] for row in roles),
            'actual exact functional teardown with no retained generations')
    expected_tools = {name: digest for name, digest in freeze['copied_inputs'].items()
                      if name != 'original-source.tar' and not name.startswith('scripts/rust-milestone-batches/')}
    require(report['passed'] and report['cleanup'] == 'project removed' and not report['resources_retained']
            and report['contract_suites'] == ['stream_regex'] and report['tool_source_sha256'] == expected_tools,
            'functional exact tool inputs and suite terminal')
    rows = report['checks']
    business = {row['name']: row['details'] for row in rows if row['name'].startswith('stream-regex-preview-')}
    require(len(rows) == 48 and len(business) == 30 and all(row['passed'] for row in rows) and all(
        all(row[key] is True for key in ('literal_match', 'expected_body_match', 'wire_match', 'sql_broker_unchanged'))
        for row in business.values()), 'all 48 actual successor functional checks')
    return business


def inputs(driver):
    freeze = driver / 'freeze.json'
    require(digest(freeze) == RUNNER_SEAL, 'exact successor performance launcher and observer seal')
    for name, expected in load(freeze)['files'].items():
        require(digest(driver / name) == expected, 'matching frozen performance input')
    manifest = driver / 'request-manifest.json'
    require(digest(manifest) == REQUEST_MANIFEST, 'unchanged Candidate94 operation and budget manifest')
    operations = load(manifest)
    require(operations['plan_sha256'] == PLAN, 'unchanged Candidate94 plan identity')
    return operations


def check_summary(summary, verified):
    require(summary['passed'] and summary['offline_and_sql_unchanged'] and summary['sample_count'] == verified['samples']
            and summary['exact_successes'] == verified['exact_requests'] and summary['bytes'] == verified['response_bytes']
            and summary['qualified_scan_rows'] == verified['qualified_scan_rows'], 'controller and literal phase work agree')
    require(approx(summary['metrics']['cpu_percent'], verified['cpu_percent'])
            and summary['metrics']['peak_working_bytes'] == verified['peak_working_bytes'], 'controller and physical phase resources agree')


def compare_summary(summary, verified):
    require(summary['passed'] == verified['passed'], 'controller and physical paired result agree')
    if 'ratios' in verified:
        for raw, independent in (('whole_cpu_ratio', 'whole_cpu'), ('whole_peak_memory_ratio', 'whole_peak_working_set'),
                                 ('throughput_ratio', 'throughput')):
            require(approx(summary[raw], verified['ratios'][independent]), 'recomputed physical paired ratio')
        require(summary['per_request'] == verified['floored_operation_latency_ratios'], 'recomputed physical paired latency')


def launches(log, order):
    labels = []
    for line in log.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict) and 'phase' in row:
            labels.append(row['phase'])
    expected = [f'measure-{backend}-1-{trial}' for trial, backend in order]
    require(labels == expected[:len(labels)], 'actual launch phase order matches fixed protocol')
    return len(labels)


def disposition(actual_exit, controller_passed, measured_pass):
    require(actual_exit != 0 or (controller_passed and measured_pass), 'exit zero must have full passing physical gates')
    require(not controller_passed or measured_pass, 'green controller needs matching physical evidence regardless of outer exit')
    return {'all_budget_gates_and_cleanup': measured_pass, 'controller_report_passed': controller_passed,
            'performance_accepted': actual_exit == 0 and measured_pass}


def reconcile(args):
    operations = inputs(args.driver)
    manifest = build(args.build)
    image, tree = manifest['image_id'], manifest['source_tree_sha256']
    cases = functional(args.functional_receipt, args.build, image, tree, args.functional_runner)
    for name, expected in operations['requests'].items():
        for backend in ('source', 'native'):
            row = cases[f'stream-regex-preview-{backend}:{name}']
            require((row['actual_status'], row['response_bytes'], row['actual_body_sha256']) ==
                    (expected['status'], expected['bytes'], expected['sha256']), 'actual optimized functional work unchanged')
    preflight = load(args.preflight)
    plan, binding = preflight['plan'], preflight['quiet_binding']
    require(canonical(plan) == PLAN and preflight['plan_sha256'] == PLAN
            and preflight['request_manifest_sha256'] == REQUEST_MANIFEST, 'actual preflight did not alter frozen budgets')
    want = {'scope': plan['scope'], 'candidate_revision': REVISION, 'candidate_image': image,
            'build_sha256': digest(args.build), 'functional_receipt_sha256': digest(args.functional_receipt),
            'plan_sha256': PLAN, 'inputs_sha256': RUNNER_SEAL}
    require(all(binding.get(key) == value for key, value in want.items()), 'exact actual benchmark admission binding')
    quiet = load(args.quiet_authorization)
    require(quiet['authorized'] is True and all(quiet.get(key) == value for key, value in binding.items()),
            'actual quiet window authorized the same boot, workload and candidate')
    cue_hash = digest(args.quiet_authorization)
    actual_exit = int(args.actual_exit.read_text().strip())
    order = [(number, backend) for number, pair in enumerate(plan['orders'], 1) for backend in pair]
    observed = launches(args.actual_log, order)
    terminal = args.attempt / 'performance.json'
    result = {'candidate_revision': REVISION, 'candidate_image': image, 'source_archive_sha256': ARCHIVE,
              'optimized_ipc_source_sha256': OPTIMIZED_IMPORTS, 'build_manifest_sha256': want['build_sha256'],
              'functional_receipt_sha256': want['functional_receipt_sha256'], 'plan_sha256': PLAN,
              'request_manifest_sha256': REQUEST_MANIFEST, 'runner_seal_sha256': RUNNER_SEAL,
              'quiet_authorization_sha256': cue_hash,
              'actual_launcher_exit': actual_exit, 'actual_started_measurement_phases': observed,
              'actual_launcher_log_sha256': digest(args.actual_log), 'performance_accepted': False,
              'phase_reviews': [], 'pairs': []}
    if not terminal.is_file():
        require(actual_exit != 0, 'exit zero requires an actual terminal report')
        return {**result, 'terminal_report_present': False, 'cleanup_verified': False}

    report = load(terminal)
    require(report['plan'] == plan and report['binding'] == binding and report['request_manifest_sha256'] == REQUEST_MANIFEST
            and report['quiet_sha256'] == cue_hash
            and report['operations'] == operations['requests'] and report['measurements_started'] == bool(observed), 'actual benchmark terminal input custody')
    result.update(terminal_report_present=True, report_sha256=digest(terminal), stage=report.get('stage'),
                  startup_preparations=previous.startup_preparations(report, order))
    cleaned = previous.cleanup(report) if 'cleanup' in report else {'passed': False}
    result['cleanup'] = cleaned
    trials = report['trials']
    require([(row['trial'], row['backend']) for row in trials] == order[:len(trials)] and len(trials) <= observed,
            'actual observed phase order and count')
    complete = {}
    for summary in trials:
        number, backend = summary['trial'], summary['backend']
        evidence = f'{backend}-1-{number}.json'
        raw = args.attempt / evidence
        require(summary['evidence'] == evidence and summary['raw_sha256'] == digest(raw), 'actual raw phase file custody')
        proof = {'trial': number, 'backend': backend, 'raw_sha256': digest(raw), 'qualified': False}
        try:
            physical = previous.phase(load(raw), plan, operations['requests'])
            check_summary(summary, physical)
        except (KeyError, ValueError) as error:
            proof['assessment'] = str(error)
        else:
            complete[number, backend] = physical
            proof.update(physical, qualified=True)
        result['phase_reviews'].append(proof)

    for number, summary in enumerate(report['pairs'], 1):
        require(summary['trial'] == number, 'actual paired order')
        source, native = [complete.get((number, side)) for side in ('source', 'candidate')]
        pair = previous.pair(source, native, plan) if source and native else {'passed': False, 'gates': {'equal_work': False}}
        compare_summary(summary, pair)
        result['pairs'].append({'trial': number, **pair})

    prepares = result['startup_preparations']
    complete_work = len(complete) == observed == len(order) == 6 and len(prepares) == 6 and all(row['qualified'] for row in prepares)
    pairs = result['pairs']
    measured_pass = complete_work and len(pairs) == 3 and all(pair['passed'] for pair in pairs) and cleaned['passed']
    result.update(disposition(actual_exit, report['passed'], bool(measured_pass)))
    if actual_exit:
        result['retained_failure'] = {key: report[key] for key in ('error_type', 'guard_failure', 'command_actual_exit') if key in report}
    if complete_work and len(pairs) == 3 and all('ratios' in row for row in pairs):
        result['median_paired_ratios'] = {key: median(row['ratios'][key] for row in pairs) for key in pairs[0]['ratios']}
        result['backend_medians'] = {side: {key: median(complete[number, side][key] for number in (1, 2, 3))
            for key in ('cpu_percent', 'peak_working_bytes')} for side in ('source', 'candidate')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    driver = ARTIFACTS / 'm7-regex-preview-ipc-performance-1'
    parser.add_argument('--driver', type=Path, default=driver)
    parser.add_argument('--functional-runner', type=Path, default=ARTIFACTS / 'm7-stream-regex-preview-ipc-runtime-1')
    parser.add_argument('--preflight', type=Path)
    parser.add_argument('--build', type=Path)
    parser.add_argument('--functional-receipt', type=Path)
    parser.add_argument('--attempt', type=Path)
    parser.add_argument('--actual-exit', type=Path)
    parser.add_argument('--actual-log', type=Path)
    parser.add_argument('--quiet-authorization', type=Path)
    parser.add_argument('--frozen-archive-preflight', type=Path)
    args = parser.parse_args()
    if args.frozen_archive_preflight:
        inputs(args.driver)
        proof = {'candidate_revision': REVISION, 'source_archive_sha256': ARCHIVE, 'plan_sha256': PLAN,
                 'installed_image_verified': False, 'optimized_ipc_source_sha256': optimized_archive(args.frozen_archive_preflight)}
    elif all(getattr(args, key) for key in ('preflight', 'build', 'functional_receipt', 'attempt', 'actual_exit', 'actual_log', 'quiet_authorization')):
        proof = reconcile(args)
    else:
        parser.error('the actual build, functional receipt, quiet receipt, terminal run, transcript and exit are required')
    print(json.dumps(proof, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
