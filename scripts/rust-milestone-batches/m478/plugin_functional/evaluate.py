"""Recompute retained Source comparisons and actual GPG evidence; no Source run."""
import argparse, hashlib, json
from pathlib import Path
from compare import validate
from gpg_trace import validate as validate_gpg, need
from origin_check import validate as validate_origin
HERE = Path(__file__).resolve().parent
ENTRY = 'production-Main plugin transport16 and retained unsigned25'

def evaluate(out, *, unsigned=False):
    report = json.loads((out / 'report.json').read_text())
    authority = json.loads((out / 'inputs/authority.json').read_text())
    wanted = json.loads((HERE / 'source-authority.json').read_text())
    rows = {}
    epochs = ('retained24', 'fresh404') if unsigned else ('transport16', 'retained24', 'fresh404')
    for epoch in epochs:
        result = {'status': 'failed'}
        rows[epoch] = result
        try:
            bound = authority[epoch]
            need(bound['source_sha256'] == wanted[epoch]['source_sha256'] and bound['plan_sha256'] == wanted[epoch]['plan_sha256'], "explicit independent invariant: bound['source_sha256'] == wanted[epoch]['source_sha256'] and bound['plan_sha256'] == wanted[epoch]['plan_sha256']")
            path = out / 'inputs' / bound['source_filename']
            need(hashlib.sha256(path.read_bytes()).hexdigest() == bound['source_sha256'], "explicit independent invariant: hashlib.sha256(path.read_bytes()).hexdigest() == bound['source_sha256']")
            source = json.loads(path.read_text())
            native = json.loads((out / (epoch + '-capture.json')).read_text())
            result['functional'] = validate(source, native)
            result['GPG'] = validate_gpg(source, native, (out / (epoch + '-Main.logs')).read_bytes())
            actual = json.loads((out / (epoch + '-Main-inspect.json')).read_text())
            need(actual['Image'] == report['candidate']['image_id'] and actual['Config']['User'] == '65532:65532', "explicit independent invariant: actual['Image'] == report['candidate']['image_id'] and actual['Config']['User'] == '65532:65532'")
            need(report['cohorts'][epoch]['actual_Main_exit'] == 0, "explicit independent invariant: report['cohorts'][epoch]['actual_Main_exit'] == 0")
            need(json.loads((out / (epoch + '-backend-after-Main.json')).read_text()) == [], "explicit independent invariant: json.loads((out / (epoch + '-backend-after-Main.json')).read_text()) == []")
            proof = (out / (epoch + '-GPG-absence.stdout')).read_text().splitlines()
            namespace = proof[0]
            marker = proof.index('HOMES')
            need(proof[marker + 1:] == [] and marker - 1 == len(result['GPG']['children']), "explicit independent invariant: proof[marker + 1:] == [] and marker - 1 == len(result['GPG']['children'])")
            for child, line in zip(result['GPG']['children'], proof[1:marker]):
                need(child['PID_namespace'] == namespace, "explicit independent invariant: child['PID_namespace'] == namespace")
                need(line == 'ABSENT ' + str(child['pid']) or (line.split(' ', 1)[0] == str(child['pid']) and line.rsplit(')', 1)[1].split()[19] != child['generation']), "explicit independent invariant: line == 'ABSENT ' + str(child['pid']) or (line.split(' ', 1)[0] == str(child['pid']) and line.rsplit(')', 1)[1].split()[19] != child['generation'])")
            result['Origin'] = validate_origin(out, epoch, authority, numeric_diagnostic=not unsigned)
            result['status'] = 'passed'
        except Exception as error:
            result['error'] = {'type': type(error).__name__, 'message': str(error)}
    default_equal = (out / 'default-before.json').read_bytes() == (out / 'default-after.json').read_bytes()
    reference_passed = all((row['status'] == 'passed' for row in rows.values())) and default_equal
    diagnostic = json.loads((out / 'fresh404-capture.json').read_text()).get('numeric_diagnostic', {})
    numeric = {'actual_status': diagnostic.get('status'), 'status': 'failed_unsupported' if diagnostic.get('status') == 501 else 'blocked_unmeasured_Source_numeric_context', 'accepted': False}
    result = {'candidate': report['candidate'], 'entrypoint': ENTRY, 'reference_functional_passed': reference_passed, 'semantic_assertions_passed': False, 'full_required_workflow': False, 'cohorts': rows, 'numeric_diagnostic': numeric, 'default62tables59sequences_literal_equal': default_equal, 'handler_drain_adversarial_qualified': False, 'Source_API_runs': 0, 'Root_full_M8_acceptance': False, 'performance_measured': False}
    if unsigned:
        need(report['scope'] == 'unsigned25' and set(report['cohorts']) == set(epochs),
             'exact ordered unsigned scope')
        need(report['passed'] is report['cleanup_verified'] is True and not diagnostic
             and report['Source_API_calls'] == 0 and report['Source_application_runs'] == 0,
             'actual unsigned owner success and no Original replay or diagnostic')
        cleanup = json.loads((out / 'independent-cleanup.json').read_text())
        need(cleanup['exact_owned_absence'] is cleanup['PID_namespace_checked'] is True
             and cleanup['normal_cleanup_report_valid'] is True and cleanup['fresh_CID_queries'] == 4,
             'actual storage, Origin and both Main generations independently retired')
        calls = [rows[epoch].get('functional', {}).get('actual_Native_TCP_cases') for epoch in epochs]
        origins = [rows[epoch].get('Origin', {}).get('actual_unsigned_reference_requests') for epoch in epochs]
        need(reference_passed and calls == [24, 1] and origins == [17, 1],
             'complete 25-call strict unsigned comparator')
        result.update(entrypoint='reuse normal24+fresh404 controller as one ordered two-context cohort',
                      semantic_assertions_passed=True, full_required_workflow=True,
                      actual_calls=25, numeric_diagnostic=None)
    (out / 'independent-functional-review.json').write_text(json.dumps(result, indent=2) + '\n')
    return result
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scope', choices=('diagnostic41', 'unsigned25'), default='diagnostic41')
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--execution', type=Path)
    args = parser.parse_args()
    try:
        unsigned = args.scope == 'unsigned25'
        if unsigned:
            execution = json.loads(args.execution.read_text())
            observed = execution['execution']
            need(execution['id'] == 'native/plugin-unsigned25' and execution['cleanup_verified'] is True
                 and type(observed['actual_exit']) is int and observed['actual_exit'] == 0
                 and observed['failure'] is None and all(observed[key] is True for key in
                     ('process_group_settled', 'stdout_EOF', 'stderr_EOF')), 'fresh coordinator execution and cleanup')
            report = json.loads((args.output / 'report.json').read_text())
            need(report['candidate'] == json.loads(args.candidate.read_text()), 'exact frozen candidate')
        print(json.dumps(evaluate(args.output, unsigned=unsigned)))
        if unsigned:
            raise SystemExit(0)
    except Exception as error:
        print(json.dumps({'semantic_assertions_passed': False, 'full_required_workflow': False, 'error': {'type': type(error).__name__, 'message': str(error)}}))
        raise SystemExit(2)
    raise SystemExit(2)
