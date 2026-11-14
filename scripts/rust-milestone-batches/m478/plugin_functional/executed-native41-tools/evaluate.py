"""Recompute retained Source comparisons and actual GPG evidence; no Source run."""
import argparse, hashlib, json
from pathlib import Path
from compare import validate
from gpg_trace import validate as validate_gpg, need
from origin_check import validate as validate_origin
HERE = Path(__file__).resolve().parent
ENTRY = 'production-Main plugin transport16 and retained unsigned25'

def evaluate(out):
    report = json.loads((out / 'report.json').read_text())
    authority = json.loads((out / 'inputs/authority.json').read_text())
    wanted = json.loads((HERE / 'source-authority.json').read_text())
    rows = {}
    for epoch in ['transport16', 'retained24', 'fresh404']:
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
            result['Origin'] = validate_origin(out, epoch, authority)
            result['status'] = 'passed'
        except Exception as error:
            result['error'] = {'type': type(error).__name__, 'message': str(error)}
    default_equal = (out / 'default-before.json').read_bytes() == (out / 'default-after.json').read_bytes()
    reference_passed = all((row['status'] == 'passed' for row in rows.values())) and default_equal
    diagnostic = json.loads((out / 'fresh404-capture.json').read_text()).get('numeric_diagnostic', {})
    numeric = {'actual_status': diagnostic.get('status'), 'status': 'failed_unsupported' if diagnostic.get('status') == 501 else 'blocked_unmeasured_Source_numeric_context', 'accepted': False}
    result = {'candidate': report['candidate'], 'entrypoint': ENTRY, 'reference_functional_passed': reference_passed, 'semantic_assertions_passed': False, 'full_required_workflow': False, 'cohorts': rows, 'numeric_diagnostic': numeric, 'default62tables59sequences_literal_equal': default_equal, 'handler_drain_adversarial_qualified': False, 'Source_API_runs': 0, 'Root_full_M8_acceptance': False, 'performance_measured': False}
    (out / 'independent-functional-review.json').write_text(json.dumps(result, indent=2) + '\n')
    return result
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(evaluate(args.output)))
    except Exception as error:
        print(json.dumps({'semantic_assertions_passed': False, 'full_required_workflow': False, 'error': {'type': type(error).__name__, 'message': str(error)}}))
        raise SystemExit(2)
    raise SystemExit(2)
