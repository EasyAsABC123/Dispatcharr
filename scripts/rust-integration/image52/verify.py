"""Offline verification of the selected image52 publication, not workload execution."""
import hashlib
import json
import math
from pathlib import Path

REQUIRED = set('auth-hash baseline-all category-browser channel-logos clean-install complete-refresh complete-refresh-original current-packaged fresh-Original17capture guide-grid-packaged live-react-stats m4-functional output-profile packaged-cached-provider-info packaged-provider-actions packaged-vod-images profile-restart provider-modal-browser proxy-auth retention100 sharedTS18 sharedTS6-all-down shipped-createsuperuser22 shipped-safe17 static-Chrome-HTML-browser strict-typed154 unified-HTML-browser user-agent'.split())
IMAGE = 'sha256:983dc2ba0b740d97abf45cd3a51bf96633b75fbce8cbe9ea678bad36647d9b44'


def verify(root, record=None):
    root = Path(root)
    record = record if record is not None else json.loads((root / 'verified-checkpoint.json').read_text())
    provenance = record['execution_support_provenance']
    assert provenance['path'] == 'execution-support-provenance.json'
    assert hashlib.sha256((root / provenance['path']).read_bytes()).hexdigest() == provenance['sha256']
    assert record['worker_graph_activation'] is False
    assert record['Original_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert record['Original_archive_sha256'] == 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
    assert record['support_revision'] == '416e1cd0020e158864851c549a1570affd3f195c'
    assert record['support_archive_sha256'] == '6605979ec54f153eb448055429be9d15aff726950168b40246474b701d7d91b1'
    plan = json.loads((root / 'package-plan.json').read_text())
    planned = {n['name']: n for n in plan['groups'] + plan['supplementary_nodes']}
    rows = record['attempts']
    assert len(rows) == 29 and len({r['name'] for r in rows}) == 29
    assert {r['required_node'] for r in rows} == REQUIRED
    assert {r['name'] for r in rows} == REQUIRED | {'guide-grid-packaged-valid-window-2'}
    assert sum(r['actual_returncode'] == 0 for r in rows) == 28
    assert [(r['name'], r['actual_returncode']) for r in rows if r['actual_returncode']] == [('guide-grid-packaged', 1)]
    assert record['required_nodes'] == 28 and record['actual_attempts'] == 29
    assert record['exit_zero_attempts'] == 28 and record['preserved_nonzero_attempts'] == 1
    assert record['performance_verified'] is False and record['full_M4_verified'] is False
    c = record['candidate']
    assert c['git_revision'] == '98e447186af099dd17496539aec46312f6cf4f62'
    assert c['image_id'] == IMAGE and c['exact_qualified_Rust_inputs'] == 788 and c['all_application_inputs'] == 1285
    assert c['manifest_sha256'] == '6b5ab542dfc6cda4f9a2187d1669f06ccd007ee7f43a1359e7812ff353afa549'
    assert c['source_archive_sha256'] == 'b019ead2a4d897ece19e0e1ce12703397dc91181ebf6112983ec9d25190fbf04'
    assert c['source_tree_sha256'] == 'ca3c42acb72734330a103d192f65f84a1e59044540f5abaa74e4faaa1e548bcc'
    credit = json.loads((root / 'input-credit-review.json').read_text())
    assert credit['passed'] is True and all(credit[k] == v for k, v in c.items())
    schedule = json.loads((root / 'Root-dispatch-schedule-review.json').read_text())
    assert schedule['passed'] is True and schedule['attempts'] == 29
    assert schedule['argv_and_cwd_exact_to_approved_plan'] is True
    assert schedule['dependencies_exited_successfully_before_dispatch'] is True
    assert schedule['max_actual_concurrency'] == 3 and schedule['heavy_shared_and_retention_solo'] is True
    authority = json.loads((root / 'Root-controller-exit-inventory.json').read_text())
    expected = {r['name']: r for r in authority['rows']}
    assert set(expected) == {r['name'] for r in rows}
    reviews = {}
    for r in rows:
        assert r['required_node'] == ('guide-grid-packaged' if r['name'] == 'guide-grid-packaged-valid-window-2' else r['name'])
        assert r['kind'] == planned[r['name']]['kind']
        assert r['requires'] == planned[r['name']]['requires']
        assert r['controller_payload'] == 'controller-results/' + r['name'] + '.json'
        raw_bytes = (root / r['controller_payload']).read_bytes()
        assert hashlib.sha256(raw_bytes).hexdigest() == r['controller_result']['sha256']
        raw = json.loads(raw_bytes)
        assert raw['name'] == r['name'] and raw['returncode'] == r['actual_returncode']
        for field in ['started_epoch', 'completed_epoch', 'argv', 'cwd']:
            assert r[field] == raw[field]
        assert all(type(r[k]) in (int, float) and math.isfinite(r[k]) and r[k] > 0 for k in ['started_epoch', 'completed_epoch'])
        assert r['started_epoch'] < r['completed_epoch']
        def normalize(value):
            return value.replace('/home/justin/github/Dispatcharr/artifacts/rust/image52-package-plan-draft-1/support', '${SUPPORT}').replace('/home/justin/github/Dispatcharr/artifacts/rust/image52-packaged-matrix-1', '${OUTPUT}').replace('/home/justin/github/Dispatcharr/artifacts/rust/candidate-build-hash-admission-image52-1/build.json', '${MANIFEST}')
        assert [normalize(a) for a in raw['argv']] == planned[r['name']]['argv']
        assert normalize(raw['cwd']) == '${SUPPORT}'
        for dependency in r['requires']:
            prior = next(x for x in rows if x['name'] == dependency)
            assert prior['actual_returncode'] == 0 and prior['completed_epoch'] <= r['started_epoch']
        assert expected[r['name']]['returncode'] == r['actual_returncode']
        assert expected[r['name']]['sha256'] == r['controller_result']['sha256']
        p = root / r['review']
        assert p.parent == root / 'reviews' and p.name == r['name'] + '.json'
        assert hashlib.sha256(p.read_bytes()).hexdigest() == r['review_sha256'] == r['review_original']['sha256']
        review = json.loads(p.read_text()); reviews[r['name']] = review
        if r['name'] != 'guide-grid-packaged':
            assert review.get('passed', review.get('qualified')) is True
        else:
            assert review['passed'] is False
        if r['name'] != 'strict-typed154':
            inventories = [v for k,v in review.items() if k.startswith('independent_') and isinstance(v, dict)]
            assert inventories
            assert all(not value for inventory in inventories for key,value in inventory.items() if key != 'project')
    assert reviews['sharedTS18']['raw_checks'] == 24
    assert reviews['sharedTS6-all-down']['raw_checks'] == 14
    h = reviews['retention100']
    assert h['late_readers'] == 100 and h['channels'] == 10 and h['concurrency'] == 32
    assert h['prefix_hash_membership_all'] is True and h['late_reader_bytes_min'] >= 1500000
    assert len(h['samples']) == 4 and h['samples'][-1]['local_bytes'] == 40938880
    assert h['samples'][-1]['redis_logical_bytes'] == 150962120
    assert len(h['late_decoder_frames']) == len(h['strict_priming_frames']) == 10
    assert min(h['late_decoder_frames'] + h['strict_priming_frames']) >= 50
    return {'verified': True, 'required_nodes': 28, 'attempts': 29, 'successful_attempts': 28, 'preserved_clock_guard_failure': 1, 'Main4s_included': False, 'performance_acceptance': False}


if __name__ == '__main__':
    print(json.dumps(verify(Path(__file__).resolve().parent), sort_keys=True))
