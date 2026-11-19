"""Bind the representative Connect capture to the current register and pinned Original."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile

from run import REGISTER_BASE, SOURCE_REVISION, SOURCE_TAR_SHA

HERE=Path(__file__).resolve().parent
ARTIFACTS=HERE.parent
REPO=ARTIFACTS/'candidate54-isolated-publication-1/repo'
sys.path.insert(0,str(HERE/'copied/scripts/rust-integration'))
from connect_matrix_contract import DENIALS, ROLES, ROUTES, VARIANTS, route_path, symbolic_path


def digest(data):
    return hashlib.sha256(data).hexdigest()


def show(path):
    return subprocess.check_output(['git','show',f'{REGISTER_BASE}:{path}'],cwd=REPO)


def summarize(path):
    if path.startswith('admin'):
        return 'django-admin'
    components=path.split('/')
    if components[0]=='api' and len(components)>1:
        return 'api/'+components[1].lstrip('^')
    return components[0] or '(root)'


def prepare():
    blob=show('docs/rust-contract-register.json')
    register=json.loads(blob)
    rows={route['resolver_ordinal']:route for route in register['runtime_routes']}
    all_ops=[(route,op) for route in rows.values() for op in route['operations']]
    assert len(all_ops)==798
    absent=[(route,op) for route,op in all_ops if 'executable_Source_contract' not in op]
    assert len(absent)==281
    histogram=Counter(summarize(route['pattern_exact']) for route,_ in absent)
    route_cells=[]
    for route in ROUTES:
        for ordinal,variant in zip((route.bare,route.formatted),VARIANTS,strict=True):
            declaration=rows[ordinal]
            [operation]=[row for row in declaration['operations'] if row['method']==route.method]
            assert 'executable_Source_contract' not in operation and operation['owning_milestone']=='M5'
            pattern=declaration['pattern_exact'].replace('/^','/')
            params={'integration':973,'subscription':311,'log':102}
            assert re.fullmatch('/'+pattern,route_path(route,variant,params))
            static=declaration['default_permission_candidates']
            assert static==['apps.accounts.permissions.IsAdmin']
            route_cells.append({'contract_id':operation['contract_id'],'key':route.key,'resolver_ordinal':ordinal,'name':declaration['name'],
                                'pattern_exact':declaration['pattern_exact'],'variant':variant,'method':route.method,
                                'path_template':symbolic_path(route,variant),'role_cases':{
                                    role:DENIALS.get(role,route.status) for role in ROLES},
                                'expected_existing_source_format_failure_candidate':variant=='json' and route.key=='nested-subscriptions'})
    assert len({case['contract_id'] for case in route_cells})==20
    protected=('apps/connect/api_views.py','apps/connect/api_urls.py','apps/connect/models.py',
               'apps/connect/serializers.py','apps/accounts/authentication.py','apps/accounts/permissions.py',
               'dispatcharr/settings.py')
    archive=HERE/'copied/original-source.tar'
    assert digest(archive.read_bytes())==SOURCE_TAR_SHA
    with tarfile.open(archive) as pinned:
        original={name:pinned.extractfile(name).read() for name in protected}
    assert b'"DEFAULT_PERMISSION_CLASSES": [\n        "apps.accounts.permissions.IsAdmin"' in original['dispatcharr/settings.py']
    assert b'def list_subscriptions(self, request, pk=None):' in original['apps/connect/api_views.py']
    assert b'EventSubscription.objects.filter(integration_id=pk)' in original['apps/connect/api_views.py']
    assert b'X-API-Key' in original['apps/accounts/authentication.py']
    assert all(key in original['apps/connect/models.py'] for key in (b'on_delete=models.CASCADE',b'EventSubscription',b'DeliveryLog'))
    assert b'"id",\n            "event",' in original['apps/connect/serializers.py']
    rust=show('rust/crates/store/src/connect_reads.rs')
    assert b'put(update).delete(delete)' in rust and b'"/api/connect/logs/"' in rust
    assert b'"/api/connect/subscriptions/' not in rust and b'"/api/connect/logs/{' not in rust
    return {'base_register_commit':REGISTER_BASE,'base_register_sha256':digest(blob),
            'partial_original_HTTP':517,'unassigned_HTTP':281,'missing_by_shared_family':histogram,
            'source_revision':SOURCE_REVISION,'original_archive_sha256':SOURCE_TAR_SHA,
            'original_sources_sha256':{name:digest(value) for name,value in original.items()},
            'rust_router_audit_sha256':digest(rust),'candidate_missing_canonical_methods_at_audit':10,
            'role_cells_per_operation':len(ROLES),'distinct_selected_declarations':20,
            'paired_cell_count':120,'physical_cell_count':240,'cases':route_cells,
            'fixture_has_one_disposable_schema_and_independent_phase_targets':True,
            'closure_blockers':[
                'Dedicated Django Admin callback scopes are larger but Native lacks the session/CSRF/form entry points',
                'Channels covers several independent producers and fixture families rather than a single owned seed',
                'Source nested list format-suffix positive request may return 500 because its custom handler lacks the format kwarg',
                'No completed full-role metadata, alternate format set, or Native full-contract acceptance exists in the register',
            ],'full_operation_promotions':0}


if __name__=='__main__':
    (HERE/'matrix-plan.json').write_text(json.dumps(prepare(),indent=2,sort_keys=True)+'\n')
