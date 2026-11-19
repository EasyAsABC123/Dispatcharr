"""Recover exact Image22 roles and operation joins without starting a fixture."""
import argparse
import ast
import collections
import hashlib
import json
from pathlib import Path
import re

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--private-root', type=Path, required=True)
parser.add_argument('--register', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
options = parser.parse_args()
root = options.private_root.resolve()
source = root/'artifacts/rust/m5-auth-full-integration-image22-2'
raw = (source/'integration.json').read_bytes()
sha = lambda data: hashlib.sha256(data).hexdigest()
assert sha(raw)=='bc1b852bd233b03bff581d52349a9ee1d7e27e96664885f86e2be4b3c96c99ae'
report = json.loads(raw)
assert report['passed'] and report['cleanup']=='project removed' and not report['resources_retained']
assert report['source_baseline']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
tools = {
    'scripts/rust-integration/run.py': '1d471fc77ba9a5c6f0b80be649316afdfb117b30cc9057251d39c70d88496d66',
    'scripts/rust-account-contracts/group_probes.py':'ebad889036d05154bb35158c7b27571f9a636db13badd6a5a2ec71f4142617de',
    'scripts/rust-account-contracts/stream_probes.py':'8747d8a217f645453c266bc4bc5285b1ad9b72198a4aa922deb21f5c59e5cb36',
    'scripts/rust-account-contracts/group_cleanup_probes.py':'ef9f8609a2ebcddc7eeefa771d4c5f24fe19ecc82908d4ffabbdb625cc3e589b',
}
controller, group, stream, cleanup = tools
forms={}
for tool, expected in tools.items():
    data=(source/'tool-source'/tool).read_bytes()
    assert sha(data)==expected==report['tool_source_sha256'][tool]
    forms[tool]=ast.unparse(ast.parse(data))
for file, expected in {
    group: ("(str(provider['id']), 200), (str(claimed['id']), 200)",
             "add('provider-only-delete-denied', invoke(base, ppath, method='DELETE'), 400)",
             "ppath = root + str(provider['id']) + '/'"),
    stream: ("compare('detail-' + suffix, root + suffix + '/', status)",
             "add('bulk-empty', invoke(base, root + 'bulk-delete/', {'stream_ids': []}, 'DELETE'), 204)",
             "add('denied-bulk-' + role, invoke(base, root + 'bulk-delete/', {'stream_ids': [manual]}, 'DELETE', accounts[role]['access']), 403)"),
    cleanup: ("root = '/api/channels/groups/'",
              "add('denied-' + role, invoke(base, root + 'cleanup/', {'include_custom': True}, 'POST', accounts[role]['access']), 403)",
              "add('anonymous-denied', invoke(base, root + 'cleanup/', {}, 'POST', None), 401)"),
}.items():
    assert all(fragment in forms[file] for fragment in expected),file
assert "for path in ('/proxy/ts/status', '/proxy/ts/stream/not-a-channel'):" in forms[controller]
assert 'old, new = (http(baseline + path), http(candidate + path))' in forms[controller]
assert "def http(url, body=None, token=None, method=None, headers=None)" in forms[controller]

route_ids={
  'ts-status':'route-df815bdf71ef9a7e0991:METHOD_UNRESOLVED',
  'ts-stream':'route-c75171aee6eb93b36a3c:METHOD_UNRESOLVED',
  'stream-detail':'route-2c78a0593b1b81930c61:GET',
  'stream-bulk':'route-ff58759b7c3b1d8432cf:METHOD_UNRESOLVED',
  'group-detail':'route-bcad6fcc7a44673cca5c:GET',
  'group-delete':'route-bcad6fcc7a44673cca5c:DELETE',
  'group-cleanup':'route-73e3a2ec965d642de991:POST',
}
checks={row['name']:(i,row) for i,row in enumerate(report['checks'])}
group_ids=[name.split('detail-',1)[1] for name in checks if name.startswith('channel-groups:detail-')]
assert group_ids==['1','2','2147483647','not-a-number']
specs=[
    ('ts-status','status-parity:/proxy/ts/status','GET','/proxy/ts/status','anonymous',controller),
    ('ts-stream','status-parity:/proxy/ts/stream/not-a-channel','GET','/proxy/ts/stream/not-a-channel','anonymous',controller),
    ('group-delete','channel-groups:provider-only-delete-denied','DELETE',f'/api/channels/groups/{group_ids[0]}/','admin',group),
]
for name in checks:
    if name.startswith(('channel-groups:detail-','streams:detail-')):
        family,label=name.split(':',1)
        suffix=label.removeprefix('detail-')
        assert suffix.isdecimal() or suffix=='not-a-number'
        group_case=family=='channel-groups'
        path_family='groups' if group_case else 'streams'
        specs.append(('group-detail' if group_case else 'stream-detail',name,'GET',
                      f'/api/channels/{path_family}/{suffix}/','admin',group if group_case else stream))
for label,role in [('bulk-empty','admin'),('bulk-includes-inactive','admin'),
                   ('denied-bulk-standard','standard'),('denied-bulk-viewer','viewer')]:
    specs.append(('stream-bulk','streams:'+label,'DELETE','/api/channels/streams/bulk-delete/',role,stream))
for label,role in [('already-clean-empty','admin'),('anonymous-denied','anonymous'),
                   ('default-removes-only-loose','admin'),('denied-standard','standard'),
                   ('denied-viewer','viewer'),('include-custom','admin'),('spaced-true-is-false','admin')]:
    specs.append(('group-cleanup','group-cleanup:'+label,'POST','/api/channels/groups/cleanup/',role,cleanup))

register_bytes=options.register.read_bytes()
assert sha(register_bytes)=='e5d512a14d24ec26a5d3c9ce0d58540172044d6c6e4a45a50d82b9bc2a46d960'
register=json.loads(register_bytes)
mapping={op['contract_id']:(route,op) for route in register['runtime_routes'] for op in route['operations']}
assert len(mapping)==798
bindings=collections.defaultdict(list)
for key,label,method,path,role,tool in specs:
    identifier=route_ids[key]
    route,op=mapping[identifier]
    assert 'executable_Source_contract' not in op and (op['method'] is None or op['method']==method)
    regexp=re.sub(r'<str:[^>]+>','[^/]+',route['pattern_exact'].replace('/^','/'))
    assert re.fullmatch(regexp,path.lstrip('/'))
    index,row=checks[label]
    detail=row['details']
    assert row['passed'] and detail['python_status']==detail['rust_status']
    assert re.fullmatch('[0-9a-f]{64}',detail['python_body_sha256'])
    bindings[identifier].append({
        'source_index':index,'label':label,'request_driver':tool,
        'method':method,'path':path,'role':role,'HTTP_status':detail['python_status'],
        'response_projection_sha256':detail['python_body_sha256'],
        'hash_scope':'retained controller response projection, including normalization when applicable',
    })
assert len(bindings)==7 and sum(map(len,bindings.values()))==22
assert len({r['source_index'] for cases in bindings.values() for r in cases})==22
contracts=[]
for identifier,cases in sorted(bindings.items()):
    route,op=mapping[identifier]
    roles=sorted({row['role'] for row in cases})
    contracts.append({
        **{k:op[k] for k in ('contract_id','method','source_action','owning_milestone')},
        'pattern_exact':route['pattern_exact'],'source_handler':route['source_handler'],
        'fixture_id':'retained-Image22-TS-stream-group',
        'observed_methods':sorted({row['method'] for row in cases}),
        'observed_roles':roles,
        'source_role_specs':{role:{'fixture_role':role,'complete':False} for role in roles},
        'assertions':['Match named retained Original controller requests to literal target and resolved operation',
                      'Match Original HTTP response status and controller projection hash'],
        'cases':cases,'fully_verified':False,
        'result':{'Source_case_observations':len(cases),'new_Source_cases_this_task':0,
                  'Native_coverage_increased':False},
    })
result={
    'scope':'Exact Original Image22 TS, streams and groups requests',
    'binding_kind':'retained_original_Image22_TS_stream_group_HTTP',
    'operation_count':len(contracts),'unique_Source_cases':22,
    'new_Source_executions':0,'fully_verified_operations':0,'Native_coverage_increased':False,
    'Original_author_credit':'Original Dispatcharr authors retain full application and algorithm credit.',
    'fixtures':{'retained-Image22-TS-stream-group':{
        'baseline_revision':report['source_baseline'],
        'private_capture':{'path':str((source/'integration.json').relative_to(root)),'sha256':sha(raw)},
        'request_tools_sha256':tools,
        'cleanup':report['cleanup'],
        'scope':'Retained request paths and role labels. Generated IDs are excluded.'}},
    'contracts':contracts,
}
options.output.write_text(json.dumps(result,indent=2)+'\n')
