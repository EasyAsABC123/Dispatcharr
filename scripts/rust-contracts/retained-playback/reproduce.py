"""Project executed Original playback leaves onto the pinned route register."""
import argparse
import ast
import collections
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import uuid
from urllib.parse import quote, unquote, urlencode, urlsplit

COMMIT = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
VOD_ROOT = 'artifacts/rust/m4-vod-source-alias-built-react-1/'
XC_ROOT = 'artifacts/rust/m4-timeshift-xc-source-1/'
STOP_ROOT = 'artifacts/rust/m4-vod-admin-stop-entry-source-214/'
PROBE = 'scripts/rust-vod-contracts/probe.py'
XC_PROBE = 'scripts/rust-timeshift-contracts/xc_probe.py'
STOP_PROBE = 'scripts/rust-vod-contracts/active_restart_http_source.py'
CATEGORY_SOURCE = 'scripts/rust-vod-api-contracts/source-vod-rest-1.json'
CATEGORY_RECEIPT = 'scripts/rust-vod-api-contracts/verified-vod-rest-source-1.json'
VOD_BARE = 'route-8b36b8a06cbc1772e58c:METHOD_UNRESOLVED'
VOD_SESSION = 'route-3a8ec6e112801f3eefc1:METHOD_UNRESOLVED'
VOD_PROFILE = 'route-c0eb97456a3910a2d90a:METHOD_UNRESOLVED'
XC_PATH = 'route-96eaf62e9698df611d43:METHOD_UNRESOLVED'
XC_QUERY = 'route-65fd01892db0a2d662ac:METHOD_UNRESOLVED'
STOP = 'route-4d905eb7a7b8071e69a7:METHOD_UNRESOLVED'
CATEGORY = 'route-d3e6d54d07a91cc0ad84:GET'

def sha(data):
    return hashlib.sha256(data).hexdigest()

def digest(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())

def read(root, name, expected):
    data = (root / name).read_bytes()
    assert sha(data) == expected, name
    return data

def receipt(path, sha256):
    return {'path':path, 'sha256':sha256}

def load(root, path, sha256):
    return json.loads(read(root, path, sha256))

def observed_original(value):
    assert value['source_commit'] == COMMIT and value['source_image_id'] == IMAGE
    assert value['passed'] is True and value['cleanup_passed'] is True

def leaf(index, part, target, method, role, status, record, identifier, **provenance):
    assert type(status) is int and 100 <= status <= 599
    return {'contract_id':identifier, 'source_index':index, 'record_json_pointer':part,
            'path':target, 'method':method, 'role':role, 'HTTP_status':status,
            'response_projection_sha256':digest(record), **provenance}

def vod_leaves(public, private):
    run_hash = '212ddaa439069566886834a46ee1685efad2cf29986137d1e94ee644fda6f006'
    seed_hash = '833fea99cf5671e401348bb6714a1b3574d69a9d64de2694bc4c387898d62311'
    alias_path = 'scripts/rust-vod-contracts/profile-alias-characterization.json'
    alias_hash = '675a7e9e29b2d0b80c785538af6d865ec166333acc0a31e6fae79c88f0395508'
    probe_hash = '431c95e35c44a0b315d8ffa54a21d3b23c2ad0362f9c880c35dc1793812ae45a'
    report = load(private, VOD_ROOT+'vod-contracts.json', run_hash)
    seed = load(private, VOD_ROOT+'vod-seed.json', seed_hash)
    alias = load(public, alias_path, alias_hash)
    assert report['source_commit'] == COMMIT and report['source_image_id'] == IMAGE
    assert report['cleanup_passed'] is True and report['passed'] is False
    differences = report['wire_comparison']['differences']
    assert len(differences) == 14
    for row in differences:
        assert row['field'] in ('headers','mint')
        def normalize(value):
            value = copy.deepcopy(value)
            headers = value['headers'] if row['field'] == 'mint' else value
            headers['allow'] = sorted(part.strip() for part in headers['allow'].split(','))
            return value
        assert normalize(row['baseline']) == normalize(row['candidate'])
    assert alias['source_commit'] == COMMIT and alias['cleanup_passed']
    assert alias['evidence'] == VOD_ROOT+'vod-contracts.json'
    bare = '/proxy/vod/movie/' + str(uuid.UUID(seed['movie_uuid']))
    assert seed['native_path'] == bare
    assert alias['results'] == report['profile_aliases']['results']
    tree = ast.parse(read(public, PROBE, probe_hash))
    probe = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'probe')
    loop = next(node for node in probe.body if isinstance(node, ast.For))
    assert isinstance(loop.iter, ast.List)
    order = ast.literal_eval(loop.iter)
    wire = report['wire']['events']
    assert [row['case'] for row in wire] == [row[0] for row in order]
    results = []
    for i, (row, (_, method, _)) in enumerate(zip(wire, order)):
        base = f'/wire/events/{i}'
        mint = row['mint']
        results.append(leaf(i, base+'/mint', bare, method, 'anonymous', mint['status'], mint, VOD_BARE))
        if method == 'GET' and mint['status'] == 301:
            target = mint['headers']['location']
            assert target == bare+'/<session>'
            results.append(leaf(i, base, target, method, 'anonymous', row['status'], row, VOD_SESSION))
    for i, row in enumerate(alias['results']):
        profile = row['profile_id']
        base = f'/profile_aliases/results/{i}'
        assert row['mint_status'] == 301
        results.append(leaf(i, base, f'{bare}/{profile}/', 'GET', 'anonymous',
                            row['mint_status'], row, VOD_PROFILE, response_phase='mint'))
        follow = row['mint_headers']['location']
        if profile == 0:
            assert follow == bare+'/0/<session>'
        else:
            assert follow == f'{bare}/{profile}/<session>/{profile}/'
    return results, {'raw_capture':receipt(VOD_ROOT+'vod-contracts.json',run_hash),
                     'raw_seed':receipt(VOD_ROOT+'vod-seed.json',seed_hash),
                     'published_alias_projection':receipt(alias_path,alias_hash),
                     'request_plan':receipt(PROBE,probe_hash),
                     'captured_controller_passed':False,
                     'controller_scope':'Combined Original/Native comparison failed on Allow header ordering. Only retained Original responses are used.',
                     'excluded':{'profile_follows':{'case_count':2,
                                                    'reason':'Both targets include an extra profile segment and do not bind to the VOD profile-session operation'}}}

def xc_leaves(public, private):
    run_hash = '0c7e1fa493e0d3e0abfc05af8232d3023c0ed1904ab991c1a0816b5be8ea4a95'
    role_path = 'scripts/rust-timeshift-contracts/python-xc-route-goldens.json'
    role_hash = 'ad2bd18076fdaba4b385dd351bda6aca7724f46434a90d98ec844ef6342446ab'
    probe_hash = 'b0406feba0dbc3f0d81e3248b29e422d491ecc201883c4876df4a14220bc8974'
    report = load(private, XC_ROOT+'vod-contracts.json', run_hash)
    golden = load(public, role_path, role_hash)
    observed_original(report)
    observed_original(golden)
    assert golden['evidence_sha256'] == run_hash
    wire = report['timeshift_xc_source']['events']
    assert golden['events'] == wire and len(wire) == 23
    assert golden['credentials_and_opaque_locations_normalized']
    tree = ast.parse(read(public, XC_PROBE, probe_hash))
    capture = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'capture')
    fetch = next(node for node in capture.body if isinstance(node, ast.FunctionDef) and node.name == 'fetch')
    fetch.body = ast.parse('observe(case, target, method)').body
    users = {role:role for role in ('owner','viewer','disabled','inactive')}
    results = []
    def observe(case, target, method):
        index = len(results)
        record = wire[index]
        assert (record['case'],record['method']) == (case,method)
        path = urlsplit(target).path
        if path.startswith('/timeshift/'):
            _, _, role, supplied, duration, timestamp, content = path.split('/')
            assert role in users and supplied in ('xc-fixture-only','wrong-fixture-only')
            normalized = f'/timeshift/<username>/<password>/{duration}/{timestamp}/{unquote(content)}'
            identifier = XC_PATH
        else:
            assert path == '/streaming/timeshift.php'
            role = 'owner' if '?' in target else 'anonymous'
            normalized = path
            identifier = XC_QUERY
        results.append(leaf(index, f'/timeshift_xc_source/events/{index}', normalized, method,
                            role, record['status'], record, identifier, request_case=case))
    # All network work stays out of this execution. The three original
    # request constructors execute against substitute fixture identifiers.
    control = lambda action, label: {'usernames':users,'channel_id':'<channel>'} if action == 'begin' else None
    ns = {'observe':observe,'build_opener':lambda *_:None,'NoRedirect':lambda:None,
          'datetime':datetime,'timezone':timezone,'urlencode':urlencode,'quote':quote,'uuid':uuid}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[capture],type_ignores=[])),XC_PROBE,'exec'),ns)
    ns['capture']('unused',control,'unused')
    assert len(results) == len(wire)
    return results, {'raw_capture':receipt(XC_ROOT+'vod-contracts.json',run_hash),
                     'published_projection':receipt(role_path,role_hash),
                     'request_plan':receipt(XC_PROBE,probe_hash),
                     'path_scope':'Credential and channel placeholders refer to fixture role and resolver path shape; raw credentials and opaque queries omitted.'}

def stop_leaves(private):
    run_hash = 'ab70b9fff814633b4b5a95bded2ecc0c6c392b9ed0f4d85fb5bd8419ef993c4c'
    probe_hash = '0e54b4fe817c4f996dcf04a597bcc9e63c452e22840a0fd5f7f27b1b5a43e78b'
    path = STOP_ROOT+'tool-source/'+STOP_PROBE
    report = load(private, STOP_ROOT+'vod-contracts.json', run_hash)
    assert report['source_commit'] == COMMIT and report['passed'] and report['cleanup_passed']
    assert report['tool_source_sha256'][STOP_PROBE] == probe_hash
    source = read(private, path, probe_hash)
    request = next(node for node in ast.walk(ast.parse(source)) if isinstance(node,ast.Call)
                   and ast.unparse(node.func) == 'urllib.request.Request' and
                   len(node.args) and ast.unparse(node.args[0]) == "base + '/proxy/vod/stop_client/'")
    kwargs={item.arg:ast.unparse(item.value) for item in request.keywords}
    assert kwargs['method'] == "'POST'" and "'Bearer ' + admin_token" in kwargs['headers']
    assert b'RefreshToken.for_user(administrator).access_token' in source
    active = report['active_vod_restart_source']
    assert active['capture_complete'] and active['original_http_capture']
    result = active['admin_stop']
    assert result['status'] == 200 and type(result['user_id']) is int
    return [leaf(0,'/active_vod_restart_source/admin_stop','/proxy/vod/stop_client/',
                 'POST','admin',result['status'],result,STOP)], {
        'raw_capture':receipt(STOP_ROOT+'vod-contracts.json',run_hash),
        'frozen_request_plan':receipt(path,probe_hash),
        'source_capture_transport':report['active_source_transport']}

def category_leaves(public, private):
    dataset_hash = '70980c8e7b8935735bdd0c699202e106ae869becba833c864be8459cbf58a3b6'
    report_hash = '6df1a9f16997b0f926383d6e5133641d0cbbb3261b6c0c6f666aee649d2b8d95'
    verified = load(public, CATEGORY_RECEIPT, report_hash)
    dataset = load(public, CATEGORY_SOURCE, dataset_hash)
    assert dataset['baseline_commit'] == COMMIT
    assert verified['sanitized_dataset_sha256'] == dataset_hash
    evidence = verified['modes']['http']
    assert evidence['passed'] and evidence['cases'] == 463
    assert evidence['cleanup'] == 'owned Compose project removed'
    raw_path, raw_hash = evidence['private_corpus'], evidence['private_corpus_sha256']
    raw = load(private, raw_path, raw_hash)
    http = dataset['modes']['http']
    assert raw['source_network_capture_complete'] and len(raw['cases']) == len(http['cases']) == 463
    results = []
    for row in http['cases']:
        if row['method'] != 'GET' or not re.fullmatch(r'/api/vod/categories/[^/.]+/',urlsplit(row['path']).path):
            continue
        index = row['index']
        original = raw['cases'][index]
        assert all(original[key] == row[key] for key in ('path','role','method'))
        observed = http['responses'][row['expected_ref']]
        assert all(original['expected'][key] == observed[key] for key in ('status','body_sha256'))
        results.append(leaf(index,f'/modes/http/cases/{index}',row['path'],row['method'],
                            row['role'] or 'anonymous',observed['status'],observed,CATEGORY,
                            response_body_sha256=observed['body_sha256']))
    assert len(results) == 15
    return results, {'raw_capture':receipt(raw_path,raw_hash),
                     'published_projection':receipt(CATEGORY_SOURCE,dataset_hash),
                     'published_capture_receipt':receipt(CATEGORY_RECEIPT,report_hash)}

def build(root, private):
    register_name='docs/rust-contract-register.json'
    register_hash='b08682e6416a669341e9721251dba49c43b6a313b358808e61ba4057262f2039'
    register = load(root, register_name, register_hash)
    by_id = {item['contract_id']:(route,item) for route in register['runtime_routes']
             for item in route['operations']}
    assert len(by_id) == 798
    cohorts={'vod-retained':vod_leaves(root,private),'xc-retained':xc_leaves(root,private),
             'vod-stop-retained':stop_leaves(private),'vod-category-retained':category_leaves(root,private)}
    fixtures = {}
    cases = collections.defaultdict(list)
    owner = {}
    for name, (rows, fixture) in cohorts.items():
        fixtures[name] = {'baseline_revision':COMMIT, 'baseline_image':IMAGE,
                          'scope':'Retained actual Original HTTP execution, partial operation evidence', **fixture}
        for row in rows:
            identifier = row['contract_id']
            cases[identifier].append(row)
            assert owner.setdefault(identifier,name) == name
    contracts = []
    for identifier, rows in sorted(cases.items()):
        route, operation = by_id[identifier]
        assert 'executable_Source_contract' not in operation and operation['fully_verified'] is False
        pattern = route['pattern_exact'].replace('/^','/')
        pattern = re.sub(r'<(?:uuid|str):[^>]+>',r'[^/]+',pattern)
        pattern = re.sub(r'<int:[^>]+>',r'[0-9]+',pattern)
        for row in rows:
            assert re.fullmatch(pattern, urlsplit(row['path']).path.lstrip('/'))
            assert operation['method'] is None or operation['method'] == row['method']
        roles=sorted({row['role'] for row in rows})
        contracts.append({
            'contract_id':identifier,'fixture_id':owner[identifier],
            'pattern_exact':route['pattern_exact'],'source_handler':route['source_handler'],
            **{name:operation[name] for name in ('method','source_action','owning_milestone')},
            'observed_methods':sorted({row['method'] for row in rows}), 'observed_roles':roles,
            'source_role_specs':{role:{'fixture_role':role,'complete':False} for role in roles},
            'assertions':['Existing exact resolver ID is consistent with retained request target shape',
                          'Status and method are retained Original HTTP observations'],
            'cases':rows, 'fully_verified':False,
            'result':{'Source_case_observations':len(rows),'new_Source_cases_this_task':0,
                      'Native_coverage_increased':False}
        })
    prior = sum('executable_Source_contract' in item for _,item in by_id.values())
    assert prior == 250 and set(cases) == {VOD_BARE,VOD_PROFILE,VOD_SESSION,XC_PATH,XC_QUERY,STOP,CATEGORY}
    candidate = copy.deepcopy(register)
    ids = {row['contract_id']:row for row in contracts}
    candidate_rows = [item for route in candidate['runtime_routes'] for item in route['operations']]
    for item in candidate_rows:
        if item['contract_id'] in ids:
            item['candidate_retained_Source_contract'] = ids[item['contract_id']]
    untouched = sum(by_id[item['contract_id']][1] == item for item in candidate_rows)
    assert untouched == 791
    return {
        'binding_kind':'retained_original_playback_VOD_XC_TCP',
        'scope':'Retained Original playback request assignments',
        'Original_author_credit':'All application and algorithm credit remains with original Dispatcharr authors.',
        'based_on_revision':'8617a53fe37678ed13234abe7522b79130aa4a92',
        'based_on_register_sha256':register_hash,
        'existing_Source_assignments_unchanged':prior,
        'operation_declarations_unchanged':len(by_id),
        'unrelated_operation_dictionaries_unchanged':untouched,
        'candidate_partial_assignments':prior+len(contracts),
        'candidate_unassigned_operations':len(by_id)-prior-len(contracts),
        'fully_verified_operations':0, 'new_Source_executions':0,'Native_coverage_increased':False,
        'operation_count':len(contracts),'unique_Source_cases':sum(map(len,cases.values())),
        'fixtures':fixtures,'contracts':contracts}

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base',type=Path,required=True)
    parser.add_argument('--private-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    args.output.write_text(json.dumps(build(args.base,args.private_root),indent=2)+'\n')
