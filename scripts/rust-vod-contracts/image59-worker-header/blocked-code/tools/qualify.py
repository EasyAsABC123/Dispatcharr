"""Header branch scope, capture validity and open compatibility stay distinct."""
import hashlib
import json
import math
import re
from origin_coverage import validate as origin_validate
from identity import verify_worker, worker


def media_total(ledger):
    rows = ledger.get('requests')
    if type(rows) is not list or not rows: raise ValueError('ActualOriginCoverage')
    ids = [r.get('id') for r in rows]
    if any(type(v) is not int for v in ids) or len(set(ids)) != len(ids) or ledger.get('invalid') != []: raise ValueError('OriginRequestIdentity')
    return sum(r.get('kind') in ('stream_GET', 'media_HEAD', 'preflight_GET') for r in rows)


def phase(value, policy, mode):
    expected = policy['Source_cases' if mode == 'source' else 'Native_cases']
    rows = value.get('cases', [])
    start,end=value.get('phase_started'),value.get('phase_completed')
    if any(type(x) not in (int,float) or not math.isfinite(x) for x in (start,end)) or not 0 < start <= end <= start+policy['backend_phase_max_seconds']: raise ValueError('BoundedPhaseClock')
    if [row.get('case') for row in rows] != expected: raise ValueError('AllFixedBranchesRequired')
    if value.get('actual_SQL_Bearer_user_verified') is not True or value.get('fixed_caps_before') != {'account': 2, 'profile': 2} or value.get('fixed_caps_after') != {'account': 2, 'profile': 2}: raise ValueError('ActualAuthAndFixedInputCaps')
    witness = value['kernel_witness']; seen = []
    if mode=='native' and (witness.get('pid')!=1 or witness.get('uid')!=65532 or witness.get('comm')!='dispatcharr-rs'): raise ValueError('ActualNativeMainIdentity')
    nonce=value.get('fixture_nonce')
    if type(nonce)is not str or not re.fullmatch('[0-9a-f]{32}',nonce):raise ValueError('OwnedFixtureNonce')
    prior=start
    for row in rows:
        began,completed=row.get('request_started'),row.get('request_completed')
        if any(type(x) not in (int,float) or not math.isfinite(x) for x in (began,completed)) or not prior <= began <= completed <= end or completed-began > policy['absolute_HTTP_observation_seconds']: raise ValueError('BoundedRequestClock')
        prior=completed
        if row.get('transport_error') or row.get('physical_close', {}).get('fd_after') != -1 or row.get('physical_close', {}).get('original_inode_absent') is not True or row.get('absolute_observation', {}).get('timer_joined') is not True: raise ValueError('PhysicalRequestOwnership')
        origin_validate(row['origin_before'],nonce);origin_validate(row['origin_after'],nonce)
        name = row['case']
        if name == 'invalid_head_diagnostic':
            if mode != 'native' or row.get('request_method')!='HEAD' or row.get('status')!=401 or row.get('body_hex')!='' or row.get('body_sha256')!=hashlib.sha256(b'').hexdigest(): raise ValueError('ActualNativeHEAD401EmptyWire')
            headers={}
            for key,val in row['headers']: headers.setdefault(key.lower(),[]).append(val)
            for key,want in [('content-length','172'),('allow','OPTIONS, GET'),('vary','Accept, origin'),('content-type','application/json'),('www-authenticate','Bearer realm="api"')]:
                if headers.get(key)!=[want]:raise ValueError('NativeHEADSourceField:'+key)
        if row.get('status') != policy['status_expectations'][name]: raise ValueError('FixedBranchStatus:' + name)
        if name in ('full200', 'range206'):
            seen.append(verify_worker(row['headers'], witness))
            if row.get('prefix_bytes') != 8192 or row.get('prefix_sha256') != policy['prefix_sha256']: raise ValueError('Actual8192BodyPrefix')
            new_media = [r for r in row['origin_after']['requests'] if r['id'] not in {x['id'] for x in row['origin_before']['requests']} and r.get('kind') == 'stream_GET']
            if not new_media or any(r.get('response_headers', {}).get('x-worker-id') != [policy['provider_response_canary']] for r in new_media): raise ValueError('ActualUpstreamSpoofCanaryRequired')
        elif worker(row['headers']) is not None: raise ValueError('SuccessOnlyWorkerHeader')
        if name in ('head405', 'invalid_get401', 'no_provider503', 'bad_uuid404', 'invalid_head_diagnostic'):
            if media_total(row['origin_before']) != media_total(row['origin_after']): raise ValueError('RejectedBranchContactedUpstream')
        if name == 'provider404_to500':
            new = [r for r in row['origin_after']['requests'] if r['id'] not in {x['id'] for x in row['origin_before']['requests']} and r.get('path_role') == 'probe1_movie']
            if not new or any(r.get('method') != 'GET' or r.get('response_status') != 404 for r in new): raise ValueError('LiteralProvider404NotRoute404Or503')
    if len(seen) != 2 or len(set(seen)) != 1: raise ValueError('StableSameProcessWorkerIdentity')
    return {'capture_valid': True, 'worker_header_branch_scope_qualified': True, 'full_header_or_M4_parity': False, 'separate_Source_HEAD_reference_applied':mode=='native', 'Native_invalid_HEAD': next((r['status'] for r in rows if r['case'] == 'invalid_head_diagnostic'), None), 'invalid_HEAD_single_case_scope_qualified':mode=='native', 'resource_acceptance': False}
