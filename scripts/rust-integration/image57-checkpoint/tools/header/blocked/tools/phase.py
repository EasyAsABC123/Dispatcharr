"""Finite SQL/Bearer Main requests. No Source or app activity on import."""
import hashlib
import json
import os
from pathlib import Path
import socket
import time
from absolute_http import Observation
from qualify import phase as qualify


def request(base, address, path, headers, method, deadline, prefix=False):
    remaining = min(8, deadline - time.monotonic())
    if remaining <= 0: raise TimeoutError('HeaderPhase90Seconds')
    owner = Observation(base, seconds=remaining, address=address)
    value = {'request_started': time.monotonic(), 'request_method': method, 'request_path_sha256': hashlib.sha256(path.encode()).hexdigest()}
    try:
        response = owner.request(path, headers, method=method)
        value.update(status=response.status_code, headers=[list(row) for row in response.raw.getheaders()], headers_received=time.monotonic())
        data = response.raw.read(8192 if prefix and response.status_code in (200,206) else 65537)
        if prefix and response.status_code in (200,206): value.update(prefix_bytes=len(data), prefix_sha256=hashlib.sha256(data).hexdigest())
        else:
            if len(data) > 65536: raise ValueError('HeaderErrorBodyCapacity')
            value.update(body_hex=data.hex(), body_sha256=hashlib.sha256(data).hexdigest())
        value['absolute_observation'] = owner.complete()
    except Exception as error: value['transport_error'] = type(error).__name__
    finally:
        value['physical_close'] = owner.cancel()
        value['request_completed'] = time.monotonic()
    return value


def execute(seed, policy, mode, base, address, output, witness, origin, caps, nonce, requester=request):
    if mode not in ('source','native'): raise ValueError('FixedBackend')
    output = Path(output); started = time.monotonic(); deadline = started + 90
    token = {'Authorization': 'Bearer ' + seed['access_token'], 'User-Agent': 'owned-worker-header'}
    me = requester(base,address,'/api/accounts/users/me/',token,'GET',deadline)
    # Persist even an auth mismatch before raising; never discard its raw data.
    auth_path=output/('worker-header-'+mode+'-auth-private.json');auth_path.write_text(json.dumps(me,indent=2)+'\n');auth_path.chmod(0o600)
    if me.get('status') != 200 or json.loads(bytes.fromhex(me['body_hex']))['id'] != seed['real_user_id']: raise ValueError('ActualSQLBearerIdentity')
    value = {'phase_started':started,'mode':mode,'actual_SQL_Bearer_user_verified':True,'kernel_witness':witness,'fixed_caps_before':caps(),'cases':[],'body_scope':'only8192prefix; deliberateclose; not continuous delivery','resource_sampler':False,'Source_invalid_HEAD_request_executed':False,'fixture_nonce':nonce}
    paths = {
        'head405': '/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-header-healthy',
        'invalid_get401': '/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-header-auth',
        'no_provider503': '/proxy/vod/movie/'+seed['missing_provider_uuid']+'/fixture-header-missing',
        'bad_uuid404': '/proxy/vod/movie/not-a-uuid/fixture-header-route',
        'full200': '/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-header-healthy',
        'range206': '/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-header-healthy',
        'provider404_to500': '/proxy/vod/movie/'+seed['error_movie_uuid']+'/fixture-header-error',
        'invalid_head_diagnostic': '/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-header-auth-head',
    }
    for name in policy['Source_cases' if mode=='source' else 'Native_cases']:
        headers = dict(token)
        if name.startswith('invalid_'): headers['Authorization']='Bearer invalid'
        if name=='range206': headers['Range']='bytes=0-8191'
        method='HEAD' if name in ('head405','invalid_head_diagnostic') else 'GET'
        before=origin()
        row=requester(base,address,paths[name],headers,method,deadline,prefix=name in ('full200','range206'))
        row.update(case=name,origin_before=before,origin_after=origin())
        if name=='invalid_head_diagnostic': row.update(static_Source_prediction=401,Source_actual_request_executed=False,parity_gate=False,compatibility_boundary_open=True)
        value['cases'].append(row)
        progress=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode()
        if len(progress)>2097152:raise ValueError('HeaderPhaseEvidenceCapacity')
        pending=output/('worker-header-'+mode+'-partial-private.json');pending.write_bytes(progress);pending.chmod(0o600)
    wait=3
    if time.monotonic()+wait>deadline: raise TimeoutError('PhaseBeforeFixedSettlementWait')
    time.sleep(wait)
    value['fixed_caps_after']=caps();value['phase_completed']=time.monotonic()
    try: value['qualification']=qualify(value,policy,mode)
    except Exception as error: value['qualification_error']=type(error).__name__+':'+str(error)[:128];value['qualification']={'capture_valid':False,'worker_header_branch_scope_qualified':False,'full_header_or_M4_parity':False}
    raw=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode()
    if len(raw)>2097152: raise ValueError('HeaderPhaseEvidenceCapacity')
    path=output/('worker-header-'+mode+'-private.json');path.write_bytes(raw);path.chmod(0o600)
    return value


def main():
    if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only': raise RuntimeError('OwnedHeaderPhaseRequired')
    import sys
    sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
    import django;django.setup()
    from django.db import connections
    from apps.m3u.models import M3UAccount,M3UAccountProfile
    root=Path('/evidence');seed=json.loads((root/'pending-main-private.json').read_text());policy=json.loads(Path('/header-root/policy.json').read_text());mode=os.environ['HEADER_MODE'];base=os.environ['HEADER_BASE']
    from urllib.parse import urlsplit
    address=socket.gethostbyname(urlsplit(base).hostname)
    witness=json.loads((root/('kernel-'+mode+'.json')).read_text());nonce=os.environ['PENDING_MAIN_NONCE']
    def origin():
        row=request('http://127.0.0.1:8080',socket.gethostbyname('origin'),'/ledger',{'X-Fixture-Nonce':nonce},'GET',time.monotonic()+3)
        if row.get('status')!=200: raise ValueError('OwnedOriginSnapshot')
        data=json.loads(bytes.fromhex(row['body_hex']))
        if data.get('invalid') or data.get('nonce_sha256')!=hashlib.sha256(nonce.encode()).hexdigest(): raise ValueError('OwnedOriginNonceOrOverflow')
        return data
    def caps():
        try:return {'account':M3UAccount.objects.get(pk=seed['provider_account_id']).max_streams,'profile':M3UAccountProfile.objects.get(pk=seed['provider_profile_id']).max_streams}
        finally:connections.close_all()
    try:value=execute(seed,policy,mode,base,address,root,witness,origin,caps,nonce)
    finally:connections.close_all()
    raise SystemExit(0 if value['qualification']['worker_header_branch_scope_qualified']else 1)


if __name__=='__main__':main()
