"""Explicit optional private-raw equality check; no auth printing or app work."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import verify as V
HERE=Path(__file__).resolve().parent
SECRET={'password','api_key','access_token','refresh_token','authorization','proxy-authorization','cookie','set-cookie','secret'}
def sanitize(v):
    if isinstance(v,dict):return{k:({'private_field_sha256':V.canonical(x)}if k.lower()in SECRET else sanitize(x))for k,x in v.items()}
    if isinstance(v,list):return[sanitize(x)for x in v]
    return v

def notification_phase(v):
    value=copy.deepcopy(v)
    for row in value['actual_auth_requests'].values():
        original=copy.deepcopy(row);body=json.loads(bytes.fromhex(row.pop('body_hex')));row['private_full_body_sha256']=row.pop('body_sha256');row['identity_projection']={k:body[k]for k in ['id','user_level','stream_limit']};row['private_full_auth_response_sha256']=V.canonical(original)
    return sanitize(value)

def verify(repo):
    repo=Path(repo).resolve()
    # Complete Main4s summary and all six phases/signals/seed/PID projections.
    m=V.load(HERE/'main4s/verify_private.py','Main53_private_four_second');m.verify(repo/'artifacts/rust/m4-vod-four-second-main-image53-1')
    h=HERE/'worker-header';paths={'source-phase.json':'image53-worker-header-main-1/worker-header-source-private.json','source-failed-summary.json':'image53-worker-header-main-1/summary-private.json','native-phase.json':'image53-worker-header-native-1/worker-header-native-private.json','native-summary.json':'image53-worker-header-native-1/summary-private.json'}
    for public,private in paths.items():
        p=repo/'artifacts/rust'/private
        if V.read(p)!=V.read(h/public):raise ValueError('ExactHeaderRawProjection:'+public)
    for mode,rawname in [('source','image53-worker-header-main-1'),('native','image53-worker-header-native-1')]:
        base=repo/'artifacts/rust'/rawname;rawauth=V.read(base/('worker-header-'+mode+'-auth-private.json'));seed=V.read(base/'pending-main-private.json');body=json.loads(bytes.fromhex(rawauth['body_hex']))
        projected={k:rawauth[k]for k in ['request_started','request_completed','status','physical_close','absolute_observation','request_path_sha256']}
        projected.update(expected_real_SQL_user_id=seed['real_user_id'],auth_response_user_id=body['id'],auth_response_user_level=body['user_level'],auth_response_stream_limit=body['stream_limit'],private_auth_raw_sha256=V.sha(base/('worker-header-'+mode+'-auth-private.json')),private_seed_sha256=V.sha(base/'pending-main-private.json'),auth_header_and_API_key_values_not_copied=True)
        public=HERE/'worker-header'/('auth-identity-projection.json'if mode=='source'else'native-auth-identity-projection.json')
        if projected!=V.read(public):raise ValueError('ExactHeaderAuthIdentityProjection')
    for attempt in (1,2):
        root=repo/('artifacts/rust/image53-notification-main-'+str(attempt));pub=HERE/'notifications'
        if notification_phase(V.read(root/'notification-Main-private.json'))!=V.read(pub/('attempt'+str(attempt)+'-phase.json')):raise ValueError('ExactSanitizedNotificationPhase')
        summary=V.read(root/'summary-private.json');summary.pop('phase',None)
        if sanitize(summary)!=V.read(pub/('attempt'+str(attempt)+'-summary.json')):raise ValueError('ExactSanitizedNotificationSummary')
        binding=V.read(pub/'raw-bindings.json')[str(attempt)]
        if V.sha(root/'notification-Main-private.json')!=binding['raw_phase_sha256']or V.sha(root/'summary-private.json')!=binding['raw_summary_sha256']:raise ValueError('ExactNotificationRawHashes')
    # Exclusion scan checks actual private JWT/API-key values, not just key names.
    secrets=[]
    for root in ['m4-vod-four-second-main-image53-1/success','m4-vod-four-second-main-image53-1/busy','m4-vod-four-second-main-image53-1/clientloss','image53-worker-header-main-1','image53-worker-header-native-1']:
        seed=V.read(repo/'artifacts/rust'/root/'pending-main-private.json');secrets.append(seed['access_token'])
    for attempt in (1,2):
        root=repo/('artifacts/rust/image53-notification-main-'+str(attempt));seed=V.read(root/'notification-seed-private.json');secrets.extend([seed['admin_token'],seed['standard_token']]);phase=V.read(root/'notification-Main-private.json')
        for row in phase['actual_auth_requests'].values():
            body=json.loads(bytes.fromhex(row['body_hex']))
            if body.get('api_key'):secrets.append(body['api_key'])
    for name in ['image53-worker-header-main-1','image53-worker-header-native-1']:
        mode='source'if name.endswith('main-1')else'native'
        row=V.read(repo/'artifacts/rust'/name/('worker-header-'+mode+'-auth-private.json'));api=json.loads(bytes.fromhex(row['body_hex'])).get('api_key')
        if api:secrets.append(api)
    for p in HERE.rglob('*'):
        if p.is_file()and'__pycache__'not in p.parts:
            raw=p.read_bytes()
            if any(s.encode()in raw for s in secrets if s):raise ValueError('ActualPrivateAuthValuePublished')
    return {'passed':True,'complete_main4s_and_header_raw_and_sanitized_notification_projections_equal':True,'private_JWT_API_values_absent':True,'private_values_printed':False,'application_executed':False}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);a=p.parse_args();print(json.dumps(verify(a.repo),indent=2))
