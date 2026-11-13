"""Optional independent private-raw projection check; public verification never needs it."""
import argparse,json,hashlib
from pathlib import Path
import verify as V
import projection_rules as P
HERE=Path(__file__).resolve().parent
def check(repo):
 bindings=V.read(HERE/'projection-bindings.json');count=0
 for name,row in bindings.items():
  raw=Path(repo)/row['private_raw_path'];V.need(V.sha(raw)==row['private_raw_sha256'],'ExactPrivateRaw:'+name)
  if 'Source-signals.json'in name:value=[json.loads(x)for x in raw.read_text().splitlines()]
  else:value=P.clean(json.loads(raw.read_bytes())if raw.suffix=='.json'else{'retained_text':raw.read_text()},row['private_raw_path'])
  if name.endswith('Main/notifications/notification-Main-private.json'):
   original=json.loads(raw.read_bytes())
   for role,v in value['actual_auth_requests'].items():
    a=original['actual_auth_requests'][role];body=json.loads(bytes.fromhex(a['body_hex']));v['identity_projection']={k:body[k]for k in ['id','user_level','stream_limit']};v['private_full_body_sha256']=a['body_sha256']
  V.need(value==V.read(HERE/name),'ExactFieldProjection:'+name);V.need(V.sha(HERE/name)==row['projection_sha256'],'ProjectionSHA');count+=1
 data=V.read(HERE/'main4s/retained.json');base=Path(repo)/'artifacts/rust/m4-vod-four-second-main-image55-1';V.need(data['summary']==V.read(base/'summary.json'),'ExactFullMain4sSummary')
 for case in ['success','busy','clientloss']:
  seed=V.read(base/case/'pending-main-private.json');V.need(data['seed_non_auth_projection'][case]=={k:v for k,v in seed.items()if k!='access_token'},'ExactSeedExclusion');V.need(data['Source_signals'][case]==[json.loads(x)for x in(base/case/'Source-request-signals.jsonl').read_text().splitlines()],'ExactSignalRows')
 return {'passed':True,'actual_private_projection_bindings':count,'Main4s_full_raw_summary_exact':True,'application_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);a=p.parse_args();print(json.dumps(check(a.repo),indent=2))
