"""Explicit retained private SHA/projection equality, without printing credentials."""
import argparse,gzip,json,hashlib
from pathlib import Path
import verify as V
HERE=Path(__file__).resolve().parent
SECRET={'password','api_key','access_token','refresh_token','authorization','proxy-authorization','cookie','set-cookie','secret'}
def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def sanitize(v):
    if isinstance(v,dict):return{k:({'private_field_sha256':canonical(x)}if k.lower()in SECRET else sanitize(x))for k,x in v.items()}
    if isinstance(v,list):return[sanitize(x)for x in v]
    return v

def verify(repo):
    repo=Path(repo).resolve();bindings=V.read(HERE/'private-raw-bindings.json');keys=[]
    for name,row in bindings.items():
        p=repo/row['private_relative_path']
        if V.sha(p)!=row['private_raw_sha256']:raise ValueError('ExactPrivateRawSHA:'+name)
        raw=json.loads(p.read_bytes())
        if name.endswith('execution-projection.json'):
            projected=raw.copy();projected['native_processes']={k:{**{a:b for a,b in x.items()if a!='argv'},'private_argv_sha256':canonical(x['argv'])}for k,x in raw['native_processes'].items()}
        elif name.endswith('container-identity-projection.json'):
            x=raw[0]if isinstance(raw,list)else raw;projected={k:x[k]for k in ['Id','Image','State']}
        else:projected=sanitize(raw)
        published=V.compressed(HERE/name)if name.endswith('.gz')else V.read(HERE/name)
        if projected!=published:raise ValueError('ExactPrivacyProjection:'+name)
        def collect(v):
            if isinstance(v,dict):
                for k,x in v.items():
                    if k.lower()in ('api_key','access_token','refresh_token')and isinstance(x,str)and len(x)>=8:keys.append(x.encode())
                    collect(x)
            elif isinstance(v,list):
                for x in v:collect(x)
        collect(raw)
    for p in HERE.rglob('*'):
        if not p.is_file()or'__pycache__'in p.parts:continue
        data=gzip.decompress(p.read_bytes())if p.suffix=='.gz'else p.read_bytes()
        if any(k in data for k in set(keys)):raise ValueError('PrivateAuthValuePublished')
    return {'passed':True,'all_explicit_private_raw_SHA_and_credential_digest_projections_exact':True,'actual_private_API_JWT_values_absent':True,'private_values_printed':False,'application_executed':False}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);a=p.parse_args();print(json.dumps(verify(a.repo),indent=2))
