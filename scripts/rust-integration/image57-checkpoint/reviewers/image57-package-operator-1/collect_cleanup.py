"""Independent exact-owned Docker inventory only; uses sealed bound tooling."""
import argparse,hashlib,json,os,re,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
S=ROOT/'artifacts/rust/image57-matrix-scheduler-1'
sys.path.insert(0,str(S));from tooling_guard import assert_inputs
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def collect(projects,out):
    if not projects or len(set(projects))!=len(projects)or not all(re.fullmatch('dispatcharr-[A-Za-z0-9_-]+',p)for p in projects):raise ValueError('ExactOwnedProjectsRequired')
    env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(json.loads((S/'inputs/environment.json').read_text()));meta=json.loads((S/'inputs/tooling-inputs.json').read_text());assert_inputs(env,meta);rows=[];inventories={'containers':[],'networks':[],'volumes':[]}
    for project in projects:
        for key,args in [('containers',['container','ls','--all','--format','{{.ID}}']),('networks',['network','ls','--format','{{.ID}}']),('volumes',['volume','ls','--format','{{.Name}}'])]:
            argv=[meta['docker']['path']]+args+['--filter','label=com.docker.compose.project='+project];r=subprocess.run(argv,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
            rows.append({'project':project,'kind':key,'argv':argv,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr});
            if r.returncode:raise ValueError('IndependentInventoryQueryFailed')
            inventories[key]+=r.stdout.splitlines()
    result={'passed':not any(inventories.values()),'scope':'independent exact-owned Docker inventories; no resource removal/creation','projects':projects,'queries':rows,'inventories':inventories,'bound_environment_file_sha256':sha(S/'inputs/environment.json'),'tooling_metadata_sha256':sha(S/'inputs/tooling-inputs.json')}
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    with out.open('x')as f:f.write(json.dumps(result,indent=2)+'\n')
    return result
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--project',action='append',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=collect(a.project,a.output);print(json.dumps({'passed':r['passed'],'projects':r['projects'],'inventories':r['inventories']}));raise SystemExit(not r['passed'])
