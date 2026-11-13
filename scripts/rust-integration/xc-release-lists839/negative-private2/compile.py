import json,os,subprocess,time,hashlib
from pathlib import Path
R=Path.cwd();p=R/'artifacts/rust/m7-xc-main-release-lists-private-2';s=p/'source';m=json.loads((p/'source-map.json').read_text());sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
assert all(sha(s/k)==v for k,v in m.items())
b=p/'build-1';b.mkdir(exist_ok=False);target=R/'artifacts/rust/target-display-timezone-2';assert target.is_dir();tmp=b/'tmp';tmp.mkdir()
env=dict(os.environ);env.update(CARGO_TARGET_DIR=str(target),TMPDIR=str(tmp),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_INCREMENTAL='0')
commands=[['cargo','fmt','--all','--','--check'],['cargo','check','--offline','--locked','-p','dispatcharr-xc-http'],['cargo','clippy','--offline','--locked','-p','dispatcharr-xc-http','--all-targets','--','-D','warnings'],['cargo','test','--offline','--locked','-p','dispatcharr-xc-http','--lib','vod_' ,'--','--nocapture'],['cargo','test','--offline','--locked','-p','dispatcharr-xc-http','--lib','--no-run']]
rows=[]
for i,cmd in enumerate(commands):
 log=b/f'{i}.log';start=time.time();print(json.dumps({'stage':i,'argv':cmd,'started':start}),flush=True)
 with log.open('xb') as f:
  process=subprocess.Popen(cmd,cwd=s/'rust',env=env,stdout=f,stderr=subprocess.STDOUT);code=process.wait()
 row={'argv':cmd,'pid':process.pid,'began':start,'ended':time.time(),'wait_exit':code,'poll_exit':process.poll(),'log':str(log),'log_sha256':sha(log)};rows.append(row);(b/'actual.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)
 if code:break
assert all(sha(s/k)==v for k,v in m.items());(b/'settlement.json').write_text(json.dumps({'all_stages':len(rows)==5,'all_actual0':all(x['wait_exit']==0 and x['poll_exit']==0 for x in rows),'source_map_unchanged':True,'settled':True,'ended':time.time(),'no_apps':True},indent=2)+'\n')
raise SystemExit(0 if len(rows)==5 and all(x['wait_exit']==0 for x in rows) else 1)
