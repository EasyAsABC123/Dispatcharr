from pathlib import Path
import os,subprocess,json,time,hashlib,shutil
O=Path(__file__).resolve().parent;R=O.parents[2];S=O/'source';expected=json.loads((O/'source-inputs.json').read_text());env=dict(os.environ,CARGO_TARGET_DIR=str(R/'artifacts/rust/m7-xc-vod-info-physical-compile-29/target'),TMPDIR=str(O/'tmp'),CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0');Path(env['TMPDIR']).mkdir(exist_ok=True);sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();waits=[]
def verify():assert{str(p.relative_to(S)):sha(p)for p in S.rglob('*')if p.is_file()}==expected
commands=[('fmt',['cargo','fmt','--all','--check']),('check',['cargo','check','--workspace','--offline','--locked']),('strict',['cargo','clippy','--workspace','--all-targets','--offline','--locked','--','-D','warnings']),('defaults',['cargo','test','--workspace','--offline','--locked'])]
for stage,argv in commands:
 verify();began=time.time()
 with(O/(stage+'.log')).open('xb')as f:
  p=subprocess.Popen(argv,cwd=S/'rust',env=env,stdout=f,stderr=subprocess.STDOUT);code=p.wait()
 verify();waits.append({'stage':stage,'argv':argv,'PID':p.pid,'PID_namespace':'actual exec namespace, host only when tool escalated','began_epoch':began,'completed_epoch':time.time(),'actual_exit':code,'all847source_inputs_unchanged':True});(O/'waits.json').write_text(json.dumps(waits,indent=2)+'\n')
 if code:print('CallbackUnion842 negative',stage,code,flush=True);raise SystemExit(code)
print('CallbackUnion842 fmt/check/strict/full defaults all0; no Docker/application/performance acceptance',flush=True)
