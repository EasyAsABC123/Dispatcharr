from pathlib import Path
import os,subprocess,json,time,hashlib
O=Path(__file__).resolve().parent;S=O/'source';repo=O.parents[2];env=dict(os.environ,CARGO_TARGET_DIR=str(repo/'artifacts/rust/target-root-backup-callbacks-qa-1'),TMPDIR=str(O/'tmp'),CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0');Path(env['TMPDIR']).mkdir(exist_ok=True);waits=[]
commands=[('format',['cargo','fmt','--all']),('format-check',['cargo','fmt','--all','--check']),('test-build',['cargo','test','-p','dispatcharr-store','--lib','--no-run','--offline','--locked']),('strict',['cargo','clippy','-p','dispatcharr-store','--all-targets','--offline','--locked','--','-D','warnings'])]
for stage,argv in commands:
 began=time.time()
 with (O/(stage+'.log')).open('xb')as f:
  p=subprocess.Popen(argv,cwd=S/'rust',env=env,stdout=f,stderr=subprocess.STDOUT);code=p.wait()
 waits.append({'stage':stage,'argv':argv,'actual_PID':p.pid,'began_epoch':began,'finished_epoch':time.time(),'actual_exit':code});(O/'waits.json').write_text(json.dumps(waits,indent=2)+'\n')
 if stage=='format':
  mapping={str(p.relative_to(S)):hashlib.sha256(p.read_bytes()).hexdigest()for p in S.rglob('*')if p.is_file()};assert len(mapping)==812 and sum(k.startswith('rust/')for k in mapping)==807;(O/'source-map.json').write_text(json.dumps(mapping,indent=2)+'\n')
 if code:print('PRIVATE TEST BUILD NEGATIVE',stage,code,flush=True);raise SystemExit(code)
print('Private Store-only callback tests format/test-build/strict all0; no physical fixture executed',flush=True)
