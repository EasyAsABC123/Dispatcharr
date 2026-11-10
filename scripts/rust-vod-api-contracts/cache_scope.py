#!/usr/bin/env python3
"""Actual disposable Redis physical ownership tests, not API parity/performance.
All application credit remains with original Dispatcharr authors/contributors.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from fixture_ports import write_port_overlay
IMAGE='redis@sha256:c5d34a98396a4775e223c74e929162c35c5a2182901ba26b50ade939941ce14b'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--frozen-rust-source',type=Path,help='Explicit coherent Rust source; defaults to freezing current workspace');args=parser.parse_args()
    out=args.output.resolve();out.mkdir(exist_ok=False,parents=True)
    report={'scope':__doc__,'passed':False,'source_api_verified':False,'performance_verified':False,'commands':[]}
    base=(args.frozen_rust_source or(ROOT/'rust')).resolve();frozen=out/'runtime/rust'
    if not(base/'Cargo.toml').is_file():raise RuntimeError('Rust source root requires Cargo.toml')
    for directory,names,files in os.walk(base):
        names[:]=[name for name in names if name not in ['target','.git','__pycache__']]
        for name in files:
            path=Path(directory)/name;target=frozen/path.relative_to(base);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['frozen_source_input']=str(base);report['source_scope']='Exact copied coherent Rust bytes; no dependency on historical private artifacts'
    report['frozen_rust_manifest_sha256']=hashlib.sha256(json.dumps({str(p.relative_to(frozen)):hashlib.sha256(p.read_bytes()).hexdigest()for p in frozen.rglob('*')if p.is_file()},sort_keys=True,separators=(',',':')).encode()).hexdigest()
    for path in [Path(__file__),ROOT/'scripts/rust-integration/fixture_ports.py']:
        target=out/'tools'/path.resolve().relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['ports']=write_port_overlay(out/'ports.yml',{'redis':[6379]});port=report['ports']['services']['redis'][0]['host']
    name='dispatcharr-cache-scope-'+uuid.uuid4().hex[:12];started=False
    def command(argv,timeout=120):
        report['commands'].append(argv);return subprocess.run(argv,cwd=ROOT,text=True,capture_output=True,check=True,timeout=timeout)
    try:
        report['image']=json.loads(command(['docker','image','inspect',IMAGE]).stdout)[0]['Id']
        command(['docker','run','-d','--rm','--name',name,'--read-only','--tmpfs','/tmp','-p','127.0.0.1:'+str(port)+':6379',IMAGE,
            'redis-server','--save','','--appendonly','no','--requirepass','playback-fixture-only']);started=True
        deadline=time.monotonic()+15
        while True:
            result=subprocess.run(['docker','exec',name,'redis-cli','--no-auth-warning','-a','playback-fixture-only','PING'],capture_output=True,text=True)
            if result.returncode==0 and result.stdout.strip()=='PONG':break
            if time.monotonic()>deadline:raise RuntimeError('Owned Redis readiness')
            time.sleep(.05)
        report['redis_version']=command(['docker','exec',name,'redis-server','--version']).stdout.strip()
        environment=dict(os.environ,REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:'+str(port),NOTIFICATION_SCOPE_FIXTURE='playback-fixture-only')
        cargo=['cargo','--manifest-path',str(frozen/'Cargo.toml'),'--target-dir',str(ROOT/'artifacts/rust/vod-auth-scope-cargo-target'),'--locked','--offline','-p','dispatcharr-store']
        clippy=[cargo[0],'clippy',*cargo[1:],'--all-targets','--','-D','warnings']
        with(out/'clippy.log').open('w')as log:subprocess.run(clippy,cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,text=True,timeout=240,check=True)
        report['tests']=[]
        for test in ['cancelled_cache_command_retains_physical_slot_and_scope','cache_logical_timeout_retains_pending_reply_and_composed_scopes','cache_cancelled_burst_cannot_reuse_sixteen_pending_slots','cache_waiter_bound_and_cancelled_queue_restore_admission','cache_and_user_agent_server_error_classifications_stay_distinct']:
            test_name='notifications::tests::'+test;argv=[cargo[0],'test',*cargo[1:],'--lib',test_name,'--','--exact','--ignored','--nocapture','--test-threads=1']
            with(out/(test+'.log')).open('w')as log:subprocess.run(argv,cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,text=True,timeout=240,check=True)
            report['tests'].append({'name':test_name,'passed':True})
        report['passed']=True
    except Exception as error:
        report.update(failure=type(error).__name__,failure_message=str(error))
    finally:
        if started:
            result=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True,timeout=30);report['cleanup']=result.returncode==0
            if not report['cleanup']:report['passed']=False
        (out/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'cleanup':report.get('cleanup'),'output':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
