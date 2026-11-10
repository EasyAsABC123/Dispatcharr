#!/usr/bin/env python3
"""Real Redis data-only reader of captured Original/native exception metadata.
Original Dispatcharr/Celery/Kombu/framework authors retain code credit/licenses.
No application, SQL, Python exception import or HTTP projection is exercised.
"""
import argparse,hashlib,json,os,shutil,subprocess,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--frozen-rust',required=True,type=Path);a=p.parse_args();source=a.frozen_rust.resolve();assert source.is_relative_to(ROOT/'artifacts/rust')and(source/'rust/Cargo.lock').is_file()
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir()),'Empty evidence output required'
 frozen=out/'frozen';hashes={}
 for folder,dirs,names in os.walk(source/'rust'):
  dirs[:]=[x for x in dirs if x not in ['target','.git','__pycache__']]
  for name in names:
   file=Path(folder)/name;assert not file.is_symlink();rel=file.relative_to(source);data=file.read_bytes();target=frozen/rel;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(rel)]=hashlib.sha256(data).hexdigest()
 (out/'tool-source').mkdir();shutil.copyfile(__file__,out/'tool-source/run_failure_reader.py')
 buildenv=dict(os.environ,CARGO_TARGET_DIR=str(out/'target'));build=subprocess.run(['cargo','build','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-job-transport','--example','result_reader_fixture','--locked','--offline','--message-format=json'],env=buildenv,capture_output=True,text=True,timeout=600);(out/'build.log').write_text(build.stdout+build.stderr);build.check_returncode()
 products=[json.loads(line)for line in build.stdout.splitlines()if line.startswith('{')];binary=next(Path(v['executable'])for v in products if v.get('executable')and v.get('target',{}).get('name')=='result_reader_fixture');shutil.copy2(binary,out/'native-result-reader');(out/'native-result-reader').chmod(0o755)
 corpus=frozen/'rust/crates/job-transport/datasets/source-index-failure-reader-1.json';rows=json.loads(corpus.read_text())['rows'];assert len(rows)==19
 name='dispatcharr-failure-reader-'+uuid.uuid4().hex[:12];database='rust_probe_backup_jobs_reader_'+uuid.uuid4().hex[:10];checks=[];report={'passed':False,'scope':'Real Redis bounded metadata decoder,19 captured Original/native records; no SQL/application/exception imports or HTTP projection','checks':checks,'source_image':IMAGE,'rust_source_sha256':hashes,'binary_sha256':hashlib.sha256((out/'native-result-reader').read_bytes()).hexdigest(),'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest(),'database_marker_only':True,'postgres_used':False,'performance_verified':False}
 def docker(args,body=None,timeout=60):return subprocess.check_output(['docker',*args],input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
 def check(label,value):
  checks.append({'name':label,'passed':bool(value)})
  if not value:raise AssertionError(label)
 def seed(metadata):
  script="import sys,json,redis,hashlib; v=json.load(sys.stdin); raw=json.dumps(v,ensure_ascii=False).encode(); r=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=9); key='celery-tasks:celery-task-meta-'+v['task_id']; r.set(key,raw,ex=60); print(hashlib.sha256(raw).hexdigest())"
  return docker(['exec','-i',name,'/dispatcharrpy/bin/python','-c',script],json.dumps(metadata)).strip()
 def read(ident):return json.loads(docker(['exec','-i','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','POSTGRES_DB='+database,name,'/evidence/native-result-reader'],json.dumps({'task_id':ident})))
 try:
  docker(['run','--rm','-d','--name',name,'--network','none','--read-only','--tmpfs','/tmp:mode=1777,size=67108864','--memory','192m','--cpus','1','-v',str(out)+':/evidence:ro','--entrypoint','redis-server',IMAGE,'--bind','127.0.0.1','--save','','--appendonly','no','--requirepass','playback-fixture-only'])
  for _ in range(100):
   try:
    if docker(['exec',name,'redis-cli','--no-auth-warning','-a','playback-fixture-only','ping']).strip()=='PONG':break
   except subprocess.CalledProcessError:pass
   time.sleep(.1)
  else:raise RuntimeError('Owned Redis readiness failed')
  for row in rows:
   metadata=row['metadata'];before=seed(metadata);value=read(metadata['task_id']);label=row['name'];check('failure-state-'+label,value['ok']and value['state']=='Failure');check('exact-result-'+label,value['result']==metadata['result']);check('exact-descriptor-'+label,value['exception']=={'class':metadata['result']['exc_type'],'arguments':metadata['result']['exc_message']});check('opaque-diagnostic-'+label,value['traceback']==metadata['traceback']and value['date_done']==metadata['date_done'])
   script="import redis,hashlib; r=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=9); key="+repr('celery-tasks:celery-task-meta-'+metadata['task_id'])+"; print(hashlib.sha256(r.get(key)).hexdigest()); assert 0<r.ttl(key)<=60"
   check('read-only-key-and-expiry-'+label,docker(['exec',name,'/dispatcharrpy/bin/python','-c',script]).strip()==before)
  for label,descriptor,expected in [('foreign-module',{'exc_module':'not_imported','exc_type':'NoPermissionError','exc_message':['denied']},'UnsupportedException'),('wrong-errno',{'exc_module':'builtins','exc_type':'IsADirectoryError','exc_message':[13,'Is a directory']},'UnsupportedException'),('oversized-string',{'exc_module':'redis.exceptions','exc_type':'NoPermissionError','exc_message':['x'*8193]},'Capacity')]:
   metadata=dict(rows[0]['metadata']);metadata['task_id']=str(uuid.uuid4());metadata['result']=descriptor;seed(metadata);value=read(metadata['task_id']);check('actual-redis-reject-'+label,not value['ok']and expected in value['error'])
  missing=str(uuid.uuid4());value=read(missing);check('missing-key-remains-pending',value=={'ok':True,'state':'Pending','task_id':missing});report['passed']=True
 except(OSError,ValueError,RuntimeError,AssertionError,subprocess.SubprocessError)as error:
  report['error_type']=type(error).__name__
  if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or'')[:65536])
 finally:
  try:docker(['rm','-f',name]);report['cleanup']='owned container removed'
  except subprocess.SubprocessError:report.update(passed=False,cleanup_error=True)
  (out/'reader-contracts.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
