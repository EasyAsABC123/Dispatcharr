#!/usr/bin/env python3
"""Original/native backup cross-load; owned copied databases only, never the default database.
Original Dispatcharr authors retain full application credit. No app/job/image acceptance.
"""
import argparse,hashlib,io,json,os,shutil,subprocess,tarfile,time,urllib.request,uuid
from pathlib import Path
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
ROOT=Path(__file__).resolve().parents[2]
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--component-build-manifest',type=Path);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Output must be empty')
 component=json.loads(a.component_build_manifest.read_text())if a.component_build_manifest else None
 if component is not None and not component.get('passed'):p.error('Successful immutable standalone component manifest required')
 tools=out/'tool-source';tool_hashes=snapshot_sources(ROOT,tools);nonce=uuid.uuid4().hex[:10];project='dispatcharr-backup-'+nonce;names=['rust_probe_backup_'+nonce+'_'+suffix for suffix in ['a','b','c']]
 report={'passed':False,'project':project,'scope':'original/native database-only v2 ZIP cross-load on three owned copied PostgreSQL17 databases','application_image_verified':False,'job_api_verified':False,'tool_source_sha256':tool_hashes};status=1
 with fixture_source(report,False,out)as directory:
  temp=Path(directory);source=temp/'source';source.mkdir();source.chmod(0o755);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT),check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio',CARGO_TARGET_DIR=str(ROOT/'rust/target'))
  captured={str(f.relative_to(ROOT)):f.read_bytes()for f in (ROOT/'rust').rglob('*')if f.is_file()and not set(f.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
  frozen=temp/'frozen'
  with tarfile.open(out/'rust-source.tar','w')as archive:
   for relative,data in sorted(captured.items()):
    info=tarfile.TarInfo(relative);info.size=len(data);info.mode=0o644;info.mtime=0;archive.addfile(info,io.BytesIO(data));target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
  report['rust_source_sha256']={name:hashlib.sha256(data).hexdigest()for name,data in captured.items()}
  overlay=out/'backup-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
  if component is not None:
   with overlay.open('a')as file:file.write('  backup-component:\n    image: '+component['tag']+'\n    volumes:\n      - '+str(out/'component-data')+':/fixture\n')
   (out/'component-data').mkdir();(out/'component-data').chmod(0o777)
  ports=out/'fixture-ports.yml';report['fixture_ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
  compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def cmd(args,body=None,timeout=180):return subprocess.check_output(args,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def helper(db,action,root='source-backups'):
   raw=cmd(compose+['exec','-T','-e','POSTGRES_DB='+db,'-e','BACKUP_ROOT=/evidence/'+root,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps(action if isinstance(action,dict)else{'action':action}))
   return json.loads(raw.splitlines()[-1])
  try:
   build=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-backups','--test','roundtrip','--no-run','--message-format=json','--locked','--offline'],env=env,capture_output=True,text=True,timeout=600);(out/'rust-build.log').write_text(build.stderr);build.check_returncode();products=[json.loads(line)for line in build.stdout.splitlines()if line.startswith('{')];exe=next(Path(v['executable'])for v in products if v.get('executable')and v.get('target',{}).get('name')=='roundtrip');binary=out/'native-backup-probe';shutil.copy2(exe,binary);binary.chmod(0o755);report['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
   cmd(compose+['up','-d','baseline']);base='http://'+cmd(compose+['port','baseline','5656']).strip();deadline=time.monotonic()+180
   while True:
    try:
     with urllib.request.urlopen(base+'/api/core/version/',timeout=2)as response:
      if response.status==200:break
    except OSError:
     if time.monotonic()>deadline:raise RuntimeError('Source readiness failed')
     time.sleep(.2)
   before=helper('dispatcharr','baseline_manifest');(out/'baseline-before.json').write_text(json.dumps(before,sort_keys=True))
   for name in names:cmd(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+name],timeout=30)
   role=names[1]+'_denied';cmd(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"CREATE ROLE "+role+" LOGIN PASSWORD 'denied-fixture-password'"],timeout=30)
   seed=helper(names[0],'seed');(out/'source-seed.json').write_text(json.dumps(seed,sort_keys=True));created=helper(names[0],'source_create');(out/'source-archive.json').write_text(json.dumps(created,sort_keys=True))
   if component is None:
    result=subprocess.run(compose+['exec','-T','-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','-e','BACKUP_DATABASE_NAMES='+json.dumps(names),'baseline','/evidence/native-backup-probe','original_native_copied_database_cross_restore','--exact','--ignored','--nocapture'],env=env,text=True,capture_output=True,timeout=600);(out/'rust-tests.log').write_text(result.stdout+result.stderr);result.check_returncode()
   else:
    inspection=json.loads(cmd(['docker','image','inspect',component['tag']]))[0]
    if inspection['Id']!=component['image_id']:raise RuntimeError('Component immutable image mismatch')
    report['component_image_id']=inspection['Id'];report['component_source_tree_sha256']=component['source_tree_sha256'];report['component_build_manifest_sha256']=hashlib.sha256(a.component_build_manifest.read_bytes()).hexdigest()
    data=out/'component-data';original=data/created['filename'];shutil.copy2(out/'source-backups'/created['filename'],original);original.chmod(0o644)
    def operation(action,**changes):
     request=dict(action=action,host='baseline',port=5432,user='dispatch',password='playback-fixture-only',database=names[1],**changes)
     return subprocess.run(compose+['run','--rm','--no-deps','-T','backup-component'],env=env,input=json.dumps(request),capture_output=True,text=True,timeout=180)
    restored=operation('restore',filename=created['filename']);(out/'component-restore.log').write_text(restored.stdout+restored.stderr);restored.check_returncode()
    assert helper(names[1],'manifest')['manifest']==seed['manifest'],'Packaged source ZIP -> native closure restore differs'
    credentials=helper(names[1],'credentials');assert credentials==dict(admin_password=True,viewer_password=True,api_key=True,groups=1,channel_profiles=1)
    archived=operation('create');(out/'component-create.log').write_text(archived.stdout+archived.stderr);archived.check_returncode();native=json.loads(archived.stdout.splitlines()[-1]);native_root=out/'native-backups';native_root.mkdir(exist_ok=True);shutil.copy2(data/native['filename'],native_root/native['filename'])
    result=helper(names[2],dict(action='source_restore',filename=native['filename']),root='native-backups');assert result['manifest']==seed['manifest'],'Packaged native closure ZIP -> original restore task differs';assert helper(names[2],'credentials')==credentials
    for fault in ['password','privilege']:
     source_fault=helper(names[1],dict(action='source_dump_fault',fault=fault));assert source_fault['failed']
     changes=dict(password='deliberately-invalid-fixture-password')if fault=='password'else dict(user=names[1]+'_denied',password='denied-fixture-password')
     # Explicit updates avoid duplicate keyword argument construction.
     request=dict(action='create',host='baseline',port=5432,user='dispatch',password='playback-fixture-only',database=names[1]);request.update(changes)
     failed=subprocess.run(compose+['run','--rm','--no-deps','-T','backup-component'],env=env,input=json.dumps(request),capture_output=True,text=True,timeout=180);assert failed.returncode!=0;assert helper(names[1],'manifest')['manifest']==seed['manifest']
    report.update(packaged_component_cross_restore=True,python_runtime_absent=component['python_runtime_absent'],runtime_manifest_sha256=component['runtime_manifest_sha256'])
    (out/'rust-tests.log').write_text('Packaged PG17 closure original/native cross-restore, exact database manifests, credentials and two actual auth/privilege boundaries passed.\n')
   after=helper('dispatcharr','baseline_manifest');assert before==after,'Default original fixture database changed'
   report.update(passed=True,default_database_unchanged=True,copied_databases=3,nonempty_tables=sum(v['count']>0 for v in seed['manifest']['tables'].values()),source_archive_sha256=hashlib.sha256((out/'source-backups'/created['filename']).read_bytes()).hexdigest(),native_checks_sha256=hashlib.sha256((out/'rust-tests.log').read_bytes()).hexdigest());status=0
  except (OSError,ValueError,RuntimeError,AssertionError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or ''))
  finally:
   try:cmd(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
   except subprocess.SubprocessError:report.update(passed=False,cleanup_error=True);status=1
 (out/'backup-contracts.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
