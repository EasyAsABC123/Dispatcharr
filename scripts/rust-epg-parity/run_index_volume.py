#!/usr/bin/env python3
"""Owned copied-volume Source producer→native index→real CurrentLookup rehearsal.
Original Dispatcharr authors retain all application/algorithm credit and licensing.
Runtime image is an OS/UID compatibility target; mounted component binary is
separately source/hash bound, not claimed to be its rebuilt application binary.
"""
import argparse,hashlib,io,json,os,shutil,subprocess,sys,tarfile,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
ORIGINAL='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--runtime-manifest',required=True,type=Path);p.add_argument('--rust-snapshot-manifest',type=Path);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);manifest=json.loads(a.runtime_manifest.read_text())
 if manifest.get('passed')is not True:p.error('Passing immutable runtime manifest required')
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for path in(ROOT/'scripts/rust-epg-parity').glob('*'):
  if path.is_file():
   data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 nonce=uuid.uuid4().hex[:10];database_name='rust_probe_backup_jobs_'+nonce;project='dispatcharr-index-volume-'+nonce;containers=[]
 report={'passed':False,'scope':'Owned copied media/data UID65532 component rehearsal; no application hook/default mount change','project':project,'copied_database':database_name,'original_revision':BASELINE,'runtime_image':manifest['image_id'],'runtime_manifest_sha256':hashlib.sha256(a.runtime_manifest.read_bytes()).hexdigest(),'tool_source_sha256':hashes,'checks':[],'performance_verified':False,'application_image_rebuilt':False};status=1
 captured={}
 if a.rust_snapshot_manifest:
  prior=json.loads(a.rust_snapshot_manifest.read_text());origin=a.rust_snapshot_manifest.parent/'rust-source.tar'
  if not prior.get('binary_sha256')or hashlib.sha256(origin.read_bytes()).hexdigest()!=prior.get('rust_source_archive_sha256'):raise ValueError('Previously compiled source snapshot binding required')
  with tarfile.open(origin)as archive:
   for member in archive.getmembers():
    if not member.isfile()or member.name not in prior['rust_source_sha256']or not member.name.startswith('rust/'):raise ValueError('Unexpected compiler snapshot member')
    value=archive.extractfile(member).read()
    if hashlib.sha256(value).hexdigest()!=prior['rust_source_sha256'][member.name]:raise ValueError('Compiler snapshot file digest mismatch')
    captured[member.name]=value
  if set(captured)!=set(prior['rust_source_sha256']):raise ValueError('Incomplete prior source snapshot')
  report['source_snapshot_origin']={'manifest':str(a.rust_snapshot_manifest),'manifest_sha256':hashlib.sha256(a.rust_snapshot_manifest.read_bytes()).hexdigest(),'previous_fixture_passed':prior.get('passed'),'scope':'Reuses exact successfully compiled source; prior fixture copy setup failure remains unaccepted.'}
 else:
  for base,directories,names in os.walk(ROOT/'rust'):
   directories[:]=sorted(name for name in directories if name not in {'target','.git','__pycache__'})
   for name in sorted(names):
    path=Path(base)/name
    if path.is_symlink()or not path.is_file():raise ValueError('Regular compiler inputs required')
    captured[str(path.relative_to(ROOT))]=path.read_bytes()
 frozen=out/'frozen-source'
 with tarfile.open(out/'rust-source.tar','w')as archive:
  for name,data in sorted(captured.items()):
   info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o644;info.mtime=0;archive.addfile(info,io.BytesIO(data));target=frozen/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 report['rust_source_sha256']={name:hashlib.sha256(data).hexdigest()for name,data in captured.items()};report['rust_source_archive_sha256']=hashlib.sha256((out/'rust-source.tar').read_bytes()).hexdigest();(out/'source-capture.json').write_text(json.dumps({'captured':True,'files':len(captured),'archive_sha256':report['rust_source_archive_sha256']}))
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir(mode=0o755);source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio');overlay=out/'fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-epg-parity')+':/index-tools:ro\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\nnetworks:\n  default:\n    internal: true\n');ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656]});compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,body=None,timeout=180):return subprocess.check_output(args,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def helper(database,action):return json.loads(command(compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action})).splitlines()[-1])
  def control(op,**fields):return json.loads(command(compose+['exec','-T','-e','POSTGRES_DB='+database_name,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@127.0.0.1:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@127.0.0.1:6379/9','baseline','/dispatcharrpy/bin/python','/index-tools/index_path_volume_control.py'],json.dumps({'op':op,**fields})).splitlines()[-1])
  def check(name,condition,**detail):
   report['checks'].append({'name':name,'passed':bool(condition),**detail})
   if not condition:raise AssertionError(name)
  before=None
  try:
   dockerfile=frozen/'Dockerfile.index-volume-component'
   dockerfile.write_text("FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS build\nWORKDIR /workspace\nCOPY rust/ ./rust/\nRUN cargo build --manifest-path rust/Cargo.toml --locked --release -p dispatcharr-job-transport --example index_worker_fixture\nFROM scratch\nCOPY --from=build /workspace/rust/target/release/examples/index_worker_fixture /component/native-index-worker\nCOPY LICENSE ATTRIBUTION.md /usr/share/doc/dispatcharr/\nCOPY rust/crates/xmltv-parser/vendor/Copyright rust/crates/xmltv-parser/vendor/NOTICES /usr/share/doc/dispatcharr/\nLABEL org.opencontainers.image.licenses=AGPL-3.0-only\n")
   packaging={}
   for filename in ['LICENSE','ATTRIBUTION.md']:
    value=(ROOT/filename).read_bytes();(frozen/filename).write_bytes(value);packaging[filename]=hashlib.sha256(value).hexdigest()
   packaging[dockerfile.name]=hashlib.sha256(dockerfile.read_bytes()).hexdigest();report['component_packaging_sha256']=packaging
   tag='dispatcharr-index-volume-component:'+nonce
   build=subprocess.run(['docker','build','-f',str(dockerfile),'-t',tag,str(frozen)],env=env,text=True,capture_output=True,timeout=1800);(out/'build.log').write_text(build.stdout+build.stderr);build.check_returncode();report['component_builder_image_id']=json.loads(command(['docker','image','inspect',tag]))[0]['Id']
   extract='dispatcharr-index-volume-extract-'+nonce;containers.append(extract);command(['docker','create','--name',extract,'--entrypoint','/component/native-index-worker',tag]);binary=out/'native-index-worker';command(['docker','cp',extract+':/component/native-index-worker',str(binary)]);binary.chmod(0o755);command(['docker','rm',extract]);report['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
   actual=json.loads(command(['docker','image','inspect',manifest['image_id']]))[0];check('immutable-runtime-image',actual['Id']==manifest['image_id']and actual['Config'].get('Labels',{}).get('org.opencontainers.image.source-tree-sha256')==manifest['source_tree_sha256'])
   command(compose+['up','-d','baseline']);deadline=time.monotonic()+180
   while True:
    try:command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],timeout=5);break
    except subprocess.SubprocessError:
     if time.monotonic()>deadline:raise RuntimeError('Original readiness failed')
     time.sleep(.2)
   before=helper('dispatcharr','baseline_manifest');command(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database_name]);helper(database_name,'seed');seed=control('seed');(out/'source-volume-seed-private.json').write_text(json.dumps(seed,indent=2));baseline=command(compose+['ps','-q','baseline']).strip()
   media=out/'media-copy';media.mkdir();data=out/'data-copy';(data/'uploads'/'epgs').mkdir(parents=True)
   def copy_namespace(path,target,label):
    archive_path=out/(label+'.tar')
    with archive_path.open('wb')as file:
     copied=subprocess.run(compose+['exec','-T','baseline','tar','-C',path,'-cf','-','.'],env=env,stdout=file,stderr=subprocess.PIPE,timeout=30);copied.check_returncode()
    if archive_path.stat().st_size>4*1024*1024:raise ValueError('Owned file snapshot size exceeded')
    with tarfile.open(archive_path)as archive:
     members=archive.getmembers();symlinks=[]
     for member in members:
      parts=Path(member.name).parts
      if member.name.startswith('/')or '..'in parts or not(member.isfile()or member.isdir()or member.issym()):raise ValueError('Unsafe owned fixture snapshot member')
      if member.issym():
       if not member.linkname.startswith(seed['root']+'/'):raise ValueError('Symlink leaves owned Source namespace')
       symlinks.append(member.name.rstrip('/'))
     if any(member.name.startswith(link+'/')for member in members for link in symlinks):raise ValueError('Owned archive follows a symlink')
     archive.extractall(target,filter='fully_trusted')
    report.setdefault('copied_file_archives',{})[label]=hashlib.sha256(archive_path.read_bytes()).hexdigest()
   copy_namespace('/app/media',media,'source-media-files');copy_namespace('/data/uploads/epgs',data/'uploads'/'epgs','source-persistent-files')
   for item in seed['files']:
    original=Path(item['path']);copied=(media/original.relative_to('/app/media'))if original.is_relative_to('/app/media')else(data/original.relative_to('/data'))
    if 'symlink_target'in item:check('copied-symlink-literal',os.readlink(copied)==item['symlink_target'])
    else:check('copied-file-byte-hash',hashlib.sha256(copied.read_bytes()).hexdigest()==item['sha256'])
   mounts=['--mount','type=bind,src='+str(media)+',dst=/app/media','--mount','type=bind,src='+str(data)+',dst=/data','--mount','type=bind,src='+str(binary)+',dst=/native-index-worker,readonly']
   name='dispatcharr-volume-owner-'+nonce;containers.append(name);command(['docker','run','--name',name,'--rm','--network','none','--read-only','--user','0:0',*mounts,'--entrypoint','/bin/sh',manifest['image_id'],'-c','chown -hR 65532:65532 /app/media /data']);check('copied-only-ownership-adjusted',True)
   def native(action,**fields):
    name='dispatcharr-volume-native-'+uuid.uuid4().hex[:12];containers.append(name);payload={'action':action,'broker_url':'redis://:playback-fixture-only@127.0.0.1:6379/8','result_url':'redis://:playback-fixture-only@127.0.0.1:6379/9','production_connector':True,'source_cwd':'/app','media_root':'/app/media','now':seed['now'],'include_context':True,**fields}
    args=['docker','run','--name',name,'--rm','-i','--network','container:'+baseline,'--read-only','--user','65532:65532','--workdir','/','--cpus','1','--memory','512m','--tmpfs','/tmp:mode=1777,size=67108864',*mounts,'-e','POSTGRES_DB='+database_name,'-e','POSTGRES_USER=dispatch','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','--entrypoint','/native-index-worker',manifest['image_id']]
    result=subprocess.run(args,env=env,input=json.dumps(payload),text=True,capture_output=True,timeout=45);(out/'last-native-private.log').write_text(result.stdout+result.stderr);result.check_returncode();value=json.loads(result.stdout.splitlines()[-1]);check('native-uid-cwd-'+action,value['uid']==65532 and value['cwd']=='/');return value['result']
   check('copied-media-data-write-readiness',native('filesystem')=={'media_writable':True,'data_writable':True})
   for case in seed['cases']:
    issued=control('publish',source_id=case['source_id']);native('run');original=control('verify',source_id=case['source_id'],epg_entry_id=case['epg_entry_id'],task_id=issued['task_id'],now=seed['now']);check('source-native-index-'+case['name'],original['index']==case['index']and original['normal_null_result']and not original['file_lock_exists'])
    lookup=native('current',epg_entry_id=case['epg_entry_id']);expected=[]if case['current']is None else[{**case['current'],'epg_data_id':case['epg_entry_id']}];check('real-store-current-'+case['name'],lookup['current']==expected)
   rollback=control('rollback');(out/'rollback-private.json').write_text(json.dumps(rollback,indent=2));check('original-files-and-ownership-rollback',rollback['all_original_files_unchanged']and rollback['source_ownership']=={'uid':1000,'gid':1000});check('source-read-rollback',all(row['current']==case['current']for row,case in zip(rollback['source_current'],seed['cases'])));check('default-database-unchanged',helper('dispatcharr','baseline_manifest')==before);report.update(passed=True,native_uid=65532,native_process_cwd='/',explicit_source_cwd='/app',explicit_media_root='/app/media',native_component_protocol_roundtrip=True,existing_production_volume_modified=False,application_hook_verified=False);status=0
  except(OSError,ValueError,RuntimeError,AssertionError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or '')[:65536])
  finally:
   for name in containers:subprocess.run(['docker','rm','--force',name],env=env,capture_output=True,timeout=15)
   if before is not None:
    try:
     report['default_database_unchanged']=helper('dispatcharr','baseline_manifest')==before
     if not report['default_database_unchanged']:report['passed']=False;status=1
    except subprocess.SubprocessError:report.update(passed=False,default_database_check_failed=True);status=1
   try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='owned containers/project removed'
   except subprocess.SubprocessError:report.update(passed=False,cleanup_error=True);status=1
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
