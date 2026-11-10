#!/usr/bin/env python3
"""Separate remaining-owner restart characterization; no recovery runtime changes.
Original Dispatcharr authors retain full application/algorithm credit.
"""
import argparse,hashlib,io,json,os,re,shutil,subprocess,tarfile,time,uuid
from pathlib import Path
from run import IMAGE,ORIGINAL,ROOT,Peer

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--reuse-build-manifest',type=Path);p.add_argument('--rust-file-overlay',action='append',default=[],metavar='TARGET=SOURCE');p.add_argument('--resources',action='store_true');p.add_argument('--admit-resources',action='store_true');p.add_argument('--cause-resources',action='store_true');p.add_argument('--lease-resources',action='store_true');a=p.parse_args()
 if a.rust_file_overlay and a.reuse_build_manifest:p.error('overlays require a fresh source capture')
 if (a.admit_resources or a.cause_resources or a.lease_resources)and not a.resources:p.error('extra resource gates require --resources')
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 report={'passed':False,'execution_passed':False,'scope':'Remaining cached-owner read/state-loss component; no full application or constructor/lifecycle parity','original_revision':ORIGINAL,'image':IMAGE,'observations':[],'checks':[],'performance_verified':False};env=dict(os.environ);name='dispatcharr-remaining-redis-'+uuid.uuid4().hex[:12];peers=[];status=1
 tools=out/'tool-source';tools.mkdir(mode=0o755);tools.chmod(0o755);report['tool_sha256']={}
 for source in sorted(Path(__file__).parent.iterdir()):
  if not source.is_file():continue
  data=source.read_bytes();(tools/source.name).write_bytes(data);(tools/source.name).chmod(0o644);report['tool_sha256'][str(source.relative_to(ROOT)if source.is_absolute()else source)]=hashlib.sha256(data).hexdigest()
 if a.reuse_build_manifest:
  old=a.reuse_build_manifest.resolve().parent;origin=json.loads(a.reuse_build_manifest.read_text());raw=(old/'rust-source.tar').read_bytes();assert hashlib.sha256(raw).hexdigest()==origin['rust_source_archive_sha256']
  with tarfile.open(fileobj=io.BytesIO(raw))as archive:entries={member.name:hashlib.sha256(archive.extractfile(member).read()).hexdigest()for member in archive if member.isfile()}
  assert entries==origin['rust_source_sha256'];(out/'rust-source.tar').write_bytes(raw);report['rust_source_sha256']=entries;report['rust_source_archive_sha256']=origin['rust_source_archive_sha256'];report['source_snapshot_origin']={'manifest':str(a.reuse_build_manifest),'sha256':hashlib.sha256(a.reuse_build_manifest.read_bytes()).hexdigest(),'previous_execution_passed':origin['execution_passed'],'scope':'Exact successfully compiled153 Rust and binaries reused; earlier setup failure retained'}
  (out/'frozen-source-ready.json').write_text(json.dumps({'closed':True,'sha256':report['rust_source_archive_sha256']}))
 else:
  captured={}
  for directory,dirs,names in os.walk(ROOT/'rust'):
   dirs[:]=sorted(v for v in dirs if v not in{'target','.git','__pycache__'})
   for filename in sorted(names):
    source=Path(directory)/filename
    if source.is_symlink()or not source.is_file():raise ValueError('Regular snapshot files required')
    captured[str(source.relative_to(ROOT))]=source.read_bytes()
  report['explicit_rust_overlays']=[]
  for item in a.rust_file_overlay:
   target,source=item.split('=',1)
   if target not in captured:raise ValueError('Overlay target must be an existing captured Rust file')
   raw=Path(source).read_bytes();report['explicit_rust_overlays'].append({'path':target,'source':str(Path(source).resolve()),'previous_sha256':hashlib.sha256(captured[target]).hexdigest(),'sha256':hashlib.sha256(raw).hexdigest()});captured[target]=raw
  with tarfile.open(out/'rust-source.tar','w')as archive:
   for path,data in sorted(captured.items()):
    item=tarfile.TarInfo(path);item.size=len(data);item.mode=0o644;item.mtime=0;archive.addfile(item,io.BytesIO(data))
  report['rust_source_sha256']={path:hashlib.sha256(data).hexdigest()for path,data in captured.items()};report['rust_source_archive_sha256']=hashlib.sha256((out/'rust-source.tar').read_bytes()).hexdigest();(out/'frozen-source-ready.json').write_text(json.dumps({'closed':True,'sha256':report['rust_source_archive_sha256']}))
 frozen=out/'native-source';frozen.mkdir()
 with tarfile.open(out/'rust-source.tar')as archive:archive.extractall(frozen,filter='data')
 def command(args,timeout=30):return subprocess.check_output(args,env=env,text=True,stderr=subprocess.PIPE,timeout=timeout).strip()
 def check(label,passed):
  report['checks'].append({'name':label,'passed':bool(passed)})
  if not passed:raise AssertionError(label)
 try:
  binaries=out/'binaries';binaries.mkdir(mode=0o755);binaries.chmod(0o755);specs={'live':('dispatcharr-clients',['--test','redis_restart_peer'],'persistent_client_owner_protocol'),'cache':('dispatcharr-store',['--lib'],'notifications::notification_restart_peer::persistent_notification_owner_protocol'),'vod':('dispatcharr-vod-http',['--lib'],'sessions::session_restart_peer::persistent_vod_owner_protocol')};report['binaries']={}
  for kind,(package,selector,test)in specs.items():
   if a.reuse_build_manifest:
    data=(old/'binaries'/kind).read_bytes();assert hashlib.sha256(data).hexdigest()==origin['binaries'][kind]['sha256'];assert origin['binaries'][kind]['test']==test
    dest=binaries/kind;dest.write_bytes(data);dest.chmod(0o755);report['binaries'][kind]=origin['binaries'][kind]
   else:
    built=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p',package,*selector,'--no-run','--message-format=json','--locked','--offline'],env=dict(env,CARGO_TARGET_DIR=str(out/'cargo-target')),text=True,capture_output=True,timeout=600);(out/(kind+'-build.jsonl')).write_text(built.stdout);(out/(kind+'-build.log')).write_text(built.stderr);built.check_returncode()
    executables=[json.loads(line)['executable']for line in built.stdout.splitlines()if line.startswith('{')and json.loads(line).get('reason')=='compiler-artifact'and json.loads(line).get('executable')]
    if len(executables)!=1:raise ValueError('One peer test executable required')
    dest=binaries/kind;shutil.copyfile(executables[0],dest);dest.chmod(0o755);report['binaries'][kind]={'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),'test':test}
  if a.resources:
   report['resource_binaries']={}
   resource_targets=[('dispatcharr-clients','admission'),('dispatcharr-clients','era')]
   if a.lease_resources:resource_targets.append(('dispatcharr-coordination','lease_physical'))
   for package,target in resource_targets:
    built=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p',package,'--test',target,'--no-run','--message-format=json','--locked','--offline'],env=dict(env,CARGO_TARGET_DIR=str(out/'cargo-target')),text=True,capture_output=True,timeout=600);(out/(target+'-build.jsonl')).write_text(built.stdout);(out/(target+'-build.log')).write_text(built.stderr);built.check_returncode()
    executables=[json.loads(line)['executable']for line in built.stdout.splitlines()if line.startswith('{')and json.loads(line).get('reason')=='compiler-artifact'and json.loads(line).get('executable')];assert len(executables)==1
    dest=binaries/target;shutil.copyfile(executables[0],dest);dest.chmod(0o755);report['resource_binaries'][target]=hashlib.sha256(dest.read_bytes()).hexdigest()
  source=out/'original-source';source.mkdir(mode=0o755);source.chmod(0o755);archive=subprocess.check_output(['git','archive',ORIGINAL],cwd=ROOT);(out/'original-source.tar').write_bytes(archive);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest();subprocess.run(['tar','--same-permissions','--no-same-owner','-xf','-','-C',str(source)],input=archive,check=True)
  channel='owned_redis_restart_'+uuid.uuid4().hex[:12];report['channel']=channel
  command(['docker','run','-d','--rm','--name',name,'--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','512m','--tmpfs','/tmp:rw,size=128m,mode=1777','--tmpfs','/data:rw,size=16m,mode=1777','--mount',f'type=bind,src={source},dst=/app,readonly','--mount',f'type=bind,src={tools},dst=/restart-tools,readonly','--mount',f'type=bind,src={binaries},dst=/native-peers,readonly','-e','REDIS_RESTART_DISPOSABLE_FIXTURE=playback-fixture-only','-e','REDIS_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','SECRET_KEY=playback-fixture-only','-e','POSTGRES_DB=rust_probe_redis_restart_only','-e','DISPATCHARR_LOG_DIR=/tmp/restart-logs','-e','RESTART_PROBE_TASK='+channel,'-e','PYTHONPATH=/app','--workdir','/app','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/restart-tools/supervisor.py']);report['container']=name
  inspect=json.loads(command(['docker','inspect',name]))[0];report['actual_image_id']=inspect['Image'];check('network-none-readonly-uid65532',inspect['HostConfig']['NetworkMode']=='none'and inspect['HostConfig']['ReadonlyRootfs']and inspect['Config']['User']=='65532:65532')
  def control(action):return json.loads(command(['docker','exec',name,'/dispatcharrpy/bin/python','/restart-tools/control.py',action]))
  def ready():
   deadline=time.monotonic()+15
   while time.monotonic()<deadline:
    try:
     if command(['docker','exec','-e','REDISCLI_AUTH=playback-fixture-only',name,'redis-cli','--no-auth-warning','ping'])=='PONG':return
    except subprocess.SubprocessError:pass
    time.sleep(.1)
   raise TimeoutError('Redis readiness')
  def runid():return next(line.split(':',1)[1]for line in command(['docker','exec','-e','REDISCLI_AUTH=playback-fixture-only',name,'redis-cli','--no-auth-warning','INFO','server']).splitlines()if line.startswith('run_id:'))
  def observe(peer,label,action,**fields):
   result=peer.ask(action,**fields);report['observations'].append({'peer':label,'command':{'action':action,**fields},'result':result});return result
  ready();report['redis_run_ids']=[runid()];original=Peer(['docker','exec','-i',name,'/dispatcharrpy/bin/python','/restart-tools/source_owners.py'],env,out/'source-peer-private.log');peers.append(original);source_init=observe(original,'original','init');check('Original actual cache and pinned methods initialized',source_init.get('ready')and not source_init['apps_ready'])
  for path,digest in source_init['source_sha256'].items():check('pinned-'+path,hashlib.sha256((source/path).read_bytes()).hexdigest()==digest)
  native={}
  for kind,(_,_,test)in specs.items():
   peer=Peer(['docker','exec','-i',name,'/native-peers/'+kind,'--exact',test,'--ignored','--nocapture','--test-threads=1','--quiet'],env,out/(kind+'-peer-private.log'));native[kind]=peer;peers.append(peer);check('native-'+kind+'-ready',observe(peer,kind,'init').get('ready'))
  check('seed1',observe(original,'original','seed',generation=1).get('result'));first=observe(original,'original','read');expected={'live':1,'vod':True,'cache':{'owned_restart':channel,'generation':1}};check('source-initial-values',all(first.get(k)==v for k,v in expected.items()))
  for kind,peer in native.items():check('native-'+kind+'-initial-value',observe(peer,kind,'read').get('result')==expected[kind])
  check('stop-only-Redis',control('stop')=={'running':False,'generation':1});observe(original,'original','read')
  for kind,peer in native.items():observe(peer,kind,'read')
  check('restart-only-Redis',control('start')=={'running':True,'generation':2});ready();report['redis_run_ids'].append(runid());check('actual-server-restarted',report['redis_run_ids'][0]!=report['redis_run_ids'][1]);lost=observe(original,'original','read');check('source-no-persistence-state-loss',lost.get('live')==0 and lost.get('vod')is False and lost.get('cache')is None)
  for kind,peer in native.items():observe(peer,kind,'read')
  check('source-reseed2',observe(original,'original','seed',generation=2).get('result'));expected['cache']['generation']=2;fresh=observe(original,'original','read');check('source-same-cached-owners-recover',all(fresh.get(k)==v for k,v in expected.items())and fresh['same_client']and fresh['same_cache'])
  recovered={kind:False for kind in native}
  for delay in [0,.1,1]:
   time.sleep(delay)
   for kind,peer in native.items():
    result=observe(peer,kind,'read');recovered[kind]|='result'in result and result['result']==expected[kind]
  accounting=observe(native['live'],'live','probe_register');report['new_client_accounting_after_restart']=False;report['new_client_ttl_after_restart']=False;report['no_duplicate_counter_after_future_flush']=False
  if 'id'in accounting:
   stored=observe(original,'original','inspect_native',id=accounting['id']);report['new_client_accounting_after_restart']=stored.get('bytes')=='8192';report['new_client_ttl_after_restart']=isinstance(stored.get('ttl'),int)and 0<stored['ttl']<=60
   time.sleep(.12);later=observe(original,'original','inspect_native',id=accounting['id']);report['no_duplicate_counter_after_future_flush']=later.get('bytes')=='8192'and 0<later.get('ttl',0)<=60

  for kind,peer in native.items():check('rebuilt-'+kind+'-ready',observe(peer,kind,'rebuild').get('ready'));check('rebuilt-'+kind+'-positive',observe(peer,kind,'read').get('result')==expected[kind])
  check('persistent-source-process',len({v['result']['pid']for v in report['observations']if v['peer']=='original'})==1)
  report.update(execution_passed=True,recovered=recovered,known_mismatch_detected=not all(recovered.values()),migration_gate_passed=all(recovered.values()),passed=all(recovered.values()),remaining_owners_tested=list(native),application_recovery_verified=False)
  accounting_ok=report['new_client_accounting_after_restart']and report['new_client_ttl_after_restart']and report['no_duplicate_counter_after_future_flush'];report['passed']&=accounting_ok;report['migration_gate_passed']&=accounting_ok
  if a.resources:
   resources=[('admission',None,10),('era',None,6),('cache','notifications::tests::cancelled_cache_command_retains_physical_slot_and_scope',1),('cache','notifications::tests::cache_logical_timeout_retains_pending_reply_and_composed_scopes',1),('cache','notifications::tests::cache_cancelled_burst_cannot_reuse_sixteen_pending_slots',1),('vod','sessions::session_restart_peer::physical_dropped_ticket_retains_request_and_session_until_actual_reply',1),('vod','sessions::session_restart_peer::physical_cancelled_setup_retains_request_and_session_until_actual_reply',1)];report['resource_tests']=[]
   if a.admit_resources:resources += [('vod','sessions::session_restart_peer::admit_reply::lost_admit_reply_retains_pending_cleanup_through_noscript_and_real_reply',1),('vod','sessions::session_restart_peer::admit_reply::delayed_admit_reply_retains_pending_cleanup_after_logical_timeout',1)]
   if a.cause_resources:resources += [('vod','sessions::session_restart_peer::recovery_causes::'+name,1)for name in ['missing_metadata_alone_stops_live_ticket_and_reopen_has_free_request_slot','lost_coordination_alone_stops_even_with_matching_metadata','replaced_session_and_provider_survive_late_old_ticket_cleanup','explicit_stop_with_owned_metadata_is_terminal']]
   if a.lease_resources:resources += [('lease_physical',None,13)]
   for i,(binary,selected,count)in enumerate(resources):
    args=['docker','exec','-e','NOTIFICATION_SCOPE_FIXTURE=playback-fixture-only',name,'/native-peers/'+binary,'--ignored','--nocapture','--test-threads=1']
    if selected:args+=['--exact',selected]
    result=subprocess.run(args,env=env,text=True,capture_output=True,timeout=90);log=result.stdout+result.stderr;path=out/('resource-'+str(i)+'-'+binary+'.log');path.write_text(log);valid=result.returncode==0 and re.search(r'test result: ok\. '+str(count)+r' passed;',log)is not None;report['resource_tests'].append({'binary':binary,'selected':selected,'expected_count':count,'passed':valid,'log':path.name})
   report['physical_owner_resources_verified']=all(t['passed']for t in report['resource_tests']);report['passed']&=report['physical_owner_resources_verified'];report['migration_gate_passed']&=report['physical_owner_resources_verified']
  status=0 if report['passed']else 1
 except Exception as error:report.update(error_type=type(error).__name__,error_message=str(error)[:1024])
 finally:
  for peer in reversed(peers):peer.close()
  try:(out/'container.log').write_text(command(['docker','logs',name]))
  except subprocess.SubprocessError:pass
  removed=subprocess.run(['docker','rm','-f',name],env=env,text=True,capture_output=True,timeout=20);report['cleanup']='owned container removed'if removed.returncode==0 else'cleanup failed';report['cleanup_passed']=removed.returncode==0
  if removed.returncode:report['passed']=False;status=1
  (out/'proof.json').write_text(json.dumps(report,indent=2));print(json.dumps({'passed':report['passed'],'execution_passed':report['execution_passed'],'evidence':str(out)}))
 return status
if __name__=='__main__':raise SystemExit(main())
