"""Bounded read-only observer of one owned Main PID namespace, no app import.
Original Dispatcharr and dependency authors retain application credit.
"""
import hashlib,json,uuid
from pathlib import Path

def capture(f,*,diagnostic_authority=None):
 diagnostic=bool(diagnostic_authority and diagnostic_authority.get('scope')=='Root-authorized exact react-workflow read-only access proof'and diagnostic_authority.get('case')==f.case=='react-workflow')
 if f.backend!='Native'or(f.case not in('m8-actions','normal-backup-restore','normal-backup-restore-partial-admission')and not diagnostic)or not f.native:raise ValueError('ExactLiveOwnedNativeMainRequired')
 out=Path(f.output)/'private'/('pool-observer-'+uuid.uuid4().hex[:10]);out.mkdir(mode=0o700)
 def save(name,value):(out/name).write_text(json.dumps(value,indent=2)+'\n')
 def inspect(ident):return json.loads(f.command([f.docker,'inspect',ident],seconds=5,secret=True))[0]
 before=inspect(f.native);cid=before['Id'];state=before['State'];assert len(cid)==64 and state['Running']and before['Image']==f.binding['candidate_image_id']and before['Config']['User']=='65532:65532'
 assert not diagnostic or diagnostic_authority['CID']==cid
 tracked=next(row for row in f.containers if row['Id']==cid);pid=state['Pid'];generation=f.pid_start(pid);assert generation is not None and tracked['pid']==pid and tracked['starttime']==generation
 guest=Path(__file__).with_name('pool_sockets.py');source=guest.read_bytes();assert len(source)<16384
 name='dispatcharr-owned-pool-'+uuid.uuid4().hex[:10];observer=None;receipts=[];cleanup=None;actual=None
 try:
  argv=[f.docker,'create','--name',name,'--label','com.docker.compose.project='+f.project,'--network','none','--pid','container:'+cid,'--read-only','--cap-drop','ALL','--cap-add','SYS_PTRACE','--cap-add','DAC_READ_SEARCH','--user','0:0','--security-opt','no-new-privileges','--pids-limit','16','--memory','64m','--cpus','0.25','--mount','type=bind,src='+str(guest.resolve())+',dst=/observer.py,readonly','--entrypoint','/usr/local/bin/dispatcharr-python',f.binding['candidate_image_id'],'-I','-B','/observer.py','--generation',generation,'--host-pid',str(pid),'--CID',cid]
  observer=f.command(argv,seconds=10).strip();assert len(observer)==64 and all(c in'0123456789abcdef'for c in observer)
  info=inspect(observer);host=info['HostConfig'];assert info['Id']==observer and info['Image']==before['Image']and info['Config']['User']=='0:0'and host['ReadonlyRootfs']is True and host['NetworkMode']=='none'and host['PidMode']=='container:'+cid
  assert {x.removeprefix('CAP_')for x in host['CapAdd']}=={'SYS_PTRACE','DAC_READ_SEARCH'}and host['CapDrop']==['ALL']and host['PidsLimit']==16 and host['Memory']==67108864 and host['NanoCpus']==250000000
  assert any(x in('no-new-privileges','no-new-privileges=true')for x in host['SecurityOpt'])
  assert len(info['Mounts'])==1 and info['Mounts'][0]['Source']==str(guest.resolve())and info['Mounts'][0]['RW']is False
  save('observer-isolation.json',{'CID':observer,'image_id':info['Image'],'HostConfig':{k:host[k]for k in('PidMode','NetworkMode','ReadonlyRootfs','CapAdd','CapDrop','PidsLimit','Memory','NanoCpus','SecurityOpt')},'pure_guest_sha256':hashlib.sha256(source).hexdigest()})
  raw=f.command([f.docker,'start','--attach',observer],seconds=15);receipt=f.cli[-1];assert receipt['actual_exit']==0 and receipt['stdout_EOF']and receipt['stderr_EOF']and receipt['process_group_settled'];assert len(raw.encode())<=65536
  stopped=inspect(observer);assert stopped['State']['Running']is False and stopped['State']['ExitCode']==0 and not stopped['State']['OOMKilled']
  actual=json.loads(raw);assert actual['actual_generation']==generation and actual['actual_UID']==['65532']*4 and actual['actual_Main_CID']==cid and actual['actual_host_PID']==pid and not actual['errors']
  after=inspect(cid);assert after['State']['Running']and after['State']['Pid']==pid and after['State']['StartedAt']==state['StartedAt']and after['Image']==before['Image']and f.pid_start(pid)==generation
  actual.update(actual_observer_CID=observer,actual_observer_exit=stopped['State']['ExitCode'],actual_wait_stdout_stderr_EOF=True,actual_Main_generation_verified_before_after=True,observer_only_read_capabilities=['SYS_PTRACE','DAC_READ_SEARCH'],production_Main_privileges_changed=False,pure_guest_sha256=hashlib.sha256(source).hexdigest(),Main_started_at=state['StartedAt'])
 finally:
  if observer:
   present=f.command([f.docker,'container','ls','--all','--no-trunc','--filter','id='+observer,'--format','{{.ID}}'],seconds=5).strip()
   if present:
    assert present==observer
    f.command([f.docker,'rm','--force',observer],seconds=5)
   absent=not f.command([f.docker,'container','ls','--all','--no-trunc','--filter','id='+observer,'--format','{{.ID}}'],seconds=5).strip();assert absent
   cleanup={'exact_owned_observer_CID':observer,'exact_CID_absent':absent,'no_production_Main_removal':True};save('cleanup.json',cleanup)
 if actual is None or not cleanup:raise AssertionError('ActualObserverProofRequired')
 actual['observer_cleanup']=cleanup;save('actual.json',actual);return actual
