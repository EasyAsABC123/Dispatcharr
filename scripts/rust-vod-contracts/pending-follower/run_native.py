"""Exact reconstructed-source real Redis/HTTP regression executor; no Main claim."""
import hashlib,json,os,re,shutil,subprocess,time,uuid,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
report={'passed':False,'scope':'Reconstructed exact748 Rust +existing real Redis/loopbackHTTP/media fixture-resolver regressions; no packaged Main/SQL/auth acceptance','tests':[]}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def docker(*args,timeout=180):return subprocess.check_output(['docker',*args],stderr=subprocess.STDOUT,timeout=timeout)
cid=None;generator=None
try:
 peers=HERE/'native-peers';peers.mkdir();wanted={'idle_entry','generation_scope','generation_scope_physical','http_recovery','pending_follower_http'}
 for line in(HERE/'test-binaries.jsonl').read_text().splitlines():
  row=json.loads(line)
  if row.get('reason')=='compiler-artifact'and row.get('executable')and row['target']['name']in wanted:
   shutil.copyfile(row['executable'],peers/row['target']['name']);(peers/row['target']['name']).chmod(0o755)
 assert{p.name for p in peers.iterdir()}==wanted
 report['binary_sha256']={p.name:sha(p)for p in peers.iterdir()}
 media=HERE/'media';media.mkdir(mode=0o777);media.chmod(0o777);evidence=HERE/'response-prefixes';evidence.mkdir(mode=0o777);evidence.chmod(0o777)
 generator='dispatcharr-follower-media-'+uuid.uuid4().hex[:12]
 command=['docker','run','--name',generator,'--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','256m','--tmpfs','/tmp:rw,size=16m,mode=1777','--mount','type=bind,src='+str(media)+',dst=/media','--entrypoint','ffmpeg',IMAGE,'-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=320x180:rate=25','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','30','-c:v','libx264','-preset','ultrafast','-threads','1','-pix_fmt','yuv420p','-g','50','-keyint_min','50','-sc_threshold','0','-b:v','1000k','-c:a','aac','-b:a','128k','-flags:v','+bitexact','-flags:a','+bitexact','-fflags','+bitexact','-movflags','+faststart','-f','mp4','/media/fixture.mp4']
 generated=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=90);(HERE/'media-generation.log').write_bytes(generated.stdout);assert generated.returncode==0;docker('rm',generator);generator=None
 report['media']={'sha256':sha(media/'fixture.mp4'),'bytes':(media/'fixture.mp4').stat().st_size,'first_8192_sha256':hashlib.sha256((media/'fixture.mp4').read_bytes()[:8192]).hexdigest()};assert report['media']['first_8192_sha256']=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
 name='dispatcharr-follower-regressions-'+uuid.uuid4().hex[:12]
 cid=docker('run','-d','--name',name,'--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','256m','--tmpfs','/tmp:rw,size=16m,mode=1777','--workdir','/tmp','-v',str(peers)+':/native-peers:ro','-v',str(media)+':/media:ro','-v',str(evidence)+':/evidence','-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','REDIS_RESTART_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_RECOVERY_MEDIA=/media/fixture.mp4','--entrypoint','redis-server',IMAGE,'--save','','--appendonly','no','--bind','127.0.0.1','--requirepass','playback-fixture-only').decode().strip()
 for _ in range(50):
  try:
   assert docker('exec',cid,'redis-cli','-a','playback-fixture-only','PING').strip().endswith(b'PONG');break
  except Exception:time.sleep(.1)
 else:raise TimeoutError('RedisReady')
 for target,selected in [('idle_entry','pending_follower_'),('idle_entry','idle_entry_'),('generation_scope','local_scope_'),('generation_scope_physical','local_physical_'),('pending_follower_http','pending_http_'),('http_recovery','actual_http_')]:
  if '--only-pending-http' in sys.argv and target!='pending_follower_http':continue
  print('START',target,selected,flush=True)
  result=subprocess.run(['docker','exec',cid,'/native-peers/'+target,selected,'--ignored','--nocapture','--test-threads=1'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=600)
  file=HERE/(target+'-'+selected+'tests.log');file.write_bytes(result.stdout);match=re.search(rb'test result: ok\. (\d+) passed; 0 failed;',result.stdout);count=int(match[1])if match else 0
  report['tests'].append({'target':target,'filter':selected,'returncode':result.returncode,'passed_count':count,'log_sha256':sha(file)});(HERE/'regression-progress.json').write_text(json.dumps(report,indent=2)+'\n');print('END',target,count,result.returncode,flush=True)
  assert result.returncode==0 and count>0
 report['passed']=True
except BaseException as error:
 report['error_type']=type(error).__name__;report['error_message']=str(error)
 if isinstance(error,subprocess.CalledProcessError):(HERE/'docker-failure.log').write_bytes(error.output or b'')
finally:
 for container in [cid,generator]:
  if container:
   try:docker('rm','-f',container)
   except Exception:report.update(passed=False,cleanup_error=True)
 report['owned_containers_removed']=not report.get('cleanup_error',False)
 (HERE/'regressions.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'tests':report['tests'],'error':report.get('error_message')}))
raise SystemExit(0 if report['passed']else 1)
