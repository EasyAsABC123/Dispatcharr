#!/usr/bin/env python3
"""Frozen four-phase diagnostic, no milestone/gain acceptance; Original authors retain credit."""
import argparse,hashlib,json,os,signal,statistics,subprocess,sys,time,uuid
from pathlib import Path
from sampler import cgroup
OWN=Path(__file__).resolve().parent;ROOT=Path(os.environ.get('DISPATCHARR_PROFILE_SUPPORT_ROOT',Path.cwd())).resolve()
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def main():
 own=cgroup(os.getpid())
 if not own.name.startswith('dispatcharr-resource-') or not own.name.endswith('.scope'):raise RuntimeError('RunInsideDedicatedOrchestratorScope')
 os.environ['DISPATCHARR_PARENT_OBSERVER_CGROUP']=str(own);os.environ['DISPATCHARR_PARENT_OBSERVER_PID']=str(os.getpid())
 p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=Path);p.add_argument('--build-manifest',required=True,type=Path);p.add_argument('--physical-proof',required=True,type=Path);p.add_argument('--exclusive-grant',required=True,help='Literal Root lease reference, not an automatic permission mechanism');a=p.parse_args()
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Fresh empty output required')
 build=json.loads(a.build_manifest.read_text());proof=json.loads(a.physical_proof.read_text())
 if build.get('passed')is not True or build.get('exit_code')!=0 or proof.get('passed')is not True or proof.get('image_id')!=build['image_id']:p.error('Actual passing image build and physical proof required')
 inputs={str(x.relative_to(ROOT)):sha(x) for x in sorted(OWN.glob('*.py'))}
 inputs[str((OWN/'protocol.json').relative_to(ROOT))]=sha(OWN/'protocol.json')
 paths=['docker/compose.rust-playback.yml','docker/compose.rust-candidate.yml','docker/compose.rust-replicas.yml','scripts/rust-baseline/dependencies.py','scripts/rust-integration/seed_accounts.py','scripts/rust-integration/playback_methods.py','scripts/rust-integration/fixture_ports.py']+[f'scripts/rust-playback-baseline/{x}'for x in ('seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini')]
 inputs.update({x:sha(ROOT/x)for x in paths})
 frozen={'scope':'ABBA diagnostic resource preflight only','order':['Original','Native','Native','Original'],'backend_order':['baseline','candidate','candidate','baseline'],'phase_budget_seconds':720,'total_budget_seconds':3000,'measurement_seconds':30,'sample_interval_seconds':1,'protocol':'v2 independent fixture content/PTS whole-window','one_batch_boundary_bytes':255868,'per_tick_advance':'observed, not required; full-window content/PTS authority required','cross_phase_equivalence':'video and audio useful-duration ranges each no wider than independently indexed conservative one-batch encoded-content PTS upper bound; no extra quantization allowance','offwindow_decode':'whole independent fixture and whole retained captures decoded toSHA256 framehash; reference120s/capture60s bounds, full-file packet position/PTS index60s','fixture':'fixed300s H264/AAC320x18025fps, owned custom Original FFmpeg profile, two viewers across two replicas, provider capacity1','gain_acceptance':False,'full_M3_acceptance':False,'GPU_VLC_recovery_scope':False,'exclusive_grant':a.exclusive_grant,'build_manifest':str(a.build_manifest.resolve()),'build_manifest_sha256':sha(a.build_manifest),'physical_proof_sha256':sha(a.physical_proof),'image_id':build['image_id'],'input_sha256':inputs,'cleanup':'every fresh phase tears down ownedCompose project; verify project container/network/volume absence before next phase; no retry/fallback','observer':'dedicated systemd user scope cgroup, mandatory nonoverlap with all owned service cgroups','accounting':'all project services incl deterministicOrigin and PG/Redis/compatibility/FFmpeg plus dedicatedobserver cgroup; backend+infra,origin,observer andwholefixture totals retained; charged vs working-set estimate explicit'}
 (out/'frozen-workload.json').write_text(json.dumps(frozen,indent=2)+'\n');result={'frozen_sha256':sha(out/'frozen-workload.json'),'preflight_only':True,'gain_accepted':False,'phases':[]};started=time.monotonic()
 for index,backend in enumerate(frozen['backend_order']):
  directory=out/f'{index+1}-{backend}';unit='dispatcharr-resource-'+uuid.uuid4().hex+'.scope';log=out/f'{index+1}-controller.log'
  argv=['systemd-run','--user','--scope','--quiet','--unit',unit,'--','python3',str(OWN/'phase.py'),'--output',str(directory),'--backends',backend,'--cases','shared-demand','--input-profile-source18','--require-active-metadata','--candidate-image',build['image_id'],'--candidate-build-manifest',str(a.build_manifest.resolve())]
  row={'phase':index+1,'backend':backend,'argv':argv,'unit':unit};result['phases'].append(row)
  with log.open('wb')as stream:
   proc=subprocess.Popen(argv,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True);deadline=time.monotonic()+720
   while proc.poll()is None and time.monotonic()<deadline and time.monotonic()-started<3000 and log.stat().st_size<=4*1024*1024:time.sleep(.2)
   if proc.poll()is None:
    row['bounded_controller_failure']=True;subprocess.run(['systemctl','--user','kill','--kill-whom=all',unit],capture_output=True,timeout=15);os.killpg(proc.pid,signal.SIGKILL)
   row['actual_exit']=proc.wait(timeout=15)
  reportpath=directory/'shared-ts.json';phase=json.loads(reportpath.read_text())if reportpath.exists()else{};identity=directory/'phase-identity.json';project=phase.get('project')or(json.loads(identity.read_text())['project']if identity.exists()else None)
  row['project']=project;row['cleanup_checks']=[]
  if row.get('bounded_controller_failure') and project:
   row['emergency_owned_cleanup']=[]
   for noun,listing,removal in [('containers',['docker','ps','-aq'],['docker','rm','-f']),('networks',['docker','network','ls','-q'],['docker','network','rm']),('volumes',['docker','volume','ls','-q'],['docker','volume','rm','-f'])]:
    q=subprocess.run(listing+['--filter','label=com.docker.compose.project='+project],capture_output=True,text=True,timeout=15)
    if q.returncode!=0:row['emergency_owned_cleanup'].append({'kind':noun,'actual_exit':q.returncode,'stderr':q.stderr});continue
    ids=q.stdout.split()
    if ids:
     q=subprocess.run(removal+ids,capture_output=True,text=True,timeout=30);row['emergency_owned_cleanup'].append({'kind':noun,'identities':ids,'actual_exit':q.returncode,'stdout':q.stdout,'stderr':q.stderr})

  for kind,cmd in [('containers',['docker','ps','-aq']),('networks',['docker','network','ls','-q']),('volumes',['docker','volume','ls','-q'])]:
   if not project:row['cleanup_checks'].append({'kind':kind,'passed':False,'reason':'no exact owned project identity'});continue
   query=cmd+['--filter','label=com.docker.compose.project='+project];q=subprocess.run(query,capture_output=True,text=True,timeout=15);row['cleanup_checks'].append({'kind':kind,'argv':query,'actual_exit':q.returncode,'stdout':q.stdout,'stderr':q.stderr,'passed':q.returncode==0 and not q.stdout.strip()})
  resources=directory/'resource-samples.json';sample=json.loads(resources.read_text())if resources.exists()else{}
  for name,group in sample.get('groups',{}).items():
   if 'CID' in group:
    query=['docker','ps','-aq','--no-trunc','--filter','id='+group['CID']];q=subprocess.run(query,capture_output=True,text=True,timeout=15);row['cleanup_checks'].append({'kind':name+'-exact-CID','argv':query,'actual_exit':q.returncode,'stdout':q.stdout,'stderr':q.stderr,'passed':q.returncode==0 and not q.stdout.strip()})
  mediafile=directory/'useful-media-v2.json';media=json.loads(mediafile.read_text())if mediafile.exists()else{}
  row['useful_media_v2']=media
  decoders=phase.get('cases',[{}])[0].get('decoders',[])if phase.get('cases')else[]
  row['valid']=row['actual_exit']==0 and phase.get('passed')is True and media.get('passed')is True and all(x['passed']for x in row['cleanup_checks'])and not phase.get('cleanup_errors')and not phase.get('cleanup_error')
  if sample.get('samples'):
   first,last=sample['samples'][0],sample['samples'][-1];row['measurement_seconds']=sample.get('elapsed_seconds');row['media_byte_deltas']=sample.get('media_byte_deltas');row['metrics']={}
   for category in ('backend_and_infrastructure','origin','observer','whole_fixture'):
    vals=[s['simultaneous_aggregate'][category]for s in sample['samples']];row['metrics'][category]={'cpu_core_seconds':(vals[-1]['cpu_usage_usec']-vals[0]['cpu_usage_usec'])/1e6,'charged_memory_median_bytes':statistics.median(v['charged_memory_current_bytes']for v in vals),'charged_memory_max_simultaneous_bytes':max(v['charged_memory_current_bytes']for v in vals),'working_set_estimate_median_bytes':statistics.median(v['working_set_estimate_bytes']for v in vals),'working_set_estimate_max_simultaneous_bytes':max(v['working_set_estimate_bytes']for v in vals)}
  row['raw_sha256']={f.name:sha(f)for f in (reportpath,resources,mediafile)if f.exists()};(out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
  if not row['valid']:break
 result['all_four_valid']=len(result['phases'])==4 and all(x['valid']for x in result['phases'])
 if result['all_four_valid']:
  limits=[r['useful_media_v2']['one_batch_PTS_seconds'] for r in result['phases']];equivalent=[]
  for stream in ('0','1'):
   spans=[viewer['streams'][stream]['useful_duration_seconds'] for phase in result['phases']for viewer in phase['useful_media_v2']['readers']];equivalent.append(max(spans)-min(spans)<=min(limits))
  result['cross_phase_useful_duration_equivalence']=equivalent;result['all_four_valid']=all(equivalent)
 if result['all_four_valid']:
  ratios={}
  for category in ('backend_and_infrastructure','origin','observer','whole_fixture'):
   ratios[category]={}
   for metric in result['phases'][0]['metrics'][category]:
    original=[r['metrics'][category][metric]for r in result['phases']if r['backend']=='baseline'];native=[r['metrics'][category][metric]for r in result['phases']if r['backend']=='candidate'];ratios[category][metric]={'Original_range':[min(original),max(original)],'Native_range':[min(native),max(native)],'descriptive_Native_over_Original_median_ratio':statistics.median(native)/statistics.median(original)if statistics.median(original)else None}
  result['descriptive_ratios_not_gain_acceptance']=ratios
 result['changed_inputs']=[name for name,digest in inputs.items()if sha(ROOT/name)!=digest];result['elapsed_seconds']=time.monotonic()-started;(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');return 0 if result['all_four_valid']and not result['changed_inputs']else 1
if __name__=='__main__':raise SystemExit(main())
