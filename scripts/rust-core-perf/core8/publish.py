#!/usr/bin/env python3
"""Independently recompute complete Core8 acceptance after parent review; no workloads/samplers."""
import argparse,hashlib,json,statistics,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
import importlib.util
_schema_path=Path(__file__).resolve().with_name('schema.py')
_schema_spec=importlib.util.spec_from_file_location('_dispatcharr_core8_schema',_schema_path)
_schema=importlib.util.module_from_spec(_schema_spec);_schema_spec.loader.exec_module(_schema)
BASE=_schema.BASE
PRODUCTION=_schema.PRODUCTION
BENCHMARK=_schema.BENCHMARK
ELF=_schema.ELF
RELEASE=_schema.RELEASE
POLICY=_schema.POLICY
identity=_schema.identity
binding=_schema.binding
parent_review=_schema.parent_review
tool_map=_schema.tool_map

def checked_math():
 # These are the exact premeasurement code bytes bundled for audit, not loaded
 # from the experiment or from an arbitrary sys.path/private SUPPORT fallback.
 import importlib.util,types
 require(sha(HERE/'ready-manifest.json')=='10049125746bec86032b207aa677912ea395332a6eccc7141d5ee6aa8fb08676','Exact premeasurement ready manifest changed')
 ready=read(HERE/'ready-manifest.json')
 for name,expected in tool_map(ready).items():
  require(sha(HERE/'measured-tools'/name)==expected,'Measured math/tool changed')
 support=HERE/'support';manifest=read(HERE/'support-manifest.json')
 actual={str(p.relative_to(support)):sha(p)for p in support.rglob('*')if p.is_file()and '__pycache__'not in p.parts}
 require(actual==manifest==ready['support_sha256'],'Complete committed31 support closure changed')
 package_name='_dispatcharr_core8_publication_math'
 package=sys.modules.get(package_name)
 if package is None:
  package=types.ModuleType(package_name);package.__path__=[str(HERE/'measured-tools')];sys.modules[package_name]=package
 require(list(package.__path__)==[str(HERE/'measured-tools')],'Publication module path changed')
 def load(name,path=None):
  qualified=package_name+'.'+name
  selected=path or HERE/'measured-tools'/(name+'.py')
  if qualified in sys.modules:
   module=sys.modules[qualified];require(Path(module.__file__)==selected,'Publication module cache changed');return module
  spec=importlib.util.spec_from_file_location(qualified,path or HERE/'measured-tools'/(name+'.py'))
  module=importlib.util.module_from_spec(spec);sys.modules[qualified]=module;spec.loader.exec_module(module);return module
 policy=load('policy');metrics=load('metrics');pairs=load('orchestrator')
 cgroup=load('cgroup',support/'scripts/rust-guide-perf/cgroup.py')
 return policy,metrics,pairs,cgroup

def read(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def require(v,msg):
 if not v:raise ValueError(msg)
def validate(root,review,load=read):
 policy,metrics,pairs,cgroup=checked_math()
 root=Path(root);used={}
 def get(name):
  p=root/name;used[name]=sha(p);return load(p)
 report=get('functional.json');experiment=get('experiment.json');frozen=get('frozen-plan.json');plan=frozen['policy']
 parent_review(review,sha(root/'experiment.json'))
 policy.validate_plan(plan)
 require(report['passed'] is True and report['performance_verified'] is True and report['cleanup']=='project removed' and report['resources_retained'] is False and report['default_database_unchanged'] is True,'Driver/cleanup/default DB failed')
 unsigned=dict(frozen);seal=unsigned.pop('plan_sha256');require(policy.digest(unsigned)==seal==experiment['plan_sha256'],'Source plan seal changed')
 require(policy.digest(plan)==frozen['policy_sha256']==policy.digest(read(HERE/'protocol.json'))==POLICY,'Policy changed')
 ready=read(HERE/'ready-manifest.json');release=read(HERE/'release-record.json')
 require(sha(HERE/'release-record.json')==RELEASE,'Release record bytes changed');identity(ready,release)
 require(frozen['native_binary_sha256']==report['native_binary_sha256']==ready['native_binary']['sha256']==sha(root/'native-core-peer')==ELF,'Measured ELF changed')
 fixture=frozen['fixture_binding'];binding(fixture,report)
 require(report['original_full_source_map_sha256']==policy.digest(read(HERE/'original-source-map.json'))=='4e03949255c204e67a73ae2f05c19277438c05aa7394d47d560c0c90c8bc65c1','Complete Original source changed')
 require(report['complete_support_map_sha256']==policy.digest(ready['support_sha256']),'Complete support identity changed')
 require(report['functional_proof_sha256']==ready['functional']['sha256'],'Premeasurement44 functional proof changed')
 for key in ['native_binary_sha256','original_archive_sha256','tools_sha256','support_sha256','functional_proof_sha256']:require(fixture[key]==report[key],'Binding mismatch '+key)
 require(report['original_archive_sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','Original archive changed')
 critical=['docker/compose.rust-playback.yml',*[f'scripts/rust-playback-baseline/{name}'for name in ['start.sh','supervisor.py','uwsgi.ini','seed.py','origin.py']],*[f'scripts/rust-integration/{name}'for name in ['run.py','backup_fixture.py','fixture_ports.py']],*[f'scripts/rust-guide-perf/{name}'for name in ['observer.py','cgroup.py','docker_stats.py']],'scripts/rust-perf/perf.py']
 require(report['tools_sha256']==tool_map(ready) and report['support_sha256']=={k:ready['support_sha256'][k]for k in critical},'Measured tools/support changed')
 for name,h in report['tools_sha256'].items():require(sha(root/'core-tools'/name)==h,'Raw tool changed')
 qualification=[];phases=[];public=[]
 expected=[]
 for v in plan['viewers']:
  for p in plan['source_only_period_candidates_ms']:expected.append((f'qual-source-{v}-{p}','source',v,p))
 period=frozen['selected_common_period_ms']
 for v in plan['viewers']:
  for trial,order in enumerate(plan['orders'],1):
   for b in order.split(','):expected.append((f'measure-{v}-{trial}-{b}',b,v,period))
 require({p.name for p in root.glob('*-resources.json')}=={x[0]+'-resources.json'for x in expected},'Extra/missing resource windows')
 require([p['label'] for p in experiment['phases']]==[x[0]for x in expected],'Missing/extra/reordered phases')
 for label,b,v,p in expected:
  value=get(label+'.json');raw=get(label+'-resources.json');normal=get(label+'-normalized.json');exit=get(label+'.json.exit.json')
  require(exit=={'worker_exit_code':0,'wrapper_retained':True},'Worker exit/wrapper failed')
  work=policy.validate_work(value,b,v,p,15,60,require_history=b=='native')
  require(work['pacing_fraction']>=.95,'Producer pacing failed')
  lifecycle_public={}
  for phase in ['between-phase','setup','drain']:
   life=get(label+'-'+phase+'-lifecycle.json');metrics.assert_no_oom(life);require(life['zero_oom_verified'] is True,'Lifecycle OOM unverified')
   lifecycle_public[phase]={'services':life['services'],'state':{k:{f:x[f] for f in ['Running','OOMKilled','ExitCode']}for k,x in life['docker_state'].items()}}
   require(all(x['Running'] is True and x['OOMKilled'] is False for x in life['docker_state'].values()),'Lifecycle state failed')
  counter=raw['counters'];require(counter['before']==raw['counter_before'],'Counter start changed')
  for service in ['core','infrastructure']:
   a=counter['before']['services'][service];z=counter['after']['services'][service]
   require(a['memory_events']['oom']==a['memory_events']['oom_kill']==z['memory_events']['oom']==z['memory_events']['oom_kill']==0,'Absolute OOM')
   require(a['cpu_quota']==200000 and a['cpu_period']==100000 and a['memory_max']==536870912,'Cgroup limits')
   require(cgroup.delta(a,z)==counter['services'][service],'Counter delta changed')
  for sample in raw['samples']:
   start=sample['sample_started_epoch'];end=sample['sample_completed_epoch']
   require(start<=end and sample['included_in_window']==(start>=raw['window_start_epoch'] and end<=raw['window_end_epoch']),'Sample window classification')
   for c in sample['containers']:require(c['memory_bytes']==max(0,c['memory_usage_bytes']-c['inactive_file_bytes']) and c['memory_limit_bytes']==536870912,'Docker working-set mismatch')
  summary=metrics.summarize(raw,plan)
  require(work==normal['work'] and summary==normal['metrics'],'Normalized evidence mismatch')
  phases.append(normal)
  if label.startswith('qual-'):
   qualification.append(dict(backend='source',viewers=v,period_ms=p,passed=True,errors=0,pacing_fraction=work['pacing_fraction'],**{k:summary[k]for k in ['core_cpu_percent','core_memory_fraction','core_throttled_period_fraction']}))
  identifiers={x['id']:k for k,x in counter['selected_services'].items()}
  samples=[{'container_memory':[{key:c[key]for key in ['id','memory_bytes','memory_usage_bytes','inactive_file_bytes','memory_limit_bytes']}for c in s['containers']],'observer_seconds':s['observer_seconds'],'start_epoch':s['sample_started_epoch'],'end_epoch':s['sample_completed_epoch'],'included_in_window':s['included_in_window'],'working_memory_bytes':s['memory_bytes'],'service_working_memory_bytes':{identifiers[c['id']]:c['memory_bytes']for c in s['containers']}}for s in raw['samples']]
  public.append(dict(label=label,backend=b,viewers=v,period_ms=p,work=work,metrics=summary,window_start_epoch=raw['window_start_epoch'],window_end_epoch=raw['window_end_epoch'],counters={k:counter[k]for k in ['before','after','services','selected_services']},samples=samples,lifecycle=lifecycle_public,peer={k:value[k]for k in ['passed','backend','viewers','period_ms','chunk_bytes','produced','producer_late','final_full_bytes_per_viewer','drain_release_seen']},readers=[{k:r.get(k,0)for k in ['bytes','delivered','historical_reads','measured']}for r in value['readers']]))
 require(qualification==experiment['qualification']==get('source-qualification.json') and policy.digest(qualification)==frozen['qualification_sha256'],'Qualification mismatch')
 require(policy.select_common_period(plan,qualification)==period,'Source common pacing changed')
 results=[]
 for v in plan['viewers']:
  for trial,order in enumerate(plan['orders'],1):
   a=next(x for x in phases if x['label']==f'measure-{v}-{trial}-source');b=next(x for x in phases if x['label']==f'measure-{v}-{trial}-native');r=pairs.pair(plan,a,b);r.update(trial=trial,order=order);require(r['passed'],'Paired budget/work failed');results.append(r)
 require(results==experiment['pairs'] and experiment['passed'] is True and experiment['performance_verified'] is True,'Pair aggregate mismatch')
 require(get('matrix-progress.json')['pairs']==results,'Progress mismatch')
 global_lifecycle={}
 for name in ['infrastructure-setup-lifecycle.json','infrastructure-completed-lifecycle.json','final-lifecycle.json']:
  life=get(name);metrics.assert_no_oom(life);require(life['zero_oom_verified'] is True,'Final lifecycle unverified')
  global_lifecycle[name]={'services':life['services'],'state':{k:{field:x[field]for field in ['Running','OOMKilled','ExitCode']}for k,x in life['docker_state'].items()}}
 require(report['resource_sample_count']==sum(len(x['samples'])for x in public),'Periodic sample aggregate mismatch')
 require(all(x['metrics']['samples']>=plan['minimum_samples']for x in public),'Fixed minimum cadence failed')
 medians={str(v):{k:statistics.median(r[k]for r in results if r['viewers']==v)for k in ['cpu_ratio','memory_ratio']}for v in [1,10,100]}
 return {'passed':True,'performance_verified':True,'format_version':2,'scope':'Immutable Core8 shared-buffer core + common PGRedis only; no HTTP/application/CLI gain transfer','production_base_commit':BASE,'production_source_map_sha256':PRODUCTION,'release_record_sha256':RELEASE,'parent_review':review,'parent_review_sha256':policy.digest(review),'source_plan':frozen,'source_qualification':qualification,'global_lifecycle':global_lifecycle,'seal':seal,'policy_sha256':frozen['policy_sha256'],'native_binary_sha256':frozen['native_binary_sha256'],'native_source_map_sha256':ready['native_source_map_canonical_sha256'],'source_revision':plan['source_revision'],'source_image':plan['source_image'],'pairs':results,'cohort_medians':medians,'phases':public,'retained_raw_sha256':used,'sample_count':sum(len(x['samples'])for x in public),'cleanup':'project removed','default_database_unchanged':True}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',required=True,type=Path);p.add_argument('--parent-review',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();v=validate(a.input,read(a.parent_review));a.output.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n');print(json.dumps(v['cohort_medians']))
