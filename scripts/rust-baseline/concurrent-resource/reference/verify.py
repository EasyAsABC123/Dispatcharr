#!/usr/bin/env python3
"""Recompute published Source coverage/budgets; no fixture or sampler execution.
SQL/tasks/media are separately bound recorded attestations, not rerun here.
"""
import argparse,contextlib,hashlib,importlib.util,json,math,sys,types
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROLES={'baseline','source','origin','worker'}
def require(value,why):
 if not value:raise ValueError(why)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
@contextlib.contextmanager
def helpers(directory,expected):
 old={n[:-3]:sys.modules.get(n[:-3])for n in expected};loaded={}
 try:
  for name in['canonical_merge.py','coverage.py','interval_coverage.py','resource_window.py']:
   require(sha(directory/name)==expected[name],'HelperHash:'+name);short=name[:-3];module=load(directory/name,short);sys.modules[short]=module;loaded[short]=module
  yield loaded
 finally:
  for name in expected:
   short=name[:-3]
   if old.get(short)is None:sys.modules.pop(short,None)
   else:sys.modules[short]=old[short]
def finite(value):return type(value)in(int,float)and math.isfinite(value)
def verify(directory=HERE,deep_repo=None):
 directory=Path(directory);patch=json.loads((directory/'captured-activation-patch.json').read_text());require(hashlib.sha256(patch['utf8_unified_diff'].encode()).hexdigest()==patch['sha256'],'ActivationPatchBytes');data=json.loads((directory/'observations.json').read_text());captured=json.loads((directory/'captured-plan.json').read_text());require(sha(directory/'captured-plan.json')==data['plan_sha256']and captured['activation_patch_sha256']==patch['sha256']and captured['controller_sha256']==data['expected_activated_file_sha256'],'CapturedActivationPlan');controller=directory.parent/'controller';policy=json.loads((controller/'policy.json').read_text());require(sha(controller/'policy.json')==data['policy_sha256'],'PolicyHash');require(data['schema']==1,'Schema');reps=data['repetitions'];require([r['repetition']for r in reps]==[1,2,3],'ExactlyThreeFixedRepetitions');require(sha(directory/'cgroup_reference.py')==data['cgroup_helper_sha256'],'CgroupHelperHash');cg=load(directory/'cgroup_reference.py','pure_cgroup_reference');results=[];first_unrecomputed=0
 with helpers(controller,data['helper_sha256'])as modules:
  for rep in reps:
   n=rep['repetition'];summary_path=directory/('repetition-'+str(n)+'.json');require(sha(summary_path)==rep['summary_sha256'],'SummaryHash');summary=json.loads(summary_path.read_text());cue=directory/('historical-cue-'+str(n)+'.json');require(sha(cue)==summary['cue_sha256'],'CueHash');require(rep['observer_errors']==[]and rep['observer_threads_drained']is True,'ObserverValidity');work=rep['workload_cgroup'];require(set(work['selected_services'])==ROLES,'FourRoles');mapping={v['id']:name for name,v in work['selected_services'].items()};require(len(mapping)==4,'DistinctIDs');previous={}
   for row in rep['resources']:
    require(set(row['service_names'])==ROLES and len(row['containers'])==4,'ResourceRoleCoverage');require(all(finite(row[k])for k in['monotonic','completed_monotonic','cpu_percent','memory_bytes']),'FiniteResourceFields');require(row['completed_monotonic']>=row['monotonic'],'ResourceClock');require(len({r['id']for r in row['containers']})==4,'ResourceDistinctIDs');cpu=0;memory=0
    for item in row['containers']:
     role=mapping[item['id']];require(item['memory_limit_bytes']==policy['caps'][role]['memory_bytes'],'ExactMemoryCap');usage=item['memory_usage_bytes'];inactive=item['inactive_file_bytes'];expected=usage-inactive if inactive<usage else usage;require(item['memory_bytes']==expected,'WorkingMemoryFormula');require(all(finite(item[k])and item[k]>=0 for k in['cpu_percent','cpu_usage_ns','sampled_monotonic','memory_bytes']),'FiniteContainerFields');memory+=item['memory_bytes'];cpu+=item['cpu_percent']
     start,end=row['cpu_intervals'][role];require(finite(start)and finite(end)and end>start and end==item['sampled_monotonic'],'CPUInterval')
     if role in previous:
      before=previous[role];require(start==before['sampled_monotonic']and item['cpu_usage_ns']>=before['cpu_usage_ns'],'CPUContinuity');calculated=(item['cpu_usage_ns']-before['cpu_usage_ns'])/(end-start)/10000000;require(math.isclose(calculated,item['cpu_percent'],rel_tol=1e-8,abs_tol=1e-8),'CPUArithmetic')
     previous[role]=item
    require(memory==row['memory_bytes']and math.isclose(cpu,row['cpu_percent'],rel_tol=1e-9,abs_tol=1e-9),'SimultaneousResourceSums')
   # Initial baseline CPU counter was not retained; do not invent it from a ratio.
   first_unrecomputed+=1
   deltas={role:cg.delta(work['before']['services'][role],work['after']['services'][role])for role in ROLES};mean=sum(x['cpu_percent_mean']for x in deltas.values());require(math.isclose(mean,work['cpu_percent_mean'],rel_tol=1e-10,abs_tol=1e-10),'WorkloadCgroupCPU')
   for role in ROLES:require(deltas[role]==work['services'][role],'WorkloadCgroupDelta')
   window=types.SimpleNamespace(samples=types.SimpleNamespace(rows=rep['resources']),censuses=types.SimpleNamespace(rows=rep['censuses']),phases=rep['phases'],workload_after=work);checks=modules['resource_window'].proposed_checks(window,policy);require(checks==rep['recorded_checks'],'RecomputedBudgetChecks');quality=[x for x in checks if'/CPU-intervals/'in x['id']or'/complete-census-intervals'in x['id']or x['id']=='required-phase-spans'];require(len(quality)==15 and all(x['passed']for x in quality),'IntervalCoverage');failed=[x for x in checks if not x['passed']];require([(x['id'],x['observed'],x['limit'])for x in failed]==[('settled/process-growth',1,0),('settled/fd-growth',144,16),('settled/socket-growth',18,16)],'PreservedSourceBudgetNegatives');require(summary['observation_coverage_valid']is True and summary['Source_budget_acceptance']is False and summary['independently_owned_cleanup_empty']is True,'ReferenceStatus');require(summary['Source_workload']['task_executions']==6 and summary['Source_workload']['durable_checks']==44 and summary['default_database_unchanged']is True,'RecordedWorkAttestation');complete=sum(modules['coverage'].eligible_census_row(r)for r in rep['censuses']);require(complete==summary['complete_eligible_census_rows'],'RecomputedEligibleCensuses')
   results.append({'repetition':n,'observation_valid':True,'Source_budget_acceptance':False,'resource_rows':len(rep['resources']),'census_rows':len(rep['censuses']),'eligible_census_rows':complete,'failed_budgets':failed})
 raw_checked=0
 if deep_repo is not None:
  root=Path(deep_repo).resolve()
  for rep in reps:
   for ref in rep['raw_bindings']+rep['media_bindings']:
    path=(root/ref['path']).resolve();require(path.is_relative_to(root),'DeepPathEscape');require(sha(path)==ref['sha256'],'DeepEvidenceHash');raw_checked+=1
 return {'scope':'published numeric projection recomputation; SQL/task/media/cleanup attestations bound but not reexecuted','reference_observation_valid_all3':True,'Source_budget_acceptance':False,'repetitions':results,'resource_rows':sum(r['resource_rows']for r in results),'census_rows':sum(r['census_rows']for r in results),'eligible_census_rows':sum(r['eligible_census_rows']for r in results),'first_CPU_rows_attested_not_counter_recomputed':first_unrecomputed,'deep_raw_hashes_checked':raw_checked,'Native_enabled':False,'performance_gain_claim':False,'M0_complete':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--deep-repo',type=Path);a=p.parse_args();print(json.dumps(verify(deep_repo=a.deep_repo),indent=2))
if __name__=='__main__':main()
