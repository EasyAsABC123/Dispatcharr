#!/usr/bin/env python3
"""Public-only numeric verification; no ignored artifacts, ELF or workloads.
Measured peers verify actual bytes; projection carries counters/work attestations
and raw hashes, not51GiB media. Original Dispatcharr authors retain credit.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import statistics
spec=importlib.util.spec_from_file_location('_core8_projection_math',Path(__file__).with_name('publish.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)

def validate(data):
    policy,metrics,pairs,cgroup=p.checked_math()
    require=p.require
    require(data.get('passed')is True and data.get('performance_verified')is True and data.get('format_version')==2,'Accepted Core8 projection required')
    require(data['production_base_commit']==p.BASE and data['production_source_map_sha256']==p.PRODUCTION
            and data['native_source_map_sha256']==p.BENCHMARK and data['native_binary_sha256']==p.ELF
            and data['release_record_sha256']==p.RELEASE and data['policy_sha256']==p.POLICY,'Identity mismatch')
    frozen=data['source_plan'];unsigned=dict(frozen);seal=unsigned.pop('plan_sha256')
    require(policy.digest(unsigned)==seal==data['seal'],'Source seal changed')
    plan=frozen['policy'];require(policy.digest(plan)==p.POLICY,'Fixed budget changed')
    policy.validate_plan(plan);p.binding(frozen['fixture_binding'],frozen['fixture_binding'])
    require(data['cleanup']=='project removed'and data['default_database_unchanged']is True,'Cleanup failed')
    require(policy.digest(data['parent_review'])==data['parent_review_sha256'],'Parent review changed')
    p.parent_review(data['parent_review'],data['retained_raw_sha256']['experiment.json'])
    period=policy.select_common_period(plan,data['source_qualification'])
    require(period==frozen['selected_common_period_ms'] and policy.digest(data['source_qualification'])==frozen['qualification_sha256'],'Source rate qualification changed')
    expected=[]
    for viewers in plan['viewers']:
        for candidate in plan['source_only_period_candidates_ms']:
            expected.append((f'qual-source-{viewers}-{candidate}','source',viewers,candidate))
    for viewers in plan['viewers']:
        for trial,order in enumerate(plan['orders'],1):
            for backend in order.split(','):expected.append((f'measure-{viewers}-{trial}-{backend}',backend,viewers,period))
    require([(row['label'],row['backend'],row['viewers'],row['period_ms'])for row in data['phases']]==expected,'Missing/extra/reordered phases')
    qualification=[];sample_count=0;life_count=0
    def lifecycle(value):
        nonlocal life_count
        metrics.assert_no_oom(value)
        require(all(row['Running']is True and row['OOMKilled']is False for row in value['state'].values()),'Lifecycle state failed')
        life_count+=1
    for row in data['phases']:
        for value in row['lifecycle'].values():lifecycle(value)
        require(set(row['lifecycle'])=={'between-phase','setup','drain'},'Lifecycle boundary missing')
        counter=row['counters']
        require(set(counter['services'])=={'core','infrastructure'},'Whole service group required')
        for name in ['core','infrastructure']:
            a=counter['before']['services'][name];z=counter['after']['services'][name]
            require(a['cpu_quota']==200000 and a['cpu_period']==100000 and a['memory_max']==536870912,'Finite limits changed')
            require(a['memory_events']['oom']==a['memory_events']['oom_kill']==z['memory_events']['oom']==z['memory_events']['oom_kill']==0,'Absolute OOM')
            require(cgroup.delta(a,z)==counter['services'][name],'Raw CPU delta changed')
        samples=[]
        for value in row['samples']:
            start=value['start_epoch'];end=value['end_epoch']
            require(start<=end and value['included_in_window']==(start>=row['window_start_epoch'] and end<=row['window_end_epoch']),'Window exclusion changed')
            for item in value['container_memory']:
                require(item['memory_bytes']==max(0,item['memory_usage_bytes']-item['inactive_file_bytes']) and item['memory_limit_bytes']==536870912,'Raw working set changed')
            samples.append({'containers':value['container_memory'],'memory_bytes':value['working_memory_bytes'],'included_in_window':value['included_in_window']})
        summary=metrics.summarize({'samples':samples,'counters':counter},plan)
        require(summary==row['metrics'],'Numeric summary changed')
        peer=dict(row['peer'],readers=row['readers'])
        work=policy.validate_work(peer,row['backend'],row['viewers'],row['period_ms'],15,60,require_history=row['backend']=='native')
        require(work==row['work']and work['pacing_fraction']>=.95,'Byte classes/pacing changed')
        if row['label'].startswith('qual-'):
            qualification.append(dict(backend='source',viewers=row['viewers'],period_ms=row['period_ms'],passed=True,errors=0,pacing_fraction=work['pacing_fraction'],**{key:summary[key]for key in ['core_cpu_percent','core_memory_fraction','core_throttled_period_fraction']}))
        sample_count+=len(samples)
    require(qualification==data['source_qualification'],'Qualification phase mismatch')
    require(set(data['global_lifecycle'])=={'infrastructure-setup-lifecycle.json','infrastructure-completed-lifecycle.json','final-lifecycle.json'},'Final lifecycle missing')
    for value in data['global_lifecycle'].values():lifecycle(value)
    require(life_count==84,'Lifecycle cardinality mismatch')
    recomputed=[]
    for viewers in plan['viewers']:
        for trial,order in enumerate(plan['orders'],1):
            source=next(row for row in data['phases']if row['label']==f'measure-{viewers}-{trial}-source')
            native=next(row for row in data['phases']if row['label']==f'measure-{viewers}-{trial}-native')
            value=pairs.pair(plan,source,native);value.update(trial=trial,order=order)
            require(value['passed'],'Pair budget failed');recomputed.append(value)
    require(recomputed==data['pairs']and sample_count==data['sample_count'],'Pair/sample aggregate changed')
    medians={str(viewers):{key:statistics.median(row[key]for row in recomputed if row['viewers']==viewers)for key in ['cpu_ratio','memory_ratio']}for viewers in [1,10,100]}
    require(medians==data['cohort_medians'],'Medians changed')
    return {'passed':True,'pairs':9,'windows':27,'samples':sample_count,'lifecycle_zero_oom_reports':life_count,'cohort_medians':medians}
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--projection',required=True,type=Path);args=parser.parse_args()
    print(json.dumps(validate(json.loads(args.projection.read_text()))))
