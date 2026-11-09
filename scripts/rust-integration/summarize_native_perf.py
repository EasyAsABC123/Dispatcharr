#!/usr/bin/env python3
"""Summarize native TS characterization without claiming full application gains.

Original Dispatcharr authors retain full credit. This reports observed metrics;
known frontend/WebSocket/event gaps remain explicit.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics


def container_metrics(report):
    samples=report['measurement']['resource_samples']
    first=next((sample['process_rss_diagnostic'] for sample in samples if 'process_rss_diagnostic' in sample),None)
    if first is None or len(first['containers'])!=len(report['services']):
        raise ValueError('Missing unambiguous single-container service/RSS mapping')
    result=[]
    for service, container in zip(report['services'],first['containers']):
        identifier=container['id']
        stats=[row for sample in samples for row in sample['containers'] if identifier.startswith(row['id'])]
        rss=[row for sample in samples for row in sample.get('process_rss_diagnostic',{}).get('containers',[]) if row['id']==identifier]
        groups={}
        for sample in rss:
            by_name={}
            for process in sample['processes']:
                by_name[process['name']]=by_name.get(process['name'],0)+process['rss_bytes']
            for name,value in by_name.items():groups[name]=max(groups.get(name,0),value)
        result.append({'service':service,'container_id':identifier,
            'cpu_percent_mean':statistics.mean(row['cpu_percent'] for row in stats),
            'docker_working_memory_bytes_peak':max(row['memory_bytes'] for row in stats),
            'process_rss_bytes_sum_peak_diagnostic':max(row['process_rss_bytes_sum_diagnostic'] for row in rss),
            'process_name_rss_sum_peaks_diagnostic':groups,
            'cpu_memory_sample_count':len(stats),'rss_sample_count':len(rss)})
    return result


def summarize(path):
    run=json.loads(path.read_text());directory=path.parent
    rows=[];quality=[]
    config=run['configuration']
    for count in config['viewers']:
        pairs=[]
        for trial in range(1,config['trials']+1):
            pair={'trial':trial}
            for backend in ('baseline','candidate'):
                name=f'{backend}-same-channel-viewers-{count}-trial-{trial}.json'
                report=json.loads((directory/name).read_text());measurement=report['measurement'];summary=measurement['summary']
                valid=not any(summary[key] for key in ('errors','empty_responses','viewers_without_data')) and not measurement['sample_errors'] and len(measurement['resource_samples'])>=3
                quality.append(valid)
                pair[backend]={'artifact':name,'measurement_valid':valid,'summary':summary,'containers':container_metrics(report),
                    'observer_seconds_total':sum(sample.get('process_rss_diagnostic',{}).get('observer_seconds',0) for sample in measurement['resource_samples'])}
            pair['candidate_over_baseline']={key:pair['candidate']['summary'][key]/pair['baseline']['summary'][key] for key in ('cpu_percent_mean','memory_bytes_peak','bytes_per_second','first_byte_ms_p95')}
            pairs.append(pair)
        rows.append({'viewers':count,'paired_trials':pairs,'paired_ratio_medians':{key:statistics.median(pair['candidate_over_baseline'][key] for pair in pairs) for key in pairs[0]['candidate_over_baseline']},
            'paired_ratio_ranges':{key:[min(pair['candidate_over_baseline'][key] for pair in pairs),max(pair['candidate_over_baseline'][key] for pair in pairs)] for key in pairs[0]['candidate_over_baseline']}})
    return {'scope':'native TS same-channel characterization only; no accepted full-application performance gain',
        'matrix_completed':run.get('matrix_completed',False),'measurement_quality_passed':all(quality),
        'application_performance_verified':False,'milestone_complete':False,
        'known_exclusions':['frontend and GUI viewing','WebSocket stats delivery','equivalent application event broadcasts','many-channel and slow-reader resource matrices'],
        'primary_scope':'Docker CPU and working-memory summed across baseline/PostgreSQL/Redis, origin, and candidate when present; source web stopped for candidate measurements',
        'rss_scope':'Per-process RSS diagnostic sums may overcount shared PostgreSQL pages; not the primary memory comparison',
        'observer_scope':'Docker stats plus Docker top RSS every >=5s; observer command durations retained; observer host work is outside container CPU totals',
        'run_artifact':str(path),'run_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'candidate_build_manifest_sha256':run['candidate_build_manifest_sha256'],
        'candidate_image_source_bound':run['candidate_image_source_bound'],
        'workload_configuration':config,'declared_resource_comparison_passed':run.get('resource_comparison_passed'),
        'counts':rows}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path);parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();result=summarize(args.run)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    return 0 if result['matrix_completed'] and result['measurement_quality_passed'] else 1


if __name__=='__main__':raise SystemExit(main())
