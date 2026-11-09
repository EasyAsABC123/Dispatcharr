#!/usr/bin/env python3
"""Publish reproducible numerical evidence from a completed frozen M4 matrix.
All original Dispatcharr authors/contributors retain application credit.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--markdown', type=Path, required=True)
    args = parser.parse_args()
    directory = args.input.parent
    report = json.loads(args.input.read_text())
    hashes = json.loads((directory/'tool-source-sha256.json').read_text())
    policy_path = directory/'tool-source/scripts/rust-m4-perf/plan.py'
    workload_path = directory/'tool-source/scripts/rust-m4-perf/workload.py'
    for path in [policy_path,workload_path]:
        assert sha(path) == hashes[str(path.relative_to(directory/'tool-source'))]
    policy = load(policy_path, 'accepted_m4_policy')
    workload = load(workload_path, 'accepted_m4_workload')
    frozen = json.loads((directory/'plan.json').read_text())
    plan = policy.validate_plan(frozen)
    assert report['plan_sha256'] == frozen['plan_sha256']
    assert report['passed'] and report['cleanup_passed'] and report['performance_verified']
    assert policy.final_acceptance(report['pairs'],plan)
    assert report['application_performance_verified'] is False
    assert report['full_m4_parity_verified'] is False
    for pair in report['pairs']:
        for backend,phase in pair['phases'].items():
            assert backend in ('source','candidate')
            rate = plan['fixture'][pair['protocol']+'_bytes_per_second']
            assert not workload.validate_phase(phase,pair['viewers'],rate,plan['budgets'],phase['selected_containers'])
        recalculated = workload.compare_pair(pair['phases']['source'],pair['phases']['candidate'],plan['budgets'])
        assert recalculated['passed'] and recalculated['ratios'] == pair['ratios']

    summaries = []
    for work in plan['workloads']:
        pairs = [pair for pair in report['pairs'] if all(pair[key]==value for key,value in work.items())]
        item = {**work,'pairs':len(pairs),'ratios':{
            key:statistics.median(pair['ratios'][key] for pair in pairs)
            for key in ('cpu_ratio','memory_ratio','throughput_ratio','first_byte_ratio')}}
        for backend in ('source','candidate'):
            phases = [pair['phases'][backend] for pair in pairs]
            item[backend] = {
                'median_mean_group_cpu_percent':statistics.median(statistics.mean(s['cpu_percent'] for s in phase['samples']) for phase in phases),
                'median_maximum_sampled_group_memory_bytes':statistics.median(max(s['memory_bytes'] for s in phase['samples']) for phase in phases),
                'median_first_byte_ms':statistics.median(phase['median_first_byte_ms'] for phase in phases)}
        summaries.append(item)
    assert [row['ratios'] for row in summaries] == [row['ratios'] for row in report['median_ratios']]
    evidence = {
        'scope':plan['scope'],'passed':True,'subset_performance_verified':True,
        'application_performance_verified':False,'full_m4_parity_verified':False,
        'completed_milestones':[],'original_authors_retain_full_application_credit':True,
        'image_id':report['candidate_image_id'],'source_commit':report['source_commit'],
        'candidate_provenance':report['candidate_provenance'],'plan_sha256':report['plan_sha256'],
        'raw_evidence_directory':str(directory),'artifact_sha256':{
            name:sha(directory/name) for name in ['result.json','plan.json','tool-source-sha256.json','candidate-build-manifest.json']},
        'summary_tool_sha256':sha(Path(__file__)),'tool_source_sha256':hashes,
        'plan':plan,'host':report['host'],'containers':report['containers'],
        'fixture_auth':report['fixture_auth'],'preflight':report['preflight'],
        'pairs':report['pairs'],'workload_summaries':summaries,'cleanup_passed':True,
        'notes':[
            'All 12 pairs and 24 measured phases passed; no discarded completed trial or changed budget.',
            'CPU is mean summed selected-container percent of one core. Memory is the maximum sampled summed Docker working memory, not instantaneous peak RSS.',
            'Each viewer keeps one HTTP connection across 15s warmup and 60s measurement; all phases have 30 samples. The load driver is outside the measured containers.',
            'Correctness covers matching 1 MiB prefixes, actual strict audio/video decoder samples, sustained delivery/rates, no EOF/errors and actual WebSocket/counter cleanup; not every later byte is hashed.',
            'Attempt 1 was interrupted after finding fixture JWT lifetime shorter than the full matrix. Available incomplete artifacts are retained without acceptance. Attempt 2 uses 2-hour synthetic tokens; production auth settings unchanged.',
            'Later unaccepted source edits do not inherit these immutable image 20 measurements. Full M4 and application gates remain open.'
        ]}
    args.output.write_text(json.dumps(evidence,indent=2)+'\n')
    lines = ['# VOD and catch-up playback resources','',
             'Full application credit belongs to the original Dispatcharr authors and contributors.','',
             'Immutable image 20 passed all 12 paired trials and 24 measured phases. These are scoped playback results; full M4 and application acceptance remain open.','',
             '| Workload | Viewers | CPU reduction | Working-memory reduction | Throughput ratio |',
             '|---|---:|---:|---:|---:|']
    for row in summaries:
        ratios=row['ratios']
        lines.append(f"| {'VOD' if row['protocol']=='vod' else 'Catch-up'} | {row['viewers']} | {(1-ratios['cpu_ratio'])*100:.1f}% | {(1-ratios['memory_ratio'])*100:.1f}% | {ratios['throughput_ratio']:.4f} |")
    lines += ['', 'Reductions use the median of three paired Rust/original ratios. Absolute values below are medians of the phase metrics; their ratio need not equal the median paired ratio.','',
              '| Workload / viewers | Original CPU (% core) | Rust CPU (% core) | Original memory (MiB) | Rust memory (MiB) |',
              '|---|---:|---:|---:|---:|']
    for row in summaries:
        source,candidate=row['source'],row['candidate']
        lines.append(f"| {row['protocol']} / {row['viewers']} | {source['median_mean_group_cpu_percent']:.3f} | {candidate['median_mean_group_cpu_percent']:.3f} | {source['median_maximum_sampled_group_memory_bytes']/1024**2:.2f} | {candidate['median_maximum_sampled_group_memory_bytes']/1024**2:.2f} |")
    lines += ['', 'The measured group includes the backend, shared PostgreSQL/Redis infrastructure and origin. Both backends use the same observed 2-CPU/512MiB backend limits; infrastructure and origin limits, fixture hashes, all numerical samples, decoder checks and pair results are preserved in the [evidence JSON](../scripts/rust-m4-perf/'+args.output.name+').','',
              'Each phase keeps one HTTP connection per viewer through 15 seconds of warmup and 60 seconds of measurement. All phases collected 30 samples. Orders alternate across three pairs. Budgets were frozen before measurement: CPU ratio≤0.80, sampled working-memory ratio≤0.95, throughput≥0.95, first-byte ratio≤1.10 with a 50ms floor, and zero delivery errors. All passed. Memory is sampled Docker working memory; it is not an instantaneous RSS peak. The host load driver is excluded.','',
              'Correctness includes matching 1 MiB prefixes and actual strict audio/video decoder samples, plus sustained byte rates, no EOF/errors, WebSocket observations and independent provider/metadata cleanup. It does not hash every later delivered byte. Fixtures are generated before sampling; no FFmpeg transcode runs during a trial.','',
              'The initial attempt was interrupted when an audit found synthetic JWTs would expire before the matrix ended. Its available incomplete artifacts remain under `artifacts/rust/m4-resources-image20-1`; no result was accepted from it. The completed run extends only synthetic fixture tokens to 2 hours and checks their remaining lifetime before the matrix and every phase. Application JWT settings, image, workloads and budgets are unchanged.','',
              f"Candidate image: `{report['candidate_image_id']}`.",
              f"Candidate source tree: `{report['candidate_provenance']['source_tree_sha256']}`.",
              f"Frozen plan: `{report['plan_sha256']}`.",'',
              'Run instructions are in [the experiment README](../scripts/rust-m4-perf/README.md). Later migration edits require their own checks; these figures cannot be attached to a newer image without remeasurement.','']
    args.markdown.write_text('\n'.join(lines))
    print(json.dumps({'passed':True,'pairs':len(report['pairs']),'phases':sum(len(row['phases']) for row in report['pairs']),'evidence':str(args.output)}))


if __name__ == '__main__':
    main()
