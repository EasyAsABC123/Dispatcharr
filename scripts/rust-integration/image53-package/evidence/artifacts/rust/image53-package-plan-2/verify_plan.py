"""Pure/file/Git package plan checks only; no Docker, Source or Main dispatch."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    plan = json.loads((HERE / 'plan.json').read_text()); support = Path(plan['support_root'])
    if plan['dispatch_authorized'] is not False or len(plan['groups']) != 28: raise ValueError('Blocked28PlanRequired')
    names = [row['name'] for row in plan['groups']]
    if len(set(names)) != 28 or any(not set(row['requires']) <= set(names) for row in plan['groups']): raise ValueError('ExactNodeDependencies')
    meta = json.loads((HERE / 'support-inputs.json').read_text())
    if len(meta['files']) != 6148 or meta['revision'] != plan['support_revision'] or sha(HERE / 'support-inputs.json') != plan['support_input_map_sha256']: raise ValueError('ExactSupportMap')
    for name, digest in meta['files'].items():
        if sha(support / name) != digest: raise ValueError('ChangedFrozenSupport:' + name)
    env = {**os.environ, **json.loads((HERE / 'environment.json').read_text())}
    if subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=support, env=env, text=True).strip() != plan['support_revision']: raise ValueError('DetachedSupportHead')
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=support, env=env, text=True).strip(): raise ValueError('FrozenTrackedSupportChanged')
    candidate = json.loads(Path(plan['candidate']['manifest']).read_text())
    if sha(plan['candidate']['manifest']) != plan['candidate']['manifest_sha256'] or candidate['image_id'] != plan['candidate']['image_id'] or candidate['git_revision'] != plan['candidate']['revision']: raise ValueError('ExactImage53Manifest')
    for row in plan['groups']:
        if row['dispatch_authorized'] is not False or row['cwd'] != str(support.resolve()) or not Path(row['argv'][1]).is_file(): raise ValueError('BlockedFrozenNode')
        if any('image52-packaged-matrix' in arg or 'candidate-build-hash-admission-image52' in arg or 'sha256:983dc2' in arg for arg in row['argv']): raise ValueError('NoOldCandidateOrOutputReuse')
        if '--measure' in row['argv'] or '--api-resource-matrix' in row['argv']: raise ValueError('NoResourceAction')
    if plan['source48_coordination_barrier']['Source48_PASS_or_business_outcome_condition'] is not False: raise ValueError('NoBusinessOutcomeScheduling')
    return {'passed':True,'required_nodes':28,'separately_blocked_new_gates':3,'support_files':6148,'Rust_inputs':791,'dispatch_executed':False,'Docker_Source_Main_performance_executed':False}


if __name__ == '__main__': print(json.dumps(main(), indent=2))
