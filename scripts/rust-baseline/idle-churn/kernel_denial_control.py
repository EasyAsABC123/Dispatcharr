"""Owned non-application kernel permission control; not Source baseline evidence."""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import time
import uuid

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(exist_ok=False)
    controller = load(HERE/'run.py', 'kernel_control_m0')
    stats_path = ROOT/'scripts/rust-guide-perf/docker_stats.py'
    stats_sha256 = hashlib.sha256(stats_path.read_bytes()).hexdigest()
    if stats_sha256 != 'bc554c7c30ba004dc5b8a6e78758ca1431696d0ce94302180b6cc5aa693ddb25':
        raise ValueError('Committed Docker sampler authority changed')
    stats_module = load(stats_path, 'kernel_docker_stats')
    name = 'dispatcharr-m0-denial-' + uuid.uuid4().hex[:10]
    result = dict(passed=False, scope=__doc__, container=name, application_source_or_credentials_changed=False,
                  no_Source_baseline_acceptance=True, docker_stats_public_path='scripts/rust-guide-perf/docker_stats.py',
                  docker_stats_sha256=stats_sha256, docker_stats_authority_commit='2fbeabcd69802ef82c7dcb2be277d622e6eba7df', full_policy_sha256=hashlib.sha256((HERE/'policy.json').read_bytes()).hexdigest())
    sampler = None
    def command(argv, timeout=10):
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        with (out/'commands.jsonl').open('a') as f:
            f.write(json.dumps(dict(command=argv, exit_code=completed.returncode, stderr=completed.stderr[:8192]))+'\n')
        completed.check_returncode()
        return completed.stdout
    try:
        ident = command(['docker', 'run', '-d', '--name', name, '--label', 'dispatcharr.m0.kernel-control='+name,
                         '--network', 'none', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                         '--cpus', '0.25', '--memory', '64m', '--pids-limit', '32', '--tmpfs', '/tmp:size=1048576,mode=1777',
                         '-v', str(HERE/'proc_snapshot.py')+':/m0-census.py:ro', '--entrypoint', '/dispatcharrpy/bin/python',
                         IMAGE, '-c', 'import time;time.sleep(120)']).strip()
        inspected = json.loads(command(['docker', 'inspect', ident]))[0]
        result['limits'] = {k:inspected['HostConfig'][k] for k in ['NanoCpus', 'Memory', 'PidsLimit', 'CapDrop', 'SecurityOpt']}
        assert result['limits']['CapDrop'] == ['ALL']
        helper = 'import ctypes,os,time;assert ctypes.CDLL(None,use_errno=True).prctl(4,0,0,0,0)==0;open("/tmp/ready","w").write(str(os.getpid()));time.sleep(16)'
        command(['docker', 'exec', '-d', ident, '/dispatcharrpy/bin/python', '-c', helper])
        pid = int(command(['docker', 'exec', ident, '/dispatcharrpy/bin/python', '-c', 'import pathlib,time;p=pathlib.Path("/tmp/ready");end=time.monotonic()+3\nwhile not p.exists() and time.monotonic()<end:time.sleep(.05)\nprint(p.read_text())']).strip())
        result['owned_nondumpable_helper_pid'] = pid
        direct = stats_module.Stats({'control': {'id':ident, 'limits': {'memory_bytes':67108864}}})
        direct.prime()
        policy = json.loads((HERE/'policy.json').read_text())
        sampler = controller.Sampler(direct, {'control': {'id':ident}}, command, policy, out/'samples.jsonl')
        sampler.label = 'kernel-denial-control'
        sampler.thread.start()
        deadline = time.monotonic()+60
        while time.monotonic()<deadline and not sampler.errors:
            denied = [row for row in sampler.rows if row['census']['control'].get('permission_denied_processes', 0)]
            if denied:
                later = [row for row in sampler.rows if row['monotonic']>denied[-1]['monotonic'] and row.get('phase_stable') and controller.complete_census(row)]
                if len(later)>=policy['budgets']['sample_minimum_per_idle']:
                    break
            time.sleep(.2)
        sampler.close()
        denied = [row for row in sampler.rows if row['census']['control'].get('permission_denied_processes', 0)]
        later = [row for row in sampler.rows if denied and row['monotonic']>denied[-1]['monotonic'] and row.get('phase_stable') and controller.complete_census(row)]
        helper_marked = any(pid in part.get('denied_process_ids', []) for row in denied for part in row['census']['control']['per_uid'])
        result.update(samples=len(sampler.rows), denied_samples=len(denied), later_complete_stable_samples=len(later),
                      helper_pid_explicitly_denied=helper_marked, all_denied_rows_incomplete=all(not row['census']['control']['complete'] for row in denied),
                      observer_errors=sampler.errors)
        result['passed'] = bool(denied and helper_marked and result['all_denied_rows_incomplete'] and len(later)>=5 and not sampler.errors)
        result['samples_sha256'] = hashlib.sha256((out/'samples.jsonl').read_bytes()).hexdigest()
    except Exception as error:
        result.update(error_type=type(error).__name__, error_message=str(error))
    finally:
        if sampler and sampler.thread.is_alive():
            sampler.close()
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, text=True, timeout=15)
        remaining = command(['docker', 'ps', '-aq', '--filter', 'name=^/'+name+'$']).strip()
        result['owned_container_removed'] = not remaining
        result['passed'] = result['passed'] and not remaining
        (out/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    return 0 if result['passed'] else 1
if __name__ == '__main__':
    raise SystemExit(main())
