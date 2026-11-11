"""Private active-core lifecycle; functional mode never constructs a sampler.
Original Dispatcharr authors retain application/algorithm credit and licenses.
"""
import json
import os
from pathlib import Path
import subprocess
import time


TEST = 'engine::core_perf_peer::actual_production_input_store_channel_history_workload'
HELD = 'engine::core_perf_peer::held_cached_and_history_payloads_retain_final_admission_after_stop'


class CoreRuntime:
    def __init__(self, compose, env, output, database, source, tools, plan):
        self.compose = compose
        self.env = env
        self.output = Path(output)
        self.database = database
        self.plan = plan
        self.active = None
        self.pending_drains = set()
        overlay = self.output / 'core-runtime.yml'
        config = {'services': {
            'baseline': {'cpus': 2.0, 'mem_limit': '512m'},
            'core': {'image': plan['source_image'], 'platform': 'linux/amd64',
                     'network_mode': 'service:baseline', 'working_dir': '/app',
                     'entrypoint': ['/bin/sh', '/core-tools/hold.sh'], 'cpus': 2.0, 'mem_limit': '512m',
                     'volumes': [str(source) + ':/app:ro', str(tools) + ':/core-tools:ro', str(self.output) + ':/evidence'],
                     'tmpfs': ['/tmp:mode=1777,size=134217728'],
                     'environment': {'DJANGO_SETTINGS_MODULE': 'dispatcharr.settings', 'DISPATCHARR_ENV': 'aio',
                         'DJANGO_SECRET_KEY': 'playback-fixture-only', 'POSTGRES_DB': database,
                         'POSTGRES_HOST': '127.0.0.1', 'POSTGRES_PORT': '5432', 'POSTGRES_USER': 'dispatch',
                         'POSTGRES_PASSWORD': 'playback-fixture-only', 'REDIS_HOST': '127.0.0.1',
                         'REDIS_PORT': '6379', 'REDIS_PASSWORD': 'playback-fixture-only',
                         'REDIS_URL': 'redis://:playback-fixture-only@127.0.0.1:6379/0',
                         'M1_CORE_DISPOSABLE_FIXTURE': 'owned-core-only', 'PYTHONDONTWRITEBYTECODE': '1',
                         'M1_CORE_PLAN': '/evidence/phase-plan.json'}}}}
        overlay.write_text(json.dumps(config, indent=2) + '\n')
        self.compose = compose + ['-f', str(overlay)]

    def command(self, args, timeout=120):
        return subprocess.check_output(self.compose + args, env=self.env, text=True, stderr=subprocess.PIPE, timeout=timeout)

    def stop_web_and_origin(self):
        value = json.loads(self.command(['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop']).splitlines()[-1])
        if value.get('running') is not False:
            raise ValueError('Original web did not stop')
        self.command(['stop', '--timeout', '10', 'origin'])
        return value

    def inspected(self, service):
        identifier = self.command(['ps', '-q', service]).strip()
        if len(identifier) != 64:
            raise ValueError('Active owned service missing')
        value = json.loads(subprocess.check_output(['docker', 'inspect', identifier], env=self.env, text=True, timeout=10))[0]
        host = value['HostConfig']
        if not value['State']['Running'] or host['NanoCpus'] != 2_000_000_000 or host['Memory'] != 512 * 1024 * 1024:
            raise ValueError('Finite core/infrastructure limits differ')
        return {'id': identifier, 'image_id': value['Image'], 'limits': {'nano_cpus': host['NanoCpus'], 'memory_bytes': host['Memory']}}

    def start(self, backend, viewers, period, phase_plan, label, held=False):
        if backend not in ['source', 'native'] or viewers not in [1, 10, 100] or period not in [250, 500, 1000]:
            raise ValueError('Declared core actor/workload required')
        output = self.output / (label + '.json')
        release = Path(str(output) + '.drain-release')
        if output.exists() or Path(str(output) + '.exit.json').exists():
            raise ValueError('Fresh phase output required')
        if release.exists():
            raise ValueError('Fresh drain barrier required')
        self.pending_drains.add(release)
        (self.output / 'phase-plan.json').write_text(json.dumps(phase_plan, indent=2) + '\n')
        start_ms = int((time.time() + 8) * 1000)
        phase_overlay = self.output / 'core-phase.yml'
        phase_overlay.write_text(json.dumps({'services': {'core': {'environment': {
            'M1_CORE_BACKEND': backend, 'M1_CORE_VIEWERS': str(viewers), 'M1_CORE_PERIOD_MS': str(period),
            'M1_CORE_START_MS': str(start_ms), 'M1_CORE_OUTPUT': '/evidence/' + output.name,
            'M1_CORE_DRAIN_RELEASE': '/evidence/' + release.name,
            'M1_CORE_TEST': HELD if held else TEST}}}}, indent=2) + '\n')
        command = self.compose + ['-f', str(phase_overlay)]
        subprocess.check_output(command + ['up', '-d', '--no-deps', 'core'], env=self.env, stderr=subprocess.PIPE, timeout=30)
        selected = {'infrastructure': self.inspected('baseline'), 'core': self.inspected('core')}
        if selected['core']['image_id'] != selected['infrastructure']['image_id']:
            raise ValueError('Both runtime containers must use the exact same pinned Original image')
        top = subprocess.check_output(['docker', 'top', selected['core']['id'], '-eo', 'pid,comm,args'], env=self.env, text=True, timeout=10)
        (self.output / (label + '-processes.log')).write_text(top)
        if backend == 'native' and any('python' in line.lower() for line in top.splitlines()[1:]):
            raise ValueError('Inactive Python process in native core container')
        state = {'backend': backend, 'viewers': viewers, 'period_ms': period, 'start_ms': start_ms,
                 'output': str(output), 'selected': selected, 'held': held,
                 'drain_release': str(release), 'drain_epoch': start_ms/1000+phase_plan['warmup_seconds']+phase_plan['measure_seconds']}
        self.active = state
        if not held:
            deadline = time.monotonic() + 8
            ready = Path(str(output) + '.ready')
            while not ready.exists():
                if Path(str(output) + '.exit.json').exists():
                    raise ValueError('Core worker exited before readiness')
                if time.monotonic() >= deadline:
                    raise TimeoutError('Core actor readiness')
                time.sleep(.05)
            if time.time() * 1000 >= start_ms:
                raise ValueError('Core actor was not ready before its immutable start')
            state['ready'] = json.loads(ready.read_text())
        return state

    def finish(self, state, timeout=90, before_remove=None):
        output = Path(state['output'])
        exit_path = Path(str(output) + '.exit.json')
        deadline = time.monotonic() + timeout
        while not exit_path.exists():
            # Functional-only mode has no counter window; its nominal end is
            # the release point. Experiment calls finish only AFTER window()
            # has recorded final counters and released this same barrier.
            if state['held'] or time.time() >= state['drain_epoch']:
                self.release(state)
            if time.monotonic() >= deadline:
                raise TimeoutError('Owned core worker completion/drain')
            time.sleep(.05)
        exited = json.loads(exit_path.read_text())
        identifier = state['selected']['core']['id']
        inspect = json.loads(subprocess.check_output(['docker', 'inspect', identifier], env=self.env, text=True, timeout=10))[0]
        logs = subprocess.check_output(['docker', 'logs', identifier], env=self.env, stderr=subprocess.STDOUT, timeout=10)
        (self.output / (output.stem + '-worker.log')).write_bytes(logs)
        if before_remove is not None:
            before_remove()
        if not inspect['State']['Running'] or exited != {'worker_exit_code': 0, 'wrapper_retained': True}:
            raise ValueError('Worker failure or cgroup wrapper exited')
        if not output.is_file():
            raise ValueError('Successful worker omitted its report')
        report = json.loads(output.read_text())
        self.command(['stop', '--timeout', '10', 'core'])
        self.command(['rm', '-f', 'core'])
        self.active = None
        self.pending_drains.discard(Path(state['drain_release']))
        return report

    def release(self, state):
        Path(state['drain_release']).touch(exist_ok=True)

    def release_all(self):
        for path in self.pending_drains:
            path.touch(exist_ok=True)
