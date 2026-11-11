#!/usr/bin/env python3
"""Scoped draft: sealed Original-only idle/churn qualification.

Original application, algorithms and fixture authors retain their full credit.
No network, subprocess, thread, fixture or sampler starts on import.
"""
import argparse
import hashlib
import http.client
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from urllib.parse import urlsplit
import uuid


HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def request_json(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError('fixture JSON exceeds64KiB')
        return json.loads(raw)


class Readers:
    def __init__(self, base, path, token, count, output, budgets):
        self.base, self.path, self.token = base, path, token
        self.count, self.output, self.budgets = count, output, budgets
        self.stop = threading.Event()
        self.barrier = threading.Barrier(count + 1)
        self.rows = [dict(status=None, requests=0, bytes=0, errors=[], first_byte_ms=None)
                     for _ in range(count)]
        self.ready = [threading.Event() for _ in range(count)]
        self.connections = [None] * count
        self.sockets = [None] * count
        self.threads = []

    def reader(self, index):
        row = self.rows[index]
        address = urlsplit(self.base)
        connection = http.client.HTTPConnection(address.hostname, address.port,
                                                timeout=self.budgets['first_byte_seconds'])
        self.connections[index] = connection
        prefix = bytearray()
        try:
            self.barrier.wait(timeout=5)
            began = time.monotonic()
            connection.connect()
            # Preserve the actual socket even if HTTP/1.0 response handling
            # detaches it from HTTPConnection while its body is still open.
            self.sockets[index] = connection.sock
            connection.request('GET', self.path, headers={'Authorization': 'Bearer ' + self.token})
            row['requests'] = 1
            response = connection.getresponse()
            row['status'] = response.status
            # Never follow redirects or retry a failed cold connection.
            if response.status != 200:
                row['errors'].append('HTTPStatus')
                return
            while not self.stop.is_set():
                block = response.read1(65536)
                if not block:
                    if not self.stop.is_set():
                        row['errors'].append('UnexpectedEOF')
                    break
                if row['first_byte_ms'] is None:
                    row['first_byte_ms'] = (time.monotonic() - began) * 1000
                    self.ready[index].set()
                row['bytes'] += len(block)
                remaining = self.budgets['prefix_bytes_per_connection'] - len(prefix)
                prefix.extend(block[:max(0, remaining)])
        except Exception as error:
            if not self.stop.is_set():
                row['errors'].append(type(error).__name__)
        finally:
            self.ready[index].set()
            connection.close()
            payload = bytes(prefix)
            (self.output / (str(index) + '.ts')).write_bytes(payload)
            row.update(prefix_bytes=len(payload), prefix_sha256=hashlib.sha256(payload).hexdigest())

    def run(self, hold_seconds):
        self.output.mkdir()
        try:
            for index in range(self.count):
                thread = threading.Thread(target=self.reader, args=(index,), daemon=True)
                self.threads.append(thread)
                thread.start()
            self.barrier.wait(timeout=5)
            deadline = time.monotonic() + self.budgets['first_byte_seconds'] + 1
            for event in self.ready:
                event.wait(max(0, deadline - time.monotonic()))
            # A fixed common hold is measured even if one connection fails;
            # successful siblings are not discarded to hide the cold defect.
            time.sleep(hold_seconds)
        finally:
            self.stop.set()
            for physical in self.sockets:
                if physical is not None:
                    try:
                        physical.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            for connection in self.connections:
                if connection is not None and connection.sock is not None:
                    try:
                        connection.sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    connection.close()
            deadline = time.monotonic() + self.budgets['connection_join_seconds']
            for thread in self.threads:
                thread.join(max(0, deadline - time.monotonic()))
            if any(thread.is_alive() for thread in self.threads):
                raise TimeoutError('ReaderJoin')
        return self.rows


class Sampler:
    def __init__(self, observer, selected, command, policy, output):
        self.observer, self.selected, self.command = observer, selected, command
        self.policy, self.output = policy, output
        self.rows, self.errors = [], []
        self.stop = threading.Event()
        self._label = 'startup'
        self.phase_epoch = 0
        self.thread = threading.Thread(target=self.loop, daemon=True)

    @property
    def label(self):
        return self._label

    @label.setter
    def label(self, value):
        if value != self._label:
            self.phase_epoch += 1
            self._label = value

    def loop(self):
        previous_epoch = None
        previous_phase = None
        previous_stable = False
        while not self.stop.is_set():
            try:
                if len(self.rows) >= self.policy['budgets']['maximum_sample_records_per_repetition']:
                    raise ValueError('SampleBound')
                began = time.monotonic()
                epoch_at_start = self.phase_epoch
                phase_at_start = self.label
                row = self.observer.sample()
                row.update(phase=phase_at_start, monotonic=began, census={})
                for service, selected in self.selected.items():
                    deadline = time.monotonic() + 5
                    def census(arguments):
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError('CensusDeadline')
                        return json.loads(self.command(['docker', 'exec', *arguments], timeout=remaining))
                    inventory = census([selected['id'], '/dispatcharrpy/bin/python', '/m0-census.py', '--inventory'])
                    if len(inventory['identities']) > 16:
                        raise ValueError('CensusIdentityBound')
                    parts = []
                    row['census'][service] = dict(complete=False, inventory_start=inventory, per_uid=parts)
                    for uid, gid in inventory['identities']:
                        part = census(['-u', str(uid)+':'+str(gid), selected['id'], '/dispatcharrpy/bin/python', '/m0-census.py'])
                        if part['collector_identity'] != [uid, gid]:
                            raise ValueError('CensusIdentityMismatch')
                        parts.append(part)
                    ending = census([selected['id'], '/dispatcharrpy/bin/python', '/m0-census.py', '--inventory'])
                    row['census'][service] = merge_census(inventory, parts, ending)
                phase_at_end = self.label
                row.update(completed_monotonic=time.monotonic(), phase_stable=(epoch_at_start == self.phase_epoch and previous_epoch == epoch_at_start and phase_at_start == phase_at_end and previous_phase == phase_at_start and previous_stable), phase_epoch=epoch_at_start)
                previous_phase, previous_stable = phase_at_start, epoch_at_start == self.phase_epoch and phase_at_start == phase_at_end
                previous_epoch = epoch_at_start
                self.rows.append(row)
                with self.output.open('a') as stream:
                    stream.write(json.dumps(row, sort_keys=True) + '\n')
            except Exception as error:
                self.errors.append(type(error).__name__)
                if 'row' in locals():
                    save(self.output.with_suffix('.failed-sample.json'), dict(error_type=type(error).__name__, partial_sample=row))
                return
            self.stop.wait(self.policy['budgets']['sample_interval_seconds'])

    def close(self):
        self.stop.set()
        self.thread.join(25)
        if self.thread.is_alive():
            raise TimeoutError('SamplerJoin')


def merge_census(inventory, parts, ending=None):
    ending = inventory if ending is None else ending
    observed = [pid for part in parts for pid in part['process_ids']]
    if len(observed) != len(set(observed)):
        raise ValueError('DuplicateCensusProcess')
    value = {key: sum(p[key] for p in parts) for key in ['processes', 'threads', 'fds', 'socket_descriptors', 'raced_processes', 'raced_descriptors']}
    value['raced_processes'] += inventory['raced_processes'] + ending['raced_processes'] + len(set(observed) ^ set(inventory['process_ids'])) + len(set(observed) ^ set(ending['process_ids']))
    value['unique_socket_inodes'] = len({inode for p in parts for inode in p['socket_inodes']})
    value['tcp_states'] = inventory['tcp_states']
    value['identity_groups'] = len(parts)
    value['inventory_start'] = inventory
    value['inventory_end'] = ending
    value['per_uid'] = parts
    value['complete'] = value['raced_processes'] == 0 and value['raced_descriptors'] == 0
    return value


def complete_census(row):
    return bool(row['census']) and all(v['processes'] > 0 and v.get('complete', True) is True and v.get('raced_processes', 0) == 0 and v.get('raced_descriptors', 0) == 0 for v in row['census'].values())


def evaluate_samples(rows, policy):
    budget = policy['budgets']
    checks = []
    stable_rows = [row for row in rows if row.get('phase_stable') is True and complete_census(row)]
    for label in ('idle-before-first-playback', 'idle-after-playback', 'idle-after-churn'):
        selected = [row for row in stable_rows if row['phase'] == label]
        checks.append(dict(id=label + '/samples', passed=len(selected) >= budget['sample_minimum_per_idle'],
                           observed=len(selected)))
        if not selected:
            continue
        cpu = statistics.mean(row['cpu_percent'] for row in selected)
        checks.append(dict(id=label + '/cpu', passed=cpu <= budget['idle_cpu_percent_one_core'], observed=cpu))
    before = [row for row in stable_rows if row['phase'] == 'idle-before-first-playback']
    for label in ('idle-after-playback', 'idle-after-churn'):
        after = [row for row in stable_rows if row['phase'] == label]
        if not before or not after:
            continue
        base_memory = statistics.median(row['memory_bytes'] for row in before)
        growth = statistics.median(row['memory_bytes'] for row in after) - base_memory
        allowed = max(budget['post_idle_memory_growth_bytes'], base_memory * budget['post_idle_memory_growth_fraction'])
        checks.append(dict(id=label + '/memory-growth', observed=growth, allowed=allowed, passed=growth <= allowed))
        for field, name in [('processes', 'process'), ('threads', 'thread'), ('fds', 'fd'),
                            ('socket_descriptors', 'socket')]:
            def total(row):
                return sum(value[field] for value in row['census'].values())
            growth = max(map(total, after)) - max(map(total, before))
            allowed = budget['post_idle_' + name + '_growth']
            checks.append(dict(id=label + '/' + name + '-growth', observed=growth,
                               allowed=allowed, passed=growth <= allowed))
    for row in rows:
        checks.append(dict(id='sample/census-coverage-labeled', passed=all(v.get('complete', not (v.get('raced_processes', 0) or v.get('raced_descriptors', 0))) == (not (v.get('raced_processes', 0) or v.get('raced_descriptors', 0))) for v in row['census'].values())))
        checks.append(dict(id='sample/whole-stack-cpu', passed=row['cpu_percent'] <= budget['whole_stack_cpu_percent_one_core']))
        checks.append(dict(id='sample/time-wait', passed=sum(value['tcp_states'].get('06', 0)
                      for value in row['census'].values()) <= budget['tcp_time_wait_ceiling']))
        for service in row['containers']:
            checks.append(dict(id='sample/memory-hard-limit', passed=service['memory_usage_bytes'] <= service['memory_limit_bytes']))
    return checks


def repetition(root, tools, controller, out, policy, ordinal, integration, ports, observer_module, smoke=False):
    report = dict(passed=False, repetition=ordinal, phases=[], checks=[], performance_verified=False, observer_contract_smoke=smoke, baseline_accepted=False, m0_closed=False)
    project = 'dispatcharr-m0-idle-' + uuid.uuid4().hex[:10]
    report['project'] = project
    out.mkdir()
    with integration.fixture_source(report, False, out) as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        archive = subprocess.check_output(['git', '-C', str(root), 'archive', policy['original_revision']])
        if hashlib.sha256(archive).hexdigest() != policy['original_archive_sha256']:
            raise ValueError('OriginalArchiveBinding')
        subprocess.run(['tar', '-x', '--no-same-owner', '-C', str(source)], input=archive, check=True)
        (source / 'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(out),
                           FIXTURE_SECONDS=str(policy['fixture']['seconds']), FIXTURE_CODEC=policy['fixture']['codec'],
                           BENCHMARK_SHUTDOWN_DELAY=str(policy['fixture']['shutdown_delay_seconds']), BENCHMARK_RETENTION_SECONDS=str(policy['fixture']['retention_seconds']), BENCHMARK_STARTUP_BEHIND=str(policy['fixture']['startup_behind_seconds']))
        source_config = dict(image=policy['original_image'], working_dir='/app',
            entrypoint=['/dispatcharrpy/bin/python', '/m4-perf/source_web.py'],
            environment=dict(DJANGO_SECRET_KEY='playback-fixture-only', POSTGRES_DB='dispatcharr', POSTGRES_USER='dispatch',
                POSTGRES_PASSWORD='playback-fixture-only', POSTGRES_HOST='baseline', POSTGRES_PORT='5432',
                REDIS_HOST='baseline', REDIS_PORT='6379', REDIS_PASSWORD='playback-fixture-only',
                DJANGO_SETTINGS_MODULE='dispatcharr.settings', DISPATCHARR_ENV='aio', DISPATCHARR_LOG_LEVEL='WARNING',
                PYTHONDONTWRITEBYTECODE='1', DISPATCHARR_TRUSTED_PROXIES='none', LANG='C.UTF-8', LC_ALL='C.UTF-8'),
            volumes=[str(source)+':/app:ro', str(tools/'scripts/rust-playback-baseline')+':/verification:ro',
                     str(tools/'scripts/rust-m4-perf')+':/m4-perf:ro'],
            tmpfs=['/data:mode=1777,size=67108864', '/app/media:mode=1777,size=67108864', '/tmp:mode=1777,size=134217728'],
            ports=['127.0.0.1::5656', '127.0.0.1::9191'], stop_grace_period='15s')
        services = dict(baseline={}, origin={}, source=source_config)
        for name, limit in policy['services'].items():
            services[name].update(cpus=limit['nano_cpus']/1e9, mem_limit=limit['memory_bytes'])
            services[name].setdefault('volumes', []).append(str(controller/'proc_snapshot.py')+':/m0-census.py:ro')
        overlay = out/'m0.json'
        save(overlay, dict(services=services))
        portfile = out/'ports.yml'
        ports.write_port_overlay(portfile, dict(baseline=[5656], origin=[8080], source=[5656, 9191]))
        compose = ['docker', 'compose', '-p', project, '-f', str(tools/'docker/compose.rust-playback.yml'),
                   '-f', str(overlay), '-f', str(portfile)]

        def command(arguments, body=None, timeout=180):
            result = subprocess.run(arguments, input=body, env=environment, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, timeout=timeout)
            if len(result.stdout) > 8*1024**2 or len(result.stderr) > 8*1024**2:
                raise ValueError('CommandOutputBound')
            if result.returncode:
                # No argv, JWT or arbitrary exception text in public evidence.
                (out/'failure-private.log').write_bytes(result.stderr)
                raise RuntimeError('FixtureCommand')
            return result.stdout

        def execute(*arguments, body=None, timeout=180):
            return command(compose+list(arguments), body, timeout)

        def endpoint(service, port):
            return 'http://' + execute('port', service, str(port)).decode().strip()

        def ready(url):
            deadline = time.monotonic()+policy['budgets']['readiness_seconds']
            while time.monotonic() < deadline:
                try:
                    request_json(url)
                    return
                except Exception:
                    time.sleep(.25)
            raise TimeoutError('Readiness')

        sampler = None
        began = time.monotonic()
        try:
            execute('up', '-d', 'baseline')
            origin = endpoint('origin', 8080)
            ready(origin+'/stats')
            ready(endpoint('baseline', 5656)+'/api/core/version/')
            seed = json.loads((out/'seed.json').read_text())
            raw = execute('exec', '-T', '-e', 'LIVE_DISPOSABLE_FIXTURE=playback-fixture-only', 'baseline',
                          '/dispatcharrpy/bin/python', '-', body=(tools/'scripts/rust-live-perf/seed.py').read_bytes())
            identity = json.loads(raw.splitlines()[-1])
            # Read selected profile identity without changing the seeded unlimited cap.
            code = ("import os;os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');"
                    "import django;django.setup();from apps.m3u.models import M3UAccount;"
                    "print(M3UAccount.get_custom_account().profiles.get(is_default=True).pk)")
            profile = int(execute('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c', code).splitlines()[-1])
            execute('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop')
            execute('up', '-d', '--no-deps', 'source')
            base = endpoint('source', 5656)
            ready(base+'/api/core/version/')
            selected = {}
            for name, limits in policy['services'].items():
                cid = execute('ps', '-q', name).decode().strip()
                inspected = json.loads(command(['docker', 'inspect', cid]))[0]
                image_info = json.loads(command(['docker', 'image', 'inspect', inspected['Image']]))[0]
                if (inspected['Config']['Image'] != policy['original_image'] or
                        policy['original_image'] not in image_info.get('RepoDigests', [])):
                    raise ValueError('PinnedImageIdentity')
                if (inspected['HostConfig']['Memory'] != limits['memory_bytes'] or
                    inspected['HostConfig']['NanoCpus'] != limits['nano_cpus']):
                    raise ValueError('ServiceLimitBinding')
                selected[name] = dict(id=cid, limits=limits)
            report['services'] = selected
            observer = observer_module.Observer(selected, tools)
            if smoke:
                report['observer_contract'] = observer.smoke()
            before = observer.counters()
            sampler = Sampler(observer, selected, command, policy, out/'samples.jsonl')
            sampler.thread.start()

            def slots():
                script = ("local p='live:channel:'..ARGV[1];local n=redis.call('SCARD',p..':clients');"
                          "if n>10 then return redis.error_reply('fixture client bound') end;"
                          "return {n,tonumber(redis.call('GET','profile_connections:'..ARGV[2]) or '0')}")
                value = json.loads(execute('exec', '-T', 'baseline', 'redis-cli', '--json', 'EVAL', script,
                                          '0', seed['channel_uuid'], str(profile)))
                return dict(clients=value[0], provider_slots=value[1], origin_active=request_json(origin+'/stats')['active'])

            def cleanup():
                deadline = time.monotonic()+policy['budgets']['cleanup_seconds']
                while True:
                    value = slots()
                    if value == dict(clients=0, provider_slots=0, origin_active=0):
                        return dict(passed=True, state=value, elapsed_seconds=policy['budgets']['cleanup_seconds']-(deadline-time.monotonic()))
                    if time.monotonic() >= deadline:
                        return dict(passed=False, state=value, elapsed_seconds=policy['budgets']['cleanup_seconds'])
                    time.sleep(.25)

            schedule = policy['schedule'] if not smoke else [dict(id='contract-idle', kind='idle', seconds=8), dict(id='contract-play', kind='play', viewers=1, hold_seconds=10)]
            for phase in schedule:
                if time.monotonic()-began > policy['budgets']['repetition_seconds']:
                    raise TimeoutError('RepetitionBudget')
                sampler.label = phase['id'] + ('/cleanup' if phase['kind'] == 'settled-idle' else '')
                result = dict(id=phase['id'])
                if phase['kind'] in ('idle', 'settled-idle'):
                    if phase['kind'] == 'settled-idle':
                        result['cleanup'] = cleanup()
                        sampler.label = phase['id']+'/retention-settle'
                        time.sleep(policy['budgets']['retention_settle_seconds'])
                    sampler.label = phase['id']
                    time.sleep(phase['seconds'])
                    result['state'] = slots()
                    result['passed'] = all(value == 0 for value in result['state'].values()) and result.get('cleanup', {'passed': True})['passed']
                else:
                    result['rounds'] = []
                    for index in range(phase.get('rounds', 1)):
                        phaseout = out/(phase['id']+'-'+str(index))
                        rows = Readers(base, seed['path'], identity['token'], phase['viewers'], phaseout, policy['budgets']).run(phase['hold_seconds'])
                        for number, row in enumerate(rows):
                            # Bounded saved media only; decode never reconnects to playback.
                            try:
                                decoded = execute('exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error',
                                    '-xerror', '-progress', 'pipe:1', '-nostats', '-i', '/evidence/'+phaseout.name+'/'+str(number)+'.ts',
                                    '-map', '0:v:0', '-map', '0:a:0', '-t', str(policy['budgets']['decoder_seconds']), '-f', 'null', '-', timeout=20)
                                row['decoder_frames'] = max([int(line.split(b'=')[1]) for line in decoded.splitlines() if line.startswith(b'frame=')], default=0)
                            except Exception as error:
                                row['errors'].append('Decode'+type(error).__name__)
                            row['passed'] = (row['status'] == 200 and row['requests'] == 1 and not row['errors'] and
                                row['first_byte_ms'] is not None and row['first_byte_ms'] <= policy['budgets']['startup_p95_milliseconds'] and
                                row['bytes'] >= phase['hold_seconds']*policy['budgets']['minimum_bytes_per_second_after_first_byte'] and
                                row['prefix_bytes'] == policy['budgets']['prefix_bytes_per_connection'] and
                                row.get('decoder_frames', 0) >= policy['budgets']['decoder_min_video_frames'])
                        result['rounds'].append(dict(index=index, viewers=rows, state_after_close=slots(), passed=all(row['passed'] for row in rows)))
                        # No cleanup wait, primer or restart between short-gap churn rounds.
                        if index+1 < phase.get('rounds', 1):
                            time.sleep(phase['gap_seconds'])
                    result['passed'] = all(row['passed'] for row in result['rounds'])
                report['phases'].append(result)
                save(out/'progress.json', report)
            sampler.close()
            report['observer_errors'] = sampler.errors
            report['census_coverage'] = dict(total=len(sampler.rows), complete=sum(complete_census(r) for r in sampler.rows), incomplete=sum(not complete_census(r) for r in sampler.rows), stable_complete=sum(r.get('phase_stable') is True and complete_census(r) for r in sampler.rows))
            report['checks'] = evaluate_samples(sampler.rows, policy) if not smoke else [dict(id='contract/three-service-census-samples', passed=sum(set(r['census']) == set(policy['services']) and complete_census(r) for r in sampler.rows) >= 2, observed_complete=sum(complete_census(r) for r in sampler.rows), observed_total=len(sampler.rows))]
            if smoke:
                report['post_reader_cleanup'] = cleanup()
                report['checks'].append(dict(id='contract/post-reader-cleanup', passed=report['post_reader_cleanup']['passed']))
            report['cgroup'] = observer.finish(before)
            if not smoke:
                report['checks'].append(dict(id='whole-window-cpu', passed=report['cgroup']['cpu_percent_mean'] <= policy['budgets']['whole_stack_cpu_percent_one_core']))
            first = [v['first_byte_ms'] for p in report['phases'] for r in p.get('rounds', []) for v in r['viewers'] if v['first_byte_ms'] is not None]
            report['startup_p95_ms'] = sorted(first)[math.ceil(.95*len(first))-1] if first else None
            report['passed'] = (not sampler.errors and all(p['passed'] for p in report['phases']) and
                                all(c['passed'] for c in report['checks']))
        except Exception as error:
            report['error_type'] = type(error).__name__
        finally:
            if sampler is not None:
                try:
                    sampler.close()
                except Exception as error:
                    report.update(passed=False, sampler_cleanup_error=type(error).__name__)
            try:
                execute('down', '--volumes', '--remove-orphans', timeout=120)
                remaining = {kind: command(['docker'] + arguments + ['--filter', 'label=com.docker.compose.project='+project]).decode().strip() for kind, arguments in {'containers':['ps', '-aq'], 'networks':['network', 'ls', '-q'], 'volumes':['volume', 'ls', '-q']}.items()}
                report['independent_owned_cleanup'] = remaining
                if any(remaining.values()):
                    raise ValueError('OwnedContainersRemain')
                report['cleanup'] = 'owned project removed'
            except Exception as error:
                report.update(passed=False, cleanup_error=type(error).__name__)
            report['elapsed_seconds'] = time.monotonic()-began
            save(out/'result.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--execute-original-only', action='store_true')
    mode.add_argument('--observer-contract-smoke', action='store_true')
    args = parser.parse_args()
    root, out = args.repo.resolve(), args.output.resolve()
    if out == HERE or HERE in out.parents:
        raise ValueError('Output must not be inside the controller directory')
    policy_raw = (HERE/'policy.json').read_bytes()
    policy = json.loads(policy_raw)
    out.mkdir(parents=True, exist_ok=False)
    # Seal policy and complete own controller before any Docker/readiness/result.
    frozen = out/'controller'
    shutil.copytree(HERE, frozen, ignore=shutil.ignore_patterns('__pycache__'))
    # Only this nonsecret collector source is readable by the observed application UIDs.
    (frozen/'proc_snapshot.py').chmod(0o444)
    save(out/'pre-execution-seal.json', dict(policy_sha256=hashlib.sha256(policy_raw).hexdigest(),
        controller_sha256={path.name: digest(path) for path in sorted(frozen.iterdir()) if path.is_file()},
        validation='Frozen before observer-contract smoke or separate explicit Root quiet baseline cue', native_comparison=False))
    # Import the sealed copy; never execute the already imported mutable controller.
    return module(frozen/'run.py', '_m0_frozen_controller').execute_frozen(root, out, args.observer_contract_smoke)


def execute_frozen(root, out, smoke):
    frozen = HERE
    policy_raw = (HERE/'policy.json').read_bytes()
    policy = json.loads(policy_raw)
    tools = out/'tools'
    tools.mkdir()
    archive = subprocess.check_output(['git', '-C', str(root), 'archive', policy['tool_commit'], 'docker', 'scripts'])
    subprocess.run(['tar', '-x', '--no-same-owner', '-C', str(tools)], input=archive, check=True)
    sys.path.insert(0, str(tools/'scripts/rust-integration'))
    sys.path.insert(0, str(tools/'scripts/rust-guide-perf'))
    integration = module(tools/'scripts/rust-integration/run.py', '_m0_fixture')
    ports = module(tools/'scripts/rust-integration/fixture_ports.py', '_m0_ports')
    observer = module(tools/'scripts/rust-guide-perf/observer.py', '_m0_observer')
    results = []
    expected_repetitions = 1 if smoke else policy['repetitions']
    for number in range(1, expected_repetitions+1):
        try:
            results.append(repetition(root, tools, frozen, out/('repetition-'+str(number)), policy, number, integration, ports, observer, smoke))
        except Exception as error:
            results.append(dict(repetition=number, passed=False, error_type=type(error).__name__))
        save(out/'summary.json', dict(passed=len(results)==expected_repetitions and all(r['passed'] for r in results),
             repetitions=results, original_only=True, native_comparison=False, m0_closed=False, baseline_accepted=False, observer_contract_smoke=smoke,
             policy_sha256=hashlib.sha256(policy_raw).hexdigest(), tool_archive_sha256=hashlib.sha256(archive).hexdigest()))
    return 0 if len(results)==expected_repetitions and all(row['passed'] for row in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
