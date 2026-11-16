"""Bounded Native one-viewer attribution inside the existing live Fixture.
No bootstrap, sampler benchmark, app mutation or Source replay on import/run.
"""
import hashlib
import json
import os
import time
from pathlib import Path


def proc_stat(pid, tid=None):
    path = Path('/proc', str(pid))
    if tid is not None:
        path = path / 'task' / str(tid)
    fields = (path / 'stat').read_text().rsplit(')', 1)[1].split()
    value = {'pid': pid, 'tid': tid, 'starttime_ticks': fields[19], 'utime_ticks': int(fields[11]), 'stime_ticks': int(fields[12]), 'parent_pid': int(fields[1]), 'threads': int(fields[17]), 'comm': (path / 'comm').read_text().strip()[:64]}
    try:
        value['sched_runtime_nanoseconds'] = int((path / 'schedstat').read_text().split()[0])
    except (FileNotFoundError, PermissionError):
        value['sched_runtime_nanoseconds'] = None
    value['sched_runtime_scope'] = 'individual thread' if tid is not None else 'leader thread only, NOT whole process'
    return value


def cgroup_path(pid):
    group = next(line[3:] for line in Path('/proc', str(pid), 'cgroup').read_text().splitlines() if line.startswith('0::'))
    root = Path('/sys/fs/cgroup').resolve()
    path = (root / group.lstrip('/')).resolve()
    if root not in path.parents:
        raise RuntimeError('OwnedServiceCgroupPathRequired')
    return path


def parse_info(text):
    if not text.startswith('# '):
        raise RuntimeError('ActualRedisINFORequired')
    return dict(line.split(':', 1) for line in text.splitlines() if line and not line.startswith('#') and ':' in line)


def run(f, readers):
    if f.backend != 'Native' or not f.native or len(readers) != 1:
        raise ValueError('ExistingNativeOneViewerOnlyAttribution')
    out = f.output / 'native-live-attribution'
    out.mkdir(mode=0o755)
    out.chmod(0o755)
    report = {'status': 'running', 'performance_acceptance': False, 'gain_accepted': False, 'full_M3_passed': False,
              'scope': 'Native actual one-viewer functional attribution, fixed6 snapshots/2s targets; not a paired benchmark',
              'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'), 'samples': [],
              'observer_work': 'host proc reads plus existing Fixture Redis INFO CLI/psycopg management observations; baseline probe CPU is charged and not subtracted'}
    def save():
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    services = {}
    ids = list(dict.fromkeys([f.native] + f.command(f.compose + ['ps', '-q']).split()))
    seen_pids = set()
    for index, cid in enumerate(ids):
        form = '{"State":{{json .State}},"Image":{{json .Image}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"NanoCpus":{{.HostConfig.NanoCpus}},"Memory":{{.HostConfig.Memory}},"PidsLimit":{{json .HostConfig.PidsLimit}}}'
        info = json.loads(f.command([f.docker, 'inspect', '--format', form, cid], secret=True))
        if not info['State'].get('Running'):
            raise RuntimeError('ActualOwnedServiceNotRunning')
        pid = int(info['State']['Pid'])
        if pid in seen_pids:
            continue  # Same exact container can be named and appear in compose ps.
        seen_pids.add(pid)
        name = 'candidate' if cid == f.native else info['Service'] or 'compose-' + str(index)
        if name in services:
            raise RuntimeError('OwnedAttributionServiceIdentityCollision')
        services[name] = {'CID': cid, 'image_id': info['Image'], 'actual_limits': {key: info[key] for key in ('NanoCpus', 'Memory', 'PidsLimit')}, 'init_pid': pid, 'init_starttime': proc_stat(pid)['starttime_ticks'], 'cgroup': str(cgroup_path(pid))}
    # Actual one-viewer guard; no request increases load while tracing.
    def live():
        r = readers[0].snapshot()
        values = f.redis(0, "local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),redis.call('GET','profile_connections:'..ARGV[2])or'0',redis.call('GET',p..':owner')or''}", f.channel_uuid, str(f.provider))
        origin = json.loads(f.get(f.origin + '/stats')[1])
        queues = f.redis(1, "return {redis.call('LLEN','celery'),redis.call('LLEN','dvr'),redis.call('HLEN','unacked'),redis.call('ZCARD','unacked_index')}")
        return {'reader': r, 'clients': values[0], 'provider_count': int(values[1]), 'owner_present': bool(values[2]), 'origin_active': origin['active'], 'observed_queue_lengths': dict(zip(('celery', 'dvr', 'unacked', 'unacked_index'), queues))}
    def process_snapshot():
        result = {}
        for name, service in services.items():
            path = Path(service['cgroup'])
            cpu = {key: int(value) for key, value in (line.split() for line in (path / 'cpu.stat').read_text().splitlines())}
            pids = [int(value) for value in (path / 'cgroup.procs').read_text().split()]
            if len(pids) > 1600:
                raise RuntimeError('ExistingFunctionalServicePidBound')
            processes = []
            for pid in pids:
                try:
                    item = proc_stat(pid)
                    tids = list(Path('/proc', str(pid), 'task').iterdir())
                    if len(tids) > 1600:
                        raise RuntimeError('ExistingFunctionalServiceThreadBound')
                    threads = []
                    for thread in tids:
                        try:
                            threads.append(proc_stat(pid, int(thread.name)))
                        except FileNotFoundError:
                            pass  # Retired race is explicit in interval derivation.
                    item['tasks'] = threads
                    processes.append(item)
                except FileNotFoundError:
                    pass
            result[name] = {'cpu_stat': cpu, 'processes': processes}
        return result
    def redis_snapshot():
        result = {}
        for section in ('commandstats', 'cpu', 'stats'):
            text = f.command(f.compose + ['exec', '-T', 'baseline', 'redis-cli', '--raw', 'INFO', section], seconds=3, secret=True)
            values = parse_info(text)
            if section == 'commandstats':
                result[section] = {key: {k: float(v) if '.' in v else int(v) for k, v in (pair.split('=', 1) for pair in raw.split(','))} for key, raw in values.items()}
            elif section == 'cpu':
                result[section] = {key: float(value) for key, value in values.items() if key.startswith('used_cpu_')}
            else:
                result[section] = {key: int(values[key]) for key in ('total_commands_processed', 'total_net_input_bytes', 'total_net_output_bytes', 'expired_keys', 'evicted_keys') if key in values}
        return result
    def pg_snapshot():
        # Exact already-owned DB, no Django bootstrap, mutations or raw SQL text
        # capture. This uses the fixture's existing management observation lane.
        script = "import psycopg,json;from psycopg.rows import dict_row;c=psycopg.connect(host='127.0.0.1',dbname=" + repr(f.database) + ",user='dispatch',password='playback-fixture-only',connect_timeout=2,application_name='owned-native-attribution',options='-c statement_timeout=1500',row_factory=dict_row);q=c.cursor();q.execute(\"SELECT xact_commit,xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,tup_inserted,tup_updated,tup_deleted,stats_reset FROM pg_stat_database WHERE datname=current_database()\");database=q.fetchone();q.execute(\"SELECT backend_type,state,wait_event_type,wait_event,count(*)::bigint AS count FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY backend_type,state,wait_event_type,wait_event ORDER BY backend_type,state,wait_event_type,wait_event LIMIT 32\");activity=q.fetchall();c.close();print(json.dumps({'database':database,'activity':activity,'probe_application':'owned-native-attribution','raw_query_text_captured':False},default=str))"
        return json.loads(f.python(script, seconds=3, secret=True))
    def process_deltas(before, after):
        values = []
        for name, group in after.items():
            old = {p['pid']: p for p in before[name]['processes']}
            for p in group['processes']:
                a = old.get(p['pid'])
                if not a or a['starttime_ticks'] != p['starttime_ticks']:
                    values.append({'service': name, 'pid': p['pid'], 'comm': p['comm'], 'identity': 'new_or_changed', 'CPU_delta_not_assumed_zero': True})
                    continue
                ticks = p['utime_ticks'] + p['stime_ticks'] - a['utime_ticks'] - a['stime_ticks']
                tasks = {t['tid']: t for t in a['tasks']}
                thread_deltas = []
                for task in p['tasks']:
                    old_task = tasks.get(task['tid'])
                    if old_task and old_task['starttime_ticks'] == task['starttime_ticks']:
                        runtime = task.get('sched_runtime_nanoseconds')
                        old_runtime = old_task.get('sched_runtime_nanoseconds')
                        thread_deltas.append({'tid': task['tid'], 'comm': task['comm'], 'CPU_ticks': task['utime_ticks'] + task['stime_ticks'] - old_task['utime_ticks'] - old_task['stime_ticks'], 'sched_runtime_nanoseconds_delta': runtime - old_runtime if runtime is not None and old_runtime is not None else None})
                stable_tasks = {(t['tid'], t['starttime_ticks']) for t in a['tasks']} == {(t['tid'], t['starttime_ticks']) for t in p['tasks']}
                nanoseconds = [t['sched_runtime_nanoseconds_delta'] for t in thread_deltas]
                values.append({'service': name, 'pid': p['pid'], 'comm': p['comm'], 'identity': 'same_generation', 'CPU_ticks': ticks, 'CPU_seconds': ticks / report['clock_ticks_per_second'], 'all_thread_generations_stable': stable_tasks, 'process_sched_runtime_nanoseconds_only_if_all_threads_retained': sum(nanoseconds) if stable_tasks and nanoseconds and all(v is not None and v >= 0 for v in nanoseconds) and (sum(nanoseconds) > 0 or ticks == 0) else None, 'threads_context_only_not_added_to_process_CPU': thread_deltas})
            gone = set(old) - {p['pid'] for p in group['processes']}
            if gone:
                values.append({'service': name, 'retired_pids': sorted(gone), 'retired_final_CPU_unobserved': True})
        return values
    report['services'] = services
    save()
    try:
        report['initial_live'] = live()
        initial = report['initial_live']
        if initial['clients'] != 1 or initial['provider_count'] != 1 or not initial['owner_present'] or initial['origin_active'] != 1 or initial['reader']['errors'] or initial['reader']['ended'] or initial['reader'].get('http_status') != 200 or initial['reader']['media_bytes'] <= 0:
            raise AssertionError('ActualHealthyOneViewerBeforeAttribution')
        began = time.monotonic()
        for index in range(6):
            wait = began + index * 2 - time.monotonic()
            if wait > 0:
                f.sleep(wait)
            started = time.monotonic()
            procs = process_snapshot()
            process_end = time.monotonic()
            redis = redis_snapshot()
            pg = pg_snapshot()
            sample = {'index': index, 'target_monotonic': began + index * 2, 'started_monotonic': started, 'process_snapshot_end_monotonic': process_end, 'ended_monotonic': time.monotonic(), 'processes': procs, 'redis': redis, 'PG': pg, 'live': live()}
            report['samples'].append(sample)
            save()
        first, last = report['samples'][0], report['samples'][-1]
        report['process_CPU'] = process_deltas(first['processes'], last['processes'])
        report['actual_process_snapshot_elapsed_seconds'] = last['process_snapshot_end_monotonic'] - first['process_snapshot_end_monotonic']
        commands = {}
        for name, value in last['redis']['commandstats'].items():
            old = first['redis']['commandstats'].get(name, {})
            commands[name] = {key: value[key] - old.get(key, 0) for key in ('calls', 'usec', 'rejected_calls', 'failed_calls') if key in value}
        report['Redis_command_deltas'] = commands
        report['Redis_command_usec_is_reported_execution_time_not_cgroup_CPU'] = True
        report['actual_empty_lane_peek_candidate'] = {'LINDEX_calls': commands.get('cmdstat_lindex', {}).get('calls'), 'EVALSHA_calls': commands.get('cmdstat_evalsha', {}).get('calls'), 'queue_family_attribution_not_proven_by_commandstats_alone': True, 'poll_delay_changed': False}
        report['PG_database_counter_deltas'] = {key: last['PG']['database'][key] - first['PG']['database'][key] for key in ('xact_commit', 'xact_rollback', 'blks_read', 'blks_hit', 'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted')}
        report['PG_stats_reset_unchanged'] = first['PG']['database']['stats_reset'] == last['PG']['database']['stats_reset']
        report['all_observed_viewers_alive_one_provider_upstream'] = all(sample['live']['clients'] == 1 and sample['live']['provider_count'] == 1 and sample['live']['origin_active'] == 1 and sample['live']['owner_present'] and not sample['live']['reader']['errors'] and not sample['live']['reader']['ended'] for sample in report['samples'])
        report['actual_reader_nonnull_byte_advance'] = last['live']['reader']['media_bytes'] > first['live']['reader']['media_bytes']
        report['actual_service_init_generations_unchanged'] = all(proc_stat(value['init_pid'])['starttime_ticks'] == value['init_starttime'] for value in services.values())
        report['status'] = 'captured' if report['all_observed_viewers_alive_one_provider_upstream'] and report['actual_reader_nonnull_byte_advance'] and report['actual_service_init_generations_unchanged'] and report['PG_stats_reset_unchanged'] else 'failed'
    except BaseException as error:
        report['status'] = 'failed'
        report['failure'] = {'type': type(error).__name__, 'label': str(error)[:160]}
    finally:
        report['caller_owns_reader_Main_fixture_cleanup'] = True
        save()
    return report
