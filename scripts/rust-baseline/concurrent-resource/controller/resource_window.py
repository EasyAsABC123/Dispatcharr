"""UNEXECUTED resource draft. No activity on import; no acceptance authority.

Application and upstream observer authors retain credit. Sample/census loops are
separate so denied/slow proc coverage cannot erase valid cgroup observations.
"""
import concurrent.futures
import json
import threading
import time
from pathlib import Path
from coverage import eligible_census_row
from interval_coverage import evaluate as interval_checks


SERVICES = {'baseline', 'source', 'origin', 'worker'}


class Journal:
    def __init__(self, path, row_limit, row_bytes, stream_bytes, budget=None):
        self.path = Path(path)
        self.row_limit, self.row_bytes, self.stream_bytes = row_limit, row_bytes, stream_bytes
        self.rows, self.used = [], 0
        self.budget=budget

    def append(self, row):
        data = (json.dumps(row, sort_keys=True) + '\n').encode()
        if len(self.rows) >= self.row_limit or len(data) > self.row_bytes or self.used + len(data) > self.stream_bytes:
            raise ValueError('ObserverJournalCapacity')
        if self.budget is not None:self.budget.charge(len(data))
        with self.path.open('ab') as stream:
            stream.write(data)
        self.used += len(data)
        self.rows.append(row)


class Window:
    def __init__(self, observer, selected, command, policy, output, corrected, registry=None, budget=None, deadline=None, final_counter_command=None):
        if set(selected) != SERVICES or len({v['id'] for v in selected.values()}) != 4:
            raise ValueError('FourServiceIdentity')
        self.observer, self.selected, self.command = observer, selected, command
        self.policy, self.output, self.corrected = policy, Path(output), corrected
        self.output.mkdir()
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.errors = []
        self.phase, self.epoch = 'startup', 0
        p = policy['resource_draft']
        self.samples = Journal(self.output/'resources.jsonl', p['maximum_resource_rows'],
                               p['maximum_json_row_bytes'], p['maximum_stream_bytes'],budget)
        self.censuses = Journal(self.output/'censuses.jsonl', p['maximum_census_rows'],
                                p['maximum_json_row_bytes'], p['maximum_stream_bytes'],budget)
        self.threads = []
        self.registry=registry;self.budget=budget;self.deadline=deadline;self.final_counter_command=final_counter_command;self.phases=[];self.phase_started=time.monotonic()
        self.before = None
        self.workload_before = self.workload_after = None
        self.started = None

    def state(self):
        with self.lock:
            return self.phase, self.epoch

    def label(self, phase):
        with self.lock:
            if self.phase != phase:
                now=time.monotonic();self.phases.append({'phase':self.phase,'epoch':self.epoch,'begin':self.phase_started,'end':now});self.phase_started=now
                self.phase, self.epoch = phase, self.epoch + 1

    def _error(self, kind, error):
        with self.lock:
            self.errors.append({'loop': kind, 'error_type': type(error).__name__,
                                'monotonic': time.monotonic()})

    def start(self):
        if self.started is not None:
            raise ValueError('SingleWindowOwner')
        self.before = self.observer.counters()
        self.started = time.monotonic()
        for kind, target in [('resource', self._resources), ('census', self._censuses)]:
            thread = threading.Thread(target=target, name='m0-'+kind, daemon=True)
            self.threads.append(thread)
            thread.start()

    def _resources(self):
        previous = None
        try:
            while not self.stop.is_set():
                began, state = time.monotonic(), self.state()
                row = self.observer.sample()
                ending = self.state()
                stable = state == ending
                row.update(monotonic=began, completed_monotonic=time.monotonic(),
                           phase=state[0], phase_epoch=state[1], phase_stable=stable,
                           cpu_interval_stable=stable and previous == state,
                           service_names=sorted(SERVICES))
                self.samples.append(row)
                previous = state if stable else None
                self.stop.wait(self.policy['resource_draft']['sample_interval_seconds'])
        except Exception as error:
            self._error('resource', error)

    def _service_census(self, selected):
        deadline = time.monotonic() + self.policy['resource_draft']['census_service_deadline_seconds']
        value = {'complete': False, 'per_uid': []}

        def read(arguments):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('CensusDeadline')
            raw = self.command(['docker', 'exec', *arguments], timeout=remaining)
            if len(raw) > self.policy['resource_draft']['maximum_json_row_bytes']:
                raise ValueError('CensusReplyCapacity')
            return json.loads(raw)

        try:
            inventory = read([selected['id'], '/dispatcharrpy/bin/python', '/m0-census.py', '--inventory'])
            value['inventory_start'] = inventory
            if len(inventory['identities']) > 16:
                raise ValueError('CensusIdentityBound')
            for uid, gid in inventory['identities']:
                part = read(['-u', str(uid)+':'+str(gid), selected['id'],
                             '/dispatcharrpy/bin/python', '/m0-census.py'])
                value['per_uid'].append(part)
                if part['collector_identity'] != [uid, gid]:
                    raise ValueError('CensusIdentityMismatch')
            ending = read([selected['id'], '/dispatcharrpy/bin/python', '/m0-census.py', '--inventory'])
            value = self.corrected.merge_census(inventory, value['per_uid'], ending)
        except Exception as error:
            # Preserve partial/denied/missing coverage, never replace it by zero.
            value['complete'] = False
            value['error_type'] = type(error).__name__
            self._error('census-service',error)
            diagnostic={'error_type':type(error).__name__,'message':str(error)[:1024],'partial_census':value}
            encoded=json.dumps(diagnostic,sort_keys=True).encode()
            if len(encoded)>self.policy['resource_draft']['maximum_json_row_bytes']:
                diagnostic={'error_type':type(error).__name__,'partial_census_bytes':len(encoded),'partial_omitted_due_to_capacity':True}
            if getattr(self,"budget",None)is not None:self.budget.charge(len(json.dumps(diagnostic,sort_keys=True).encode())+1)
            (self.output/('census-failed-'+str(selected['id'])[:12]+'.json')).write_text(json.dumps(diagnostic,sort_keys=True)+'\n')
            raise
        return value

    def collect_cycle(self,pool):
        began,state=time.monotonic(),self.state()
        pending={name:pool.submit(self._service_census,value)for name,value in self.selected.items()}
        values={}
        for name,future in pending.items():
            try:values[name]=future.result()
            except BaseException as error:
                diagnostic=self.output/('census-failed-'+str(self.selected[name]['id'])[:12]+'.json')
                values[name]={'complete':False,'error_type':type(error).__name__,'message':str(error)[:1024],'partial_retained':diagnostic.exists()}
        ending=self.state();row={'monotonic':began,'completed_monotonic':time.monotonic(),'phase':state[0],'phase_epoch':state[1],'phase_stable':state==ending,'census':values,'complete':all(v.get('complete')is True for v in values.values())}
        self.censuses.append(row)
        return row

    def _censuses(self):
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4)as pool:
                while not self.stop.is_set():
                    self.collect_cycle(pool)
                    if self.errors:return
                    self.stop.wait(self.policy['resource_draft']['census_interval_seconds'])
        except BaseException as error:self._error('census',error)

    def check(self):
        if self.errors:
            raise RuntimeError('ResourceObserverFailure')
        if self.deadline is not None:self.deadline.check()
        if time.monotonic() - self.started > self.policy['resource_draft']['resource_window_deadline_seconds']:
            raise TimeoutError('ResourceWindowDeadline')

    def hold(self, phase, seconds):
        self.label(phase)
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            self.stop.wait(min(.1, max(0, deadline-time.monotonic())))

    def begin_workload(self):
        self.label('concurrent-import-playback')
        self.workload_before = self.observer.counters()

    def end_workload(self):
        if self.workload_before is None or self.workload_after is not None:
            raise ValueError('WorkloadWindowOwnership')
        self.workload_after = self.observer.finish(self.workload_before)
        self.label('decoder-and-reader-close')

    def close(self):
        if hasattr(self,"closed_result"):return self.closed_result
        self.stop.set();p=self.policy['resource_draft']['supervision'];grace=time.monotonic()+p['grace_join_seconds']
        for thread in self.threads:thread.join(max(0,grace-time.monotonic()))
        escalated=any(t.is_alive()for t in self.threads)
        stop_error=None
        if escalated:
            try:
                if self.registry is None:raise RuntimeError('NoOwnedStopAuthority')
                self.registry.stop()
                with self.registry.lock:items=[x for x in self.registry.active.values()if x is not None]
                for item in items:item.abort('ObserverShutdownEscalation')
            except BaseException as error:stop_error=type(error).__name__
        final=time.monotonic()+p['final_join_seconds']
        for thread in self.threads:thread.join(max(0,final-time.monotonic()))
        alive=[t.name for t in self.threads if t.is_alive()]
        with self.lock:
            now=time.monotonic();self.phases.append({'phase':self.phase,'epoch':self.epoch,'begin':self.phase_started,'end':now})
        result={'accepted':False,'draft_only':True,'errors':self.errors,'resource_window_seconds':None if self.started is None else time.monotonic()-self.started,'observer_threads_drained':not alive,'undrained_threads':alive,'escalated':escalated,'owned_stop_error':stop_error,'workload':self.workload_after,'phases':self.phases,'resource_rows':len(self.samples.rows),'census_rows':len(self.censuses.rows),'complete_census_rows':sum(eligible_census_row(r)for r in self.censuses.rows)}
        if not alive and not escalated and self.before is not None:
            try:
                if self.final_counter_command is None:raise RuntimeError('CleanupCounterAuthorityMissing')
                result['whole_resource_window']=self.observer.finish(self.before,command=self.final_counter_command)
            except BaseException as error:result['counter_finish_error']=type(error).__name__;self._error('counter-finish',error)
        if alive or stop_error:self._error('observer-undrained',RuntimeError(stop_error or 'ThreadJoin'))
        (self.output/'window.json').write_text(json.dumps(result,indent=2)+'\n')
        self.closed_result=result
        return result


def proposed_checks(window, policy):
    """Unexecuted inherited-budget arithmetic, never sets acceptance true."""
    p = policy['resource_draft']
    budget = p['budgets_inherited_without_tuning']
    checks = []
    if hasattr(window,'phases'):checks.extend(interval_checks(window.samples.rows,window.censuses.rows,window.phases,policy))
    for label in ['idle-before-first-playback', 'idle-after-import-playback']:
        resources = [r for r in window.samples.rows if r['phase'] == label and r['cpu_interval_stable']]
        census = [r for r in window.censuses.rows if r['phase'] == label and eligible_census_row(r)]
        checks.append(dict(id=label+'/complete-census-count', observed=len(census),
                           passed=len(census) >= p['minimum_complete_stable_idle_censuses']))
        checks.append(dict(id=label+'/cpu', passed=bool(resources) and
                           sum(r['cpu_percent'] for r in resources)/len(resources) <= budget['idle_cpu_percent_one_core']))
    before = [r for r in window.samples.rows if r['phase'] == 'idle-before-first-playback' and r['phase_stable']]
    after = [r for r in window.samples.rows if r['phase'] == 'idle-after-import-playback' and r['phase_stable']]
    import statistics
    if before and after:
        base = statistics.median(r['memory_bytes'] for r in before)
        growth = statistics.median(r['memory_bytes'] for r in after) - base
        limit = max(budget['post_idle_memory_growth_bytes'], base*budget['post_idle_memory_growth_fraction'])
        checks.append(dict(id='settled/memory-growth', observed=growth, limit=limit, passed=growth <= limit))
    else:
        checks.append(dict(id='settled/memory-growth', passed=False, reason='MissingFootprint'))
    censuses = window.censuses.rows
    for field, name in [('processes','process'), ('threads','thread'), ('fds','fd'), ('socket_descriptors','socket')]:
        phases = [[sum(r['census'][s][field] for s in SERVICES) for r in censuses
                   if r['phase'] == phase and eligible_census_row(r)]
                  for phase in ['idle-before-first-playback', 'idle-after-import-playback']]
        growth = max(phases[1])-max(phases[0]) if all(phases) else None
        checks.append(dict(id='settled/'+name+'-growth', observed=growth,
                           limit=budget['post_idle_'+name+'_growth'],
                           passed=growth is not None and growth <= budget['post_idle_'+name+'_growth']))
    for row in window.samples.rows:
        checks.append(dict(id='sample/hard-memory', passed=all(v['memory_usage_bytes'] <= v['memory_limit_bytes'] for v in row['containers'])))
        checks.append(dict(id='sample/whole-stack-cpu', passed=row['cpu_percent'] <= budget['whole_stack_cpu_percent_one_core']))
    for row in censuses:
        if eligible_census_row(row):
            checks.append(dict(id='sample/time-wait', passed=sum(row['census'][s]['tcp_states'].get('06',0) for s in SERVICES) <= budget['tcp_time_wait_ceiling']))
    workload = window.workload_after
    checks.append(dict(id='workload/complete-cgroup', passed=workload is not None and
                       set(workload['services']) == SERVICES and workload['cpu_percent_mean'] <= budget['whole_stack_cpu_percent_one_core']))
    eligible = [r for r in window.samples.rows if r['phase'] == 'concurrent-import-playback' and r['cpu_interval_stable']]
    checks.append(dict(id='workload/minimum-samples', observed=len(eligible), passed=len(eligible) >= p['minimum_workload_resource_samples']))
    return checks
