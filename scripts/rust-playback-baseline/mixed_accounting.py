"""Private Source-only mixed accounting attachment; no independent sampler.
Original Dispatcharr authors retain all application/algorithm credit/licenses.
"""
import importlib.util
import json
from pathlib import Path
import subprocess
import threading
import time
import urllib.request
from m0_accounting import Accounting, provider_capacity

ROOT=Path(__file__).resolve().parents[2]

def unlimited_fixture_counter_matches(state):
    # Pinned Source reserve_profile_slot only INCRs when max_streams > 0.
    # This original mixed seed is unlimited; finite-capacity cases remain separate.
    return state['provider_capacity']['kind']=='unlimited' and state['profile']['max_streams']==0 and state['provider_slots']==0


def upstream_observation_matches(scenario, state, expected, events):
    if state['origin_active']==expected:return True
    if scenario!='failover' or state['origin_active']!=0:return False
    stamps={x['name']:x['monotonic']for x in events}
    observed=state.get('origin_observed_monotonic')
    return observed is not None and stamps.get('fault-ack',float('inf'))<=observed<=stamps.get('first-recovery',float('-inf'))


class Hook:
    def __init__(self, context, compose, ids):
        self.context=context;self.compose=compose;self.ids=ids;self.lock=threading.RLock()
        self.overhead=[];self.states=[];self.transitions=[];self.active_state_done=False
        self.active_state_allowed=context.get('active_state_allowed',True);self.events=[]
        self.scenario=context.get('scenario','mixed');self.expected_clients=context.get('expected_clients',10)
        self.expected_upstreams=context.get('expected_upstreams',1);self.before_zero_required=context.get('before_zero_required',True)
        self.resource_stop=threading.Event();self.resource_thread=None;self.resource_errors=[]
        self.retention_seconds=context['retention_settle_seconds']
        self.bindings=self.inspection()
        spec=importlib.util.spec_from_file_location('mixed_qualified_census',ROOT/'scripts/rust-baseline/idle-churn/run.py')
        self.census=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.census)
        budgets={'fast_floor_Bps':225000,'slow_receive_buffer_bytes':4096,'slow_read_bytes':1024,'slow_delay_seconds':.25,'minimum_existing_active_resource_samples':3} if self.scenario=='mixed' else {'probe_timeout_seconds':45,'stall_seconds':2,'recovery_observation_seconds':7,'prefault_bytes':250000,'capture_limit_bytes':33554432}
        budgets=context.get('declared_budgets',budgets)
        budgets.update(absolute_CPU=None,absolute_memory=None,postclose_growth=None)
        self.ledger=Accounting(scenario='Source-'+self.scenario+'+explicitpostcloseaccounting',source_inputs=context['source_inputs'],container_bindings=self.bindings,
            contract_ids=['route-c75171aee6eb93b36a3c:METHOD_UNRESOLVED'],observed_method='GET',
            declared_budgets=budgets,
            max_records=1024,max_record_bytes=1048576)

    def command(self, args, timeout=8):
        result=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
        if len(result.stdout)>1048576 or len(result.stderr)>1048576:raise ValueError('Accounting command bound')
        if result.returncode:raise RuntimeError('Accounting command: '+result.stderr[:1024])
        return result.stdout

    def inspection(self):
        records=json.loads(self.command(['docker','inspect',*self.ids]))
        return {x['Id']:dict(id=x['Id'],started_at=x['State']['StartedAt'],image=x['Image'],
                            nano_cpus=x['HostConfig']['NanoCpus'],memory_bytes=x['HostConfig']['Memory'])for x in records}

    def state(self, label):
        began=time.monotonic();bindings=self.inspection();seed=self.context['seed'];stream=seed['stream_pk']
        if type(stream)is not int or stream<1:raise ValueError('Owned stream ID')
        channels=self.context.get('selected_channels',[seed])
        if not 1<=len(channels)<=10 or any(type(x['stream_pk'])is not int or x['stream_pk']<1 for x in channels):raise ValueError('Owned channel/stream bound')
        if len(channels)>1:
            stream_ids=','.join(str(x['stream_pk'])for x in channels)
            query="SELECT json_agg(r) FROM (SELECT DISTINCT p.id,p.max_streams,p.is_active FROM m3u_m3uaccountprofile p JOIN dispatcharr_channels_stream s ON s.m3u_account_id=p.m3u_account_id WHERE p.is_default AND s.id IN("+stream_ids+")) r"
            profiles=json.loads(self.command(self.compose+['exec','-T','baseline','psql','-U','dispatch','-d','dispatcharr','-t','-A','-c',query]))
            if len(profiles)!=1:raise ValueError('Existing same-provider many-channel fixture required')
            profile=profiles[0]
            lua="local rows={};local total=0;for i=1,#ARGV-1 do local n=redis.call('SCARD','live:channel:'..ARGV[i]..':clients');table.insert(rows,n);total=total+n end;return {total,tonumber(redis.call('GET','profile_connections:'..ARGV[#ARGV])or'0'),rows}"
            values=json.loads(self.command(self.compose+['exec','-T','baseline','redis-cli','--json','EVAL',lua,'0',*[x['channel_uuid']for x in channels],str(profile['id'])]))
        else:
            query="SELECT json_build_object('id',p.id,'max_streams',p.max_streams,'is_active',p.is_active) FROM m3u_m3uaccountprofile p JOIN dispatcharr_channels_stream s ON s.m3u_account_id=p.m3u_account_id WHERE p.is_default AND s.id="+str(stream)
            profile=json.loads(self.command(self.compose+['exec','-T','baseline','psql','-U','dispatch','-d','dispatcharr','-t','-A','-c',query]))
            lua="local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),tonumber(redis.call('GET','profile_connections:'..ARGV[2])or'0')}"
            values=json.loads(self.command(self.compose+['exec','-T','baseline','redis-cli','--json','EVAL',lua,'0',seed['channel_uuid'],str(profile['id'])]))
        with urllib.request.urlopen(self.context['origin']+'/stats',timeout=3)as response:origin=json.load(response)
        origin_observed=time.monotonic()
        census={}
        for ident in self.ids:
            def collect(args):return json.loads(self.command(['docker','exec',*args,ident,'/dispatcharrpy/bin/python','/m0-census.py'],timeout=5))
            inventory=json.loads(self.command(['docker','exec',ident,'/dispatcharrpy/bin/python','/m0-census.py','--inventory'],timeout=5))
            parts=[collect(['-u',str(uid)+':'+str(gid)])for uid,gid in inventory['identities']]
            ending=json.loads(self.command(['docker','exec',ident,'/dispatcharrpy/bin/python','/m0-census.py','--inventory'],timeout=5))
            census[ident]=self.census.merge_census(inventory,parts,ending)
        value=dict(label=label,origin_observed_monotonic=origin_observed,clients_by_channel=dict(zip([x['channel_uuid']for x in channels],values[2]if len(values)>2 else [values[0]])),profile=profile,provider_capacity=provider_capacity(profile['max_streams']),clients=values[0],provider_slots=values[1],origin_active=origin['active'],census=census,
                   binding_equal=bindings==self.bindings,started_monotonic=began,completed_monotonic=time.monotonic())
        if not value['binding_equal']:raise ValueError('Source accounting identity changed')
        self.overhead.append(dict(kind='state_callback',label=label,seconds=value['completed_monotonic']-began))
        self.states.append(value)
        return value

    def before(self):
        state=self.state('before');now=time.monotonic();self.ledger.begin('before',monotonic=now,bindings=self.bindings,state=state)
        self.ledger.end('before',monotonic=now,bindings=self.bindings,state=state,readers_physically_closed=True)

    def begin_active(self, began):
        self.ledger.begin('active',monotonic=began,bindings=self.bindings,state={})

    def tag(self):
        with self.lock:return self.ledger.active_phase,self.ledger.epoch

    def observe(self, tag, row, started, completed):
        with self.lock:
            self.overhead.append(dict(kind='existing_resource_observer',seconds=completed-started,phase=tag[0]))
            if tag[0] is None or tag!=self.tag():
                if len(self.transitions)>=1024:raise ValueError('Accounting transition record bound')
                self.transitions.append(dict(from_phase=tag[0],current_phase=self.ledger.active_phase,started=started,completed=completed,observer_record=row))
                return
            active_state=None
            if tag[0]=='active'and not self.active_state_done and self.active_state_allowed:
                active_state=self.state('active');self.active_state_done=True
            self.ledger.observation(phase=tag[0],epoch=tag[1],started_monotonic=started,completed_monotonic=time.monotonic(),
                bindings=self.bindings,observer_record={'existing_resource_observer':row,'state_callback':active_state},
                census_complete=bool(active_state and all(x['complete']for x in active_state['census'].values())))

    def allow_active_state(self):
        self.active_state_allowed=True

    def event(self, name):
        with self.lock:
            if len(self.events)>=16:raise ValueError('Accounting event bound')
            self.events.append(dict(name=name,monotonic=time.monotonic()))

    def start_resources(self, observe):
        if self.resource_thread is not None:raise ValueError('Second resource sampler refused')
        def loop():
            while not self.resource_stop.is_set():
                try:
                    tag=self.tag();started=time.monotonic();row=observe(self.ids)
                    self.observe(tag,row,started,time.monotonic())
                except Exception as error:
                    self.resource_errors.append(type(error).__name__)
                    self.resource_stop.set()
                self.resource_stop.wait(1)
        self.resource_thread=threading.Thread(target=loop)
        self.resource_thread.start()

    def stop_resources(self):
        self.resource_stop.set()
        if self.resource_thread:
            self.resource_thread.join(12)
            if self.resource_thread.is_alive():raise RuntimeError('Accounting sampler did not settle')

    def after_readers(self, readers):
        if not all(x.get('physical_socket_closed')for x in readers):raise RuntimeError('Mixed physical reader close incomplete')
        with self.lock:
            state=self.state('post-physical-close');now=time.monotonic()
            self.ledger.end('active',monotonic=now,bindings=self.bindings,state=state,readers_physically_closed=True)
            self.ledger.begin('retention',monotonic=now,bindings=self.bindings,state=state)

    def finish(self):
        with self.lock:
            state=self.state('retention-settled');self.ledger.end('retention',monotonic=time.monotonic(),bindings=self.bindings,state=state,readers_physically_closed=True)
            active=next(x for x in self.states if x['label']=='active')
            qualified=((not self.before_zero_required or self.states[0]['clients']==0) and active['clients']==self.expected_clients and unlimited_fixture_counter_matches(active) and upstream_observation_matches(self.scenario,active,self.expected_upstreams,self.events)
                       and state['clients']==state['provider_slots']==state['origin_active']==0 and not self.resource_errors)
            return dict(functional_hook_qualified=qualified,ledger=self.ledger.snapshot(),states=self.states,collection_overhead=self.overhead,
                        transition_observations_excluded=self.transitions,one_existing_resource_sampler=True,
                        application_credentials_caps_or_workload_changed=False,resource_acceptance=False,
                        events=self.events, resource_errors=self.resource_errors, scope='Functional hook accounting only; service CPU/memory caps absent and provider0unlimited; no new gain/finitecapacity/growthacceptance')
