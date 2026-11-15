"""Read owned v2 cgroups directly; no synthetic docker CPU percentages."""
import json,os,re,subprocess,time
from pathlib import Path

def cgroup(pid):
    lines=Path('/proc/'+str(pid)+'/cgroup').read_text().splitlines()
    group=next(line[3:] for line in lines if line.startswith('0::'))
    root=Path('/sys/fs/cgroup').resolve();path=(root/group.lstrip('/')).resolve()
    if root not in path.parents:raise RuntimeError('CgroupPathOutsideRoot')
    return path

def counters(path):
    def pairs(name):return {k:int(v) for k,v in (line.split() for line in (path/name).read_text().splitlines())}
    cpu=pairs('cpu.stat');memory=pairs('memory.stat');charged=int((path/'memory.current').read_text());inactive=memory.get('inactive_file')
    if inactive is None:raise RuntimeError('InactiveFileMissing')
    peak=path/'memory.peak'
    return {'swap_current_bytes':int((path/'memory.swap.current').read_text()),'cpu_stat':cpu,'memory_events':pairs('memory.events'),'charged_memory_current_bytes':charged,'inactive_file_bytes':inactive,'working_set_estimate_bytes':max(0,charged-inactive),'lifetime_memory_peak_context_only_bytes':int(peak.read_text()) if peak.exists() else None}

# One instrumentation-only interpreter in the SAME charged observer scope.
# It performs no app/DB/cache/network work and retains all original clock gates.
COUNTER_FILES=('cpu.stat','memory.stat','memory.current','memory.swap.current','memory.events','memory.peak')
COUNTER_MESSAGE_LIMIT=131072

def process_generation(pid):
    try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
    except FileNotFoundError:return None

class CounterFiles:
    def __init__(self,groups):
        self.files={};self.identities=[];self.closed=False
        try:
            for group,item in groups.items():
                self.files[group]={}
                for name in COUNTER_FILES:
                    try:fd=os.open(Path(item['path'])/name,os.O_RDONLY|os.O_CLOEXEC)
                    except FileNotFoundError:
                        if name=='memory.peak':continue
                        raise
                    self.files[group][name]=fd;stat=os.fstat(fd)
                    self.identities.append({'group':group,'file':name,'device':stat.st_dev,'inode':stat.st_ino})
        except BaseException:
            self.close();raise
    def read(self):
        values={};timing=[]
        for group,files in self.files.items():
            raw={}
            for name,fd in files.items():
                start=time.monotonic();cpu=time.process_time_ns();data=os.pread(fd,65536,0);end=time.monotonic()
                if not data or len(data)>=65536:raise RuntimeError('CounterFileSizeOrEOF')
                raw[name]=data.decode('ascii');timing.append({'group':group,'file':name,'monotonic_start':start,'monotonic_end':end,'process_cpu_nanoseconds':time.process_time_ns()-cpu,'bytes':len(data)})
            def pairs(name):return {k:int(v)for k,v in(line.split()for line in raw[name].splitlines())}
            cpu=pairs('cpu.stat');memory=pairs('memory.stat');charged=int(raw['memory.current']);inactive=memory.get('inactive_file')
            if inactive is None:raise RuntimeError('InactiveFileMissing')
            values[group]={'swap_current_bytes':int(raw['memory.swap.current']),'cpu_stat':cpu,'memory_events':pairs('memory.events'),'charged_memory_current_bytes':charged,'inactive_file_bytes':inactive,'working_set_estimate_bytes':max(0,charged-inactive),'lifetime_memory_peak_context_only_bytes':int(raw['memory.peak'])if'memory.peak'in raw else None}
        return values,timing
    def close(self):
        if self.closed:return self.receipt
        self.closed=True;fds=[fd for files in self.files.values()for fd in files.values()];errors=[]
        for fd in fds:
            try:os.close(fd)
            except OSError as error:errors.append(error.errno)
        absent=True
        for fd in fds:
            try:os.fstat(fd);absent=False
            except OSError as error:
                if error.errno!=9:absent=False
        self.receipt={'opened_counter_files':len(fds),'all_counter_FDs_closed':absent and not errors,'close_errno':errors}
        return self.receipt

def counter_worker_main():
    import select,sys
    counter=None;completed=False;failure=None;pid=os.getpid();generation=process_generation(pid)
    def receive():
        data=sys.stdin.buffer.readline(COUNTER_MESSAGE_LIMIT+1)
        if not data or len(data)>COUNTER_MESSAGE_LIMIT or not data.endswith(b'\n'):raise RuntimeError('CounterControlEOFOrBound')
        return json.loads(data)
    def send(value):
        data=json.dumps(value,separators=(',',':')).encode()+b'\n'
        if len(data)>COUNTER_MESSAGE_LIMIT:raise RuntimeError('CounterReplyBound')
        sys.stdout.buffer.write(data);sys.stdout.buffer.flush()
    try:
        setup=receive()
        if setup.get('command')!='prepare' or str(cgroup(pid))!=setup['observer_cgroup']:raise RuntimeError('CounterWorkerScopeMismatch')
        counter=CounterFiles(setup['groups']);send({'kind':'ready','PID':pid,'generation':generation,'same_observer_cgroup':True,'counter_file_identities':counter.identities})
        start=receive()
        if start.get('command')=='cancel':return
        if start.get('command')!='start':raise RuntimeError('CounterStartProtocol')
        began=float(start['began'])
        for index in range(61):
            # Cancellation is checked even when a prior counter/read ran late;
            # missed clocks still fail their unchanged recorded thresholds.
            if select.select([sys.stdin.buffer],[],[],0)[0]:
                if receive().get('command')!='cancel':raise RuntimeError('CounterCancelProtocol')
                break
            delay=began+index-time.monotonic()
            if delay>0 and select.select([sys.stdin.buffer],[],[],delay)[0]:
                if receive().get('command')!='cancel':raise RuntimeError('CounterCancelProtocol')
                break
            started=time.monotonic();cpu=time.process_time_ns();values,timing=counter.read();ended=time.monotonic()
            send({'kind':'sample','index':index,'monotonic_start':started,'monotonic_end':ended,'groups':values,'counter_file_reads':timing,'counter_sweep_process_cpu_nanoseconds':time.process_time_ns()-cpu})
        else:completed=True
    except BaseException as error:
        failure=type(error).__name__
        try:send({'kind':'failure','error_type':failure})
        except BaseException:pass
    finally:
        closed=counter.close()if counter else{'opened_counter_files':0,'all_counter_FDs_closed':True,'close_errno':[]}
        try:send({'kind':'final','PID':pid,'generation':generation,'measurement_completed':completed,'failure_type':failure,**closed})
        except BaseException:pass
    if failure:raise SystemExit(1)

class CounterWorker:
    def __init__(self,groups,output):
        import sys
        self.output=output;self.pending=bytearray();self.final=None;self.closed=False;self.receipt={};self.stderr_path=output/'counter-sampler.stderr';self.stderr=self.stderr_path.open('wb');os.fchmod(self.stderr.fileno(),0o600)
        self.process=subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve()),'--counter-worker'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.stderr,start_new_session=False)
        self.initial_generation=process_generation(self.process.pid)
        try:
            self.send({'command':'prepare','groups':groups,'observer_cgroup':str(cgroup(os.getpid()))})
            self.ready=self.receive(10)
            if self.ready.get('kind')!='ready'or self.ready.get('PID')!=self.process.pid or self.ready.get('generation')!=self.initial_generation or not self.ready.get('same_observer_cgroup'):raise RuntimeError('ActualCounterWorkerReadiness')
        except BaseException:
            self.close();raise
    def send(self,value):
        data=json.dumps(value,separators=(',',':')).encode()+b'\n'
        if len(data)>COUNTER_MESSAGE_LIMIT:raise RuntimeError('CounterControlBound')
        self.process.stdin.write(data);self.process.stdin.flush()
    def receive(self,seconds):
        import select
        deadline=time.monotonic()+seconds
        while b'\n'not in self.pending:
            left=deadline-time.monotonic()
            if left<=0 or not select.select([self.process.stdout],[],[],max(0,left))[0]:raise TimeoutError('CounterWorkerReply')
            data=os.read(self.process.stdout.fileno(),65536)
            if not data:raise RuntimeError('CounterWorkerUnexpectedEOF')
            self.pending.extend(data)
            if len(self.pending)>COUNTER_MESSAGE_LIMIT:raise RuntimeError('CounterReplyBound')
        line,_,rest=self.pending.partition(b'\n');self.pending=bytearray(rest);value=json.loads(line)
        if value.get('kind')=='final':self.final=value
        return value
    def start(self,began):self.send({'command':'start','began':began})
    def sample(self,index):
        value=self.receive(5)
        if value.get('kind')!='sample'or value['index']!=index:raise RuntimeError('ActualCounterSampleIndex')
        return value
    def close(self):
        if self.closed:return self.receipt
        self.closed=True;forced=False;errors=[];stdout_EOF=False
        try:
            if self.process.poll()is None:
                try:self.send({'command':'cancel'})
                except (BrokenPipeError,OSError):pass
            self.process.stdin.close();deadline=time.monotonic()+5
            while self.final is None and time.monotonic()<deadline:
                try:self.receive(max(.001,deadline-time.monotonic()))
                except (RuntimeError,TimeoutError):break
            try:self.process.wait(timeout=max(.001,deadline-time.monotonic()))
            except subprocess.TimeoutExpired:
                forced=True;self.process.terminate()
                try:self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=2)
            # The exited sole worker no longer owns stdout/stderr write ends.
            trailing=self.process.stdout.read(COUNTER_MESSAGE_LIMIT+1);stdout_EOF=not trailing and not self.pending
            if trailing or self.pending:errors.append('UnexpectedTrailingCounterProtocol')
        finally:
            self.process.stdout.close();self.stderr.close()
        self.receipt={'PID':self.process.pid,'generation':self.initial_generation,'actual_exit':self.process.returncode,'forced':forced,'generation_absent':process_generation(self.process.pid)!=self.initial_generation,'stdout_EOF':stdout_EOF,'stderr_writer_settled':self.process.returncode is not None,'stderr_bytes':self.stderr_path.stat().st_size,'protocol_errors':errors,'worker_final':self.final,'same_accounting_group':str(cgroup(os.getpid()))}
        self.receipt['cleanup_passed']=self.receipt['actual_exit']==0 and not forced and self.receipt['generation_absent']and stdout_EOF and not self.receipt['stderr_bytes']and not errors and self.final is not None and self.final.get('all_counter_FDs_closed')is True
        (self.output/'counter-sampler-settlement.json').write_text(json.dumps(self.receipt,indent=2)+'\n')
        return self.receipt


def measure(command,compose,docker,backend,readers,origin_snapshot,snapshot,channel,profile,output):
    own=cgroup(os.getpid())
    if not own.name.startswith('dispatcharr-resource-') or not own.name.endswith('.scope'):raise RuntimeError('DedicatedObserverScopeRequired')
    ids=command(compose+['ps','-q']).split()
    if not ids:raise RuntimeError('OwnedContainersMissing')
    groups={'observer':{'path':str(own),'category':'observer','PID':os.getpid()}}
    parent=Path(os.environ['DISPATCHARR_PARENT_OBSERVER_CGROUP']).resolve()
    if not parent.name.startswith('dispatcharr-resource-') or not parent.name.endswith('.scope'):raise RuntimeError('DedicatedOrchestratorScopeRequired')
    groups['orchestrator']={'path':str(parent),'category':'observer','PID':int(os.environ['DISPATCHARR_PARENT_OBSERVER_PID'])}
    if cgroup(groups['orchestrator']['PID'])!=parent:raise RuntimeError('ActualOrchestratorCgroupMismatch')
    for cid in ids:
        info=json.loads(command([docker,'inspect',cid]))[0];service=info['Config']['Labels']['com.docker.compose.service']
        if not info['State']['Running']:raise RuntimeError('OwnedServiceNotRunning')
        groups[service]={'CID':cid,'image_id':info['Image'],'limits':{k:info['HostConfig'][k]for k in ('NanoCpus','Memory','MemorySwap','PidsLimit')},'PID':info['State']['Pid'],'path':str(cgroup(info['State']['Pid'])),'category':'origin'if service=='origin'else'observer'if service=='publication-observer'else'backend_and_infrastructure'}
    expected={'baseline','origin','publication-observer'} if backend=='baseline' else {'baseline','origin','candidate','publication-observer'}
    if set(groups)-{'observer','orchestrator'}!=expected:raise RuntimeError('UnexpectedOwnedServiceSet')
    primary=[g for g in groups.values()if g['category']=='backend_and_infrastructure']
    if sum(g['limits']['NanoCpus']for g in primary)!=3000000000 or sum(g['limits']['Memory']for g in primary)!=3*1024**3 or sum(g['limits']['PidsLimit']for g in primary)!=1024 or any(g['limits']['MemorySwap']!=g['limits']['Memory']for g in primary):raise RuntimeError('FrozenEquivalentFullGraphLimits')
    if groups['origin']['limits']['NanoCpus']!=2000000000 or groups['origin']['limits']['Memory']!=512*1024**2:raise RuntimeError('FrozenOriginLimits')
    paths=[Path(g['path']) for g in groups.values()]
    if any(a==b or a in b.parents or b in a.parents for i,a in enumerate(paths) for b in paths[i+1:]):raise RuntimeError('OverlappingCgroupAccounting')
    def generation(pid):
        try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
        except FileNotFoundError:return None
    inventory={name:command([docker,'top',g['CID'],'-eo','pid,ppid,pgid,args']) for name,g in groups.items() if 'CID'in g}
    if not any('postgres'in v for v in inventory.values()) or not any('redis-server'in v for v in inventory.values()):raise RuntimeError('InfrastructureProcessIdentityMissing')
    ffmpeg=[]
    for service,text in inventory.items():
        for line in text.splitlines()[1:]:
            if 'vlc'in line and 'http://origin:8080/live/'in line:
                pid=int(line.split()[0]);same=cgroup(pid)==Path(groups[service]['path']);ffmpeg.append({'service':service,'hostPID':pid,'generation':generation(pid),'row':line,'same_service_cgroup':same})
    if len(ffmpeg)!=1 or not all(r['same_service_cgroup'] for r in ffmpeg):raise RuntimeError('ActualSharedVLCCgroupRequired')
    result={'scope':'normal loaded graph live-only phase','backend':backend,'groups':groups,'initial_process_inventory':inventory,'actual_VLC':ffmpeg,'samples':[],'useful_work_passed':False,'performance_acceptance':False}
    target=output/'resource-samples.json'
    counter_worker=CounterWorker(groups,output)
    result['counter_sampler_initial']=counter_worker.ready
    try:
        warmup_deadline=time.monotonic()+15
        while time.monotonic()<warmup_deadline:time.sleep(min(.1,warmup_deadline-time.monotonic()))
        began=time.monotonic();initial_origin=origin_snapshot();previous=None;counter_worker.start(began)
        result['fixed_window_monotonic']=[began,began+60]
        result['post_window_drain_deadline_monotonic']=began+65
        for index in range(61):
            counter=counter_worker.sample(index);received=time.monotonic();started=counter['monotonic_start'];ended=counter['monotonic_end'];values=counter['groups']
            readers_now=[r.snapshot() for r in readers]
            totals={}
            for category in ('backend_and_infrastructure','origin','observer','whole_fixture'):
                selected=[v for name,v in values.items() if category=='whole_fixture'or groups[name]['category']==category]
                totals[category]={key:sum(v[key] for v in selected) for key in ('charged_memory_current_bytes','inactive_file_bytes','working_set_estimate_bytes')}
                totals[category]['cpu_usage_usec']=sum(v['cpu_stat']['usage_usec'] for v in selected)
            sample={'index':index,'monotonic_start':started,'monotonic_end':ended,'groups':values,'simultaneous_aggregate':totals,'readers':readers_now,'origin':origin_snapshot(),'metadata':snapshot(channel,profile),'counter_file_reads':counter['counter_file_reads'],'counter_sweep_process_cpu_nanoseconds':counter['counter_sweep_process_cpu_nanoseconds'],'observer_received_counter_monotonic':received}
            if previous is not None:sample['both_media_advance']=all(r['media_bytes']>p['media_bytes'] and not r['ended'] and not r['errors'] and r['sync_errors']==0 for r,p in zip(readers_now,previous))
            result['samples'].append(sample);target.write_text(json.dumps(result,indent=2)+'\n');previous=readers_now
    finally:
        result['counter_sampler_receipt']=counter_worker.close()
        target.write_text(json.dumps(result,indent=2)+'\n')
    if not result['counter_sampler_receipt']['cleanup_passed']or not result['counter_sampler_receipt']['worker_final']['measurement_completed']:raise RuntimeError('ActualCounterSamplerNotNormallySettled')
    result['warmup_seconds']=15
    result['elapsed_seconds']=result['samples'][-1]['monotonic_start']-result['samples'][0]['monotonic_start']
    result['maximum_sample_lag_seconds']=max(abs(s['monotonic_start']-(began+s['index']))for s in result['samples'])
    result['maximum_counter_read_skew_seconds']=max(s['monotonic_end']-s['monotonic_start']for s in result['samples'])
    result['fixed_clock_passed']=result['maximum_sample_lag_seconds']<=.05 and result['maximum_counter_read_skew_seconds']<=.05 and abs(result['elapsed_seconds']-60)<=.05
    result['media_byte_deltas']=[b['media_bytes']-a['media_bytes'] for a,b in zip(result['samples'][0]['readers'],result['samples'][-1]['readers'])]
    first_values=result['samples'][0]['groups'];last_values=result['samples'][-1]['groups']
    result['OOM_events_delta']={name:last_values[name]['memory_events'].get('oom',0)-first_values[name]['memory_events'].get('oom',0)for name in groups}
    result['OOM_kills_delta']={name:last_values[name]['memory_events'].get('oom_kill',0)-first_values[name]['memory_events'].get('oom_kill',0)for name in groups}
    result['actual_VLC_generation_stable']=all(generation(row['hostPID'])==row['generation'] and row['generation']is not None for row in ffmpeg)
    result['final_origin']=origin_snapshot();result['final_metadata']=snapshot(channel,profile)
    result['Source_headroom_passed']=True
    if backend=='baseline':
        source_first=first_values['baseline']['cpu_stat'];source_last=last_values['baseline']['cpu_stat']
        periods=source_last.get('nr_periods',0)-source_first.get('nr_periods',0);throttled=source_last.get('nr_throttled',0)-source_first.get('nr_throttled',0)
        cpu_cores=(source_last['usage_usec']-source_first['usage_usec'])/1000000/result['elapsed_seconds']
        peak=max(s['groups']['baseline']['charged_memory_current_bytes']for s in result['samples'])
        result['Source_headroom_passed']=cpu_cores<=2.55 and(throttled/periods if periods else 0)<=.05 and peak<=.9*groups['baseline']['limits']['Memory']
    result['live_state_passed']=result['fixed_clock_passed']and result['actual_VLC_generation_stable']and result['Source_headroom_passed']and all(v['memory_events'].get('oom',0)==0 and v['memory_events'].get('oom_kill',0)==0 for s in result['samples']for v in s['groups'].values())and all(v['swap_current_bytes']==0 for s in result['samples']for v in s['groups'].values())and not any(result['OOM_events_delta'].values())and not any(result['OOM_kills_delta'].values())and all(s['metadata']['state']=='active' and s['metadata']['provider_slots']==1 and s['metadata']['clients']==len(readers) and s['metadata']['all_owned_user_id'] and s['metadata']['owned_identity_count']==len(readers) and s['origin']['active']==1 and s['origin']['connections']==initial_origin['connections'] and all(not r['ended'] and not r['errors'] and r['sync_errors']==0 and r.get('http_status')==200 for r in s['readers']) for s in result['samples'])
    result['useful_work_passed']=False  # Decoded/indexed whole-window qualification occurs off-window.
    result['accounting_notes']={'charged':'raw cgroup v2 memory.current; includes charged file cache','working_set':'max(0,memory.current-memory.stat inactive_file), estimate; not RSS','aggregate':'same tick sequential file reads in one dedicated instrumentation interpreter within SAME charged observer cgroup; exact file/read and sweep clocks retained; sum simultaneous samples, never sum lifetime peaks','origin_and_observer':'included in whole_fixture, also separately reported','peak':'per-group lifetime memory.peak context only; startup/decoder may differ from measurement window','cpu':'raw cumulative usage_usec plus monotonic sample timestamps; differences only across fixed measurement endpoints'}
    target.write_text(json.dumps(result,indent=2)+'\n')

    return {'resource_file':str(target),'useful_work_passed':False,'live_state_passed':result['live_state_passed'],'media_byte_deltas':result['media_byte_deltas']}

if __name__=='__main__':
    import sys
    if sys.argv[1:]!=['--counter-worker']:raise SystemExit('Owned counter worker mode required')
    counter_worker_main()
