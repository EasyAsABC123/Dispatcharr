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
    return {'cpu_stat':cpu,'charged_memory_current_bytes':charged,'inactive_file_bytes':inactive,'working_set_estimate_bytes':max(0,charged-inactive),'lifetime_memory_peak_context_only_bytes':int(peak.read_text()) if peak.exists() else None}

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
        groups[service]={'CID':cid,'image_id':info['Image'],'PID':info['State']['Pid'],'path':str(cgroup(info['State']['Pid'])),'category':'origin' if service=='origin' else 'backend_and_infrastructure'}
    expected={'baseline','origin','python-replica'} if backend=='baseline' else {'baseline','origin','candidate','candidate-replica'}
    if set(groups)-{'observer','orchestrator'}!=expected:raise RuntimeError('UnexpectedOwnedServiceSet')
    paths=[Path(g['path']) for g in groups.values()]
    if any(a==b or a in b.parents or b in a.parents for i,a in enumerate(paths) for b in paths[i+1:]):raise RuntimeError('OverlappingCgroupAccounting')
    inventory={name:command([docker,'top',g['CID'],'-eo','pid,ppid,pgid,args']) for name,g in groups.items() if 'CID'in g}
    if not any('postgres'in v for v in inventory.values()) or not any('redis-server'in v for v in inventory.values()):raise RuntimeError('InfrastructureProcessIdentityMissing')
    ffmpeg=[]
    for service,text in inventory.items():
        for line in text.splitlines()[1:]:
            if 'ffmpeg'in line and 'http://origin:8080/live/'in line:
                pid=int(line.split()[0]);same=cgroup(pid)==Path(groups[service]['path']);ffmpeg.append({'service':service,'hostPID':pid,'row':line,'same_service_cgroup':same})
    if len(ffmpeg)!=1 or not all(r['same_service_cgroup'] for r in ffmpeg):raise RuntimeError('ActualSharedFFmpegCgroupRequired')
    result={'scope':'diagnostic preflight only','backend':backend,'groups':groups,'initial_process_inventory':inventory,'actual_FFmpeg':ffmpeg,'samples':[],'useful_work_passed':False,'performance_acceptance':False}
    target=output/'resource-samples.json';began=time.monotonic();initial_origin=origin_snapshot();previous=None
    for index in range(31):
        delay=began+index-time.monotonic()
        if delay>0:time.sleep(delay)
        started=time.monotonic();values={name:counters(Path(g['path'])) for name,g in groups.items()};ended=time.monotonic()
        readers_now=[r.snapshot() for r in readers]
        totals={}
        for category in ('backend_and_infrastructure','origin','observer','whole_fixture'):
            selected=[v for name,v in values.items() if category=='whole_fixture'or groups[name]['category']==category]
            totals[category]={key:sum(v[key] for v in selected) for key in ('charged_memory_current_bytes','inactive_file_bytes','working_set_estimate_bytes')}
            totals[category]['cpu_usage_usec']=sum(v['cpu_stat']['usage_usec'] for v in selected)
        sample={'index':index,'monotonic_start':started,'monotonic_end':ended,'groups':values,'simultaneous_aggregate':totals,'readers':readers_now,'origin':origin_snapshot(),'metadata':snapshot(channel,profile)}
        if previous is not None:sample['both_media_advance']=all(r['media_bytes']>p['media_bytes'] and not r['ended'] and not r['errors'] and r['sync_errors']==0 for r,p in zip(readers_now,previous))
        result['samples'].append(sample);target.write_text(json.dumps(result,indent=2)+'\n');previous=readers_now
    result['elapsed_seconds']=result['samples'][-1]['monotonic_start']-result['samples'][0]['monotonic_start']
    result['media_byte_deltas']=[b['media_bytes']-a['media_bytes'] for a,b in zip(result['samples'][0]['readers'],result['samples'][-1]['readers'])]
    result['final_origin']=origin_snapshot();result['final_metadata']=snapshot(channel,profile)
    result['live_state_passed']=all(s['metadata']['state']=='active' and s['metadata']['provider_slots']==1 and s['metadata']['clients']==2 and s['origin']['active']==1 and s['origin']['connections']==initial_origin['connections'] and all(not r['ended'] and not r['errors'] and r['sync_errors']==0 and r.get('http_status')==200 for r in s['readers']) for s in result['samples'])
    result['useful_work_passed']=False  # Decoded/indexed whole-window qualification occurs off-window.
    result['accounting_notes']={'charged':'raw cgroup v2 memory.current; includes charged file cache','working_set':'max(0,memory.current-memory.stat inactive_file), estimate; not RSS','aggregate':'same tick sequential file reads, read start/end retained; sum simultaneous samples, never sum lifetime peaks','origin_and_observer':'included in whole_fixture, also separately reported','peak':'per-group lifetime memory.peak context only; startup/decoder may differ from measurement window','cpu':'raw cumulative usage_usec plus monotonic sample timestamps; differences only across fixed measurement endpoints'}
    target.write_text(json.dumps(result,indent=2)+'\n')

    return {'resource_file':str(target),'useful_work_passed':False,'live_state_passed':result['live_state_passed'],'media_byte_deltas':result['media_byte_deltas']}
