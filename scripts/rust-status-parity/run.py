#!/usr/bin/env python3
"""Compare Rust status projection with unchanged upstream ChannelStatus methods."""
import argparse
import ast
import hashlib
import json
import logging
from pathlib import Path
import random
import subprocess
import sys
import types

ROOT=Path(__file__).resolve().parents[2]
REFERENCE=ROOT/'apps/proxy/live_proxy/channel_status.py'

class Redis:
    def __init__(self,channel,metadata,clients,index):
        self.channel,self.metadata,self.clients,self.index=channel,metadata,clients,index
    def hgetall(self,key):
        if key.endswith(':metadata'):return self.metadata
        return self.clients.get(key.rsplit(':',1)[-1],{})
    def get(self,key):return str(self.index) if key.endswith(':index') else None
    def scard(self,key):return len(self.clients)
    def smembers(self,key):return sorted(self.clients)
    def hmget(self,key,*names):
        row=self.hgetall(key);return [row.get(name) for name in names]
    def exists(self,key):return False
    def scan(self,*args,**kwargs):return (0,[])
    def ttl(self,key):return -2
    def srem(self,*args):return 0


def oracle(channel,metadata,clients,index,detailed,now):
    tree=ast.parse(REFERENCE.read_text())
    cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='ChannelStatus')
    cls.body=[n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name in ('_calculate_bitrate','_execute_redis_command','get_basic_channel_info','get_detailed_channel_info')]
    constants=ast.parse((ROOT/'apps/proxy/live_proxy/constants.py').read_text())
    fields=next(n for n in constants.body if isinstance(n,ast.ClassDef) and n.name=='ChannelMetadataField')
    keys=ast.parse((ROOT/'apps/proxy/live_proxy/redis_keys.py').read_text())
    keys.body=[n for n in keys.body if isinstance(n,ast.ClassDef) and n.name=='RedisKeys']
    proxy=types.SimpleNamespace(redis_client=Redis(channel,metadata,clients,index),stream_managers={})
    scope={'time':types.SimpleNamespace(time=lambda:now),'ProxyServer':types.SimpleNamespace(get_instance=lambda:proxy),
           'ClientManager':types.SimpleNamespace(remove_ghost_clients=lambda *args,**kwargs:[]),'logger':logging.getLogger(__name__),
           'TS_PACKET_SIZE':188,'ConnectionError':ConnectionError,'TimeoutError':TimeoutError,'DatabaseError':RuntimeError,'close_old_connections':lambda:None}
    sys.modules['apps.m3u.models']=types.SimpleNamespace(M3UAccountProfile=types.SimpleNamespace(objects=types.SimpleNamespace(filter=lambda **kwargs:types.SimpleNamespace(first=lambda:types.SimpleNamespace(name=metadata.get('m3u_profile_name','Default'))))))
    exec(compile(ast.Module(body=[fields],type_ignores=[]),'<original-fields>','exec'),scope)
    exec(compile(keys,'<original-keys>','exec'),scope)
    exec(compile(ast.Module(body=[cls],type_ignores=[]),str(REFERENCE),'exec'),scope)
    method='get_detailed_channel_info' if detailed else 'get_basic_channel_info'
    return getattr(scope['ChannelStatus'],method)(channel)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--binary',type=Path,default=ROOT/'rust/target/debug/examples/status_parity')
    args=parser.parse_args()
    rng=random.Random(20261109)
    now=2_000_000_000.0
    cases=[]
    for count in range(64):
        meta={'state':'active','owner':'fixture-owner','url':'http://fixture/live.ts','stream_profile':'Proxy','channel_name':'Fixture','init_time':str(now-rng.randrange(1,10000)),
              'total_bytes':str(rng.randrange(1,1024**4)),'state_changed_at':str(now-42)}
        if count%2:meta.update(stream_id='12',stream_name='Example',m3u_profile='5',m3u_profile_name='Default',logo_id='6')
        if count%3==0:meta.update(video_codec='h264',resolution='1280x720',source_fps='25.0',audio_codec='aac',audio_channels='2',ffmpeg_speed='1.0',stream_type='HTTP')
        if count%5==0:meta.pop('total_bytes')
        clients=[]
        for i in range(count%10):
            clients.append({'client_id':f'client-{i}','user_agent':'Fixture','worker_id':'fixture-worker','ip_address':'127.0.0.1','user_id':'19','output_format':'mpegts','output_profile_id':None,
                            'connected_at':now-20,'last_active':now-1,'last_active_ago':1.0,'bytes_sent':188,'avg_rate_KBps':.1,'current_rate_KBps':.2})
        raw={c['client_id']:{k:str(v) for k,v in c.items() if k not in ('client_id','last_active_ago','output_profile_id')} for c in clients}
        for detailed in [False,True]:
            case={'channel_id':'fixture','metadata':meta,'clients':clients,'index':count,'detailed':detailed,'now':now}
            cases.append((case,oracle('fixture',meta,raw,count,detailed,now)))
    process=subprocess.run([str(args.binary)],input=''.join(json.dumps(case)+'\n' for case,_ in cases),text=True,capture_output=True,check=True)
    results=[json.loads(line) for line in process.stdout.splitlines()]
    failures=[]
    if len(results)!=len(cases):raise RuntimeError('RPC response count mismatch')
    for i,((case,expected),actual) in enumerate(zip(cases,results)):
        if json.dumps(expected,sort_keys=True)!=json.dumps(actual,sort_keys=True):failures.append({'case':i,'expected':expected,'actual':actual})
    report={'scope':'legacy-status-component-parity','passed':not failures,'cases':len(cases),'application_performance_verified':False,
            'reference_sha256':hashlib.sha256(REFERENCE.read_bytes()).hexdigest(),'failures':failures,
            'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),
            'source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__).resolve(),ROOT/'rust/crates/stream-http/src/status.rs',ROOT/'rust/crates/stream-http/examples/status_parity.rs']}}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'cases':len(cases),'failures':len(failures)}))
    return bool(failures)

if __name__=='__main__':raise SystemExit(main())
