"""Source busy archive seek characterization; no capability/provider URLs saved.

All application behavior credit belongs to original Dispatcharr contributors.
Boundary quota observations do not prove there was no transient counter gap.
"""
import hashlib
import json
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from idle_probe import fetch


def capture(base, origin, seed, observe):
    events=[]
    for mode,new_range in [('middle-seek','bytes=1880000-1880255'),
                           ('plain-reconnect',None),
                           ('near-eof-probe','bytes=14999900-15000155')]:
        status,_,_=fetch(origin,'/archive-control',body={'mode':'all'},headers={'Content-Type':'application/json'})
        if status!=200:
            raise RuntimeError('busy probe origin reset failed')
        status,_,body=fetch(base,'/api/catchup/sessions/',body={
            'channel_uuid':seed['channel_uuid'],'start':'2026-11-09T11:00:00Z','duration':30},
            headers={'Authorization':'Bearer '+seed['users']['owner']['token'],'Content-Type':'application/json'})
        if status!=201:
            raise RuntimeError('busy probe capability mint failed')
        cap=json.loads(body)
        first=urlopen(Request(base+cap['playback_url'],headers={
            'Range':'bytes=0-','User-Agent':'fixture-busy-'+mode}),timeout=20)
        try:
            initial=first.read(65536)
            before=observe(cap['session_id'],True)
            began=time.monotonic()
            headers={'User-Agent':'fixture-busy-'+mode}
            if new_range is not None:headers['Range']=new_range
            try:
                second=urlopen(Request(base+cap['playback_url'],headers=headers),timeout=20)
            except HTTPError as error:
                second=error
            try:
                # A plain reconnect intentionally closes a bounded prefix of
                # the full response; byte ranges are read to their bounded EOF.
                payload=second.read(65536 if new_range is None else 2_000_001)
                if len(payload)>2_000_000:raise ValueError('busy probe exceeds bounded capture')
                event={'case':mode,'range':new_range,'status':second.status,
                    'headers':{k.lower():v for k,v in second.headers.items() if k.lower() in ('content-type','content-length','content-range','accept-ranges')},
                    'captured_bytes':len(payload),'captured_sha256':hashlib.sha256(payload).hexdigest(),
                    'capture_is_prefix':new_range is None,'elapsed_seconds':round(time.monotonic()-began,3),
                    'before':before,'during_replacement':observe(cap['session_id'],True),
                    'initial_prefix_bytes':len(initial),'initial_prefix_sha256':hashlib.sha256(initial).hexdigest()}
            finally:
                second.close()
            # Near-EOF probes must leave the first transfer running. Read one
            # bounded chunk rather than draining that full30second archive.
            remainder=first.read(65536 if mode=='near-eof-probe' else 2_000_001)
            event['old_response_after_replacement_bytes']=len(remainder)
            event['old_response_read_was_bounded']=True
        finally:
            first.close()
        event['after_both_closed']=observe(cap['session_id'],True)
        time.sleep(9)
        event['after_grace']=observe(cap['session_id'],True)
        _,_,stats=fetch(origin,'/archive-stats')
        event['origin']=json.loads(stats)
        events.append(event)
    return {'format_version':1,'scope':'original busy middle seek, plain reconnect and tail probe boundary characterization',
            'capture_complete':len(events)==3,'events':events,
            'continuous_quota_sampling_verified':False,'application_parity_verified':False,'performance_verified':False}
