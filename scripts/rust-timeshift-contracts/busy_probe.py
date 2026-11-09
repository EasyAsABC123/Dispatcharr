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
            first_body_began=time.monotonic()
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
                if new_range is None:
                    time.sleep(2.5)
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
            if mode=='near-eof-probe':
                time.sleep(max(0,2.5-(time.monotonic()-first_body_began)))
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
            'capture_complete':len(events)==3,'events':events,'controlled_established_body_min_seconds':2.5,
            'continuous_quota_sampling_verified':False,'application_parity_verified':False,'performance_verified':False}


def compare(source,candidate):
    differences=[]
    left={event['case']:event for event in source['events']}
    right={event['case']:event for event in candidate['events']}
    for case in sorted(left.keys()|right.keys()):
        if case not in left or case not in right:
            differences.append({'case':case,'field':'present'});continue
        original,rewrite=left[case],right[case]
        for field in ['range','status','headers','captured_bytes','captured_sha256','capture_is_prefix','initial_prefix_bytes','initial_prefix_sha256']:
            if original.get(field)!=rewrite.get(field):differences.append({'case':case,'field':field})
        for phase in ['before','during_replacement','after_grace']:
            for field in ['api_exists','pool_exists','pool_busy','profile_connections','stats_client_count']:
                if original[phase].get(field)!=rewrite[phase].get(field):differences.append({'case':case,'field':phase+'.'+field})
        if case!='near-eof-probe' and rewrite['old_response_after_replacement_bytes']>=2_000_001:
            differences.append({'case':case,'field':'old-response-not-terminated-within-bounded-read'})
        if case=='near-eof-probe' and rewrite['old_response_after_replacement_bytes']!=65536:
            differences.append({'case':case,'field':'old-response-did-not-continue'})
        if original['origin']!=rewrite['origin']:differences.append({'case':case,'field':'origin-range-order-and-fixture'})
    if any(side.get('controlled_established_body_min_seconds')!=2.5 for side in [source,candidate]):
        differences.append({'case':'configuration','field':'controlled-body-lifetime'})
    return {'captured_busy_controls_match':not differences,'differences':differences,
        'normalizations':['elapsed time and queued old-response bytes are observed, not equated','immediate client-close scheduling is observed; established grace is checked after 9 seconds','busy statistics byte totals reflect differently buffered delivery and remain a separate contract'],
        'continuous_quota_sampling_verified':False,'application_parity_verified':False,'performance_verified':False}
