"""Actual catch-up WebSocket capture with bounded memory and capability redaction.
Original application credit belongs to Dispatcharr authors/contributors.
"""
import sys,time,json,threading,copy,math
from pathlib import Path
from urllib.parse import urlsplit,quote
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'rust-realtime-contracts'))
from ws_client import Client
INITIAL={'type':'connection_established','data':{'success':True,'message':'WebSocket connection established successfully'}}

def capture(ws_base,token,exercise):
    address=urlsplit(ws_base);peer=Client(address.hostname,address.port or 80,'/ws/?token='+quote(token,safe=''))
    if peer.status!=101 or peer.receive()!={'message':INITIAL}:raise RuntimeError('Catch-up observer didnotauthenticate')
    events=[];errors=[];seen={};stopped=threading.Event();total=0
    def sanitize(stats):
        for session in stats['timeshift_sessions']:
            raw=session['session_id'];replacement=seen.setdefault(raw,'<session-'+str(len(seen)+1)+'>')
            if session['stats_channel_id']!=str(session['channel_id'])+'_'+raw:raise RuntimeError('Invalid statistics channel/session relation')
            session['session_id']=replacement
            session['stats_channel_id']=str(session['channel_id'])+'_'+replacement
            for connection in session['connections']:
                for key in ['session_id','client_id']:
                    if key in connection:
                        if connection[key]!=raw:raise RuntimeError('Invalid client/session relation')
                        connection[key]=replacement
        return stats
    def read():
        nonlocal total
        try:
            while not stopped.is_set():
                value=peer.optional(.2)
                if value is None:continue
                if 'closed' in value:
                    if not stopped.is_set():raise RuntimeError('Catch-up observer closedearly')
                    return
                message=value.get('message',{});data=message.get('data') or {}
                if data.get('type')!='timeshift_stats':continue
                if message.get('type')!='update' or data.get('success') is not True or not isinstance(data.get('stats'),str):raise RuntimeError('Changed catch-up envelope')
                raw=data['stats'];total+=len(raw.encode('utf-8'))
                if total>16*1024*1024 or len(events)>=1024:raise RuntimeError('Bounded catch-up event capture exceeded')
                stats=sanitize(json.loads(raw))
                events.append({'type':'update','data':{'success':True,'type':'timeshift_stats','stats':stats}})
        except Exception as error:errors.append(type(error).__name__)
    reader=threading.Thread(target=read,daemon=True);reader.start()
    try:
        result=exercise()
        time.sleep(.5)
    finally:
        stopped.set();reader.join(2)
        if reader.is_alive():peer.socket.shutdown(2);reader.join(1)
        peer.close()
    if errors:raise RuntimeError('Catch-up event capture failed: '+','.join(errors))
    counts=[value['data']['stats']['total_connections'] for value in events]
    if not events or not any(count>0 for count in counts) or counts[-1]!=0:raise RuntimeError('Missing active/final-empty catch-up snapshots')
    return result,{'format_version':1,'scope':'Actual catch-up HTTP media plus WebSocket fullstats publication; capabilities redacted','capture_complete':True,'events':events,'session_count':len(seen),'captured_bytes':total,'application_parity_verified':False,'performance_verified':False}

def compare(source,candidate):
    differences=[]
    if not source.get('capture_complete') or not candidate.get('capture_complete'):differences.append('Incomplete capture')
    if len(source['events'])!=len(candidate['events']):differences.append('Snapshot count differs')
    def stable(event):
        event=copy.deepcopy(event);stats=event['data']['stats']
        if not math.isfinite(stats['timestamp']):differences.append('Invalid timestamp')
        stats.pop('timestamp')
        count=0
        for session in stats['timeshift_sessions']:
            session.pop('position_anchor_at',None)
            for conn in session['connections']:
                count+=1
                if not isinstance(conn['bytes_streamed'],int) or conn['bytes_streamed']<0:differences.append('Invalid byte counter')
                if not math.isfinite(conn['avg_bitrate_kbps']) or conn['avg_bitrate_kbps']<0:differences.append('Invalid bitrate')
                if not math.isfinite(conn['connected_at']) or not isinstance(conn['duration'],int):differences.append('Invalid connected/duration clock')
                for key in ['connected_at','duration','avg_bitrate_kbps','bytes_streamed']:conn.pop(key)
        if count!=stats['total_connections']:differences.append('Connection count doesnotmatchfullDTO')
        return event
    for left,right in zip(source['events'],candidate['events']):
        if stable(left)!=stable(right):differences.append('Stable complete catch-up DTO differs')
    return {'captured_packaged_timeshift_events_match':not differences,'differences':differences,'normalizations':['Actual capability IDs replaced by first-seen session ordinals','Own timestamps/elapsed/byte-rate progress validated; stable full DTOs and snapshot counts compared'],'application_parity_verified':False,'performance_verified':False}
