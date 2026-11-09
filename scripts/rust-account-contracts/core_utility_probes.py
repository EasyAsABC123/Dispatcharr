"""Original Dispatcharr core read contracts; only scalar/digest evidence is saved."""
import hashlib
import json
from urllib.parse import urlencode

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()
def run(http,baseline,candidate,accounts,record):
    def compare(name,path,expected=200,role='admin'):
        try:
            token=accounts[role]['access'] if role else None
            a=http(baseline+path,token=token);b=http(candidate+path,token=token)
            record('core-utilities:'+name,a[0]==b[0]==expected and digest(a[1])==digest(b[1]),{'python_status':a[0],'rust_status':b[0],'python_body_sha256':digest(a[1]),'rust_body_sha256':digest(b[1])})
            return a,b
        except Exception as error:record('core-utilities:'+name,False,{'error_type':type(error).__name__});return None
    for role,status in [('admin',200),('standard',200),('viewer',200),('inactive',401),(None,401)]:compare('timezones:'+str(role),'/api/core/timezones/',status,role)
    result=compare('timezones:query-ignored','/api/core/timezones/?limit=1&search=invalid')
    if result:
        for label,(_,value) in zip(['python','rust'],result):
            record('core-utilities:dataset:'+label,value.get('count')==433 and len(value.get('timezones',[]))==433 and value.get('timezones')==sorted(value.get('timezones',[])) and sum(map(len,value.get('grouped',{}).values()))==433,{'count':value.get('count')})
    root='/api/core/system-events/'
    known=compare('events:known-display-witness',root+'?event_type=vod_stop&limit=1')
    if known:
        for label,(_,value) in zip(['python','rust'],known):
            events=value.get('events',[])
            record('core-utilities:known-display:'+label,len(events)==1 and events[0].get('event_type_display')=='VOD Stopped',{'event_count':len(events)})
    for role,status in [('standard',403),('viewer',403),('inactive',401),(None,401)]:compare('events:permission:'+str(role),root,status,role)
    kind='rust-probe-event-kind'
    for name,params,status in [('owned',{'event_type':kind},200),('one',{'event_type':kind,'limit':1},200),('offset',{'event_type':kind,'offset':1,'limit':1},200),('empty-limit',{'event_type':kind,'limit':0},200),('offset-past-end',{'event_type':kind,'offset':1000},200),('clamped',{'event_type':kind,'limit':1001},200),('negative-empty-slice',{'event_type':kind,'offset':3,'limit':-1},200),('missing-kind',{'event_type':'rust-probe-event-missing'},200),('known-display',{'event_type':'vod_stop','limit':1},200),('invalid-limit',{'limit':'bad'},500),('fraction',{'limit':'1.0'},500),('blank-limit',{'limit':''},500),('negative-offset',{'offset':-1},500),('negative-stop',{'limit':-1},500),('underscore',{'event_type':kind,'offset':'0_0','limit':'1_0'},200),('big-limit',{'event_type':kind,'limit':'9'*100},200),('big-offset',{'event_type':kind,'offset':'9'*100},500),('big-empty-offset',{'event_type':kind,'offset':'9'*100,'limit':0},200)]:
        compare('events:'+name,root+'?'+urlencode(params),status)
    result=compare('events:semantic-fixture',root+'?'+urlencode({'event_type':kind}))
    if result:
        for label,(_,value) in zip(['python','rust'],result):
            events=value.get('events',[])
            passed=len(events)==3 and value.get('total')==3 and [event['details'].get('ordinal') for event in events]==[3,2,1] and all(event.get('event_type_display')==kind and event.get('timestamp','').endswith('+00:00') for event in events) and events[0].get('channel_id')=='00000000-0000-0000-0000-000000000000' and events[-1].get('channel_name') is None
            record('core-utilities:events-fixture:'+label,passed,{'event_count':len(events),'total':value.get('total')})
