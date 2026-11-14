"""Qualify actual packaged-Main GPG events; no PID-namespace guessing."""
import datetime,json,os,re,math

def need(value,label):
    if not value:raise ValueError(label)
def epoch(value):return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def optional(value):
    if not isinstance(value,str):return value
    if value=='None':return None
    m=re.fullmatch(r'Some\((.*)\)',value)
    if m:
        try:return json.loads(m[1])
        except ValueError:return m[1]
    return value

def read_events(raw):
    rows=[]
    for line in raw.decode('utf8').splitlines():
        position=line.find('{')
        if position<0:
            need('dispatcharr::plugin_gpg' not in line,'GPG event must use declared bounded JSON subscriber');continue
        try:row=json.loads(line[position:])
        except ValueError:
            need('dispatcharr::plugin_gpg' not in line,'malformed GPG JSON event');continue
        if row.get('target')!='dispatcharr::plugin_gpg':continue
        need('trace_error' not in row and type(row.get('fields')) is dict,'complete bounded GPG event, no observer error')
        allowed={'event','pid','start_generation','pid_namespace','role','home_id','stdin_bytes','stdin_complete','stdout_eof','stderr_eof','stdout_bytes','stderr_bytes','wait_observed','wait_exit_code','wait_signal','kill_ok','pipe_eof_observed','remove_ok','home_absent','error_kind'}
        need(set(row['fields'])<=allowed and len(line[position:].encode())<4096,'bounded metadata-only event fields')
        fields={k:optional(v) for k,v in row['fields'].items()}
        need(type(row['epoch_seconds']) in (int,float) and math.isfinite(row['epoch_seconds']) and row['epoch_seconds']>=0 and type(fields.get('event')) is str,'typed actual event clock')
        rows.append({'epoch_seconds':row['epoch_seconds'],**fields})
    return rows

def validate(source,native,raw,numeric_mode='legacyguard-v1'):
    need(numeric_mode in ('legacyguard-v1','finite-v1'),'explicit qualified candidate numeric mode')
    need(native['capture_complete'] is True and native['actual_request_count']==native['completed_capture_count']==len(source['requests']),'complete actual cohort required for trace acceptance')
    events=read_events(raw);spawns=[e for e in events if e['event']=='spawn'];joined=[e for e in events if e['event']=='joined'];homes=[e for e in events if e['event']=='home_drop']
    need(not any(e['event'] in ('spawn_error','timeout_settled') for e in events),'no unqualified process failure or timeout')
    need(len(spawns)==len(joined),'every actual spawned GPG child has joined receipt')
    joins={(e['pid'],e['start_generation']):e for e in joined};need(len(joins)==len(joined),'unique actual GPG generations')
    for start in spawns:
        need(type(start['pid']) is int and start['pid']>0 and type(start['start_generation']) is str and start['start_generation'].isdecimal() and int(start['start_generation'])>0,'positive actual child generation')
        need(re.fullmatch(r'pid:\[[0-9]+\]',start['pid_namespace']) is not None,'actual native PID namespace')
        row=joins[(start['pid'],start['start_generation'])]
        need(all(row[k] is True for k in ['stdin_complete','stdout_eof','stderr_eof','wait_observed']),'actual stdin/EOF/wait completion')
        need(all(type(row[k]) is int and 0<=row[k]<=65536 for k in ['stdout_bytes','stderr_bytes']),'bounded actual pipe counts')
        need(start['epoch_seconds']<=row['epoch_seconds'] and start['role']==row['role'] and start['home_id']==row['home_id'],'same child chronology/role/home')
    need(len({e['home_id'] for e in homes})==len(homes) and {e['home_id'] for e in homes}=={e['home_id'] for e in spawns},'every actual owned temporary home retired')
    need(all(e['remove_ok'] is True and e['home_absent'] is True for e in homes),'actual home deletion/absence')
    expected=source.get('GPG_observer_terminal',{}).get('value',{}).get('direct_GPG_processes',[])
    for reference,request in zip(source['requests'],native['requests']):
        actual=[e for e in spawns if epoch(request['began_utc'])<=e['epoch_seconds']<=epoch(request['completed_utc'])]
        wanted=[e for e in expected if e['case']==reference['index']]
        need(len(actual)==len(wanted),'exact actual GPG child count for retained case'+str(reference['index']))
        for start,want in zip(actual,wanted):
            row=joins[(start['pid'],start['start_generation'])];role='import' if '--import' in want['argv'] else 'verify'
            observed=row['wait_exit_code'] if row['wait_exit_code'] is not None else -row['wait_signal']
            need(start['role']==role and observed==os.waitstatus_to_exitcode(want['actual_wait_result'][1]),'actual GPG wait matches retained measured result')
    covered=[e for e in spawns if any(epoch(r['began_utc'])<=e['epoch_seconds']<=epoch(r['completed_utc']) for r in native['requests'])]
    diagnostic=native.get('numeric_diagnostic')
    if diagnostic:
        extra=[e for e in spawns if epoch(diagnostic['began_utc'])<=e['epoch_seconds']<=epoch(diagnostic['completed_utc'])]
        if numeric_mode=='legacyguard-v1':
            need(diagnostic['status']==501 and len(extra)==1 and extra[0]['role']=='import','bound legacy numeric guard501/import only')
            need(joins[(extra[0]['pid'],extra[0]['start_generation'])]['wait_exit_code']==0,'legacy numeric import successful')
        else:
            need(diagnostic['status']==200 and diagnostic['json']['signature_verified']is False,'bound finite mode rejects stale signature at HTTP200')
            need(len(extra)==2 and [e['role']for e in extra]==['import','verify'],'bound finite mode actual import+verify')
            need([joins[(e['pid'],e['start_generation'])]['wait_exit_code']for e in extra]==[0,1],'bound finite stale signature actual import0/verify1')
        covered+=extra
    need(len(covered)==len(spawns),'no unexplained GPG process outside observed calls')
    return {'passed':True,'actual_children':len(spawns),'actual_homes_removed':len(homes),'children':[{'pid':e['pid'],'generation':e['start_generation'],'PID_namespace':e['pid_namespace']} for e in spawns],'observed_waits':[{'pid':e['pid'],'generation':e['start_generation'],'exit':e['wait_exit_code'],'signal':e['wait_signal']} for e in joined]}
