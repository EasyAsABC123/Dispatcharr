"""Actual native scheduler process-crash/restart compared with pinned Original.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Only fixture peer timing barriers; no queue reinsertion/task replacement.
"""
import datetime as dt, json, os, select, signal, subprocess, time
from pathlib import Path
import source_scheduler_crash as reference
assert reference.report['passed']
f = reference.f
report = {'passed': False, 'checks': list(reference.report['checks']), 'cases': [],
          'scope': 'Actual native scheduler publication→process hardloss before/after SQLsync→fresh native restart, compared with actual Original; timing barriers only, no automatic timer/main/general calendar claim'}
process = None
buffer = b''
sources = []

def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value: raise AssertionError(name)

def request(action):
    global buffer
    process.stdin.write((json.dumps({'action': action})+'\n').encode()); process.stdin.flush()
    end = time.monotonic()+20
    while time.monotonic()<end:
        while b'\n' in buffer:
            line, buffer = buffer.split(b'\n',1)
            if line.startswith(b'NATIVE_DATABASE_SCHEDULER_JSON='):
                return json.loads(line.split(b'=',1)[1])
        ready,_,_=select.select([process.stdout],[],[],.1)
        if ready:
            data=os.read(process.stdout.fileno(),65536)
            if not data: raise RuntimeError('Native crash peer exited')
            buffer+=data
            if len(buffer)>2*1024*1024: raise RuntimeError('Peer response bound')
    raise TimeoutError('Native scheduler command deadline')

def start(mode=None, marker=None):
    global process,buffer
    buffer=b''
    env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database)
    if mode: env.update(SCHEDULER_CRASH_MODE=mode,SCHEDULER_CRASH_GATE=str(marker))
    with Path('/evidence/native-scheduler-crash-private.log').open('ab') as log:
        process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
    setup=request('setup')
    check('native-crash-startup-defaults',len(setup['installed'])==6 and not setup['failures'])

def state(row):
    row.refresh_from_db()
    return {'enabled':row.enabled,'count':row.total_run_count,'last_run_present':row.last_run_at is not None}

def projection(value):
    return {'enabled':value['enabled'],'count':value['count'],'last_run_present':value['last_run_at'] is not None}

try:
    source=f.EPGSource(name='rust-owned-native-scheduler-crash-inactive',source_type='xmltv',is_active=False,custom_properties={})
    f.EPGSource.objects.bulk_create([source]); sources.append(source.pk)
    interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS)
    for expected in reference.report['cases']:
        mode=expected['name']; f.broker.flushdb(); f.backend.flushdb()
        f.PeriodicTask.objects.all().update(enabled=False); f.PeriodicTasks.update_changed()
        marker=Path('/evidence/native-scheduler-crash-'+mode+'.gate'); assert not marker.exists()
        start(mode,marker)
        row=f.create('rust-owned-native-crash-'+mode,f.refresh_epg_data.name,[source.pk],{},interval)
        request('refresh');request('refresh')
        process.stdin.write(b'{"action":"tick"}\n');process.stdin.flush()
        deadline=time.monotonic()+20
        while not marker.exists():
            if process.poll() is not None:raise RuntimeError('Native exited before crash barrier')
            if time.monotonic()>deadline:raise TimeoutError('Native crash barrier')
            time.sleep(.02)
        at_loss=state(row);queued=f.queue()
        check('native-'+mode+'-exact-publication-before-loss',len(queued)==1)
        check('native-'+mode+'-exact-SQL-sync-boundary',at_loss==projection(expected['at_hardloss']))
        os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);process=None
        check('native-'+mode+'-no-final-sync-after-hardloss',state(row)==at_loss)
        start();value=request('tick');envelopes=f.queue();after=state(row)
        check('native-'+mode+'-exact-restart-delay',value['delay_seconds']==expected['delay'])
        check('native-'+mode+'-exact-restart-SQL',after==projection(expected['after_restart']))
        check('native-'+mode+'-exact-restart-publication-count',len(envelopes)==len(expected['envelopes']))
        identifiers=[v['headers']['id']for v in envelopes]
        check('native-'+mode+'-new-publication-identities',len(set(identifiers))==len(identifiers))
        check('native-'+mode+'-no-consumption-or-result',f.broker.hlen('unacked')==0 and f.backend.dbsize()==0 and all(f.app.AsyncResult(v).state=='PENDING'for v in identifiers))
        drained=request('shutdown');check('native-'+mode+'-restart-drained',drained['drained']);process.stdin.close();process.wait(timeout=10);check('native-'+mode+'-restart-exit0',process.returncode==0);process=None
        report['cases'].append({'name':mode,'at_hardloss':at_loss,'after_restart':after,'delay':value['delay_seconds'],'envelopes':envelopes})
    report['passed']=True
finally:
    if process is not None and process.poll() is None:
        os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
    f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb()
    report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()]
    Path('/evidence/source-native-scheduler-crash-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
