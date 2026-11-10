"""Actual Source producer/native FileJob orphan worker/Source AsyncResult contract.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
"""
import hashlib,json,os,subprocess
from pathlib import Path
def reset(context):
    before = context['before']
    source_after = context['after']
    connection = context['connection']
    functions = context['functions']
    # Retain and temporarily drop ONLY fixture-generated fault code so reset
    # cannot accidentally execute the fault under test.
    declarations = []
    with connection.cursor() as cursor:
        for name in functions:
            if not name.startswith(('rust_orphan_fault_', 'rust_orphan_marker_')) or not name.replace('_', '').isalnum():
                raise ValueError('Owned function required')
            cursor.execute('SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname=%s', [name])
            definition = cursor.fetchone()[0]
            cursor.execute('SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE p.proname=%s', [name])
            triggers = [row[0] for row in cursor.fetchall()]
            declarations.append((definition, triggers))
            cursor.execute(f'DROP FUNCTION {name}() CASCADE')
    for row in before['intervals']:
        context['IntervalSchedule'].objects.bulk_create([context['IntervalSchedule'](**row)], ignore_conflicts=True)
    for row in before['tasks']:
        model = context['PeriodicTask']
        if not model.objects.filter(pk=row['id']).exists():
            model.objects.bulk_create([model(**row, enabled=False, args='[]')])
    for model, rows in [(context['EPGSource'],before['epg_refs']), (context['M3UAccount'],before['m3u_refs'])]:
        for row in rows: model.objects.filter(pk=row['id']).update(refresh_task_id=row['refresh_task_id'])
    marker = context['PeriodicTasks']
    marker.objects.filter(ident=1).delete()
    for row in before['beat_marker']: marker.objects.bulk_create([marker(**row)])
    with connection.cursor() as cursor:
        for sequence in context['sequences']: cursor.execute(f'ALTER SEQUENCE {sequence} RESTART WITH 1')
        for function, triggers in declarations:
            cursor.execute(function)
            for trigger in triggers: cursor.execute(trigger)
    snapshot = context['snapshot']
    arguments = ([row['id']for row in before['tasks']], [row['id']for row in before['intervals']],
                 [row['id']for row in before['crons']], [row['id']for row in before['epg_refs']], [row['id']for row in before['m3u_refs']])
    if snapshot(*arguments) != before: raise AssertionError('Exact owned fixture reset required')
    return arguments

def compare_worker(context):
    before=context['before'];source_after=context['after'];source_meta=context['meta']
    arguments=reset(context)
    broker=context['broker'];backend=context['backend'];app=context['app']
    broker.flushdb();backend.flushdb()
    issued=context['refresh_epg_data'].delay(context['missing'])
    hold=context['label']in ['sole','marker2_fault']
    value={'action':'run','task_id':issued.id,'source_id':context['missing'],
           'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+os.environ['POSTGRES_DB'],
           'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0',
           'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],
           'hold_orphan_marker':hold}
    peer=subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','refresh_worker::peer::native_refresh_worker_peer','--nocapture'],input=json.dumps(value),text=True,capture_output=True,timeout=40)
    with Path('/evidence/native-orphan-worker-private.log').open('a')as file:file.write(peer.stdout+peer.stderr)
    peer.check_returncode();audit=json.loads(next(line.split('NATIVE_REFRESH_WORKER_JSON=',1)[1]for line in peer.stdout.splitlines()if 'NATIVE_REFRESH_WORKER_JSON='in line))
    raw=backend.get(app.backend.get_key_for_task(issued.id));meta=json.loads(raw)
    after=context['snapshot'](*arguments)
    def normalized(value):
        return {key:rows if key!='beat_marker'else[{'ident':row['ident'],'changed':not any(row==old for old in before['beat_marker'])}for row in rows]for key,rows in value.items()}
    check=context['check'];label=context['label']
    check('native-worker-Source-AsyncResult-'+label,app.AsyncResult(issued.id).get(timeout=2)==source_meta['result']and meta['status']==source_meta['status']and meta['traceback']==source_meta['traceback']and meta['children']==source_meta['children'])
    check('native-worker-exact-orphan-effects-'+label,normalized(after)==normalized(source_after)and context['sequence_values']()==context['report']['worker_cases'][-1]['marker_attempts'])
    check('native-worker-metadata-TTL-'+label,meta['task_id']==issued.id and 'parent_id'not in meta and 3590<=backend.ttl(app.backend.get_key_for_task(issued.id))<=3600)
    check('native-worker-perrun-owner-drain-'+label,audit['calls']==1 and audit['default_dispatch_calls']==0 and audit['caller_released']and audit['worker_drained']and audit['service_drained']and audit['transport_drained']and audit['physical_jobs']==0 and audit['held_orphan_marker']==hold and broker.llen('celery')==0 and broker.hlen('unacked')==0 and context['cache'].get('task_lock_refresh_epg_data_'+str(context['missing']))is None)
    context['report'].setdefault('native_worker_cases',[]).append({'name':label,'source_id':context['missing'],'source_task_id':source_meta['task_id'],'native_task_id':issued.id,'metadata':meta,'metadata_sha256':hashlib.sha256(raw).hexdigest(),'audit':audit,'after':after,'marker_attempts':context['sequence_values']()})
