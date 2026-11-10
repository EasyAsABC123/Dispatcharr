"""Owned fixture reset and exact original/native nonconcurrent orphan comparison.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
Not imported by the accepted original-only tools.
"""
import datetime
import json
import os
import subprocess
from pathlib import Path

def compare(context):
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
    result = subprocess.run(['/evidence/native-orphan-cleanup','orphan_cleanup::native_orphan_cleanup_peer','--ignored','--exact','--nocapture'],
                            input=json.dumps({'source_id':context['missing']}), text=True, capture_output=True, timeout=30)
    with Path('/evidence/native-orphan-private.log').open('a')as file:file.write(result.stdout+result.stderr)
    result.check_returncode()
    raw = next(line.split('NATIVE_ORPHAN_JSON=',1)[1]for line in result.stdout.splitlines()if 'NATIVE_ORPHAN_JSON='in line)
    actual=json.loads(raw)
    native_after=snapshot(*arguments)
    def normalized(value):
        return {key: rows if key!='beat_marker' else [{'ident':row['ident'],'changed':not any(row==old for old in before['beat_marker'])}for row in rows]
                for key, rows in value.items()}
    context['check']('native_helper_return_'+context['label'],actual['helper_return']==context['value'])
    context['check']('native_exact_rows_refs_marker_'+context['label'],normalized(native_after)==normalized(source_after))
    source_attempts=context['report']['cases'][-1]['marker_attempts']
    context['check']('native_exact_marker_attempts_'+context['label'],context['sequence_values']()==source_attempts)
    context['check']('native_physical_drain_'+context['label'],actual['physical_jobs']==0)
    context['report'].setdefault('native_cases',[]).append({'name':context['label'],'source_id':context['missing'],'outcome':actual,'after':native_after,'marker_attempts':context['sequence_values']()})

def resources(context):
    source = 9_000_099
    if context['EPGSource'].objects.filter(pk=source).exists(): raise AssertionError('Owned missing source required')
    interval=context['IntervalSchedule'].objects.create(every=8999,period='seconds')
    context['owned_intervals'].append(interval.pk)
    task=context['PeriodicTask'](name=f'epg_source-refresh-{source}',task='apps.epg.tasks.refresh_epg_data',args='[]',enabled=False,interval=interval)
    context['PeriodicTask'].objects.bulk_create([task]);context['owned_tasks'].append(task.pk)
    context['PeriodicTasks'].objects.update_or_create(ident=1,defaults={'last_update':datetime.datetime(2020,1,2,3,4,5,tzinfo=datetime.timezone.utc)})
    before=context['snapshot']([task.pk],[interval.pk],[],[],[])
    context['fault']('django_celery_beat_periodictask',task.pk)
    result=subprocess.run(['/evidence/native-orphan-cleanup','orphan_cleanup::native_orphan_cleanup_peer','--ignored','--exact','--nocapture'],input=json.dumps({'source_id':source,'action':'resources'}),text=True,capture_output=True,timeout=30)
    with Path('/evidence/native-orphan-private.log').open('a')as file:file.write(result.stdout+result.stderr)
    result.check_returncode()
    actual=json.loads(next(line.split('NATIVE_ORPHAN_JSON=',1)[1]for line in result.stdout.splitlines()if 'NATIVE_ORPHAN_JSON='in line))
    context['check']('native_cancelled_sql_and_pool_waiters_retain_physical_ownership',actual['resource_cases']==2 and actual['caller_drops']==2 and actual['physical_jobs']==0)
    context['check']('native_aborted_p0001_jobs_rollback_before_drain',context['snapshot']([task.pk],[interval.pk],[],[],[])==before)
    context['report']['native_resources']=actual
    with context['connection'].cursor()as cursor:
        for function in context['functions']:cursor.execute(f'DROP FUNCTION IF EXISTS {function}() CASCADE')
    context['functions'].clear()
