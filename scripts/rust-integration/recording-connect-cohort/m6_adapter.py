"""Bounded hooks for the shared real Recording/M6 Compose milestone cohort.

Original Dispatcharr, Celery, Django and Channels authors retain all algorithm,
application and protocol credit. Importing this module starts no applications.
The coordinator/Fixture owns applications, command settlement and exact cleanup.
"""
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import time
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[3]
WIRE = ROOT / 'rust/crates/job-transport/datasets/source-default-stats-wire.json'
TASKS = {
    'cleanup': 'celery.backend_cleanup',
    'scanner': 'core.tasks.scan_and_process_files',
    'recurring': 'apps.channels.tasks.maintain_recurring_recordings',
    'expiration': 'apps.m3u.tasks.check_account_expirations',
    'stats': 'apps.proxy.tasks.fetch_channel_stats',
}


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n')


def directory(fixture):
    return Path(fixture.output) / ('m6-' + fixture.case)


def prepare_context(fixture, output):
    """Seed only declared SQL/files, before the actual backend is started.

    Disable existing Beat rows to avoid accidentally racing the explicitly
    published cases. Real Source workers may install their own rows on startup;
    these are captured, never silently deleted while workers are running.
    """
    if fixture.case != 'grouped-m6':
        return
    out = directory(fixture)
    out.mkdir(parents=True, exist_ok=False)
    seeded = fixture.sql("""
from core.models import CoreSettings
from apps.channels.models import RecurringRecordingRule
from django_celery_beat.models import PeriodicTask, PeriodicTasks
PeriodicTask.objects.all().update(enabled=False)
PeriodicTasks.update_changed()
RecurringRecordingRule.objects.all().update(enabled=False)
for key, changes in [('system_settings', {'time_zone':'UTC','auto_import_mapped_files':True}), ('stream_settings', {'m3u_hash_key':'url'})]:
    row,_=CoreSettings.objects.get_or_create(key=key,defaults={'name':key,'value':{}})
    value=row.value.copy() if isinstance(row.value,dict) else {}
    if isinstance(changes,dict): value.update(changes)
    else: value=changes
    CoreSettings.objects.filter(pk=row.pk).update(value=value)
print(json.dumps({'beat':list(PeriodicTask.objects.values('id','name','task','enabled')), 'settings':list(CoreSettings.objects.filter(key__in=['system_settings','stream_settings']).values('key','value'))},default=str))
""")
    # Main UID owns only this context's cache roots; no Original source is mounted.
    fixture.python("import pathlib,os;root=pathlib.Path(" + repr('/shared/' + fixture.case) + ");\nfor name in ['cached_m3u','cached_epg','cached_programs']:\n p=root/name;p.mkdir(exist_ok=True);os.chown(p,65532,65532);p.chmod(0o755)\n")
    save(out / 'prestart-seed.json', seeded)


def raw_backend(fixture, task):
    return fixture.redis(2, "local a=redis.call('GET',ARGV[1]);local b=redis.call('GET',ARGV[2]);return {a or '',b or '',redis.call('TTL',ARGV[1]),redis.call('TTL',ARGV[2])}",
                         'celery-tasks:celery-task-meta-' + task,
                         'celery-task-meta-' + task)


def result(fixture, task, seconds=60):
    def read():
        values = raw_backend(fixture, task)
        if values[0] and values[1]:
            raise RuntimeError('ConflictingBackendNamespaces')
        raw = values[0] or values[1]
        if not raw:
            return None
        value = json.loads(raw)
        if value.get('status') not in ('SUCCESS', 'FAILURE', 'REVOKED'):
            return None
        return {'raw': raw, 'metadata': value, 'ttl': values[2] if values[0] else values[3]}
    return fixture.wait(read, seconds, 'ActualBackendTerminal:' + task)


def publish(fixture, label, name):
    """Project a retained Source no-argument envelope into this owned namespace.

    This is consumer interoperability, NOT a Native HTTP/Beat producer claim.
    Scanner/recurring descendants are observed actual application publications.
    """
    authority = WIRE.read_bytes()
    rows = json.loads(authority)['defaults']
    envelope = copy.deepcopy(next(row['envelope'] for row in rows if row['task'] == name))
    ident = str(uuid.uuid4())
    envelope['headers'].update(id=ident, root_id=ident, parent_id=None, eta=None)
    if envelope['headers'].get('expires') is not None:
        envelope['headers']['expires'] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=120)).isoformat()
    envelope['properties'].update(correlation_id=ident, delivery_tag=str(uuid.uuid4()))
    raw = json.dumps(envelope, ensure_ascii=True)
    record = {'task_id': ident, 'scope': 'retained Source-frame projection into real consumer; not Native producer',
              'authority': str(WIRE.relative_to(ROOT)), 'authority_sha256': hashlib.sha256(authority).hexdigest(),
              'raw_frame': raw, 'epoch_before_publish': time.time()}
    path = directory(fixture) / (label + '-publication.json')
    save(path, record)
    record['actual_LPUSH_length'] = fixture.redis(1, "return redis.call('LPUSH','celery',ARGV[1])", raw)
    save(path, record)
    return ident


def queue(fixture):
    return fixture.redis(1, "return {redis.call('LRANGE','celery',0,63),redis.call('LRANGE','dvr',0,63),redis.call('HLEN','unacked')}")


def subscribe(fixture):
    channel = 'specific.m6_' + uuid.uuid4().hex + '!receiver'
    return fixture.sql("from channels.layers import get_channel_layer;from asgiref.sync import async_to_sync;layer=get_channel_layer();channel=" + repr(channel) + ";async_to_sync(layer.group_add)('updates',channel);print(json.dumps({'channel':channel,'key':layer.prefix+channel.split('!')[0]+'!'}))")


def events(fixture, subscription):
    return fixture.sql("import redis,os;from channels.layers import get_channel_layer;r=redis.Redis(host='localhost',password=os.environ['REDIS_PASSWORD'],db=0);layer=get_channel_layer();rows=r.zrange(" + repr(subscription['key']) + ",0,512);\nif len(rows)>512 or sum(map(len,rows))>4194304:raise RuntimeError('ActualEventsBudget')\nprint(json.dumps([layer.deserialize(row)for row in rows],default=str))")


def sql_snapshot(fixture):
    return fixture.sql("""
from apps.m3u.models import M3UAccount, M3UAccountProfile
from apps.epg.models import EPGSource, EPGData, ProgramData
from apps.channels.models import RecurringRecordingRule, Stream
from core.models import SystemNotification
from django_celery_beat.models import PeriodicTask
print(json.dumps({'accounts':list(M3UAccount.objects.values()),'profiles':list(M3UAccountProfile.objects.values()),'epg_sources':list(EPGSource.objects.values()),'epg_channels':list(EPGData.objects.values()),'programs':list(ProgramData.objects.values()),'streams':list(Stream.objects.values()),'rules':list(RecurringRecordingRule.objects.values()),'recordings':list(Recording.objects.values()),'periodic':list(PeriodicTask.objects.values()),'notifications':list(SystemNotification.objects.values())},default=str))
""")


def run_cell(fixture, label, callback):
    out = directory(fixture)
    record = {'name': label, 'status': 'running', 'started_epoch': time.time()}
    save(out / (label + '.json'), record)
    try:
        record['observation'] = callback()
        record['status'] = 'passed'
    except Exception as error:
        record.update(status='failed', exception=type(error).__name__, message=str(error), traceback=traceback.format_exc())
    finally:
        # Preserve independent observations even after a failed assertion.
        for key, read in [('queue_after', lambda: queue(fixture)), ('sql_after', lambda: sql_snapshot(fixture))]:
            try:
                record[key] = read()
            except Exception as error:
                record[key + '_error'] = repr(error)
        record['finished_epoch'] = time.time()
        save(out / (label + '.json'), record)
    return record


def terminal(fixture, label, task_name):
    ident = publish(fixture, label, task_name)
    observed = result(fixture, ident)
    save(directory(fixture) / (label + '-backend.json'), observed)
    meta = observed['metadata']
    if meta['status'] != 'SUCCESS' or meta.get('traceback') is not None or meta['task_id'] != ident:
        raise AssertionError(('ActualTaskFailure', meta))
    if not 3450 <= observed['ttl'] <= 3600:
        raise AssertionError(('ActualResultExpiry', observed['ttl']))
    return ident, observed


def cleanup_case(fixture):
    key = 'celery-tasks:celery-task-meta-m6-sentinel-' + uuid.uuid4().hex
    payload = json.dumps({'status': 'SUCCESS', 'result': {'retained': True}})
    fixture.redis(2, "return redis.call('SET',ARGV[1],ARGV[2],'EX',3600)", key, payload)
    ident, observed = terminal(fixture, 'cleanup', TASKS['cleanup'])
    after = fixture.redis(2, "return {redis.call('GET',ARGV[1]),redis.call('TTL',ARGV[1])}", key)
    if after[0] != payload or not 3450 <= after[1] <= 3600 or observed['metadata']['result'] is not None:
        raise AssertionError(('RedisBackendCleanupIsNotNoop', after))
    return {'task_id': ident, 'backend': observed, 'sentinel_after': after}


def scanner_case(fixture):
    stamp = uuid.uuid4().hex[:10]
    m3u = fixture.case_data / 'm3us' / ('m6-' + stamp + '.m3u')
    xml = fixture.case_data / 'epgs' / ('m6-' + stamp + '.xml')
    m3u.write_text('#EXTM3U\n#EXTINF:-1 tvg-id="m6-' + stamp + '" group-title="Milestone",Milestone channel\n' + fixture.origin + '/stream.ts\n')
    now = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
    fmt = lambda value: value.strftime('%Y%m%d%H%M%S +0000')
    xml.write_text('<?xml version="1.0"?><tv><channel id="m6-' + stamp + '"><display-name>Milestone channel</display-name></channel><programme start="' + fmt(now) + '" stop="' + fmt(now + dt.timedelta(minutes=10)) + '" channel="m6-' + stamp + '"><title>Milestone programme</title></programme></tv>')
    for path in (m3u, xml):
        path.chmod(0o644)
        os.utime(path, (time.time() - 10, time.time() - 10))
    # Source refresh imports programmes for mapped EPG entries. A brand-new
    # unmapped XML source legitimately has no programme work. Seed an actual
    # existing XML source/mapping without invoking save-signal task producers;
    # Scanner still discovers the M3U account and publishes all work naturally.
    epg_seed = fixture.sql("from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel;source=EPGSource(name=" + repr(xml.stem) + ",source_type='xmltv',file_path=" + repr('/data/epgs/' + xml.name) + ",is_active=True,custom_properties={});EPGSource.objects.bulk_create([source]);epg=EPGData(tvg_id=" + repr('m6-' + stamp) + ",name='Milestone channel',epg_source_id=source.pk);EPGData.objects.bulk_create([epg]);Channel.objects.filter(pk=" + str(fixture.channel_id) + ").update(epg_data_id=epg.pk);print(json.dumps({'source_id':source.pk,'epg_id':epg.pk,'mapped_channel':" + str(fixture.channel_id) + "}))")
    save(directory(fixture) / 'scanner-mapped-epg-seed.json', epg_seed)
    ident, observed = terminal(fixture, 'scanner', TASKS['scanner'])
    # Observe actual descendants, including both Source on-create and process
    # publications. No synthetic completed-parent witness is introduced.
    def settled_children():
        children = publications(fixture)
        rows = [row for row in children if row['headers'].get('parent_id') == ident]
        names = {row['headers']['task'] for row in rows}
        required = {'apps.m3u.tasks.refresh_m3u_groups', 'apps.m3u.tasks.refresh_single_m3u_account', 'apps.epg.tasks.refresh_epg_data'}
        if not required <= names:
            return None
        outcomes = []
        for row in rows:
            task = row['headers']['id']
            values = raw_backend(fixture, task)
            raw = values[0] or values[1]
            if not raw:
                return None
            outcomes.append({'publication': row, 'backend_raw': raw, 'metadata': json.loads(raw)})
        return outcomes
    outcomes = fixture.wait(settled_children, 90, 'ActualScannerDescendants')
    save(directory(fixture) / 'scanner-descendants.json', outcomes)
    if any(row['metadata']['status'] != 'SUCCESS' for row in outcomes):
        raise AssertionError('ActualScannerChildFailure')
    def imported():
        observed_sql = sql_snapshot(fixture)
        return observed_sql if any(row.get('title') == 'Milestone programme' for row in observed_sql['programs']) else None
    sql = fixture.wait(imported, 60, 'ActualMappedProgrammeCommit')
    if not any(row.get('file_path') == '/data/m3us/' + m3u.name for row in sql['accounts']):
        raise AssertionError('MissingActualM3UAccount')
    if not any(row.get('tvg_id') == 'm6-' + stamp for row in sql['streams']):
        raise AssertionError('MissingActualSingleM3UStreamImport')
    if not any(row.get('title') == 'Milestone programme' for row in sql['programs']):
        raise AssertionError('MissingActualRefreshProgramme')
    return {'task_id': ident, 'backend': observed, 'children': outcomes, 'sql': sql, 'mapped_epg_seed': epg_seed}


def publications(fixture):
    path = fixture.case_data / 'm6-publications.jsonl'
    if not path.exists():
        return []
    if path.stat().st_size > 1048576:
        raise RuntimeError('PublicationLedgerBudget')
    return [json.loads(line) for line in path.read_text().splitlines()]


def recurring_case(fixture):
    seeded = fixture.sql("""
from apps.channels.models import RecurringRecordingRule, Channel
from django.utils import timezone
from datetime import timedelta,time
day=(timezone.now()+timedelta(days=2)).date()
Channel.objects.filter(pk=""" + str(fixture.channel_id) + """).update(epg_data_id=None,logo_id=None)
rule=RecurringRecordingRule.objects.create(channel_id=""" + str(fixture.channel_id) + """,days_of_week=[day.weekday()],start_time=time(12,0),end_time=time(12,1),enabled=True,name=Channel.objects.get(pk=""" + str(fixture.channel_id) + """).name,start_date=day,end_date=day)
print(json.dumps({'id':rule.pk,'day':str(day)}))
""")
    ident, observed = terminal(fixture, 'recurring', TASKS['recurring'])
    if observed['metadata']['result'] != 1:
        raise AssertionError(('ActualRecurringCount', observed))
    first = sql_snapshot(fixture)
    again_id, again = terminal(fixture, 'recurring-repeat', TASKS['recurring'])
    if again['metadata']['result'] != 0:
        raise AssertionError(('ActualRecurringIdempotence', again))
    if len(sql_snapshot(fixture)['recordings']) != len(first['recordings']):
        raise AssertionError('DuplicateRecurringRecording')
    return {'rule': seeded, 'task_id': ident, 'backend': observed, 'repeat_id': again_id, 'repeat': again, 'sql': first}


def expiration_case(fixture):
    seed = fixture.sql("""
from apps.m3u.models import M3UAccountProfile
from core.models import SystemNotification
from django.utils import timezone
from datetime import timedelta
base=M3UAccountProfile.objects.get(pk=""" + str(fixture.provider) + """)
now=timezone.now();profiles=[]
for name,delta in [('M6 warning',timedelta(days=2,hours=2)),('M6 expired',timedelta(hours=-1)),('M6 extended',timedelta(days=10))]:
    p=M3UAccountProfile(m3u_account_id=base.m3u_account_id,name=name,is_active=True,is_default=False,exp_date=now+delta)
    M3UAccountProfile.objects.bulk_create([p]);profiles.append({'id':p.pk,'name':name,'expiration':p.exp_date.isoformat()})
stale=SystemNotification.objects.create(notification_key='xc-exp-warning-m6-stale',notification_type='warning',title='Stale milestone',message='Owned stale milestone',is_active=True)
print(json.dumps({'profiles':profiles,'stale':stale.notification_key}))
""")
    subscription = subscribe(fixture)
    ident, observed = terminal(fixture, 'expiration', TASKS['expiration'])
    sql = sql_snapshot(fixture)
    keys = {row['notification_key'] for row in sql['notifications']}
    warning, expired, extended = [row['id'] for row in seed['profiles']]
    required = {'xc-exp-warning-' + str(warning), 'xc-exp-expired-' + str(expired)}
    if not required <= keys or seed['stale'] in keys or any(key.endswith('-' + str(extended)) for key in keys):
        raise AssertionError(('ActualExpirationNotifications', sorted(keys)))
    emitted = events(fixture, subscription)
    if not any(row.get('data', {}).get('notification_key') == seed['stale'] for row in emitted):
        raise AssertionError('MissingActualStaleDismissalEvent')
    return {'task_id': ident, 'backend': observed, 'seed': seed, 'sql': sql, 'events': emitted}


def stats_case(fixture, label, active):
    subscription = subscribe(fixture)
    ident, observed = terminal(fixture, label, TASKS['stats'])
    def actual_stats():
        rows = events(fixture, subscription)
        candidates = [row for row in rows if row.get('data', {}).get('type') == 'channel_stats']
        return candidates or None
    emitted = fixture.wait(actual_stats, 15, 'ActualLiveStatisticsEvent')
    decoded = [json.loads(row['data']['stats']) for row in emitted]
    if active:
        if not any(row['count'] >= 1 and any(channel.get('client_count', 0) >= 1 for channel in row['channels']) for row in decoded):
            raise AssertionError('MissingRealActiveViewerStatistics')
    elif not any(row['count'] == 0 and row['channels'] == [] for row in decoded):
        raise AssertionError('NonemptyStatsInEmptyContext')
    return {'task_id': ident, 'backend': observed, 'events': emitted, 'decoded_stats': decoded,
            'scope': 'actual retained engine/Source live builder with real Channels event; no replacement stats source'}


def run_selected_families(fixture, output):
    if fixture.case != 'grouped-m6':
        raise ValueError('WrongResetBoundary')
    out = directory(fixture)
    save(out / 'before.json', {'sql': sql_snapshot(fixture), 'queue': queue(fixture)})
    ledger = '/shared/' + fixture.case + '/m6-publications.jsonl'
    fixture.role('m6-publications', ['/dispatcharrpy/bin/python', '/normal-dvr-tools/broker_monitor.py'], extra={'NORMAL_DVR_LEDGER': ledger})
    cells = []
    try:
        fixture.wait(lambda: (fixture.case_data / 'm6-publications.ready').exists(), 10, 'ActualM6PublicationObserver')
        for label, callback in [('cleanup', cleanup_case), ('scanner', scanner_case), ('recurring', recurring_case), ('expiration', expiration_case), ('empty-stats', lambda f: stats_case(f, 'empty-stats', False))]:
            cells.append(run_cell(fixture, label, lambda callback=callback: callback(fixture)))
    finally:
        fixture.role('m6-publications', [], stop=True)
    save(out / 'actual-publications.json', publications(fixture))
    report = {'status': 'passed' if all(row['status'] == 'passed' for row in cells) else 'failed', 'backend': fixture.backend,
              'cells': cells, 'scope': 'experimental selected families only; full Source47/Beat/concurrency/performance not accepted',
              'unmeasured': ['same-family ordinary concurrency6', 'held physical M3U Ready/close under pool8', 'full Beat interval/crontab/election', 'Source47 declarations beyond selected14', 'paired resource measurements']}
    save(out / 'report.json', report)
    return report


def active_viewer_cases(fixture, output):
    out = directory(fixture)
    out.mkdir(parents=True, exist_ok=True)
    return run_cell(fixture, 'active-stats', lambda: stats_case(fixture, 'active-stats', True))
