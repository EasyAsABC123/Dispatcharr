"""Actual backup HTTP/worker/archive gate in an existing owned milestone context.
Original Dispatcharr/dependency authors retain all application/protocol credit.
No application/fixture scheduler is created; Root must grant the existing lease.
Raw task/download tokens and database dumps remain private.
"""
import hashlib,json,time,urllib.error,urllib.parse,urllib.request,uuid
from pathlib import Path


def private_api(fixture,output,label,method,path,body=None,admin=True):
    headers={'Authorization':'Bearer '+fixture.actual_admin_token()}if admin else{}
    data=None if body is None else json.dumps(body).encode()
    if data is not None:headers['Content-Type']='application/json'
    request=urllib.request.Request(fixture.base_url+path,data=data,headers=headers,method=method)
    try:reply=urllib.request.urlopen(request,timeout=fixture.remaining(15))
    except urllib.error.HTTPError as error:reply=error
    with reply:
        raw=reply.read(1048577)
        if len(raw)>1048576:raise RuntimeError('BackupHTTPReplyBudget')
        status=reply.status;value=json.loads(raw)if raw else None
    label=label+'-'+uuid.uuid4().hex[:8]
    private=output/'private';private.mkdir(mode=0o700,exist_ok=True);p=private/(label+'.json');p.write_bytes(raw);p.chmod(0o600)
    # Neither query tokens nor body token values appear in the public metadata.
    (output/(label+'-reply.json')).write_text(json.dumps({'status':status,'body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'private_raw_reply':str(p),'credential_role':'admin'if admin else'none_or_signed','method':method},indent=2)+'\n')
    return status,value


def prepare_context(fixture):
    if fixture.case!='normal-backup':return
    # Actual production initialization is exercised: AppConfig creates missing
    # backup_settings before cron sync; SQL here is declared fixture setup.
    fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;CoreSettings.objects.filter(key='backup_settings').delete();PeriodicTask.objects.filter(name='backup-scheduled-task').delete();PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();print(json.dumps({'backup_settings_present':False}))")
    target='/shared/'+fixture.case+'/backups'
    fixture.python("import os;from pathlib import Path;p=Path("+repr(target)+");p.mkdir(exist_ok=True);os.chown(p,65532,65532);p.chmod(0o755);alias=Path('/data/backups');\nif alias.is_symlink():alias.unlink()\nelif alias.exists():\n if any(alias.iterdir()):raise RuntimeError('UnownedBackupDirectory')\n alias.rmdir()\nalias.symlink_to(p,target_is_directory=True)")


def fingerprint(fixture):
    return fixture.sql("import sys;sys.path.insert(0,'/normal-dvr-tools');from backup_fingerprint_guest import snapshot;print(json.dumps(snapshot()))")


def archive_gate(fixture,filename):
    if '/'in filename or not filename.startswith('dispatcharr-backup-')or not filename.endswith('.zip'):raise AssertionError('ActualBackupFilename')
    path=fixture.case_data/'backups'/filename
    if not path.is_file():raise AssertionError('ActualArchiveMissing')
    script="""import hashlib,json,zipfile
from pathlib import Path
p=Path(__PATH__);digest=hashlib.sha256()
with p.open('rb')as source:
 for block in iter(lambda:source.read(1048576),b''):digest.update(block)
with zipfile.ZipFile(p)as archive:
 names=archive.namelist()
 if len(names)>128:raise RuntimeError('ArchiveDirectoryBudget')
 with archive.open('metadata.json')as data:raw=data.read(65537)
 if len(raw)>65536:raise RuntimeError('ArchiveMetadataBudget')
 metadata=json.loads(raw)
 with archive.open('database.dump')as data:signature=data.read(5).decode('ascii')
print(json.dumps({'members':names,'metadata':metadata,'database_signature':signature,'archive_bytes':p.stat().st_size,'archive_sha256':digest.hexdigest()}))
""".replace('__PATH__',repr('/shared/'+fixture.case+'/backups/'+filename))
    value=json.loads(fixture.python(script))
    if sorted(value['members'])!=['database.dump','metadata.json']or value['metadata']['format']!='dispatcharr-backup'or value['metadata']['version']!=2 or value['metadata']['database_type']!='postgresql'or value['database_signature']!='PGDMP':raise AssertionError('ActualV2PostgresArchive')
    return value


def source_owned_restore(fixture,filename,expected,output):
    target='rust_probe_backup_'+uuid.uuid4().hex[:16];tag='backup-restore-'+uuid.uuid4().hex[:12]
    observer_path='/shared/'+fixture.case+'/restore-target-observer.jsonl'
    script="""import os,time,json,signal,psycopg
from pathlib import Path
running=[True];signal.signal(signal.SIGTERM,lambda *_:running.__setitem__(0,False))
c=psycopg.connect(dbname='postgres',user=os.environ['POSTGRES_USER'],password=os.environ.get('POSTGRES_PASSWORD'),host='/var/run/postgresql');c.autocommit=True;x=c.cursor();path=Path(__OUTPUT__);last=None;last_emit=0
path.with_suffix('.ready').write_text('actual PostgreSQL observer connected')
with path.open('x')as result:
 while running[0]:
  x.execute('SELECT datname,application_name FROM pg_stat_activity WHERE application_name=%s',(__TAG__,));rows=x.fetchall();now=time.monotonic()
  if rows!=last or now-last_emit>=1:
   result.write(json.dumps({'epoch':time.time(),'actual_targets':rows})+'\\n');result.flush();last=rows;last_emit=now
   if result.tell()>262144:raise RuntimeError('RestoreTargetObserverBudget')
  time.sleep(.005)
c.close()
""".replace('__OUTPUT__',repr(observer_path)).replace('__TAG__',repr(tag))
    fixture.role('backup-restore-target-observer',['/dispatcharrpy/bin/python','-c',script]);fixture.wait(lambda:(fixture.case_data/'restore-target-observer.ready').exists(),10,'ActualRestoreTargetObserver')
    # Private controlled DB/Redis identity is set BEFORE Django imports. Original
    # services, pg_restore and migrations run unchanged against only this target.
    guest="""import os,sys,json,hashlib,psycopg
from pathlib import Path
from urllib.parse import quote
from psycopg import sql
name=__TARGET__;source=__SOURCE__;assert name.startswith('rust_probe_backup_')and name!=source
control=psycopg.connect(dbname='postgres',user=os.environ['POSTGRES_USER'],password=os.environ.get('POSTGRES_PASSWORD'),host='/var/run/postgresql');control.autocommit=True;cursor=control.cursor();cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
os.environ['POSTGRES_DB']=name;os.environ['PGAPPNAME']=__TAG__
password=quote(os.environ.get('REDIS_PASSWORD',''),safe='');base='redis://:'+password+'@127.0.0.1:6379/'
os.environ.update(REDIS_URL=base+'12',REDIS_DB='12',CELERY_BROKER_URL=base+'13',CELERY_RESULT_BACKEND=base+'14')
import redis
clients=[redis.Redis.from_url(base+str(i))for i in (12,13,14)]
if any(client.dbsize()for client in clients):raise RuntimeError('UnownedRestoreRedisNamespace')
sys.path.insert(0,'/app');sys.path.insert(0,'/normal-dvr-tools');sys.argv.append('shell');os.environ['DJANGO_SETTINGS_MODULE']='dispatcharr.settings'
import django;django.setup()
from django.conf import settings
from django.db import connection,connections
from apps.backups import services
from django.core.management import call_command
from backup_fingerprint_guest import snapshot
result={'owned_target':name,'source_database_distinct':name!=source,'actual_Django_database':settings.DATABASES['default']['NAME']}
try:
 assert settings.DATABASES['default']['NAME']==name
 args=services._get_pg_args();assert args[args.index('-d')+1]==name
 result['actual_Source_pg_args_target']=args[args.index('-d')+1]
 services.restore_backup(Path(__ARCHIVE__))
 result['before_migrate']=snapshot()
 call_command('migrate',interactive=False,verbosity=0)
 result['after_migrate']=snapshot()
 effects=[]
 for db,client in zip((12,13,14),clients):
  keys=sorted(k.decode()for k in client.scan_iter(count=128))
  if len(keys)>1024:raise RuntimeError('RestoreRedisEffectBudget')
  frames=[]
  for raw in client.lrange('celery',0,127):
   value=json.loads(raw);frames.append({'task':value.get('headers',{}).get('task'),'id':value.get('headers',{}).get('id')})
  effects.append({'database':db,'key_names':keys,'task_headers':frames,'celery_length':client.llen('celery')})
 result['isolated_Redis_effects']=effects
finally:
 connections.close_all();cursor.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)));result['actual_owned_target_drop']=True;control.close()
Path(__RESULT__).write_text(json.dumps(result,sort_keys=True))
""".replace('__TARGET__',repr(target)).replace('__SOURCE__',repr(fixture.database)).replace('__TAG__',repr(tag)).replace('__ARCHIVE__',repr('/shared/'+fixture.case+'/backups/'+filename)).replace('__RESULT__',repr('/shared/'+fixture.case+'/owned-source-restore.json'))
    try:fixture.python(guest,seconds=180,secret=True)
    finally:
        # This outer fence also covers setup/import failure before the Source
        # service's inner finally. Only this freshly minted owned target is used.
        cleanup="import os,json,psycopg;from psycopg import sql;name="+repr(target)+";assert name.startswith('rust_probe_backup_');c=psycopg.connect(dbname='postgres',user=os.environ['POSTGRES_USER'],password=os.environ.get('POSTGRES_PASSWORD'),host='/var/run/postgresql');c.autocommit=True;x=c.cursor();x.execute('SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)',(name,));present=x.fetchone()[0];x.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH(FORCE)').format(sql.Identifier(name)));x.execute('SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)',(name,));assert not x.fetchone()[0];print(json.dumps({'owned_target':name,'present_before_cleanup':present,'absent_after_cleanup':True}));c.close()"
        try:
            receipt=json.loads(fixture.python(cleanup,seconds=20,secret=True));(output/'owned-restore-cleanup.json').write_text(json.dumps(receipt,indent=2)+'\n')
        finally:fixture.role('backup-restore-target-observer',[],stop=True)
    report=json.loads((fixture.case_data/'owned-source-restore.json').read_text());observed=[json.loads(line)for line in(fixture.case_data/'restore-target-observer.jsonl').read_text().splitlines()];targets={row[0]for item in observed for row in item['actual_targets']}
    report['passively_observed_libpq_subprocess_targets']=sorted(targets);report['passive_observer_scope']='PGAPPNAME-tagged psql/pg_restore only; Django target independently asserted from settings'
    if targets!={target}or report['actual_Django_database']!=target or report['actual_Source_pg_args_target']!=target or report['before_migrate']!=expected or report['after_migrate']!=expected or not report['actual_owned_target_drop']:raise AssertionError('ActualOwnedSourceRestoreMismatchOrMissingTargetProof')
    return report


def run(fixture,m6_module):
    if fixture.case!='normal-backup':raise ValueError('ExistingNormalBackupContextRequired')
    out=Path(fixture.output)/'normal-backup';out.mkdir(exist_ok=False);cells=[];state={}
    def cell(name,callback,requires=()):
        row={'name':name,'status':'running','began_epoch':time.time()};cells.append(row);m6_module.save(out/(name+'.json'),row)
        if any(key not in state for key in requires):row.update(status='not_run',reason='actual prerequisite unavailable',requires=list(requires));m6_module.save(out/(name+'.json'),row);return
        try:row.update(observation=callback(),status='passed')
        except Exception as error:
            private=out/'private';private.mkdir(mode=0o700,exist_ok=True);path=private/(name+'-controller-error.txt');path.write_text(repr(error));path.chmod(0o600);row.update(status='failed',error_type=type(error).__name__,private_error=str(path))
        finally:row['ended_epoch']=time.time();m6_module.save(out/(name+'.json'),row)
    def startup():
        data=fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask;print(json.dumps({'settings':CoreSettings.objects.filter(key='backup_settings').count(),'schedule':PeriodicTask.objects.filter(name='backup-scheduled-task').count()}))")
        if data!={'settings':1,'schedule':1}:raise AssertionError(('ActualSourceStartupDefaults',data))
        return data
    def binaries():
        values={}
        for name in ['pg_dump','pg_restore','psql']:
            argv=[fixture.docker,'exec',fixture.native,'/usr/local/bin/'+name,'--version']if fixture.backend=='Native'else fixture.compose+['exec','-T','baseline','/usr/lib/postgresql/17/bin/'+name,'--version']
            text=fixture.command(argv,20)
            if '(PostgreSQL) 17.'not in text:raise AssertionError(('ActualPG17Client',name))
            values[name]=text.strip()
        return values
    def timezone(zone):
        actual=fixture.sql("from core.models import CoreSettings;row=CoreSettings.objects.get(key='system_settings');print(json.dumps({'id':row.pk,'value':row.value}))")
        value=dict(actual['value']);value['time_zone']=zone;code,result=private_api(fixture,out,'timezone-'+zone.replace('/','-'),'PATCH','/api/core/settings/'+str(actual['id'])+'/',{'value':value})
        if code!=200:raise AssertionError('ActualCachedTimezonePATCH')
        state['zone']=zone;return {'actual_Core_PATCH':code,'zone':zone}
    def begin_monitor():
        fixture.role('normal-backup-publications',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':'/shared/'+fixture.case+'/normal-backup-publications.jsonl'})
        fixture.wait(lambda:(fixture.case_data/'normal-backup-publications.ready').exists(),10,'ActualBackupPublicationObserver')
        return {'actual_MONITOR_ready':True}
    def frames(name,after):
        path=fixture.case_data/'normal-backup-publications.jsonl'
        if path.stat().st_size>1048576:raise RuntimeError('BackupPublicationBudget')
        return [row for row in map(json.loads,path.read_text().splitlines())if row['headers']['task']==name and row['observed_epoch']>=after]
    def create(label):
        began=time.time();code,body=private_api(fixture,out,label+'-create','POST','/api/backups/create/',{})
        if code!=202:raise AssertionError(('ActualBackupHTTP202',code))
        task=body['task_id'];uuid.UUID(task);token=body['task_token'];state[label+'-task']=task
        code,_=private_api(fixture,out,label+'-invalid-token','GET','/api/backups/status/'+task+'/?token=invalid',admin=False)
        if code!=403:raise AssertionError('InvalidBackupTokenDenial')
        def finished():
            code,value=private_api(fixture,out,label+'-status','GET','/api/backups/status/'+task+'/?token='+urllib.parse.quote(token),admin=False)
            if code!=200:raise AssertionError(('ActualBackupStatusHTTP',code))
            return value if value.get('state')in('completed','failed')else None
        terminal=fixture.wait(finished,180,'ActualBackupTerminal:'+label)
        if terminal['state']!='completed':raise AssertionError('ActualBackupCaughtFailureSeePrivateStatus')
        name=terminal['result']['filename'];state[label+'-archive']=name
        if (fixture.case_data/'backups'/name).stat().st_size!=terminal['result']['size']:raise AssertionError('ActualReportedArchiveSize')
        from datetime import datetime
        from zoneinfo import ZoneInfo
        actual_local=datetime.strptime(name,'dispatcharr-backup-%Y.%m.%d.%H.%M.%S.zip').replace(tzinfo=ZoneInfo(state['zone'])).timestamp()
        if actual_local<began-2 or actual_local>time.time()+2:raise AssertionError('ActualPerJobFilenameTimezone')
        observed=fixture.wait(lambda:next((r for r in frames('apps.backups.tasks.create_backup_task',began)if r['headers']['id']==task),None),10,'ActualBackupRootPublication')
        return {'HTTP202':True,'actual_task_id':task,'actual_raw_publication':observed,'status_token_and_raw_replies_private':True,'result_filename':name,'archive':archive_gate(fixture,name)}
    def snapshot_source():
        fixture.actual_admin_token()
        fixture.sql("from core.models import CoreSettings;CoreSettings.objects.bulk_create([CoreSettings(key='backup-owned-unicode',name='Backup owned Unicode',value={'text':'ångström 日本語 🚀'})]);print(json.dumps(True))")
        state['source-snapshot']=fingerprint(fixture);return state['source-snapshot']
    def scheduled(label,expected_status):
        from datetime import datetime,timedelta
        from zoneinfo import ZoneInfo
        now=datetime.now(ZoneInfo(state['zone']))+timedelta(minutes=1)
        expression=f'{now.minute} {now.hour} * * *';began=time.time()
        code,value=private_api(fixture,out,label+'-schedule','PUT','/api/backups/schedule/update/',{'enabled':True,'cron_expression':expression,'retention_count':1})
        if code!=200:raise AssertionError(('ActualSchedulePUT',code))
        publication=fixture.wait(lambda:next(iter(frames('apps.backups.tasks.scheduled_backup_task',began)),None),90,'ActualBeatBackupPublication:'+label)
        task=publication['headers']['id']
        def finished():
            code,value=private_api(fixture,out,label+'-status','GET','/api/backups/status/'+task+'/',admin=True)
            if code!=200:raise AssertionError(('ActualScheduledStatus',code))
            return value if value.get('state')in('completed','failed')else None
        terminal=fixture.wait(finished,180,'ActualScheduledArchiveTerminal:'+label)
        if terminal['state']!=expected_status:raise AssertionError(('ActualScheduledState',terminal['state']))
        if expected_status=='completed':
            if terminal['result']['deleted_count']<1:raise AssertionError('ActualRetentionDeleteCount')
            name=terminal['result']['filename'];state['retained-UTC-archive']=name;validated=archive_gate(fixture,name)
            return {'actual_publication':publication,'status':'completed','deleted_count':terminal['result']['deleted_count'],'archive':validated}
        names=sorted(p.name for p in(fixture.case_data/'backups').glob('dispatcharr-backup-*.zip'))
        if names!=[state['retained-UTC-archive']]:raise AssertionError(('FilenameRetentionMustKeepUTCNotMtimeNewest',names))
        return {'actual_publication':publication,'actual_caught_state':'failed','actual_remaining_filename':names[0],'fresh_scheduled_file_deleted':True,'raw_error_private':True}
    try:
        cell('startup-default-row',startup)
        cell('packaged-PG-clients',binaries)
        cell('actual-timezone-UTC',lambda:timezone('UTC'))
        cell('nonempty-source-snapshot',snapshot_source)
        cell('actual-publication-observer',begin_monitor)
        cell('normal-create-status-ZIP',lambda:create('initial'))
        cell('owned-Source-restore',lambda:source_owned_restore(fixture,state['initial-archive'],state['source-snapshot'],out),('initial-archive','source-snapshot'))
        cell('actual-scheduled-UTC-retention',lambda:scheduled('UTC-scheduled','completed'),('initial-archive','zone'))
        cell('actual-timezone-Honolulu',lambda:timezone('Pacific/Honolulu'))
        cell('per-job-timezone-create',lambda:create('Honolulu'))
        cell('actual-filename-retention-self-delete',lambda:scheduled('Honolulu-scheduled','failed'),('retained-UTC-archive','Honolulu-archive','zone'))
        def disable():
            code,result=private_api(fixture,out,'schedule-disable','PUT','/api/backups/schedule/update/',{'enabled':False})
            observed=fixture.sql("from django_celery_beat.models import PeriodicTask;print(json.dumps({'remaining':PeriodicTask.objects.filter(name='backup-scheduled-task').count()}))")
            if code!=200 or result['enabled']is not False or observed['remaining']!=0:raise AssertionError('ActualBackupScheduleDelete')
            return observed
        cell('schedule-disable',disable)
    finally:
        if 'normal-backup-publications'in fixture.roles:
            try:fixture.role('normal-backup-publications',[],stop=True)
            except Exception as error:cells.append({'name':'backup-observer-stop','status':'failed','error_type':type(error).__name__})
    for missing in ['held-backup-subprocess-drain-deadline','per-file-delete-refusal-continuation','historical-production-restore']:
        m6_module.save(out/(missing+'.json'),{'name':missing,'status':'not_run','reason':'not implemented in this bounded callback; no parity or performance claim'})
    passed={row['name']for row in cells if row['status']=='passed'}
    report={'status':'passed'if all(row['status']=='passed'for row in cells)else'failed','cells':cells,'normal_HTTP_create_and_Beat_producers_actual':{'normal-create-status-ZIP','actual-scheduled-UTC-retention','actual-filename-retention-self-delete'}.issubset(passed),'Source_owned_restore_disposable_only':'owned-Source-restore'in passed,'declared_restore_scope':'fresh disposable PostgreSQL prefix target and preimport Redis12/13/14 only','Source16_replayed':False,'performance_verified':False,'full_M8_M9_passed':False,'unmeasured':['held-pgdump/table-lock pending and beyond-deadline shutdown','per-file delete-refusal continuation','Source subprocess error-text parity','production/historical restore lifecycle','per-job filename timezone comparison beyond selectedzones']}
    m6_module.save(out/'report.json',report);return report
