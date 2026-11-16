"""Additive backup physical controls inside the existing milestone controller.
Original Dispatcharr/dependency authors retain all application/protocol credit.
No apps, workers, pools or fixture scheduler are created by this callback.
"""
import json,time,uuid
from pathlib import Path


def prepare_context(fixture):
    if fixture.case not in ('backup-held-graceful','backup-held-deadline','backup-delete-refusal'):return
    fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;from apps.backups.scheduler import DEFAULTS;row,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':dict(DEFAULTS)});value=dict(row.value);value['schedule_enabled']=False;row.value=value;row.save(update_fields=['value']);PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();print(json.dumps({'declared_fixture_schedule_enabled':False}))")
    script="""import os,json
from pathlib import Path
p=Path(__ROOT__);p.mkdir(exist_ok=True);os.chown(p,65532,65532);p.chmod(0o1777)
alias=Path('/data/backups')
if alias.is_symlink():alias.unlink()
elif alias.exists():
 if any(alias.iterdir()):raise RuntimeError('UnownedBackupDirectory')
 alias.rmdir()
alias.symlink_to(p,target_is_directory=True)
print(json.dumps({'declared_owned_backup_mode':oct(p.stat().st_mode&0o7777),'actual_setup_euid':os.geteuid()}))
""".replace('__ROOT__',repr('/shared/'+fixture.case+'/backups'))
    fixture.python(script)


def _result_scope(fixture):
    prior=getattr(fixture,'_backup_physical_result_scope',None)
    stopped=not fixture.native if fixture.backend=='Native'else 'celery'not in fixture.roles
    if stopped and prior is not None and prior[0][:2]==(fixture.case,fixture.database):return prior[1]
    identity=(fixture.case,fixture.database,fixture.native if fixture.backend=='Native'else fixture.roles['celery']['pid'])
    cached=getattr(fixture,'_backup_physical_result_scope',None)
    if cached is not None and cached[0]==identity:return cached[1]
    # Bind the actual running worker's result URL, never the management guest's
    # unrelated defaults. Credentials remain private and are never returned.
    if fixture.backend=='Native':
        inspected=json.loads(fixture.command([fixture.docker,'inspect',fixture.native],secret=True))[0]
        environment=dict(item.split('=',1)for item in inspected['Config']['Env'])
        configured=environment['CELERY_RESULT_BACKEND']
        binding="os.environ['CELERY_RESULT_BACKEND']="+repr(configured)
    else:
        worker=fixture.roles['celery'];pid=int(worker['pid']);start=worker['starttime']
        # Read the exact private config used by the retained role owner. A
        # UID0 guest without SYS_PTRACE cannot assume it may read UID65532's
        # /proc/environ. The real owner status binds this launch config to the
        # same running child/owner generations and exact argv/socket.
        binding="record="+repr(worker)+";config_path=Path(record['socket']).with_suffix('.json');assert config_path.stat().st_uid==0 and config_path.stat().st_mode&0o777==0o600;config=json.loads(config_path.read_text());assert config['name']=='celery' and config['socket']==record['socket'] and config['args']==record['actual_argv'];sys.path.insert(0,'/normal-dvr-tools');from role_owner import rpc;actual=rpc(record,'status');assert actual['running'] and all(actual[k]==record[k]for k in ['pid','starttime','owner_pid','owner_starttime','socket','actual_argv']);os.environ['CELERY_RESULT_BACKEND']=config['environment']['CELERY_RESULT_BACKEND']"
    probe="import os,sys,json;from pathlib import Path;"+binding+";sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');from dispatcharr.celery import app;backend=app.backend;marker='backup-scope-probe';key=backend.get_key_for_task(marker).decode();assert key.endswith(marker);print(json.dumps({'database':int(backend.client.connection_pool.connection_kwargs['db']),'actual_task_key_prefix':key[:-len(marker)],'authority':'actual Source backend.get_key_for_task with running worker result URL'}))"
    scope=json.loads(fixture.python(probe,secret=True))
    if scope['database']!=2 or scope['actual_task_key_prefix']!='celery-tasks:celery-task-meta-':raise AssertionError('ActualConfiguredBackupResultScopeChanged')
    fixture._backup_physical_result_scope=(identity,scope)
    return scope


def _terminal(fixture,task):
    scope=_result_scope(fixture)
    text=fixture.redis(scope['database'],"return redis.call('GET',ARGV[1])or false",scope['actual_task_key_prefix']+task)
    if not text:return None
    result=json.loads(text)
    return result if result.get('status')in ('SUCCESS','FAILURE','REVOKED')else None


def _blocked_dump(fixture):
    return fixture.sql("with connection.cursor()as c:\n c.execute(\"SELECT pid,application_name,wait_event_type,wait_event FROM pg_stat_activity WHERE datname=current_database() AND application_name='pg_dump' AND wait_event_type='Lock'\");print(json.dumps(c.fetchall()))")


def _lock(fixture):
    ready='/shared/'+fixture.case+'/backup-lock.ready'
    script="""import os,signal,time,json,psycopg
from pathlib import Path
running=[True];signal.signal(signal.SIGTERM,lambda *_:running.__setitem__(0,False))
c=psycopg.connect(dbname=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],host='/var/run/postgresql')
x=c.cursor();x.execute('LOCK TABLE dispatcharr_channels_recording IN ACCESS EXCLUSIVE MODE')
Path(__READY__).write_text(json.dumps({'database':os.environ['POSTGRES_DB'],'pid':os.getpid(),'actual_lock_acquired':True}))
while running[0]:time.sleep(.05)
c.rollback();c.close()
""".replace('__READY__',repr(ready))
    fixture.role('backup-table-lock',['/dispatcharrpy/bin/python','-c',script])
    fixture.wait(lambda:(fixture.case_data/'backup-lock.ready').exists(),10,'actual-backup-lock-held')
    receipt=json.loads((fixture.case_data/'backup-lock.ready').read_text())
    if receipt['database']!=fixture.database:raise AssertionError('OwnedDatabaseLockRequired')
    return receipt


def _children(fixture):
    if fixture.backend=='Native':
        text=fixture.command([fixture.docker,'top',fixture.native,'-eo','pid,args'])
        rows=[]
        for line in text.splitlines()[1:]:
            fields=line.split(None,1)
            if len(fields)==2 and any(Path(word).name=='pg_dump'for word in fields[1].split()[:4]):
                pid=int(fields[0]);rows.append({'host_pid':pid,'starttime':fixture.pid_start(pid),'actual_executable':'pg_dump'})
        return rows
    return json.loads(fixture.python("import json,os;from pathlib import Path;rows=[]\nfor path in Path('/proc').glob('[0-9]*/cmdline'):\n try:\n  args=path.read_bytes().split(b'\\0');executable=Path(args[0].decode()).name if args[0]else '';stat=(path.parent/'stat').read_text().rsplit(')',1)[1].split();status=(path.parent/'status').read_text()\n except(FileNotFoundError,PermissionError,ProcessLookupError):continue\n if executable=='pg_dump':rows.append({'namespace_pid':int(path.parent.name),'starttime':stat[19],'actual_executable':executable,'actual_uid_line':next(x for x in status.splitlines()if x.startswith('Uid:'))})\nprint(json.dumps(rows))"))


def _same_child_alive(fixture,child):
    if 'host_pid'in child:
        try:return fixture.pid_start(child['host_pid'])==child['starttime']
        except(FileNotFoundError,ProcessLookupError):return False
    script="from pathlib import Path;p=Path('/proc')/"+repr(str(child['namespace_pid']))+"/'stat';print(p.read_text().rsplit(')',1)[1].split()[19]if p.exists()else'')"
    return fixture.python(script).strip()==child['starttime']


def run_held(fixture,m6_module,backup_module):
    """Two distinct contexts; same packaged Main/backend, no signal monkeypatch."""
    deadline=fixture.case=='backup-held-deadline'
    if fixture.case not in ('backup-held-graceful','backup-held-deadline'):raise ValueError('OwnedHeldBackupContextRequired')
    if deadline and fixture.backend!='Native':
        return {'status':'not_run','reason':'Source warm-stop characterized separately; Native configured deadline is not a Source contract'}
    out=Path(fixture.output)/fixture.case;out.mkdir(exist_ok=False);report={'status':'running','backend':fixture.backend,'configured_Native_shutdown_seconds':45,'expected_fail_closed':deadline,'Source_authority_new':fixture.backend=='Original'};m6_module.save(out/'report.json',report)
    children=[];task=None
    try:
        report['actual_result_scope']=_result_scope(fixture)
        report['actual_lock']=_lock(fixture)
        code,body=backup_module.private_api(fixture,out,'held-create','POST','/api/backups/create/',{})
        if code!=202:raise AssertionError(('ActualHeldCreate',code))
        task=body['task_id'];uuid.UUID(task);report['actual_task_id']=task
        report['actual_blocked_pg_dump_SQL']=fixture.wait(lambda:_blocked_dump(fixture),20,'actual-pg-dump-lock-wait')
        children=fixture.wait(lambda:_children(fixture),10,'actual-owned-pg-dump-child')
        report['actual_children_before_TERM']=children
        pending=_terminal(fixture,task)
        if pending is not None:raise AssertionError('BlockedDumpCannotAlreadyBeTerminal')
        report['actual_pending_before_TERM']=True
        if fixture.backend=='Native':
            inspect=json.loads(fixture.command([fixture.docker,'inspect',fixture.native]))[0]
            configured=dict(item.split('=',1)for item in inspect['Config']['Env'])
            if configured.get('RUST_SHUTDOWN_SECONDS')!='45':raise AssertionError('DeclaredNativeShutdownDeadlineMismatch')
            if not deadline:
                # Release only after the actual container stop signal was sent,
                # independently observed via its StopSignal and stopping state is
                # not inferable; use explicit settled docker kill --signal TERM.
                fixture.command([fixture.docker,'kill','--signal','TERM',fixture.native],10)
                report['actual_SIGTERM_command_settled']=True
                fixture.sleep(.3)
                if not _blocked_dump(fixture)or not all(_same_child_alive(fixture,c)for c in children):raise AssertionError('PhysicalChildMustStillBeHeldAfterTERM')
                fixture.role('backup-table-lock',[],stop=True)
                name=fixture.native
                fixture.command([fixture.docker,'wait',name],55)
                info=json.loads(fixture.command([fixture.docker,'inspect',name]))[0]
                logs=fixture.command([fixture.docker,'logs',name],secret=True,allow=True)
                private=out/'private';private.mkdir(mode=0o700,exist_ok=True);log=private/'Main-stderr-stdout.txt';log.write_text(logs);log.chmod(0o600)
                result={'actual_exit':info['State']['ExitCode'],'oom_killed':info['State']['OOMKilled'],'actual_started_at':info['State']['StartedAt'],'actual_finished_at':info['State']['FinishedAt'],'explicit_SIGTERM_only_once':True}
                m6_module.save(out/'actual-Main-exit.json',result)
                fixture.command([fixture.docker,'rm',name]);fixture.native=None
                if result['actual_exit']!=0:raise AssertionError('ActualGracefulMainExit0Required')
            else:
                result=fixture.stop_native(expected=1,seconds=55)
                report['held_through_actual_process_exit']=True
            report['actual_Main_stop']=result
            if result['oom_killed']or result['actual_exit']in (137,143):raise AssertionError('NoForcedOrOOMExitAccepted')
            report['actual_child_generations_absent']=all(not _same_child_alive(fixture,c)for c in children)
            if not report['actual_child_generations_absent']:raise AssertionError('ActualDumpChildStillAlive')
        else:
            record=fixture.roles['celery']
            begin=json.loads(fixture.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import begin_stop;print(json.dumps(begin_stop("+repr(record)+")))"))
            if not begin['actual_master_SIGTERM_sent']or not begin['signal_settled']:raise AssertionError('ActualCeleryMasterTERMRequired')
            report['actual_Source_begin_stop']=begin
            fixture.sleep(.3)
            if not _blocked_dump(fixture)or not all(_same_child_alive(fixture,c)for c in children):raise AssertionError('SourceWarmStopMustRetainActualChild')
            fixture.role('backup-table-lock',[],stop=True)
            report['actual_Source_stop']=fixture.role('celery',[],stop=True)
            report['actual_child_generations_absent']=all(not _same_child_alive(fixture,c)for c in children)
            if not report['actual_child_generations_absent']:raise AssertionError('ActualSourceDumpChildStillAlive')
        if not deadline:
            terminal=fixture.wait(lambda:_terminal(fixture,task),20,'actual-backup-terminal-after-drain')
            report['actual_backend']=terminal
            if terminal['status']!='SUCCESS'or terminal['result'].get('status')!='completed':raise AssertionError('ActualDrainedArchiveSuccessRequired')
            report['actual_archive']=backup_module.archive_gate(fixture,terminal['result']['filename'])
        else:
            report['actual_backend_after_failed_drain']=_terminal(fixture,task)
            if report['actual_backend_after_failed_drain']is not None:raise AssertionError('BlockedUnknownTaskCannotClaimCompletion')
        report['status']='passed'
    except Exception as error:
        private=out/'private';private.mkdir(mode=0o700,exist_ok=True);p=private/'controller-error.txt';p.write_text(repr(error));p.chmod(0o600);report.update(status='failed',error_type=type(error).__name__,private_error=str(p))
    finally:
        if 'backup-table-lock'in fixture.roles:
            try:report['actual_lock_release']=fixture.role('backup-table-lock',[],stop=True)
            except Exception as error:report.update(status='failed',cleanup_error=type(error).__name__)
        report['full_milestone_passed']=False;report['performance_verified']=False;m6_module.save(out/'report.json',report)
    return report


def run_delete_refusal(fixture,m6_module,backup_module):
    """Real retention catches one selected non-file and continues afterward.
    Directory refusal is meaningful for both root and UID65532; chmod alone is
    deliberately not used as proof that a root process cannot unlink a file.
    """
    if fixture.case!='backup-delete-refusal':raise ValueError('OwnedRefusalContextRequired')
    out=Path(fixture.output)/fixture.case;out.mkdir(exist_ok=False);report={'status':'running','Source_authority_new':fixture.backend=='Original'}
    try:
        script="""import os,json
from pathlib import Path
root=Path(__ROOT__);names=['dispatcharr-backup-2000.03.01.00.00.00.zip','dispatcharr-backup-2000.02.01.00.00.00.zip','dispatcharr-backup-2000.01.01.00.00.00.zip']
for name in (names[0],names[2]):
 p=root/name;p.write_bytes(b'owned old archive placeholder; retention only stats/unlinks');os.chown(p,65532,65532);p.chmod(0o444)
refused=root/names[1];refused.mkdir();os.chown(refused,65532,65532);refused.chmod(0o555)
print(json.dumps({'ordered_old_entries':names,'refusal_kind':'directory rejected by actual is_file guard, independent of root DAC bypass','directory_mode':oct(root.stat().st_mode&0o7777),'entry_modes':[oct((root/n).stat().st_mode&0o7777)for n in names]}))
""".replace('__ROOT__',repr('/shared/'+fixture.case+'/backups'))
        report['actual_result_scope']=_result_scope(fixture)
        report['declared_owned_entries']=json.loads(fixture.python(script))
        from datetime import datetime,timedelta
        # Actual Core timezone API, not an injected clock or task frame.
        settings=fixture.sql("from core.models import CoreSettings;r=CoreSettings.objects.get(key='system_settings');print(json.dumps({'id':r.pk,'value':r.value}))")
        value=dict(settings['value']);value['time_zone']='UTC'
        code,_=backup_module.private_api(fixture,out,'UTC-policy','PATCH','/api/core/settings/'+str(settings['id'])+'/',{'value':value})
        if code!=200:raise AssertionError('ActualUTCPolicyRequired')
        now=datetime.utcnow()+timedelta(minutes=1);expression=f'{now.minute} {now.hour} * * *'
        code,result=backup_module.private_api(fixture,out,'schedule','PUT','/api/backups/schedule/update/',{'enabled':True,'cron_expression':expression,'retention_count':1})
        if code!=200:raise AssertionError('ActualScheduleRequired')
        def done():
            names={p.name for p in (fixture.case_data/'backups').glob('dispatcharr-backup-*.zip')}
            old=report['declared_owned_entries']['ordered_old_entries']
            return names if old[0]not in names and old[2]not in names and old[1]in names and len(names)==2 else None
        remaining=fixture.wait(done,110,'actual-retention-continued-after-refused-middle-entry')
        # Independently read actual backend, not merely filesystem outcome.
        def actual_result():
            script="local out={};local cursor='0';local seen=0;repeat local page=redis.call('SCAN',cursor,'MATCH',ARGV[1],'COUNT',128);cursor=page[1];for _,k in ipairs(page[2])do seen=seen+1;if seen>1024 then return redis.error_reply('OwnedBackendBudget')end;local raw=redis.call('GET',k);if string.len(raw)>262144 then return redis.error_reply('OwnedResultBudget')end;local r=cjson.decode(raw);if r.status=='SUCCESS'and type(r.result)=='table'and r.result.deleted_count==2 then table.insert(out,r)end end until cursor=='0';return cjson.encode(out)"
            scope=_result_scope(fixture);raw=fixture.redis(scope['database'],script,scope['actual_task_key_prefix']+'*');rows=json.loads(raw);return rows if len(rows)==1 else None
        backend=fixture.wait(actual_result,20,'actual-retention-backend-deleted-two')
        filename=backend[0]['result']['filename']
        report.update(actual_backend=backend,actual_remaining_entries=sorted(remaining),actual_archive=backup_module.archive_gate(fixture,filename),status='passed',root_DAC_denial_claimed=False)
    except Exception as error:
        private=out/'private';private.mkdir(mode=0o700,exist_ok=True);p=private/'controller-error.txt';p.write_text(repr(error));p.chmod(0o600);report.update(status='failed',error_type=type(error).__name__,private_error=str(p))
    finally:
        try:
            report['actual_disable_HTTP_status']=backup_module.private_api(fixture,out,'schedule-disable','PUT','/api/backups/schedule/update/',{'enabled':False})[0]
            if report['actual_disable_HTTP_status']!=200:report['status']='failed'
        except Exception as error:report.update(status='failed',disable_error=type(error).__name__)
        report['full_milestone_passed']=False;report['performance_verified']=False;m6_module.save(out/'report.json',report)
    return report
