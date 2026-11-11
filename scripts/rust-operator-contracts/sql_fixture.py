import os,sys,json,subprocess,time
from pathlib import Path
assert os.environ.get('OPERATOR_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_operator_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from core.models import CoreSettings,SystemNotification,NETWORK_ACCESS_KEY
from apps.accounts.models import User
from django.db import DatabaseError
from django.db import connection
import psycopg
settings=dict(host='127.0.0.1',port=5432,dbname=os.environ['POSTGRES_DB'],user='dispatch',password='playback-fixture-only')
monitor=psycopg.connect(**settings,autocommit=True);actor=psycopg.connect(**settings,autocommit=False)
rows=[]
def seq():
 with connection.cursor() as c:c.execute('SELECT last_value,is_called FROM core_systemnotification_id_seq');return list(c.fetchone())
def invoke(role,command):
 arguments=[sys.executable,'/app/manage.py'] if role=='Source' else ['/evidence/operator-resource-tests']
 return subprocess.Popen(arguments+command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=dict(os.environ,DATABASE_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/'+os.environ['POSTGRES_DB'],REDIS_URL='redis://:playback-fixture-only@127.0.0.1:6379/0'))
def collect(proc):
 out,err=proc.communicate(timeout=25);return {'exit_code':proc.returncode,'stdout':out.decode(),'stderr':err.decode()}
try:
 for role in ['Source','native']:
  SystemNotification.objects.all().delete()
  builtin=SystemNotification.objects.create(notification_key='network_security_m3u_epg',source='developer',title='Owned existing',message='Owned existing')
  before=seq();result=collect(invoke(role,['reset_user_network','--user','owned-no-such-user']))
  after=seq();marker=SystemNotification.objects.create(notification_key='owned-next-notice-'+role,source='system',title='Owned next',message='Owned next')
  rows.append({'id':'sequence-existing-'+role,'role':role,'before_sequence':before,'after_command_sequence':after,'later_new_notice_id':marker.pk,'after_marker_sequence':seq(),'result':result})
  # Avoid row-state or signal setup assumptions: the actor changes values under
  # an actual row lock while the unmodified command saves its prior snapshot.
  CoreSettings.objects.filter(key='owned_network_renamed').update(key=NETWORK_ACCESS_KEY)
  setting,_=CoreSettings.objects.update_or_create(key=NETWORK_ACCESS_KEY,defaults={'name':'Owned before','value':{'before':True}})
  with actor.cursor() as c:
   c.execute('SELECT id FROM core_coresettings WHERE id=%s FOR UPDATE',[setting.pk]);assert c.fetchone()
   c.execute("UPDATE core_coresettings SET name='Owned concurrent',key='owned_network_renamed' WHERE id=%s",[setting.pk])
  proc=invoke(role,['reset_network_access']);deadline=time.monotonic()+12;waiting=None
  while time.monotonic()<deadline:
   with monitor.cursor() as c:
    c.execute("SELECT query FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE 'UPDATE%core_coresettings%' LIMIT 1")
    value=c.fetchone()
   if value:waiting=value[0][:512];break
   if proc.poll() is not None:break
   time.sleep(.02)
  if not waiting:
   actor.rollback();result=collect(proc);raise AssertionError(('physical UPDATE wait absent',role,result))
  actor.commit();result=collect(proc)
  after=list(CoreSettings.objects.filter(pk=setting.pk).values('id','key','name','value'))
  rows.append({'id':'full-save-concurrent-rename-'+role,'role':role,'physically_waiting_update':waiting,'after':after,'result':result})
  setting,_=CoreSettings.objects.update_or_create(key=NETWORK_ACCESS_KEY,defaults={'name':'Owned before delete','value':{'before':True}})
  with actor.cursor() as c:
   c.execute('SELECT id FROM core_coresettings WHERE id=%s FOR UPDATE',[setting.pk]);assert c.fetchone()
   c.execute('DELETE FROM core_coresettings WHERE id=%s',[setting.pk])
  proc=invoke(role,['reset_network_access']);deadline=time.monotonic()+12;waiting=None
  while time.monotonic()<deadline:
   with monitor.cursor() as c:
    c.execute("SELECT query FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE 'UPDATE%core_coresettings%' LIMIT 1")
    value=c.fetchone()
   if value:waiting=value[0][:512];break
   if proc.poll() is not None:break
   time.sleep(.02)
  if not waiting:
   actor.rollback();result=collect(proc);raise AssertionError(('physical delete UPDATE wait absent',role,result))
  actor.commit();result=collect(proc);after=list(CoreSettings.objects.filter(pk=setting.pk).values('id','key','name','value'))
  rows.append({'id':'full-save-concurrent-delete-'+role,'role':role,'snapshot_id':setting.pk,'physically_waiting_update':waiting,'after':after,'result':result})
  user=User.objects.create(username='rust-operator-delete-'+role,password='!',custom_properties={'allowed_networks':['192.0.2.0/24'],'keep':True})
  with actor.cursor() as c:
   c.execute('SELECT id FROM accounts_user WHERE id=%s FOR UPDATE',[user.pk]);assert c.fetchone()
   c.execute('DELETE FROM accounts_user WHERE id=%s',[user.pk])
  proc=invoke(role,['reset_user_network','--user',user.username]);deadline=time.monotonic()+12;waiting=None
  while time.monotonic()<deadline:
   with monitor.cursor() as c:
    c.execute("SELECT query FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE 'UPDATE%accounts_user%' LIMIT 1")
    value=c.fetchone()
   if value:waiting=value[0][:512];break
   if proc.poll() is not None:break
   time.sleep(.02)
  if not waiting:
   actor.rollback();result=collect(proc);raise AssertionError(('physical user UPDATE wait absent',role,result))
  actor.commit();result=collect(proc);after=list(User.objects.filter(pk=user.pk).values('id','username','custom_properties'))
  rows.append({'id':'user-update-fields-concurrent-delete-'+role,'role':role,'snapshot_id':user.pk,'physically_waiting_update':waiting,'after':after,'result':result})
 source_seq=rows[0];native_seq=rows[4];assert source_seq['before_sequence']==source_seq['after_command_sequence'];assert source_seq['later_new_notice_id']==source_seq['before_sequence'][0]+1
 assert native_seq['after_command_sequence']==native_seq['before_sequence'];assert native_seq['later_new_notice_id']==native_seq['before_sequence'][0]+1
 assert rows[1]['after'][0]['name']=='Owned before' and rows[1]['after'][0]['key']==NETWORK_ACCESS_KEY
 assert rows[5]['after'][0]['name']=='Owned before' and rows[5]['after'][0]['key']==NETWORK_ACCESS_KEY
 assert rows[2]['after']==[{'id':rows[2]['snapshot_id'],'key':NETWORK_ACCESS_KEY,'name':'Owned before delete','value':{}}] and rows[6]['after']==[{'id':rows[6]['snapshot_id'],'key':NETWORK_ACCESS_KEY,'name':'Owned before delete','value':{}}]
 assert all(rows[index]['result']['exit_code']==0 for index in [0,1,2,4,5,6])
 assert rows[3]['result']['exit_code']==rows[7]['result']['exit_code']==1
 assert all('Reset allowed_networks for ' not in rows[index]['result']['stdout'] and rows[index]['after']==[] for index in [3,7])
 assert 'User.NotUpdated: Save with update_fields did not affect any rows.' in rows[3]['result']['stderr']
 assert 'User save did not affect any rows' in rows[7]['result']['stderr']
 Path('/evidence/operator-cli-source.json').write_text(json.dumps({'passed':True,'scope':'Actual Original/native scoped SQL oracle: existing-notice sequence/next-ID and physically blocked full-save rename/delete; not wholeCLI parity','native_parity':True,'Source_user_zero_row_exception':{'qualified_name':User.NotUpdated.__module__+'.'+User.NotUpdated.__qualname__,'is_DatabaseError_subclass':issubclass(User.NotUpdated,DatabaseError)},'cases':rows,'sequence_gap_closed':True,'snapshot_gap_closed':True,'concurrent_delete_resurrection_gap_closed':True,'user_update_fields_zero_row_gap_closed':True},indent=2)+'\n')
finally:
 actor.rollback();actor.close();monitor.close()
