"""Raw psycopg3 repeatable-read full schema/rows/sequence audits; no app imports."""
import json,os,re,types
from pathlib import Path
from public_manifest import manifest
from redis_channels import inventory
NAMED11=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
def require(ok,label):
 if not ok:raise ValueError(label)
def owned(name):require(type(name)is str and re.fullmatch(r'rust_probe_rest_reads_[a-f0-9]{10}',name)is not None,'ExactOwnedRestDatabase');return name
def cache(database):
 import redis
 clients=[redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=n,socket_connect_timeout=2,socket_timeout=2)for n in range(4)]
 try:
  require(clients[0].get('rust-probe-rest-reads-owner')==owned(database).encode(),'ActualOwnedRedisContext')
  return [inventory(c,n)for n,c in enumerate(clients)]
 finally:
  for c in clients:c.close()
def sql(database):
 import psycopg
 require(database=='dispatcharr'or owned(database)==database,'DefaultReadOnlyOrOwned')
 connection=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname=database,connect_timeout=3,options='-c default_transaction_read_only=on -c statement_timeout=5000',autocommit=False)
 class Adapter:
  ops=types.SimpleNamespace(quote_name=lambda name:'"'+name.replace('"','""')+'"')
  def cursor(self):return connection.cursor()
 try:
  with connection.cursor()as c:
   c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');c.execute('SHOW transaction_read_only');require(c.fetchone()==('on',),'ActualReadOnlyOn');c.execute('SHOW transaction_isolation');require(c.fetchone()==('repeatable read',),'ActualRepeatableRead');c.execute('SELECT current_database()');require(c.fetchone()==(database,),'ActualDatabaseBinding')
  full=manifest(Adapter(),json.loads(Path('/native-inputs/schema62-authority.json').read_text()))
  return {'public62':full,'named11':{'tables':{n:full['tables'][n]for n in NAMED11},'sequences':{n:full['sequences'][n+'_id_seq']for n in NAMED11}}}
 finally:connection.close()
def main():
 require(os.environ.get('BACKUP_PHYSICAL_AUDIT')=='owned-native-only','AuditAuthority');stage=os.environ['BACKUP_PHYSICAL_AUDIT_STAGE'];database=os.environ['BACKUP_PHYSICAL_AUDIT_DATABASE'];label=os.environ['BACKUP_PHYSICAL_AUDIT_LABEL'];require(re.fullmatch('[a-z-]{1,40}',label)is not None,'FixedAuditLabel')
 if stage=='default':require(database=='dispatcharr','DefaultOnly');value=sql(database)
 elif stage=='owned':value=sql(owned(database))
 elif stage=='cache':value=cache(owned(database))
 else:raise ValueError('KnownAuditStage')
 path=Path('/evidence')/(label+'.json');require(not path.exists(),'FreshAuditPath');path.write_text(json.dumps({'passed':True,'value':value,'Django_or_Source_application_loaded':False},indent=2)+'\n');print(json.dumps({'passed':True,'output':str(path)}))
if __name__=='__main__':main()
