"""Fixed Source26 real production Main comparison; no testpeer or info hook."""
import hashlib,json,math,os,time
from pathlib import Path
from urllib.parse import urlencode
from absolute_http import Observation
from definition import cases,query

ACTIONS={'get_vod_categories','get_vod_streams','get_series_categories','get_series'}
NAMES=[r+':'+a for r in ['admin','standard','basic','hide_adult','disabled']for a in ['get_vod_categories','get_vod_streams','get_series_categories','get_series']]+['movie-filter-present','movie-filter-absent','series-filter-present','series-filter-absent','invalid-password','missing-password']
def reference(value):
 if type(value)is not dict or type(value.get('cases'))is not list or [r.get('name')for r in value['cases']]!=NAMES:raise ValueError('ExactSource26Names')
 if len({r['name']for r in value['cases']})!=26:raise ValueError('UniqueSource26')
 for row in value['cases']:
  if row['action']not in ACTIONS or type(row['status'])is not int or row['status']not in[200,401]:raise ValueError('FixedRetainedCaseStatus')
  raw=bytes.fromhex(row['body_hex'])
  if len(raw)>1048576 or hashlib.sha256(raw).hexdigest()!=row['body_sha256']:raise ValueError('RetainedBodyBinding')
 if [r['status']for r in value['cases']].count(200)!=24 or [r['status']for r in value['cases']].count(401)!=2:raise ValueError('ExactRetainedStatusCounts')
 return value
def write(path,value):
 raw=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode()
 if len(raw)>8388608:raise ValueError('BoundedPrivateEvidence')
 path.write_bytes(raw);path.chmod(0o600)
def observation_valid(row):
 try:
  begin=row['began_monotonic'];end=row['completed_monotonic'];observed=row['absolute_observation'];closed=row['physical_close']
  clocks=[begin,end,observed['completed'],observed['deadline']]
  if any(type(n)not in[int,float]or not math.isfinite(n)for n in clocks):return False
  if not(0<=begin<=observed['completed']<=observed['deadline']<=begin+8 and observed['completed']<=end):return False
  if observed.get('timer_joined')is not True or closed.get('timer_joined')is not True:return False
  if type(closed.get('fd_after'))is not int or closed['fd_after']!=-1 or closed.get('original_inode_absent')is not True:return False
  if type(row.get('status'))is not int or not 100<=row['status']<=599:return False
  body=bytes.fromhex(row['body_hex'])
  headers=row['headers']
  return len(body)<=1048576 and hashlib.sha256(body).hexdigest()==row['body_sha256']and type(headers)is list and 0<len(headers)<=128 and all(type(pair)in[list,tuple]and len(pair)==2 and all(type(text)is str and len(text)<=65536 for text in pair)and pair[0]for pair in headers)
 except (KeyError,TypeError,ValueError):return False

OWN11=['accounts_user','m3u_m3uaccount','vod_vodcategory','vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation','core_outputprofile','core_coresettings']
def named(value):return {'tables':{k:value['tables'][k]for k in OWN11},'sequences':{k+'_id_seq':value['sequences'][k+'_id_seq']for k in OWN11}}
def raw_redis():
 import redis
 used=0;result=[]
 for database in range(4):
  client=redis.Redis(host='127.0.0.1',port=6382,password='playback-fixture-only',db=database,socket_connect_timeout=2,socket_timeout=2);rows=[];cursor=0
  try:
   for _ in range(128):
    cursor,keys=client.scan(cursor=cursor,count=100)
    for key in keys:
     if len(key)>1024 or len(rows)>=1000:raise ValueError('RawRedisKeyCapacity')
     dump=client.dump(key)
     if dump is None:continue
     if len(dump)>1048576:raise ValueError('RawRedisDumpCapacity')
     kind=client.type(key).decode();row={'key_hex':key.hex(),'type':kind,'dump_hex':dump.hex(),'pttl':client.pttl(key)}
     if kind=='string':
      if client.strlen(key)>1048576:raise ValueError('RawRedisGETCapacity')
      raw=client.get(key);row['value_hex']=None if raw is None else raw.hex()
     elif kind=='list':
      size=client.llen(key)
      if size>128:raise ValueError('RawRedisListCapacity')
      row['list_hex']=[v.hex()for v in client.lrange(key,0,-1)]
     used+=len(json.dumps(row).encode())
     if used>8388608:raise ValueError('AllRawRedis8MiB')
     rows.append(row)
    if cursor==0:break
   else:raise ValueError('RawRedisScanCapacity')
  finally:client.close()
  result.append({'database':database,'rows':sorted(rows,key=lambda r:r['key_hex'])})
 return result

def main():
 import fixture_db as DB
 DB.setup('audit')
 root=Path('/capture');out=Path('/evidence')
 binding=json.loads((root/'reference-inputs.json').read_text())
 for name,expected in binding['private_inputs'].items():
  if hashlib.sha256((root/name).read_bytes()).hexdigest()!=expected:raise ValueError('ReferenceInputBinding')
 expected=reference(json.loads((root/'retained-source26.json').read_text()))
 seed=json.loads((root/'retained-seed-private.json').read_text())
 declared=[case for case in cases()if case['action']in ACTIONS]
 if [case['name']for case in declared]!=NAMES:raise ValueError('DefinitionSubsetBeforeRequests')
 import socket
 address='127.0.0.1';socket.inet_pton(socket.AF_INET,address)
 started=time.monotonic();deadline=started+90;rows=[]
 try:
  for case,golden in zip(declared,expected['cases'],strict=True):
   remaining=min(8,deadline-time.monotonic())
   if remaining<=0:raise TimeoutError('FixedNative90')
   before=DB.fingerprint();remaining=min(8,deadline-time.monotonic())
   if remaining<=0:raise TimeoutError('FixedNative90BeforeSend')
   owner=Observation('http://127.0.0.1:9192',remaining,address=address)
   row={'name':case['name'],'role':case['role'],'action':case['action'],'began_monotonic':time.monotonic(),'before':before,'Main_destination':'127.0.0.1:9192 shared baseline network namespace','actual_Host':'127.0.0.1:5657','before_named11':named(before),'before_raw_Redis':raw_redis()}
   try:
    args=query(case,seed);path='/player_api.php?'+urlencode(args);row['request_query_private']=dict(args);row['request_target_sha256']=hashlib.sha256(path.encode()).hexdigest();row['method']='GET'
    response=owner.request(path,{'User-Agent':'owned-real-Main-Source26-compare','Host':'127.0.0.1:5657'},method='GET')
    raw=response.raw.read(1048577)
    if len(raw)>1048576:raise ValueError('BodyCapacity')
    headers=list(response.raw.getheaders());row.update(status=response.status_code,headers=headers,body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),absolute_observation=owner.complete())
    content_type=[v for k,v in headers if k.casefold()=='content-type']
    expected_type=[v for k,v in golden['headers']if k.casefold()=='content-type']
    row['parity']={'status':response.status_code==golden['status'],'exact_body':raw.hex()==golden['body_hex'],'content_type':content_type==expected_type}
   except Exception as error:row.update(error_type=type(error).__name__,error_message=str(error)[:256])
   finally:
    try:row['physical_close']=owner.cancel()
    except BaseException as error:row['cleanup_error']=type(error).__name__
    row['completed_monotonic']=time.monotonic()
    try:row['after']=DB.fingerprint();row['after_named11']=named(row['after']);row['after_raw_Redis']=raw_redis()
    except BaseException as error:row['SQL_after_error']=type(error).__name__
    rows.append(row);write(out/'Main26-partial-private.json',{'cases':rows,'fixed_names':NAMES})
   if row.get('cleanup_error')or row.get('SQL_after_error'):raise RuntimeError('IncompletePhysicalOrSQLCapture')
  valid=len(rows)==26 and time.monotonic()<=deadline and all(observation_valid(r)for r in rows)
  # The captured close schema is checked by the host independent validator too.
  nonmutation=all(r['before']==r['after']for r in rows)
  parity=all(set(r.get('parity',{}))=={'status','exact_body','content_type'}and all(r['parity'].values())for r in rows)
  report={'scope':'Real release Main Source26 catalogue body/status/CT slice; exact retained seed and Host context, not cfgtest/info/fullM7','phase_started':started,'phase_completed':time.monotonic(),'fixed_names':NAMES,'cases':rows,'capture_complete':len(rows)==26,'observer_valid':valid,'owned_SQL_unchanged':nonmutation,'body_status_content_type_parity':parity,'full_M7_or_performance':False,'actual_production_Main':True,'IANA_or_request_started_cache_parity':False,'info_actions_or_fixed_clock_hook':False,'declared_configured_XC_max_rows':50000,'actual_destination_IP':address,'actual_Host_authority':'127.0.0.1:5657'}
  write(out/'Main26-private.json',report)
  print(json.dumps({'cases':len(rows),'capture_complete':report['capture_complete'],'observer_valid':valid,'SQL_unchanged':nonmutation,'parity':parity}))
  return 0 if valid and nonmutation and parity else 1
 finally:DB.connections.close_all()
if __name__=='__main__':raise SystemExit(main())
