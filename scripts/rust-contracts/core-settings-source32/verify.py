#!/usr/bin/env python3
"""Portable projected Source32 proof. Never imports application code or falls back to private data."""
import gzip,hashlib,io,json,math,re,sys,datetime
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'validation'))
from projected_effects import validate_capture

def need(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(name):return json.loads((ROOT/name).read_bytes())
def clock(s):
 need(type(s)is str and len(s)<=64,'TypedUTCClock');d=datetime.datetime.fromisoformat(s);need(d.tzinfo is not None,'AwareClock');return d.timestamp()
def observations():
 ref=load('reference.json');manifest=load('projection-manifest.json');raw=(ROOT/'observations.json.gz').read_bytes();need(len(raw)<=4194304 and hashlib.sha256(raw).hexdigest()==manifest['compressed']['sha256']and len(raw)==manifest['compressed']['bytes'],'ExactBoundedCompressedProjection')
 with gzip.GzipFile(fileobj=io.BytesIO(raw))as f:blob=f.read(33554433)
 need(len(blob)<=33554432 and len(blob)==manifest['projection']['bytes']and hashlib.sha256(blob).hexdigest()==manifest['projection']['sha256'],'ExactBoundedProjectionBytes');need(ref['original_raw_sha256']==manifest['original_raw']['sha256'],'OriginalPrivateRawBinding');return json.loads(blob)
def coverage(d):
 authority=load('coverage-authority.json');need(authority['source_raw_sha256']==load('reference.json')['original_raw_sha256'],'CoverageRawBinding');found={}
 def walk(x,path='$'):
  if isinstance(x,list):
   for i,v in enumerate(x):walk(v,path+'['+str(i)+']')
  elif isinstance(x,dict):
   if {'audit','public_manifest','raw_Redis','Channels','settings','users'}<=set(x):
    need(x['audit']['database']['sha256']==d['database']['sha256']and x['audit']['database']['bytes']==d['database']['bytes'],'ExactOwnedDatabaseEvidence')
    found[path]={'users':[u['id']for u in x['users']],'raw_Redis':[{'database':r['database'],'row_keys':[v['key_sha256']for v in r['rows']],'record_hashes':[v['original_record']['sha256']for v in r['rows']]}for r in x['raw_Redis']],'Channels_frame_hashes':[f['raw']['sha256']for f in x['Channels']['frames']]}
    for r in x['raw_Redis']:
     need(r['complete']is True and r['destructive_commands_or_consumption']is False,'CompleteNonDestructiveRedisInventory')
     for v in r['rows']:
      need(v['type']in ['string','list','set','hash','zset','stream']and type(v['pttl'])is int and v['pttl']>=-1 and re.fullmatch('[0-9a-f]{64}',v['key_sha256'])is not None and re.fullmatch('[0-9a-f]{64}',v['original_record']['sha256'])is not None,'TypedRawRedisEvidence')
      if 'key_literal_UTF8'in v:need(hashlib.sha256(v['key_literal_UTF8'].encode()).hexdigest()==v['key_sha256'],'LiteralSafeKeyBoundToOriginalBytes')
    need(x['events']==[],'LiteralEmptySystemEventCoverage')
    need(x['Channels']['receive_pop_or_ack_calls']==0 and x['Channels']['unknown_or_setup_frames_consumed']is False,'NonConsumingChannelsCoverage')
   for k,v in x.items():walk(v,path+'.'+k)
 walk(d);need(len(found)==authority['snapshot_count']==69 and found==authority['snapshots'],'ExactActual69SnapshotCoverage')
def validate_observations(d):
 ref=load('reference.json');planpath=ROOT/'executed-executor/definition/plan.json';need(sha(planpath)==ref['plan_sha256'],'Exact32Definition');plan=json.loads(planpath.read_bytes());need(sha(ROOT/'executed-executor/seal.json')==ref['Source_executor_seal_sha256']=='a56abb23005deed4791f08cc986bb175098ca40dc699f156abb93f4927f6c5d8','ExactExecutedSource32Seal');need(ref['schema']=='core-settings-Source32-projection-v1'and ref['Original_revision']==plan['Original_git']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and ref['projected_observations_executed']is False and ref['Source_controller_exit']==0 and ref['raw_recorder_passed']is False,'ExactFrameworkScope')
 need(d['Original_git']==ref['Original_revision']and d['stage']=='exercise'and d['startup']['default_graph_or_web_startup_acceptance']is False,'ExactActualSourceAndScope');need(d['receiver_fixture_context']['registered']is True and d['receiver_fixture_context']['group']=='updates'and d['receiver_fixture_context']['Redis_database']==1 and d['receiver_fixture_context']['non_consuming']is True and d['receiver_fixture_context']['Source_methods_replaced']is False and d['receiver_cleanup']=={'own_membership_discarded':True,'frames_consumed':0,'mailbox_deleted_by_fixture':False},'LiteralOwnedNonConsumingReceiverScope');need(d['passed']is False and d['recorder_is_capture_only']is True and d['connections_close_called']is True and d['phase']=='complete'and d['startup']['should_skip_initialization']is True,'CaptureOnlyFalsePreserved');coverage(d)
 life=load('lifecycle.json');begin=clock(life['source_process_result']['began_utc']);end=clock(life['source_process_result']['completed_utc']);previous=begin
 for r in d['requests']:
  a=clock(r['began_utc']);b=clock(r['completed_utc']);need(previous<=a<=b<=end and r['actual_request_invoked']is True,'ActualRequestClockWindow');previous=b
  expected={}if r['role']=='anonymous'else{'HTTP_X_API_KEY':{'projection':'authentication header byte evidence',**ref['role_key_byte_evidence'][r['role']]}}
  need(r['raw_headers_private']==expected,'ExactRoleHeaderEvidence')
 result=validate_capture(d,plan,ROOT/'executed-executor/definition');need(result['qualified_requests']==32,'All32Qualified');return result

def validate_lifecycle():
 life=load('lifecycle.json');need(life['actual_controller_exit']==0 and life['actual_controller_passed']is True and life['raw_recorder_passed']is False and life['recorder_is_capture_only']is True and life['Root_review_status']is True,'SeparateActualControllerAndRecorder')
 commands=life['commands'];need(len(commands)==20 and len({r['name']for r in commands})==20,'Complete20ActualCommands')
 for r in commands+[life['source_process_result']]:
  need(r['exit']==0 and r['physically_settled']is True and r['pipe_threads_settled']is True and r['collection_invalid']is None and type(r['pid'])is int and r['pid']>1 and type(r['starttime'])is str and r['starttime'].isdigit()and clock(r['began_utc'])<=clock(r['completed_utc']),'ActualCommandPhysicalSettlement')
 need(life['source_process_result']in[{k:r[k]for k in life['source_process_result']}for r in commands],'SourceCommandActualIdentity')
 need(life['cleanup_within90']is True and type(life['cleanup_elapsed_seconds'])in[int,float]and math.isfinite(life['cleanup_elapsed_seconds'])and 0<=life['cleanup_elapsed_seconds']<=90 and life['owned_cleanup_independently_verified']is True,'ActualCleanup90')
 need(life['owned_backends_after_Source']==[]and life['project_inventory']=={'container':[],'network':[],'volume':[]}and life['exact_container_inventory']['raw'].strip()=='','ExactSQLAndProjectEmpty')
 need(len(life['PID_absence'])==8 and all(r['original_process_absent']is True and(r['after_starttime']is None or r['after_starttime']!=r['starttime'])for r in life['PID_absence']),'EightActualOriginalProcessesAbsent')
 need(len(life['independent_Docker_queries'])==4 and all(r['actual_exit']==0 and r['stdout']==r['stderr']==''for r in life['independent_Docker_queries']),'FourFreshIndependentEmptyQueries')
 default=load('default-audits.json');need(default['raw_files_exact_equal']is True and default['before']==default['after']and default['original_before_sha256']==default['original_after_sha256']and life['default_named11_full62tables59sequences_unchanged']is True,'LiteralDefaultAuditEquality');audit=default['before'];schema=load('executed-executor/inputs/schema62-authority.json');need(len(audit['named11']['tables'])==len(audit['named11']['sequences'])==11 and sorted(audit['public62']['tables'])==schema['tables']and sorted(audit['public62']['sequences'])==schema['sequences'],'MandatoryDefault11And62_59Coverage')
 return {'actual_controller_exit':0,'commands':20,'service_processes':8,'cleanup_seconds':life['cleanup_elapsed_seconds'],'default_tables':62,'default_sequences':59}
def verify_inventory():
 d=load('packet-inventory.json');actual={str(p.relative_to(ROOT)):sha(p)for p in ROOT.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-inventory.json'};need(actual==d,'ExactPublicPayloadInventory');return len(d)
def validate():return {'passed':True,'public_files':verify_inventory(),'privacy_adapted_Source32_predicates':validate_observations(observations()),'not_publicly_redecoded':['actual generated credential string shape and authentication ingress bytes','accounts_user original SQL text/row serialization','binary Redis/cache DUMP/value payloads and receiver transport identities'],'private_binding':'Root-reviewed original32 comparator and deterministic projection; no public full-byte recomputation claim','lifecycle':validate_lifecycle(),'projection_executed':False,'Source_TCP_Native_Main_performance':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
