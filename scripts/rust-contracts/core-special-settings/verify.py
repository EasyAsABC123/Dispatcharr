"""Finite projected evidence verifier. No application imports or private fallback.
Raw authentication, Redis codec membership and process identity predicates remain
private-review obligations; this program verifies only retained public fields.
"""
import datetime, hashlib, json, math
from pathlib import Path
from cache_primitives import decode
HERE=Path(__file__).resolve().parent
GROUPS=['system_settings','network_access','reverse_proxy_auth','proxy_settings','stream_settings','dvr_settings']
RANGES=[range(1,9),range(9,19),range(19,27),range(27,35),range(35,44),range(44,52)]
EPOCHS=['Source51','Native-first-HOST1','Native-corrected']
sha=lambda b:hashlib.sha256(b).hexdigest()
canonical=lambda v:json.dumps(v,sort_keys=True,separators=(',',':')).encode()
def need(value,label):
 if not value:raise ValueError(label)
def changed(a,b):return sorted(k for k in a if a[k]!=b[k])
def keyed(rows,key):
 d={r[key]:r for r in rows};need(len(d)==len(rows),'DuplicateIdentities:'+key);return d
def new(a,b,key):return [v for k,v in keyed(b,key).items()if k not in keyed(a,key)]
def ct(headers):return next((v for k,v in headers.items()if k.lower()=='content-type'),None)
def epoch(s):return datetime.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def strip_notices(rows):return [{k:v for k,v in r.items()if k not in ('created_at','updated_at')}for r in rows]
def body(row,control=False):
 raw=bytes.fromhex(row['response_body_hex']);need(len(raw)==row['response_body_bytes']<=65536 and sha(raw)==row['response_body_sha256'],'LiteralResponseBytes')
 if control:need(json.loads(raw)==row['json'],'ControlJSONBytes')
 else:need(sha(bytes.fromhex(row['request_body_hex']))==row['body_sha256'],'LiteralRequestBytes')
def verify(data):
 need(data['groups']==GROUPS and list(data['epochs'])==EPOCHS,'MandatoryEpochsAndGroups')
 origins=data['origins'];need(len(origins)==18 and {(r['epoch'],r['group'])for r in origins}=={(e,g)for e in EPOCHS for g in GROUPS},'RawBindingCoverage')
 for row in origins:need(type(row['bytes'])is int and row['bytes']>0 and len(bytes.fromhex(row['private_raw_sha256']))==32,'RawDigestShape')
 catalog=data['schema_catalog'];need(bool(catalog),'SchemaCatalogNonempty')
 for digest,value in catalog.items():need(sha(canonical(value))==digest and set(value)=={'columns','indexes','constraints'},'SchemaCatalogHash')
 counts={'Source51':0,'Native-first-HOST1':0,'Native-corrected':0};controls={};first_mismatch=[];task_cases=[];channel_cases=[];predicates=0
 source={}
 for name in EPOCHS:
  cohorts=data['epochs'][name];need([c['group']for c in cohorts]==GROUPS,'CohortCoverage')
  for group,ids,cohort in zip(GROUPS,RANGES,cohorts):
   need(cohort['capture_valid']is True and cohort['raw_recorder_passed']is False,'CaptureOnlyFalsePreserved')
   rows=cohort['cases'];need([r['id']for r in rows]==list(ids),'MandatoryCaseCoverage')
   counts[name]+=len(rows)
   if name=='Source51':source[group]=rows
   else:
    journal=cohort['controls'];expected=['/__special_state__']
    for _ in rows:
     expected+=['/__special_state__','/__special_warm_group__']
     if group in ('proxy_settings','stream_settings'):expected+=['/__special_warm_proxy__'if group=='proxy_settings'else'/__special_warm_ua__']
     expected+=['/__special_state__']*3
    expected+=['/__special_state__','/__special_stop__']
    need([r['path']for r in journal]==expected,'ControlJournalCoverage')
    controls[name]=controls.get(name,0)+len(journal)
    for c in journal:
     need(c['actual_request_attempted']is True and c['status']==200 and c['exception']is None and c['close_exception']is None,'ControlOutcome')
     need(c['physical_close']=={'fd_after':-1,'original_inode_absent':True,'timer_joined':True},'ControlPhysicalClose');body(c,True)
     if c['path'].startswith('/__special_warm_'):need(c['json'].get('loaded')is True,'WarmCompleted')
     if c['path']=='/__special_warm_group__':need(c['json']['group']==group,'WarmIdentity')
     if c['path']=='/__special_stop__':need(c['json']=={'stop_requested':True},'StopOutcome')
   for row in rows:
    body(row);need(row['group']==group and row['source_ip']in ('127.0.0.1','127.0.0.2'),'ActualCaseScope')
    start,end=epoch(row['began_utc']),epoch(row['completed_utc']);duration=row['duration_seconds'];need(math.isfinite(duration)and 0<=duration<=8 and 0<=end-start<=8,'HTTPClock')
    if name!='Source51':
     need(row['physical_close']=={'fd_after':-1,'original_inode_absent':True,'timer_joined':True},'APIPhysicalClose')
     need(start<=row['began_epoch']<=row['completed_epoch']<=end+.001,'EpochUTCConsistency')
    for label in ('before','after'):
     s=row[label];sql=s['SQL'];full=sql['full6259'];need(full['schema_sha256']in catalog,'SchemaReference')
     need(len(full['tables'])==62 and len(full['sequences'])==59,'MandatorySQLCoverage')
     for t in full['tables'].values():need(type(t['count'])is int and t['count']>=0 and len(bytes.fromhex(t['sha_like_md5']))==16,'SQLDigestShape')
     for f in s['local']['owned_log_files']:need(len(bytes.fromhex(f['content_hex']))==f['bytes']and sha(bytes.fromhex(f['content_hex']))==f['sha256'],'LocalFileBytes')
     keyed(s['broker_frames'],'raw_frame_sha256');keyed(s['Channels_frames'],'raw_frame_sha256')
     need(len(s['raw_effects']['default_UA'])<=1,'DefaultUACoverage')
     for cache in s['raw_effects']['default_UA']:need(cache['key']==':1:coresettings:default_user_agent'and cache['type']=='string'and type(decode(bytes.fromhex(cache['value_hex'])))is str,'DefaultUAString')
    if name=='Source51':continue
    ref=source[group][row['id']-ids.start]
    for key in ('id','name','group','role','method','path','request_body_hex','body_sha256','source_ip'):need(row[key]==ref[key],'SourceLiteralInput:'+key)
    parity=(row['status'],row['response_body_hex'],ct(row['headers']))==(ref['status'],ref['response_body_hex'],ct(ref['headers']))
    if name=='Native-first-HOST1':
     if not parity:first_mismatch.append(row['id'])
    else:need(parity,'LiteralStatusBodyContentTypeParity')
    sb,sa,nb,na=ref['before'],ref['after'],row['before'],row['after']
    for label,s,n in [('before',sb,nb),('after',sa,na)]:
     need(s['SQL']['settings']==n['SQL']['settings'],'LiteralSettingsRows');need(strip_notices(s['SQL']['notices'])==strip_notices(n['SQL']['notices']),'NoticeScalars')
     need(s['local']==n['local'],'LiteralProcessAndFileEffects')
     need(s['SQL']['full6259']['schema_sha256']==n['SQL']['full6259']['schema_sha256'],'NoSchemaMutation')
     need(set(s['named_group_cache'])==set(n['named_group_cache']),'NamedCacheCoverage')
     need(len(s['raw_effects']['conditions'])==len(n['raw_effects']['conditions']),'ConditionCount')
     for condition in n['raw_effects']['conditions']:need(condition['type']=='string'and decode(bytes.fromhex(condition['value_hex']))=={'owned':True},'ConditionSentinelValue')
     need([decode(bytes.fromhex(x['value_hex']))for x in s['raw_effects']['default_UA']]==[decode(bytes.fromhex(x['value_hex']))for x in n['raw_effects']['default_UA']],'DefaultUALiteralState')
    need(nb['SQL']['full6259']['schema_sha256']==na['SQL']['full6259']['schema_sha256'],'SchemaStable')
    for part in ('tables','sequences'):
     need(set(nb['SQL']['full6259'][part])==set(na['SQL']['full6259'][part])==set(sb['SQL']['full6259'][part])==set(sa['SQL']['full6259'][part]),'SQLKeysExact')
     need(changed(nb['SQL']['full6259'][part],na['SQL']['full6259'][part])==changed(sb['SQL']['full6259'][part],sa['SQL']['full6259'][part]),'SQLDeltaSet')
    old_notices=keyed(nb['SQL']['notices'],'id');source_old=keyed(sb['SQL']['notices'],'id')
    for src,notice in zip(sa['SQL']['notices'],na['SQL']['notices']):
     old=source_old.get(src['id']);own=old_notices.get(notice['id'])
     if old and old['created_at']==src['created_at']:need(own and own['created_at']==notice['created_at'],'NoticeCreatedAtRetained')
     if not old or old['updated_at']!=src['updated_at']:need(row['began_epoch']<=epoch(notice['updated_at'])<=row['completed_epoch'],'NoticeUpdatedRequestClock')
    for key,old in sb['named_group_cache'].items():
     after=sa['named_group_cache'][key];ownold=nb['named_group_cache'][key];ownafter=na['named_group_cache'][key]
     if ':groupver:'not in key:need((old,after)==(ownold,ownafter),'CacheLiteralValues');continue
     for x,y in [(old,ownold),(after,ownafter)]:need((x is None and y is None)or(type(x)is int and type(y)is int and y>0),'VersionRepresentation')
     if old==after:need(ownold==ownafter,'VersionUnchanged')
     else:need(type(ownafter)is int and ownafter!=ownold and int(row['began_epoch']*1e9)<=ownafter<=int(row['completed_epoch']*1e9),'VersionOwnRequestClock')
    st=new(sb['broker_frames'],sa['broker_frames'],'raw_frame_sha256');nt=new(nb['broker_frames'],na['broker_frames'],'raw_frame_sha256');need(len(st)==len(nt),'NewBrokerCount')
    if name=='Native-corrected'and nt:task_cases.append(row['id'])
    for x,y in zip(st,nt):
     for key in ('database','queue_key_hex','task','args'):need(x[key]==y[key],'BrokerRouteArguments')
     xkw,ykw=dict(x['kwargs']),dict(y['kwargs']);xt=xkw.pop('preacquired_lock_token',None);yt=ykw.pop('preacquired_lock_token',None);need(xkw==ykw and (xt is None)==(yt is None),'BrokerKeywords')
     if yt:
      for account in ykw['preacquired_lock_ids']:
       locks=[l for l in na['raw_effects']['locks']if l['database']==y['database']and l['key']=='task_lock_refresh_single_m3u_account_'+str(account)]
       need(len(locks)==1 and locks[0]['private_value_sha256']==yt['private_token_sha256']and 0<locks[0]['pttl']<=7200000,'BrokerLockDigestRelationship')
    sc=new(sb['Channels_frames'],sa['Channels_frames'],'raw_frame_sha256');nc=new(nb['Channels_frames'],na['Channels_frames'],'raw_frame_sha256')
    need([x['decoded_safe_envelope']for x in sc]==[x['decoded_safe_envelope']for x in nc],'ChannelsDelta')
    if name=='Native-corrected'and nc:channel_cases.append(row['id'])
    predicates+=1
 need(counts==dict.fromkeys(EPOCHS,51)and controls=={'Native-first-HOST1':290,'Native-corrected':290},'WholeCountCoverage')
 need(first_mismatch==[12,16],'HistoricalFailurePreserved')
 need(task_cases==[38,46,47,49,50]and channel_cases==[11,15,18],'ObservedPublicationCases')
 return {'public_projected_predicates_passed':True,'case_comparisons':predicates,'requests_per_epoch':counts,'controls':controls,'first_HOST1_literal_mismatches':first_mismatch,'corrected_literal_matches':51,'new_broker_cases':task_cases,'new_Channels_cases':channel_cases,'raw3520_recomputation':False,'Main_fullM5_or_performance':False}
def verify_metadata(root):
 authority=json.loads((root/'authority.json').read_text())
 required={'provenance.json','qa.json','agent-review-receipts.json','privacy-inventory.json','privacy-full-packet.json','verification.json','additive-UA-review.json','original-inputs.json'}
 need(set(authority['metadata'])==required,'MandatoryMetadataCoverage')
 for name,digest in authority['metadata'].items():need(sha((root/name).read_bytes())==digest,'ExactMetadataAuthority:'+name)
 original=json.loads((root/'original-inputs.json').read_text())
 need(original['git']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and bool(original['paths']),'PinnedOriginalCodeAuthority')
 need({str(p.relative_to(root/'original'))for p in(root/'original').rglob('*')if p.is_file()}==set(original['paths']),'ExactOriginalCodeCoverage')
 for name,digest in original['paths'].items():need(sha((root/'original'/name).read_bytes())==digest,'OriginalCodeHash:'+name)
 provenance=json.loads((root/'provenance.json').read_text())
 need(provenance['Original_image']=='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'and provenance['Original_archive_sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalImageAndArchive')

def main():
 verify_metadata(HERE)
 raw=(HERE/'retained.json').read_bytes();authority=json.loads((HERE/'authority.json').read_text());need(sha(raw)==authority['retained_sha256'],'ExactRetainedAuthority');result=verify(json.loads(raw))
 provenance=json.loads((HERE/'provenance.json').read_text());qa=json.loads((HERE/'qa.json').read_text())
 need(provenance['base_git']=='bccc7ff313631152fb269dfbd1eda1437d4cc5e8'and provenance['Original_git']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','ExactSourceRevisions')
 need(provenance['Main_or_fullM5_or_performance']is False and provenance['qualified_source_count']==890 and provenance['rust_inputs']==885 and provenance['coupled_inputs']==5,'ComponentScopeOnly')
 need(qa['default_counts']=={'passed':833,'ignored':749,'result_blocks':172},'ActualDefaultCounts')
 need([r['stage']for r in qa['actual_commands']]==['fmt','check','strict','defaults','realtime-no-run','store-no-run'],'ActualStageCoverage')
 for row in qa['actual_commands']:need(row['actual_exit']==0 and math.isfinite(row['started_epoch'])and row['completed_epoch']>=row['started_epoch'],'ActualStageWait')
 for file,key in [('base882.json','base882_sha256'),('qualified890.json','qualified890_sha256'),('selected16.json','selected16_sha256')]:need(sha((HERE/'source'/file).read_bytes())==authority[key],'InputMapAuthority')
 need(qa['source_map_sha256']==authority['qualified890_sha256'],'QAInputBinding')
 print(json.dumps(result,sort_keys=True))
if __name__=='__main__':main()
