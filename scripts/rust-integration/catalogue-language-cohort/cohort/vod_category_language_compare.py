"""Offline typed projection of retained language lifecycle captures; no application calls.
Original Dispatcharr/dependency authors retain all application and algorithm credit.
"""
import copy,hashlib,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
import vod_category_language_cases as helper
import vod_shared_metadata_compare as shared
import vod_dense_list_compare as dense
load=dense.load;exact=dense.exact;state=dense.state
ROOT_FILES=(*shared.ROOT_FILES,'journal')
TAG_FILES=('SQL-before','SQL-after','request','HTTP','WS','journal','cache-before','cache-after','provider-before','provider-after','broker-before','broker-after')
SEED='declared-after-B-seed'
SEED_FILES=shared.SEED_FILES

def canonical_delta(directory,name,previous,sqlstate,kind):
 values={key:load(Path(directory)/name/(key+'.json'))for key in(TAG_FILES if kind=='tag'else SEED_FILES)};before=values['SQL-before'];after=values['SQL-after'];exact(state(before),state(previous),'Raw preceding phase to declared delta exact')
 cb=sqlstate.canonical(before);effects=sqlstate.advance(name,before,after,None);ca=sqlstate.canonical(after)
 for key in ['cache','provider','broker']:exact(values[key+'-before'],values[key+'-after'],('No unrelated effect during declared '+kind,key))
 result={'before':shared.safe_sql(cb),'after':shared.safe_sql(ca),'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()}}
 if kind=='tag':
  response=values['HTTP'];assert response['context']==helper.CASE and response['method']=='PATCH'and response['credential_role']=='admin'and response['status']==200 and response['body']=={'message':'Group settings updated successfully'};assert response['path'].endswith('/group-settings/')
  exact(values['journal']['publications'],[],'Tag API emits no task');result.update(request=values['request'],HTTP=response,ordered_WS=values['WS'])
 else:result.update(setup_result=values['setup-result'],seed_script=values['seed-script'])
 return after,result

def project(directory,backend_kind='Source'):
 directory=Path(directory);initial=load(directory/helper.STAGES[0][0]/'SQL-before.json');shared.namespace(initial,initial);sqlstate=shared.SQLState(initial,{})
 owners={role:next(row for row in initial['m3u_m3uaccount']if row['name']==name)for role,name in [('A','Owned VOD ingest'),('B','Owned VOD shared B')]};phases={};previous=None;roots=set()
 for label,role in helper.STAGES:
  tag=None
  if label in helper.TAGS:previous,tag=canonical_delta(directory,label+'-tag-API',previous,sqlstate,'tag')
  values={key:load(directory/label/(key+'.json'))for key in ROOT_FILES};before=values['SQL-before'];after=values['SQL-after'];owner=owners[role]
  if previous is not None:exact(state(before),state(previous),'Complete raw chain around sole seed and declared tag API')
  cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after,owner['id']);ca=sqlstate.canonical(after)
  ident,backend=shared.task(values['frame'],values['backend'],owner['id'],owner['name'],before,after);assert ident not in roots;roots.add(ident)
  exact([(row['task'],row['id'])for row in values['journal']['publications']],[(helper.TASK,ident)],'One actual natural root, no children')
  exact(values['cache-before'],values['cache-after'],'Ordinary refresh cache stable')
  HTTP=values['HTTP'];exact(HTTP,{'context':helper.CASE,'method':'POST','path':'/api/m3u/accounts/'+str(owner['id'])+'/refresh-vod/','credential_role':'admin','status':202,'body':{'message':'VOD refresh initiated for account '+owner['name']}},'Selected root API envelope')
  phases[label]={'selected_owner':role,'declared_tag_API':tag,'before':shared.safe_sql(cb),'after':shared.safe_sql(ca),'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'HTTP':HTTP,'backend':backend,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate)}
  previous=after
  if label=='language-B-shared':previous,seed=canonical_delta(directory,SEED,previous,sqlstate,'seed');phases[label]['declared_seed']=seed
 return {'status':'actual_'+backend_kind+'_language11_capture_projected','actual_Source_roots':len(roots)if backend_kind=='Source'else 0,'actual_Native_roots':len(roots)if backend_kind=='Native'else 0,'full_rows_support_all11sequences_preserved':True,'raw_chain':'Exact across11roots,6normaladminPATCHdeltas andoneexplicitseed','phases':phases,'performance_verified':False,'full_milestone_accepted':False}

def gz(path):
 import gzip
 raw=Path(path).read_bytes();assert len(raw)<=4*1024*1024
 value=gzip.decompress(raw);assert len(value)<=8*1024*1024
 return json.loads(value),value

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

SAFE_RECEIPT=('actual_exit','failure','stdout_EOF','stderr_EOF','process_group_settled','output_bytes_discarded','argv_sha256','stdout_sha256','stderr_sha256')
def receiver_name(value):
 import re,uuid
 assert isinstance(value,str)
 match=re.fullmatch(r'specific\.aggregate_([0-9a-f]{32})!receiver',value)
 assert match and uuid.UUID(hex=match[1]).version==4,'Typed actual aggregate receiver UUIDv4'
 return value

def validate_receiver_proof(proof,name,actual_files=False,fixture=None):
 import base64
 receiver_name(name);exact(proof['group'],'updates','Actual group-add group');exact(proof['receiver'],name,'Actual receiver identity')
 expected={'actual_group_add_completed':True,'receiver':name};exact(proof['returned'],expected,'Actual completed group-add reply')
 receipt=proof['safe_receipt'];assert receipt['actual_exit']==0 and receipt['failure']is None and receipt['stdout_EOF']is True and receipt['stderr_EOF']is True and receipt['process_group_settled']is True and receipt['output_bytes_discarded']==0
 raw=base64.b64decode(proof['raw_stdout_base64'],validate=True);assert len(raw)<=4096
 exact(json.loads(raw),expected,'Exact actual ready stdout');digest=hashlib.sha256(raw).hexdigest();exact(digest,receipt['stdout_sha256'],'Ready reply receipt hash');exact(digest,proof['actual_stdout']['sha256'],'Ready stdout hash')
 if actual_files:
  rp=Path(proof['actual_receipt']['path']);sp=Path(proof['actual_stdout']['path']);assert fixture is not None and rp.parent.resolve()==Path(fixture).resolve()and sp.parent.resolve()==(Path(fixture)/'private').resolve()and rp.stem==sp.stem and rp.name.startswith('command-'),'Actual current owned fixture receipt';assert sha(rp)==proof['actual_receipt']['sha256']and sha(sp)==digest and sp.read_bytes()==raw
  observed=load(rp);exact({key:observed[key]for key in SAFE_RECEIPT},receipt,'Actual current receipt fields');exact(str(sp),observed['stdout'],'Actual stdout path')
 return name

def capture_receiver(f,name):
 import base64
 receipt=f.cli[-1];stdout=Path(receipt['stdout']);rp=f.output/(stdout.stem+'.json')
 proof={'group':'updates','receiver':name,'returned':json.loads(stdout.read_bytes()),'safe_receipt':{key:receipt[key]for key in SAFE_RECEIPT},'actual_receipt':{'path':str(rp),'sha256':sha(rp)},'actual_stdout':{'path':str(stdout),'sha256':sha(stdout)},'raw_stdout_base64':base64.b64encode(stdout.read_bytes()).decode()}
 validate_receiver_proof(proof,name,True,f.output);return proof

def bind_tag_receivers(oracle,native,addon,directory):
 assert addon['schema']=='ActualOwnedTagReceiverGroupAdd1'and addon['group']=='updates'and addon['Source_controller_exit']==0 and addon['Source_replayed']is False and addon['Source_oracle_changed']is False
 for filename,key in [('matching_guest.py','matching_guest_sha256'),('m3u_refresh_all_cases.py','aggregate_helper_sha256')]:assert sha(Path(__file__).parent/filename)==addon[key]
 labels=set(helper.TAGS);exact(set(addon['proofs']),labels,'Exactly six Source tag receivers');source_names=set();native_names=set()
 for label in helper.TAGS:
  sw=oracle['phases'][label]['declared_tag_API']['ordered_WS'];nw=native['phases'][label]['declared_tag_API']['ordered_WS']
  for value in(sw,nw):exact(set(value),{'actual_receiver','events','raw_events_base64'},'Exact tag receiver observation shape');exact(value['events'],[],'No tag events');exact(value['raw_events_base64'],[],'No tag raw events')
  source=validate_receiver_proof(addon['proofs'][label],sw['actual_receiver']);actual=validate_receiver_proof(load(Path(directory)/(label+'-tag-API')/'receiver-begin-proof.json'),nw['actual_receiver'],True,Path(directory).parent.parent)
  assert source not in source_names and actual not in native_names and actual not in{p['receiver']for p in addon['proofs'].values()},'Independent distinct actual group-add receivers'
  source_names.add(source);native_names.add(actual);nw['actual_receiver']=source
 return native

def compare_retained_Source(f,directory):
 refs=f.binding['retained_source_vod_category_language'];assert set(refs)=={'qualification','projection','report','execution','cleanup','Root_cleanup','identity','index','provenance'}
 for ref in refs.values():assert sha(ref['path'])==ref['sha256']
 public=Path(refs['index']['path']).parent;index=load(refs['index']['path']);q=load(refs['qualification']['path']);identity=load(refs['identity']['path']);report=load(refs['report']['path'])
 assert q['Source_category_language_capture_qualified']and q['actual_Source_cases_passed']==11 and q['actual_controller_exit']==0 and q['Source17_replayed']is False
 assert identity['Original_git_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and identity['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'and identity['actual_Source_roots']==11 and identity['actual_Native_roots']==0
 terminal=load(refs['execution']['path']);assert terminal['actual_exit']==0 and terminal['failure']is None and terminal['stdout_EOF']and terminal['stderr_EOF']and terminal['process_group_settled']and load(refs['Root_cleanup']['path'])['passed']
 assert report['status']=='passed'and report['all_eleven_context_observations_complete']and len(report['cases'])==11 and all(row['status']=='passed'and row['Source_observation_complete']for row in report['cases'])
 for row in index['files']:
  path=public/row['path'];assert path.resolve().is_relative_to(public.resolve())and sha(path)==row['sha256']and path.stat().st_size==row['bytes']
 oracle,canonical=gz(refs['projection']['path']);assert hashlib.sha256(canonical).hexdigest()==q['semantic_projection']['uncompressed_canonical_sha256']and len(canonical)==q['semantic_projection']['uncompressed_bytes']
 source_initial,_=gz(public/'SQL'/helper.STAGES[0][0]/'SQL-before.json.gz');native_initial=load(Path(directory)/helper.STAGES[0][0]/'SQL-before.json')
 for name in shared.OWNED:
  left=next(row for row in source_initial['m3u_m3uaccount']if row['name']==name);right=next(row for row in native_initial['m3u_m3uaccount']if row['name']==name)
  exact(left['id'],right['id'],('UnsupportedFixtureAccountNamespace',name))
 receiver_ref=f.binding['retained_source_vod_category_language_tag_receivers'];assert sha(receiver_ref['path'])==receiver_ref['sha256'];addon=load(receiver_ref['path']);exact(addon['Source_public_index_sha256'],refs['index']['sha256'],'Receiver Source index binding');exact(addon['Source_execution_sha256'],refs['execution']['sha256'],'Receiver Source execution binding')
 native=project(directory,backend_kind='Native');native=json.loads(json.dumps(native,allow_nan=False));bind_tag_receivers(oracle,native,addon,directory);exact(oracle['phases'],native['phases'],'Full language11 rows/PKs/all11seq/API/provider/WS/tag/seed comparison')
 for stage in [label for label,_ in helper.STAGES]+[SEED]:
  source_before,_=gz(public/'SQL'/stage/'SQL-before.json.gz');source_after,_=gz(public/'SQL'/stage/'SQL-after.json.gz');native_after=load(Path(directory)/stage/'SQL-after.json')
  for table in ['vod_movie','vod_series','vod_episode']:
   previous={row['id']for row in source_before[table]};actual={row['id']:row for row in native_after[table]}
   for row in source_after[table]:
    if row['id']not in previous:
     dense.uuid4(row['uuid']);dense.uuid4(actual[row['id']]['uuid']);assert row['uuid']!=actual[row['id']]['uuid'],'Independent real new model UUIDv4'
 for (label,_),source in zip(helper.STAGES,report['cases']):
  dense.uuid4(source['actual_root']);actual=load(Path(directory)/label/'frame.json')['headers']['id'];dense.uuid4(actual);assert actual!=source['actual_root'],'Independent actual root UUIDv4'
 from workflow import save
 save(Path(directory).parent/'Source-comparison.json',{'status':'passed','actual_Source_roots':11,'actual_Native_roots':11,'Source17_replayed':False,'full_rows_support_literalPK_all11seq':True,'complete_tag_seed_root_raw_chain':True,'Source_public_index_sha256':refs['index']['sha256'],'no_generic_business_scrubbing':True,'performance_verified':False,'full_milestone_accepted':False})
