"""Qualify actual captured producer/CAS evidence; never accepts an app stub."""
import base64,hashlib,json,re,tarfile,uuid
from pathlib import Path

def claim_authority(binding):
 # Resolve immutable build archive, never mutable working-tree source or a
 # hardcoded image/revision. All identities must agree with CURRENT binding.
 build_path=Path(binding['candidate_build']);proof_path=Path(binding['candidate_physical_proof']);map_path=Path(binding['candidate_source_map'])
 build_raw=build_path.read_bytes();proof_raw=proof_path.read_bytes();map_raw=map_path.read_bytes();build=json.loads(build_raw);proof=json.loads(proof_raw);mapping=json.loads(map_raw)
 expected={str(Path(row['path']).resolve()):row['sha256']for row in binding['inputs']}
 for path,raw in [(build_path,build_raw),(proof_path,proof_raw),(map_path,map_raw)]:
  if expected.get(str(path.resolve()))!=hashlib.sha256(raw).hexdigest():raise ValueError('FrozenCandidateEvidenceChanged')
 if not build['passed']or build['exit_code']!=0 or not proof['passed']or proof['actual_build_wait_exit']!=0:raise ValueError('ActualPassedBuildAndProofRequired')
 if build['git_revision']!=proof['git_revision']or build['git_revision']!=binding['candidate_git_revision']or build['image_id']!=proof['image_id']or build['image_id']!=binding['candidate_image_id']:raise ValueError('CandidateIdentityMismatch')
 if not proof['all_archive_files_byteexact_Git']or not proof['runtime_Rust_map_exact_validated_candidate']:raise ValueError('PhysicalSourceProofRequired')
 if hashlib.sha256(map_raw).hexdigest()!=binding['candidate_source_map_sha256']:raise ValueError('CandidateSourceMapChanged')
 archive=Path(build['source_archive']);archive=archive if archive.is_absolute()else build_path.parent/archive
 digest=hashlib.sha256()
 with archive.open('rb')as stream:
  for chunk in iter(lambda:stream.read(1048576),b''):digest.update(chunk)
 if digest.hexdigest()!=build['source_archive_sha256']:raise ValueError('CandidateArchiveChanged')
 target='rust/crates/job-transport/src/transport.rs'
 with tarfile.open(archive)as source:
  member=source.getmember(target)
  if not member.isfile()or member.size>1048576:raise ValueError('CandidateTransportSourceBounds')
  raw=source.extractfile(member).read()
 transport_sha=hashlib.sha256(raw).hexdigest()
 if transport_sha!=mapping[target]or transport_sha!=build['source_sha256'][target]:raise ValueError('CandidateTransportSourceChanged')
 script=re.search(rb'const CLAIM_CREATE: &str = r#"(.*?)"#;',raw,re.S).group(1).decode()
 # Known protocol, derived SHA. A different future Lua program needs review;
 # arbitrary EVALSHA calls cannot inherit compare-and-pop authority.
 required=["if redis.call('TYPE',KEYS[1]).ok~='list'then return 0 end","if redis.call('LINDEX',KEYS[1],-1)~=ARGV[1]then return 0 end","redis.call('RPOP',KEYS[1]);return 1"]
 if [line.strip()for line in script.splitlines()if line.strip()]!=required:raise ValueError('UnsupportedClaimScriptProgram')
 return {'script':script,'sha1':hashlib.sha1(script.encode()).hexdigest(),'sha256':hashlib.sha256(script.encode()).hexdigest(),'candidate_git':binding['candidate_git_revision'],'candidate_image':binding['candidate_image_id'],'transport_sha256':transport_sha,'source_map_sha256':hashlib.sha256(map_raw).hexdigest(),'build_sha256':hashlib.sha256(build_raw).hexdigest(),'physical_proof_sha256':hashlib.sha256(proof_raw).hexdigest(),'archive_sha256':digest.hexdigest()}

def actual_claim(rows,index,payload,authority):
 row=rows[index];command=row.get('command','');script=authority['script'];escaped=script.replace('\\','\\\\').replace('\n',r'\n').replace('\r',r'\r').replace('\t',r'\t')
 accepted=command=='EVALSHA '+authority['sha1']+' 1 celery '+payload or any(command=='EVAL '+program+' 1 celery '+payload for program in [script,escaped])
 return accepted and row.get('db')==1 and len(rows[index+1:index+4])==3 and all(r.get('db')==1 and r.get('client_type')=='lua'and r.get('client_address')=='lua'and r.get('command')==wanted for r,wanted in zip(rows[index+1:index+4],['TYPE celery','LINDEX celery -1','RPOP celery']))

def qualify(path,cases,image,inspection,binding):
 rows=[json.loads(v)for v in Path(path).read_text().splitlines()];assert len(rows)<=20000
 if inspection['Image']!=image or not inspection['State']['Running']:return {'passed':False,'primary_error':'ActualBoundMainGenerationRequired'}
 authority=claim_authority(binding)
 if authority['candidate_image']!=image:return {'passed':False,'primary_error':'ClaimAuthorityImageMismatch'}
 hostname=inspection['Config']['Hostname'];expected_origin='gen1@'+hostname
 result={'passed':False,'journal_sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest(),'actual_rows':len(rows),'raw_journal_private_until_review':True,'cases':[],'claim_authority':authority,'reply_identity_scope':'UUIDv3+actualPIDhostname; shared Source-qualified identity implementation reused; opaque app/thread OID inputs are not fabricated'}
 for case in cases:
  if 'task_id'not in case:result['cases'].append({'name':case['name'],'passed':False,'primary_error':'NoActualTaskIdentity'});continue
  ident=case['task_id'];matches=[]
  for index,row in enumerate(rows):
   command=row.get('command','')
   if row.get('db')!=1 or not command.upper().startswith('LPUSH CELERY '):continue
   try:
    frame=json.loads(command.split(' ',2)[2])
    if isinstance(frame,str):frame=json.loads(frame)
   except (ValueError,TypeError):continue
   if frame.get('headers',{}).get('id')==ident:matches.append((index,row,frame))
  value={'name':case['name'],'task_id':ident,'actual_LPUSH_count':len(matches),'native_API_producer':case['entrypoint']=='actual Main API producer'};checks={'exactly_one_actual_publication':len(matches)==1}
  if len(matches)==1:
   index,row,frame=matches[0];raw=json.dumps(frame,separators=(',',':')).encode();value['captured_frame']=frame;value['monitor_publication_row']=row
   checks['root_id_correlation']=frame['headers']['root_id']==ident and frame['properties']['correlation_id']==ident and frame['headers']['parent_id']is None
   checks['actual_origin']=frame['headers']['origin']==expected_origin if value['native_API_producer']else True
   checks['UUIDv3_reply']=uuid.UUID(frame['properties']['reply_to']).version==3
   checks['UUIDv4_delivery_tag']=uuid.UUID(frame['properties']['delivery_tag']).version==4
   checks['normal_result_policy']=frame['headers']['ignore_result']is False
   body=json.loads(base64.b64decode(frame['body']))
   if value['native_API_producer']:
    args=case['actual_input_args'];kwargs=case['actual_input_kwargs'];checks['actual_API_args_kwargs']=body[:2]==[args,kwargs];checks['tuple_args_repr']=frame['headers']['argsrepr']==repr(tuple(args));checks['kwargs_repr']=frame['headers']['kwargsrepr']==repr(kwargs)
   else:checks['projected_noarg_body']=body[:2]==[[],{}]
   checks['no_callback_authority']=body[2]=={'callbacks':None,'errbacks':None,'chain':None,'chord':None}
   payload=row['command'].split(' ',2)[2];claim_indexes=[i for i in range(index+1,len(rows))if actual_claim(rows,i,payload,authority)];candidate_claims=[rows[i]for i in claim_indexes];value['actual_CAS_lua_rows']=[rows[i+1:i+4]for i in claim_indexes]
   checks['actual_compare_and_pop_script_with_same_task']=len(candidate_claims)==1;value['actual_CAS_rows']=candidate_claims
   checks['actual_backend_same_identity']=case.get('backend',{}).get('metadata',{}).get('task_id')==ident
  value['checks']=checks;value['passed']=all(checks.values());result['cases'].append(value)
 result['passed']=len(result['cases'])==9 and all(c['passed']for c in result['cases']);return result
