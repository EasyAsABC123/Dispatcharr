"""Pure Source16 predictions/effects validation. Unexpected outcomes remain raw."""
import base64,datetime,hashlib,io,json,zipfile
from binding import require,initial_aliases,absorb_created,absorb_completed,validate_row
from fixture_guards import dataset
from projection import evidence

def queue_frames(snapshot):
 rows=snapshot['redis'][1]['rows'];out=[]
 for row in rows:
  if row['type']=='list':
   for raw in row['list_hex']:out.append((bytes.fromhex(row['key_hex']).decode(),json.loads(bytes.fromhex(raw))))
 return out

def validate_envelope(frame,aliases,queue):
 require(frame['headers']['id']==aliases['created_task']and frame['headers']['task']=='apps.backups.tasks.create_backup_task'and frame['properties']['delivery_info']['routing_key']==queue,'ExactOwnBrokerTaskIdentity');body=json.loads(base64.b64decode(frame['body'],validate=True));require(body[0]==[]and body[1]=={},'ActualZeroArgumentTask');return True

def validate_archive(raw):
 require(len(raw)<=8388608,'BoundedStreamArchive')
 with zipfile.ZipFile(io.BytesIO(raw))as z:
  require(z.namelist()==['database.dump','metadata.json']and len(set(z.namelist()))==2 and sum(x.file_size for x in z.infolist())<=67108864,'BoundedExactV2ZIP');meta=json.loads(z.read('metadata.json'));require(set(meta)=={'format','version','database_type','database_file','created_at'}and meta['format']=='dispatcharr-backup'and meta['version']==2 and meta['database_type']=='postgresql'and meta['database_file']=='database.dump'and z.read('database.dump')[:5]==b'PGDMP','RealPostgreSQLCustomDumpV2')
  import datetime
  created=datetime.datetime.fromisoformat(meta['created_at']);require(created.tzinfo is not None and created.utcoffset()==datetime.timedelta(0),'ActualUTCArchiveMetadata');return meta

def check_seed(report,plan):
 nonce=report['nonce'];users=report['seed_users'];require(len(users)==3,'ExactThreeSeedUsers')
 for row,role in zip(users,plan['roles']):
  require(row['id']==role['id']and row['username']=='rust-probe-backupjob-'+nonce+'-'+role['alias']and row['api_key']==evidence(('backup-fixture-'+nonce+'-'+role['alias']).encode())and row['password']==evidence(b'!')and row['is_active']==role['is_active']and row['user_level']==role['user_level']and row['custom_properties']==role['custom_properties'],'ExactSeedRolesAliasesFields')
 require(report['seed_snapshot']['users']==users and report['initial_owned_snapshot']['users']==[],'ExactInitialAndSeedUsersRetained')
 locks=report['locks_after_preclaim'];require([r['key']for r in locks]==['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']and all(r['raw_hex']=='31'and 0<r['pttl']<=300000 for r in locks),'ExactNXLockPreclaim')
 return True

def compare(report,plan,worker_ready,worker_prerun,worker_postrun,worker_returned,final_snapshot):
 require(report['capture_valid']is True and report['actual_request_count']==16 and len(report['requests'])==16 and report['connections_close_called']is True,'CompleteActualSource16');require(report['force_authenticate_used']is False and report['Source_TCP_or_Native']is False and report['startup_skip']is True and report['APIClient_exception_policy']=={'raise_request_exception':False,'enforce_csrf_checks':True},'DeclaredFrameworkCredentialScope');check_seed(report,plan);require(report['owned_startup_scope']['plugin_configs']==[]and report['owned_startup_scope']['recordings']==[],'OwnedPluginRecordingScope');aliases=initial_aliases(report['nonce']);stable=report['seed_snapshot'];expected_db='rust_probe_backupjob_'+report['nonce'];dataset(stable['audit'],expected_db);checks=0
 for row,spec in zip(report['requests'],plan['cases']):
  validate_row(row,spec,aliases);require(row['raw_body_ascii']==spec['raw_body_ascii']and row['content_type']==spec['content_type'],'ExactRawRequestBody');require(row['transport']=='http_response'and row['capture_complete']is True and row['framework_exception']is None and row['status']==spec['predicted_status_UNMEASURED'],'MeasuredStatusMatchesPredeclaredPrediction');redacted=spec['ordinal']in(4,16);raw=b'' if redacted else bytes.fromhex(row['body_hex']);require((row['body_hex']=={'redacted':True,'bytes':row['body_bytes'],'sha256':row['body_sha256']})if redacted else(len(raw)==row['body_bytes']and hashlib.sha256(raw).hexdigest()==row['body_sha256']),'ExactLiteralOrExplicitOmittedBodyEvidence');checks+=3
  if spec['ordinal']not in(4,16):require(json.loads(raw)==row['json']and row['streaming']is False,'LiteralJSONBody')
  for side in('before','after'):
   state=row[side];require(state['audit']==stable['audit']and state['users']==stable['users']and state['events']==stable['events'],'AllTarget11SequencesFullUsersEventsUnchanged');require([r['database']for r in state['redis']]==[1,2,3],'AllThreeRawRedisScopes');dataset(state['audit'],expected_db)
  if spec['ordinal']==4:
   absorb_created(aliases,row['json']);frames=queue_frames(row['after']);require(len(frames)==1 and frames[0][0]==expected_db,'OneOwnedQueuedTask');envelope=frames[0][1];validate_envelope(envelope,aliases,expected_db);require(queue_frames(row['before'])==[],'NoEarlierTaskEnqueue')
  elif spec['ordinal']<=3:require(queue_frames(row['before'])==queue_frames(row['after'])==[],'RefusalsDoNotEnqueue')
  elif spec['ordinal']<=12:
   require(queue_frames(row['before'])==queue_frames(row['after'])and len(queue_frames(row['after']))==1,'NoStatusRequestEnqueueConsume')
  if spec['ordinal']<=12:require(row['before']['files']==row['after']['files']==[],'NoArchiveBeforeWorker')
  if spec['ordinal']in(7,8,12):require(row['json']=={'state':'pending'},'LiteralPending')
  if spec['ordinal']in(5,6):require(row['json']=={'detail':'Authentication required'}and not any(k.lower()=='www-authenticate'for k in row['headers']),'Manual401NoInventedAuthChallenge')
  if spec['ordinal']in(9,11):require(row['json']=={'detail':'Invalid task token'},'LiteralInvalidToken')
  if spec['ordinal']==10:require(any(k.lower()=='www-authenticate'for k in row['headers']),'BadJWTRunsBeforeAllowAnyToken')
  if spec['ordinal']==13:
   backend=json.loads(bytes.fromhex(report['actual_completed_backend']['raw_hex']));absorb_completed(aliases,backend['task_id'],backend['result']);require(row['json']=={'state':'completed','result':backend['result']},'LiteralOwnCompletedResult')
  if spec['ordinal']==14:require(row['json']==report['requests'][12]['json'],'AdminAndSignedSameCompletedResult')
  if spec['ordinal']==15:require(len(row['json'])==1 and row['json'][0]['name']==aliases['completed_filename']and row['json'][0]['size']==report['requests'][12]['json']['result']['size'],'ExactListedCreatedArchive')
  if spec['ordinal']==16:
   require(row['streaming']is True and row['body_bytes']<=8388608 and row['after']['files']==[{'name':aliases['completed_filename'],'bytes':row['body_bytes'],'sha256':row['body_sha256']}],'OmittedStreamEvidenceMatchesActualFile')
   proof=report['_portable_archive_proof'];require(proof['original_stream_bytes']==row['body_bytes']and proof['original_stream_sha256']==row['body_sha256']and proof['raw_archive_omitted']is True,'ExplicitArchiveOmissionBinding');require([x['name']for x in proof['members']]==['database.dump','metadata.json']and sum(x['uncompressed_bytes']for x in proof['members'])<=67108864 and proof['database_dump_magic_hex']=='5047444d50','RetainedPrivateArchiveDerivedReceipts');meta=proof['metadata'];require(meta['format']=='dispatcharr-backup'and meta['version']==2 and meta['database_type']=='postgresql'and meta['database_file']=='database.dump','DerivedV2Metadata');require(datetime.datetime.fromisoformat(worker_prerun['observed_utc'])<=datetime.datetime.fromisoformat(meta['created_at'])<=datetime.datetime.fromisoformat(worker_postrun['observed_utc']),'OwnWorkerArchiveClockBounds')
  checks+=6
 require(aliases==report['aliases'],'NoAliasDrift');
 for key,frame in queue_frames(worker_ready['snapshot']):
  if key!=expected_db:require((key,frame)in queue_frames(final_snapshot),'UnknownStartupFramesRetainedUnconsumed')
 require(worker_ready['consumer_queues']==[expected_db]and worker_ready['passive_observer']is True,'OnlyOwnedQueueConsumed');require(worker_prerun['task_id']==worker_postrun['task_id']==aliases['created_task']and worker_prerun['task_name']==worker_postrun['task_name']=='apps.backups.tasks.create_backup_task'and worker_prerun['request']['is_eager']is False and worker_postrun['state']=='SUCCESS'and worker_postrun['retval']==report['requests'][12]['json']['result'],'RealTaskIdentityAndSUCCESS');require(worker_returned['consumed_task_ids']==[aliases['created_task']]and worker_returned['connections_close_called']is True,'OnlyOneConsumedTaskAndWorkerReturned')
 for stage in(worker_ready,worker_prerun,worker_postrun):
  require(all(x['raw_hex']=='31'and x['pttl']>0 for x in stage['locks']),'OriginalStartupLocksRemainHeld');state=stage['snapshot'];require(state['audit']==stable['audit']and state['users']==stable['users']and state['events']==stable['events'],'RealWorkerNoTargetSQLEffects')
 require(final_snapshot['audit']==stable['audit']and final_snapshot['users']==stable['users']and final_snapshot['events']==stable['events'],'FinalTargetSQLUnchanged');return {'passed':True,'actual_requests':16,'portable_projected_predicates':checks,'original_private_effect_checks':144,'public_archive_redecoded':False,'real_worker_tasks':1,'framework_force_authenticate':False,'Source_TCP_Native_fullstartup_defaultRouting_restore_schedule_claim':False}
