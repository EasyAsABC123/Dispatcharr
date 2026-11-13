"""Pure copied-input/PID guards; no SQL, process, socket or fixture on import."""
import hashlib,json,re

def database(name):
 if not isinstance(name,str)or not re.fullmatch(r'rust_pending_[a-z0-9_]{1,50}',name)or len(name)>63:raise ValueError('OwnedCopiedDatabase')
 return name

def dataset(value):
 database(value['database'])
 if not isinstance(value['tables'],dict)or not value['tables']or not isinstance(value['sequences'],dict):raise ValueError('DatasetShape')
 for row in value['tables'].values():
  if type(row['rows'])is not int or not 0<=row['rows']<=10000 or not re.fullmatch('[0-9a-f]{64}',row['sha256']):raise ValueError('TableFingerprintShape')
 for seq in value['sequences'].values():
  if type(seq['last_value'])is not int or type(seq['is_called'])is not bool:raise ValueError('SequenceFingerprintShape')
 actual=hashlib.sha256(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode()).hexdigest()
 if actual!=value['dataset_sha256']:raise ValueError('FingerprintDigestBinding')
 return actual

def clones(template,source,native,expected_names):
 if [v['database']for v in[template,source,native]]!=list(expected_names)or len(set(expected_names))!=3:raise ValueError('IndependentDatabaseBindings')
 values=[dataset(v)for v in[template,source,native]]
 if len(set(values))!=1:raise ValueError('SourceNativeInputDrift')
 return {'independent_names':True,'named_tables_and_sequences_equal':True,'dataset_sha256':values[0],'full_database_hash_claimed':False}

def stop_record(row,role,nonce,read_starttime):
 if row.get('role')!=role or row.get('nonce')!=nonce:raise ValueError('OwnedPIDBinding')
 pid=row.get('pid')
 if type(pid)is not int or not 1<pid<2**31:raise ValueError('OwnedPIDRange')
 if not isinstance(row.get('starttime'),str)or not row['starttime'].isdigit()or read_starttime(pid)!=row['starttime']:raise ValueError('OwnedPIDReused')
 return pid


def hold_observation(boundary,control_complete,released_at,target=2.4,maximum=2.75):
 elapsed=released_at-boundary;control=control_complete-boundary
 value={'actual_seconds':elapsed,'control_work_seconds':control,'declared_target_seconds':target,'maximum_seconds':maximum,'overshoot_seconds':max(0,elapsed-target),'control_completed_within_target':0<=control<=target,'within_declared_bound':target<=elapsed<=maximum,'same_declared_target':True,'exact_pause_identity_claimed':False}
 if not value['control_completed_within_target']or not value['within_declared_bound']:raise ValueError('DeclaredHoldTimingBound')
 return value

def other_reservation(before,after,profile_key,other_uuid,user_id,global_count):
 for row in[before,after]:
  ledger=row['ledger'];meta=row['metadata']
  fence=ledger.get('fence')
  if not isinstance(fence,str)or not re.fullmatch(r'[1-9][0-9]{0,18}',fence)or int(fence)>9223372036854775807:raise ValueError('OtherMoviePositiveFence')
  if not row['owner_token']or row['owner_token']!=row['owner_confirm']or ledger.get('token')!=row['owner_token']or ledger.get('owner')!=row['owner_key']or ledger.get('profile')!=profile_key or ledger.get('reserved')!='1':raise ValueError('OtherMoviePrivateReservation')
  if meta.get('m3u_profile_id')!=profile_key.removeprefix('profile_connections:')or meta.get('content_uuid')!=other_uuid or meta.get('user_id')!=str(user_id)or meta.get('active_streams')!='1'or not meta.get('rust_owner_token'):raise ValueError('OtherMoviePrivateIdentity')
 if before!=after:raise ValueError('OtherMoviePrivateOwnerChanged')
 if global_count!=1:raise ValueError('OtherMovieGlobalReservation')
 return{'same_private_owner_ledger_identity':True,'reserved':True,'native_global_count':global_count,'full_Source_counter_parity_claimed':False}

def early_stop(value):
 if value.get('stop')is None:raise ValueError('EarlyStopConsumed')
 return True
