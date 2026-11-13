"""Pure role/task/path binding; no application or worker imports."""
import hashlib,hmac,re
from projection import evidence
from urllib.parse import quote,urlencode
SECRET=b'backup-job-fixture-only'
FUNCTIONS=['create_backup']*4+['backup_status']*10+['list_backups','download_backup']
def require(ok,label):
 if not ok:raise ValueError(label)
def token(task):return hmac.new(SECRET,task.encode(),hashlib.sha256).hexdigest()[:32]
def initial_aliases(nonce):
 require(re.fullmatch('[a-f0-9]{10}',nonce)is not None,'OwnedNonce');unknown='never-created-'+nonce;return {'unknown_task':unknown,'unknown_task_token':evidence(token(unknown).encode())}
def absorb_created(aliases,response):
 require('created_task'not in aliases and type(response)is dict and set(response)=={'detail','task_id','task_token'}and response['detail']=='Backup started','ExactSingleCreatedResult');task=response['task_id'];require(type(task)is str and re.fullmatch('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}',task)is not None and task!=aliases['unknown_task'],'NaturalCreatedTaskID');require(response['task_token']==evidence(token(task).encode()),'CreatedTokenOwnTaskHMAC');aliases.update(created_task=task,created_task_token=response['task_token'])
def absorb_completed(aliases,task_id,result):
 require(task_id==aliases['created_task']and set(result)=={'status','filename','size'}and result['status']=='completed'and type(result['size'])is int and result['size']>0,'ExactOwnCompletedTask');name=result['filename'];require(type(name)is str and len(name)<=255 and name.startswith('dispatcharr-backup-')and name.endswith('.zip')and not any(c in name for c in('..','/','\\')),'CompletedFilenameAlias');aliases['completed_filename']=name
def path_for(spec,aliases):
 path=spec['path_template']
 for alias in('created_task','unknown_task','completed_filename'):
  marker='{'+alias+'}'
  if marker in path:require(alias in aliases,'BoundObservedAlias:'+alias);path=path.replace(marker,quote(aliases[alias],safe=''))
 require('{'not in path and'}'not in path,'NoUnboundTarget');selector=spec['token_selector']
 if selector is not None:
  value='invalid-fixture-token' if selector=='invalid_literal' else token(aliases['created_task'] if selector=='created_task_token' else aliases['unknown_task']);original=path+'?'+urlencode({'token':value});path={'redacted':True,'template':path+'?token=<redacted>','original_bytes':len(original.encode()),'original_sha256':hashlib.sha256(original.encode()).hexdigest()}
 return path
def validate_row(row,spec,aliases):
 require(row['ordinal']==spec['ordinal']and row['name']==spec['name']and row['method']==spec['method']and row['path']==path_for(spec,aliases)and row['auth_selectors']==spec['auth'],'ExactPlannedRoleMethodTaskTokenTarget')
 resolver=row['resolver'];require(resolver['module']=='apps.backups.api_views'and resolver['function']==FUNCTIONS[spec['ordinal']-1],'RealResolvedFunctionIdentity')
 if spec['ordinal']in range(5,15):require(resolver['kwargs']=={'task_id':aliases['unknown_task']if spec['ordinal']==12 else aliases['created_task']},'ExactTaskAliasNotSelfConsistentPathOnly')
 elif spec['ordinal']==16:require(resolver['kwargs']=={'filename':aliases['completed_filename']},'ExactCompletedFileAlias')
 else:require(resolver['kwargs']=={},'ExactNoTargetKwargs')
 return True
