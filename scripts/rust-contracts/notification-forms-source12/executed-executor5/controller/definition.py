"""Pure exact Source12 definition/binding checks; no application imports."""
import hashlib,json,tarfile,ast
from pathlib import Path
def definition_root(module_file):
 controller=Path(module_file).resolve().parent
 return Path('/form-root')if controller==Path('/notification-tools')else controller.parent
ROOT=definition_root(__file__)

def require(ok,label):
 if not ok:raise ValueError(label)
def equal(a,b):
 return type(a)is type(b)and(set(a)==set(b)and all(equal(a[k],b[k])for k in a)if isinstance(a,dict)else len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))if isinstance(a,list)else a==b)
def validate(plan,root=None):
 root=ROOT if root is None else Path(root)
 require(plan['state']=='UNEXECUTED_SOURCE12_DEFINITION_REVIEW'and plan['requests_executed']==0 and plan['execution_authorized']is False,'UnexecutedDefinition')
 require(plan['seed_aliases']=={'aliasDelete':10,'foreign':11,'derived_missing':1000011},'ExactSeedAliases')
 require(equal(plan['roles'],[{'alias':'admin','id':1,'user_level':10,'is_active':True,'custom_properties':{}},{'alias':'standard','id':2,'user_level':1,'is_active':True,'custom_properties':{}},{'alias':'admin_denied','id':3,'user_level':10,'is_active':True,'custom_properties':{'allowed_networks':{'UI':'192.0.2.0/24'}}}]),'ExactThreeRoleFields')
 require(equal(plan['global_network_settings'],{'UI':'127.0.0.0/8'}),'ExactGlobalNetwork')
 wanted=[]
 for pk,key in [(10,'owned.forms.delete'),(11,'owned.forms.foreign')]:wanted.append({'id':pk,'notification_key':key,'notification_type':'info','priority':'normal','source':'system','title':'Owned forms '+str(pk),'message':'Source12 owned seed only','action_data':{},'is_active':True,'admin_only':False,'expires_at':None,'created_at':'2020-01-01T00:00:00Z','updated_at':'2020-01-01T00:00:00Z'})
 require(equal(plan['seed_notifications'],wanted),'ExactNotificationSeedFields')
 tree=ast.parse((root/'inputs/Original/core/models.py').read_bytes());model=next(x for x in tree.body if isinstance(x,ast.ClassDef)and x.name=='NotificationDismissal');field=next(x.value for x in model.body if isinstance(x,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='action_taken'for t in x.targets));require(isinstance(field,ast.Call)and isinstance(field.func,ast.Attribute)and field.func.attr=='CharField'and {x.arg:ast.literal_eval(x.value)for x in field.keywords}=={'max_length':50,'blank':True,'null':True},'PinnedActualActionFieldDefinition')
 require(all(r['action_taken']is None or(type(r['action_taken'])is str and len(r['action_taken'])<=50)for r in plan['seed_dismissals']),'PinnedNullableCharFieldActionSeedType')
 require(equal(plan['seed_dismissals'],[{'id':1,'notification_id':10,'user_id':1,'dismissed_at':'2020-01-01T01:00:00Z','action_taken':None},{'id':2,'notification_id':10,'user_id':2,'dismissed_at':'2020-01-01T02:00:00Z','action_taken':'accepted'},{'id':3,'notification_id':11,'user_id':2,'dismissed_at':'2020-01-01T03:00:00Z','action_taken':None}]),'ExactDismissalPairsClocksAndActions')
 body=json.loads((root/'inputs/body-definition-plan.json').read_text());require(equal(plan['cases'],body['cases']),'ExactReviewedBodyCaseOrderRolesTargets')
 require(plan['natural_aliases']=={'FormA':{'from_actual_response_ordinal':1,'JSON_field':'id'},'FormB':{'from_actual_response_ordinal':7,'JSON_field':'id'}},'ExactNaturalAliasAuthorities')
 for case in plan['cases']:
  raw=(root/'inputs'/case['body_path']).read_bytes();require(len(raw)==case['body_bytes']and hashlib.sha256(raw).hexdigest()==case['body_sha256'],'ExactRawBodyBytes')
 for row in plan['actual_dependencies']:
  f=root/row['path'];require(f.is_file()and not f.is_symlink()and f.stat().st_size==row['bytes']and hashlib.sha256(f.read_bytes()).hexdigest()==row['sha256'],'ActualImageDependencyByteBinding')
 require(len(plan['actual_dependencies'])==len({r['runtime_path']for r in plan['actual_dependencies']})==55,'Exact55ActualDependencyClosure')
 require(hashlib.sha256((root/'inputs/original-source.tar').read_bytes()).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','PinnedFullGitArchive')
 with tarfile.open(root/'inputs/original-source.tar')as archive:
  for relative,digest in plan['Original_inputs_sha256'].items():require(hashlib.sha256(archive.extractfile(relative).read()).hexdigest()==digest==hashlib.sha256((root/'inputs/Original'/relative).read_bytes()).hexdigest(),'ExactOriginalBlob')
 return True

def target(case,aliases):
 alias=case['target_alias']
 if alias=='collection':return '/api/core/notifications/'
 require(alias in aliases and type(aliases[alias])is int and aliases[alias]>0,'ActualBoundRequestAlias')
 return '/api/core/notifications/'+str(aliases[alias])+'/'

def validate_row(row,case,aliases):
 require(row['index']==case['index']and row['method']==case['method']and row['actor']==case['actor']and row['target_alias']==case['target_alias']and row['path']==target(case,aliases),'RequestTargetBoundToReviewedAliasNotSelfConsistency')
 require(row['body_sha256']==case['body_sha256']and row['body_bytes']==case['body_bytes']and row['content_type']==case['content_type'],'LiteralRequestBodyContentTypeBinding')
 return True
