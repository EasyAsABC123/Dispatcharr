"""Pure20-request Source authority checks; no normalization or app import."""
import datetime,hashlib,json
from seed_binding import ACTIONS,expected_actor,validate_seed,validate_row_binding,validate_auth_headers
N='core_systemnotification';D='core_notificationdismissal'
def require(ok,label):
 if not ok:raise ValueError(label)
def clock(v):
 d=datetime.datetime.fromisoformat(v.replace('Z','+00:00'));require(d.utcoffset()==datetime.timedelta(),'UTCClock');return d
def own_clock(row,value):require(clock(row['began_utc'])<=clock(value)<=clock(row['completed_utc']),'OwnRequestClock')
def no_other_tables(before,after,except_tables=()):
 require(set(before['tables'])==set(after['tables'])and set(before['sequences'])==set(after['sequences']),'ExactAuditNames')
 for name in before['tables']:
  if name not in except_tables:require(before['tables'][name]==after['tables'][name],'UnrelatedTable:'+name)
 for name in before['sequences']:
  if name.split('.')[1].removesuffix('_id_seq')not in except_tables:require(before['sequences'][name]==after['sequences'][name],'UnrelatedSequence:'+name)
def next_sequence(before,after,table,ids):
 b=before['sequences']['public.'+table+'_id_seq'];a=after['sequences']['public.'+table+'_id_seq'];first=b['last_value']+int(b['is_called']);require(ids==list(range(first,first+len(ids)))and a=={'last_value':ids[-1],'is_called':True},'NaturalSequence:'+table)
def serialized(notice,actor,dismissals):
 out={k:v for k,v in notice.items()if k!='updated_at'};out['is_dismissed']=any(r['user_id']==actor and r['notification_id']==notice['id']for r in dismissals);return out
def exact_serialized(actual,expected):
 require(set(actual)==set(expected),'SerializerKeys')
 for k,v in expected.items():require(clock(actual[k])==clock(v)if k in('created_at','expires_at')and v is not None else actual[k]==v,'SerializerField:'+k)
def compare(actual,plan):
 require(actual['capture_valid']is True and actual['actual_request_count']==len(actual['requests'])==20,'All20ActualCaptures');require(actual['force_authenticate_used']is False and actual['real_default_JWT_and_APIKey_backends']is True and actual['Source_TCP_or_Native']is False,'RealAuthFrameworkScope');validate_seed(actual,plan);previous=None;new_notice_id=None
 for row,spec in zip(actual['requests'],plan['cases']):
  validate_row_binding(row,spec);validate_auth_headers(row,spec,actual);require(row['transport']=='http_response'and row['status']==spec['predicted_status_UNMEASURED']and row['capture_complete']is True,'PredeclaredStatusAndLiteralCapture');raw=bytes.fromhex(row['body_hex']);require(len(raw)==row['body_bytes']<=65536 and hashlib.sha256(raw).hexdigest()==row['body_sha256'],'RawBodyBinding');require(json.loads(raw)==row['json'],'LiteralJSONBody');require(row['framework_exception']is None,'NoExpectedFrameworkException');actor=expected_actor(spec,plan);auth=row['actual_auth_observation'];require(auth['user_id']==actor and auth['is_authenticated']is(actor is not None),'ActualActorBinding')
  if actor is not None:
   expected_backend='rest_framework_simplejwt.authentication.JWTAuthentication'if'bearer'in spec['auth']else'apps.accounts.authentication.ApiKeyAuthentication';require(auth['backend_class']==expected_backend,'ActualBackendPrecedence')
  before,after=row['before_notifications'],row['after_notifications'];ba,aa=row['before_audit'],row['after_audit'];number=spec['ordinal'];body=row['json'];require(row['before_users']==row['after_users'],'NoCredentialUserMutation');require(row['before_events']==row['after_events'],'NoUnexpectedLockoutEventAtTwoFailures')
  bh=row['before_cache']['api_key_failed_auth_history'];ah=row['after_cache']['api_key_failed_auth_history']
  expected_before=0 if number<=15 else 1 if number==16 else 2
  require((0 if bh is None else len(bh))==expected_before,'FiniteActualFailureBudgetContinuity')
  if number in(15,16):
   old=[]if bh is None else bh;require(type(ah)is list and len(ah)==len(old)+1 and ah[1:]==old,'ExactlyOneActualAPIKeyFailureNoReset');own_clock(row,datetime.datetime.fromtimestamp(ah[0],datetime.timezone.utc).isoformat())
  else:require(bh==ah,'NoAPIKeyFailureBudgetConsumption')
  if previous is not None:require(previous==before,'SequentialNoticeStateContinuity')
  previous=after
  if number in(4,18,20):
   no_other_tables(ba,aa,(D,));require(before['notifications']==after['notifications'],'DismissNoNoticeMutation');old=before['dismissals'];new=[r for r in after['dismissals']if r not in old];require(all(r in after['dismissals']for r in old),'DismissRetainsOtherRows');targets=[1]if number==4 else[2]if number==18 else[n['id']for n in before['notifications']if n['is_active']and not any(d['user_id']==actor and d['notification_id']==n['id']for d in old)];require(len(new)==len(targets)and{r['notification_id']for r in new}==set(targets)and all(r['user_id']==actor and r['action_taken']is None for r in new),'ExactOwnMissingPairs');require(all(set(r)=={'id','user_id','notification_id','dismissed_at','action_taken'}for r in new),'ExactDismissalColumns')
   for r in new:own_clock(row,r['dismissed_at'])
   next_sequence(ba,aa,D,sorted(r['id']for r in new))
   if number==20:require(body=={'success':True,'dismissed_count':len(new)},'ExactDismissAllBody')
   else:require(body=={'success':True,'message':'Notification dismissed','notification_key':'credential.'+('A'if number==4 else'B')},'ExactDismissBody')
  elif number==9:
   no_other_tables(ba,aa,(N,));require(before['dismissals']==after['dismissals'],'CreateNoDismissalMutation');new=[r for r in after['notifications']if r not in before['notifications']];require(len(new)==1 and all(r in after['notifications']for r in before['notifications']),'ExactlyOneCreatedNotice');n=new[0];new_notice_id=n['id'];require({k:v for k,v in n.items()if k not in('id','created_at','updated_at')}=={'notification_key':'credential.created','title':'Hello','message':'World','notification_type':'info','priority':'normal','source':'system','action_data':{},'is_active':True,'admin_only':False,'expires_at':None},'CreateDefaults');own_clock(row,n['created_at']);own_clock(row,n['updated_at']);next_sequence(ba,aa,N,[new_notice_id]);exact_serialized(body,serialized(n,actor,after['dismissals']))
  elif number==10:
   no_other_tables(ba,aa,(N,));require(ba['sequences']==aa['sequences']and before['dismissals']==after['dismissals'],'PatchNoSequenceOrDismissal');new=next(r for r in after['notifications']if r['id']==1);old=next(r for r in before['notifications']if r['id']==1);wanted=[dict(r,updated_at=new['updated_at'])if r['id']==1 else r for r in before['notifications']];require(after['notifications']==wanted and clock(new['updated_at'])>clock(old['updated_at']),'EmptyPatchOnlyOwnUpdatedClock');own_clock(row,new['updated_at']);exact_serialized(body,serialized(new,actor,after['dismissals']))
  else:
   require(before==after and ba==aa,'NoMutationReadRefusalOrAuthentication')
   if spec['method']=='GET'and row['status']==200:
    notices=[n for n in after['notifications']if n['is_active']and(n['expires_at']is None or clock(n['expires_at'])>clock(row['began_utc']))and(not n['admin_only']or next(r['user_level']for r in plan['roles']if r['id']==actor)>=10)];notices.sort(key=lambda n:(n['priority'],clock(n['created_at'])),reverse=True);unread=[n for n in notices if not any(d['user_id']==actor and d['notification_id']==n['id']for d in after['dismissals'])]
    if ACTIONS[number-1]=='retrieve':exact_serialized(body,serialized(next(n for n in notices if n['id']==1),actor,after['dismissals']))
    elif ACTIONS[number-1]=='unread_count':require(body=={'unread_count':len(unread)},'ExactUnreadCount')
    else:
     selected=notices if'include_dismissed=true'in row['path']else unread;require(set(body)=={'notifications','count','unread_count'}and body['count']==body['unread_count']==len(selected)and len(body['notifications'])==len(selected),'ExactListCounts')
     for got,n in zip(body['notifications'],selected):exact_serialized(got,serialized(n,actor,after['dismissals']))
 return {'passed':True,'actual_Source_requests':20,'setup_actors':6,'real_auth_framework_only':True,'Source_TCP_or_Native_acceptance':False,'requests_without_budget_reset':True,'new_notice_id_observed':new_notice_id,'role_or_network_full_endpoint_coverage':False}
