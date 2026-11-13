"""Pure SQL/clock/body ownership checks; synthetic controls cannot qualify Main."""
import datetime
import hashlib
import math

NOTICE='core_systemnotification'
DISMISS='core_notificationdismissal'
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings',NOTICE,DISMISS}


def utc(value):
    if type(value)is not str:raise ValueError('LiteralUTCClock')
    d=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None or d.utcoffset()!=datetime.timedelta():raise ValueError('UTCClockRequired')
    return d


def audit(value):
    if type(value)is not dict or set(value)!= {'tables','sequences'}:raise ValueError('LiteralSQLSnapshot')
    if type(value['tables'])is not dict or set(value['tables'])!=TABLES:raise ValueError('AllElevenNamedTables')
    if type(value['sequences'])is not dict or set(value['sequences'])!=TABLES:raise ValueError('AllElevenSequences')
    for key,rows in value['tables'].items():
        if type(rows)is not list or len(rows)>10000:raise ValueError('BoundedNamedRows')
    for name,s in value['sequences'].items():
        if type(s)is not dict or set(s)!={'last_value','is_called'} or type(s['last_value'])is not int or type(s['is_called'])is not bool:raise ValueError('LiteralSequenceState')
    return value


def untouched(before,after,except_tables=()):
    audit(before);audit(after)
    if set(before['tables'])!=set(after['tables']) or set(before['sequences'])!=set(after['sequences']):raise ValueError('ExactNamedAuditSet')
    for table in before['tables']:
        if table not in except_tables and before['tables'][table]!=after['tables'][table]:raise ValueError('UnrelatedSQLMutation:'+table)
    for table in before['sequences']:
        if table not in except_tables and before['sequences'][table]!=after['sequences'][table]:raise ValueError('UnrelatedSequenceMutation:'+table)


def validate(value,policy):
    if value.get('actual_SQL_Bearer_roles_verified')is not True:raise ValueError('ActualNormalBearerRoles')
    auth=value.get('actual_auth_requests')
    if type(auth)is not dict or set(auth)!={'admin','standard'}:raise ValueError('MandatoryRealAuthRequestCoverage')
    for role,row in auth.items():
        import json
        if row.get('status')!=200 or json.loads(bytes.fromhex(row['body_hex'])).get('id')!=value[role+'_user_id'] or row.get('physical_close',{}).get('fd_after')!=-1 or row['physical_close'].get('original_inode_absent')is not True or row.get('absolute_observation',{}).get('timer_joined')is not True:raise ValueError('SQLLinkedActualCredentialRequest')
    rows=value.get('cases')
    if type(rows)is not list or [x.get('name')for x in rows]!=[x['name']for x in policy['cases']]:raise ValueError('AllFixedCases')
    started,ended=value.get('phase_started'),value.get('phase_completed')
    if any(type(t)not in (int,float)or not math.isfinite(t)for t in (started,ended))or not 0<started<=ended<=started+90:raise ValueError('FinitePhase90')
    prior=started;last=None;new_id=None;dismissal_id=None
    for row,expected in zip(rows,policy['cases']):
        begin,end=row.get('started'),row.get('completed')
        if any(type(t)not in (int,float)or not math.isfinite(t)for t in (begin,end))or not prior<=begin<=end<=ended or end-begin>8:raise ValueError('AbsoluteRequest8')
        prior=end
        if row.get('status')!=expected['status']or row.get('transport_error'):raise ValueError('FixedLiteralStatus')
        if row.get('physical_close',{}).get('fd_after')!=-1 or row['physical_close'].get('original_inode_absent')is not True or row.get('absolute_observation',{}).get('timer_joined')is not True:raise ValueError('PhysicalHTTPClosure')
        raw=bytes.fromhex(row['body_hex'])
        if len(raw)>65536 or hashlib.sha256(raw).hexdigest()!=row['body_sha256']:raise ValueError('ExactBoundedBody')
        import json
        body=None if row['status']==204 and raw==b'' else json.loads(raw)
        before,after=audit(row['before']),audit(row['after'])
        if last is not None and before!=last:raise ValueError('NoInterveningSequenceOrRowMutation')
        last=after;name=row['name']
        if name=='admin-create':
            untouched(before,after,(NOTICE,))
            new=[x for x in after['tables'][NOTICE]if x not in before['tables'][NOTICE]]
            if len(new)!=1 or len(after['tables'][NOTICE])!=len(before['tables'][NOTICE])+1:raise ValueError('ExactlyOneCreatedNotice')
            new=new[0];new_id=new['id']
            if type(new_id)is not int or new_id<=0:raise ValueError('ActualSQLNoticeID')
            defaults={'notification_key':value['owned_notification_key'],'notification_type':'info','priority':'normal','source':'system','title':'Hello','message':'World','action_data':{},'is_active':True,'admin_only':False,'expires_at':None}
            if any(new.get(k)!=v for k,v in defaults.items()):raise ValueError('Source8CreateDefaults')
            projected={**defaults,'id':new_id,'created_at':new['created_at'],'is_dismissed':False}
            if set(body)!=set(projected)or any(body[k]!=v for k,v in projected.items()if k!='created_at')or utc(body['created_at'])!=utc(new['created_at']):raise ValueError('BodyLinksExactOwnSQLNotice')
            if not utc(row['began_utc'])<=utc(new['created_at'])<=utc(new['updated_at'])<=utc(row['completed_utc']):raise ValueError('OwnCreationCausalClock')
            before_seq,after_seq=before['sequences'][NOTICE],after['sequences'][NOTICE]
            expected_next=before_seq['last_value']+int(before_seq['is_called'])
            if new_id!=expected_next or after_seq!={'last_value':new_id,'is_called':True}:raise ValueError('ExactlyOneNoticeSequenceUse')
        elif name=='standard-dismiss':
            untouched(before,after,(DISMISS,))
            new=[x for x in after['tables'][DISMISS]if x not in before['tables'][DISMISS]]
            if len(new)!=1 or len(after['tables'][DISMISS])!=len(before['tables'][DISMISS])+1:raise ValueError('ExactlyOneDismissal')
            new=new[0];dismissal_id=new['id']
            if new['notification_id']!=new_id or new['user_id']!=value['standard_user_id']or new['action_taken']is not None:raise ValueError('OwnUserNoticeDismissal')
            if not utc(row['began_utc'])<=utc(new['dismissed_at'])<=utc(row['completed_utc']):raise ValueError('OwnDismissalClock')
            b=before['sequences'][DISMISS]
            if dismissal_id!=b['last_value']+int(b['is_called'])or after['sequences'][DISMISS]!={'last_value':dismissal_id,'is_called':True}:raise ValueError('ExactlyOneDismissalSequenceUse')
        elif name=='standard-truthy-repeat':
            untouched(before,after,(DISMISS,))
            if before['sequences']!=after['sequences']:raise ValueError('TruthyRepeatNoSequence')
            expected_rows=[dict(x,action_taken='accepted')if x['id']==dismissal_id else x for x in before['tables'][DISMISS]]
            if after['tables'][DISMISS]!=expected_rows:raise ValueError('TruthyRepeatOnlyActionTaken')
        elif name=='empty-patch':
            untouched(before,after,(NOTICE,))
            if before['sequences']!=after['sequences']:raise ValueError('PatchNoSequence')
            old=[x for x in before['tables'][NOTICE]if x['id']==new_id]
            new=[x for x in after['tables'][NOTICE]if x['id']==new_id]
            if len(old)!=1 or len(new)!=1:raise ValueError('OwnPatchTarget')
            old,new=old[0],new[0]
            if not utc(row['began_utc'])<=utc(new['updated_at'])<=utc(row['completed_utc']) or utc(new['updated_at'])<=utc(old['updated_at']):raise ValueError('OwnPatchClockAdvance')
            expected_rows=[dict(x,updated_at=new['updated_at'])if x['id']==new_id else x for x in before['tables'][NOTICE]]
            if after['tables'][NOTICE]!=expected_rows:raise ValueError('EmptyPatchOnlyUpdatedAt')
            projected={k:v for k,v in new.items()if k!='updated_at'};projected['is_dismissed']=False
            if set(body)!=set(projected)or any(body[k]!=v for k,v in projected.items()if k!='created_at')or utc(body['created_at'])!=utc(new['created_at']):raise ValueError('PatchOwnSavedSnapshot')
        elif name=='cascade-delete':
            untouched(before,after,(NOTICE,DISMISS))
            if before['sequences']!=after['sequences']:raise ValueError('DeleteNoSequence')
            if after['tables'][NOTICE]!=[x for x in before['tables'][NOTICE]if x['id']!=new_id]:raise ValueError('DeleteOnlyTarget')
            if after['tables'][DISMISS]!=[x for x in before['tables'][DISMISS]if x['notification_id']!=new_id]:raise ValueError('DeleteOnlyTargetDismissalCascade')
            if raw!=b''or body is not None:raise ValueError('DeleteEmpty204')
        elif before!=after:raise ValueError('NoMutation:'+name)
        if name=='required-put'and body!={k:['This field is required.']for k in ('notification_key','title','message')}:raise ValueError('PutExactRequiredFields')
        if name=='duplicate-create'and body!={'notification_key':['system notification with this notification key already exists.']}:raise ValueError('DefinitionDerivedDuplicateError')
        if name in ('standard-dismiss','standard-empty-repeat','standard-truthy-repeat')and body!={'success':True,'message':'Notification dismissed','notification_key':value['owned_notification_key']}:raise ValueError('ExactDismissBody')
    return {'normal_Bearer_Main_notification_slice_qualified':True,'Source8_framework_not_TCP':True,'Native_definition_extra_cases':True,'Source48_bounded_JSON_mutation_rules':True,'Source_credential_TCP_parity':False,'physical_SQL_concurrency_cancel_qualified':False,'full_endpoint_or_Source48_parity':False,'performance_acceptance':False}
