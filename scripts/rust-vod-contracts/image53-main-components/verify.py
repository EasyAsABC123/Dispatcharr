"""Independent bounded retained Main53 component validators; no app or network work."""
import contextlib
import copy
import hashlib
import importlib
import importlib.util
import json
import math
from pathlib import Path
import re
import sys
HERE=Path(__file__).resolve().parent
IMAGE='sha256:3f599a036bf0ba15f7d47f4f28016777fba9586cb328afd10a25cac16293f4f0'
APP_REV='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'
AUTHORITY_SHA='4dc353237798c4d41e6c880641751c3eef568f0bc1ec6c23b42a6451ed11c761'
def require(ok,label):
    if not ok:raise ValueError(label)
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def read(path):
    p=Path(path);require(p.is_file()and not p.is_symlink()and p.stat().st_size<=4194304,'BoundedRegularJSON');return json.loads(p.read_bytes())
def hex64(x):return type(x)is str and re.fullmatch('[0-9a-f]{64}',x)is not None
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
@contextlib.contextmanager
def tools(which):
    names=['qualify','identity','origin_coverage','source_reference'];saved={x:sys.modules.get(x)for x in names};old=list(sys.path)
    for x in names:sys.modules.pop(x,None)
    sys.path.insert(0,str(HERE/'inputs'/which/'blocked/tools'))
    try:yield importlib.import_module('qualify')
    finally:
        for x in names:
            sys.modules.pop(x,None)
            if saved[x]is not None:sys.modules[x]=saved[x]
        sys.path[:]=old

def cleanup(remaining,root,project):
    require(remaining=={'containers':'','networks':'','volumes':''},'OwnedProjectEmpty')
    require(set(root)=={'container','network','volume'},'AllThreeIndependentInventories')
    for row in root.values():require(row.get('exit')==0 and row.get('stdout')==row.get('stderr')==''and 'label=com.docker.compose.project='+project in row.get('argv',[]),'IndependentOwnedInventory')

def auth_projection(row,expected):
    require(row.get('status')==200 and type(expected)is int and row.get('auth_response_user_id')==row.get('expected_real_SQL_user_id')==expected and row.get('auth_response_user_level')==10 and row.get('auth_response_stream_limit')==0,'SQLBearerProjection')
    require(row.get('physical_close',{}).get('fd_after')==-1 and row['physical_close'].get('original_inode_absent')is True and row.get('absolute_observation',{}).get('timer_joined')is True,'AuthPhysicalClosure')
    require(hex64(row.get('private_auth_raw_sha256'))and hex64(row.get('private_seed_sha256')),'AuthPrivateBindings')

def header(source,failed,native,summary,source_auth,native_auth,stderr,root):
    policy=read(HERE/'inputs/header-native/blocked/policy.json')
    with tools('header-native')as q:
        reference=importlib.import_module('source_reference').validate(source,failed,source_auth,stderr,policy)
        result=q.phase(native,policy,'native')
        require(native.get('qualification')==result,'RecordedHeaderQualifierAgreement')
        origin=importlib.import_module('origin_coverage');origin.validate(summary['origin_terminal'],summary['fixture_nonce'],terminal=True)
    require(summary.get('passed')is True and summary.get('Source_VOD_requests_rerun')is False and summary.get('native')==native and summary.get('candidate',{}).get('image_id')==IMAGE,'ActualNativeOnlySummary')
    require(summary.get('retained_Source6_offline_reference')==reference and summary.get('resource_sampler')is False and summary.get('full_M4_or_header_parity')is False,'HeaderScopePreserved')
    audit=load(HERE/'main4s/validate.py','main53_header_audit')
    audit.audit(summary['default_before'],True);audit.audit(summary['default_after'],True);require(summary['default_before']==summary['default_after']and summary.get('default_named_tables_sequences_unchanged')is True,'HeaderDefaultNineExact')
    auth_projection(native_auth,native_auth.get('expected_real_SQL_user_id'))
    require(native['phase_started']<=native_auth['request_started']<=native_auth['request_completed']<=native['cases'][0]['request_started'],'ActualAuthBeforeBranches')
    require(root.get('passed')is True and root.get('actual_controller_exit')==0,'RootHeaderReview');cleanup(summary['remaining'],root['owned_cleanup_queries'],summary['project'])
    require(result['Native_invalid_HEAD']==405 and result['invalid_HEAD_compatibility_open']is True and result['Source_invalid_HEAD_observed']is False,'LiteralNative405DiagnosticNotSourceProof')
    return {'passed':True,'Source6_offline':reference,'Native_routed_statuses':[r['status']for r in native['cases'][:-1]],'Native_invalid_HEAD':405,'Source_invalid_HEAD_measured':False,'original_Source7_exit':1,'Native_exit':0,'unrouted_missing_SPA_template_excluded':True,'full_M4_or_header_or_route_parity':False}

def notification(phase,summary,root,negative=False):
    policy=read(HERE/'inputs/notification-fixed/blocked/policy.json')
    projected=copy.deepcopy(phase)
    roles=phase.get('actual_auth_requests');require(type(roles)is dict and set(roles)=={'admin','standard'},'MandatoryCredentialRoles')
    for role,row in roles.items():
        identity=row.get('identity_projection');require(type(identity)is dict and set(identity)=={'id','user_level','stream_limit'}and identity['id']==phase[role+'_user_id']and identity['user_level']==(10 if role=='admin'else 1)and identity['stream_limit']==0,'ActualRoleIdentityProjection')
        require(hex64(row.get('private_full_body_sha256'))and hex64(row.get('private_full_auth_response_sha256'))and 'body_hex'not in row,'PrivateAuthBodyNotPublished')
        # Adapter for the retained qualifier's ID-only auth test; these bytes
        # are explicitly synthetic validation input, NOT actual HTTP body bytes.
        projected['actual_auth_requests'][role]['body_hex']=json.dumps({'id':identity['id']}).encode().hex()
    for row in phase['cases']:
        for state in (row['before'],row['after']):
            users=state['tables'].get('accounts_user');require(type(users)is list and {u.get('id')for u in users}=={phase['admin_user_id'],phase['standard_user_id']},'SQLBothActualUsersRequired')
            for user in users:
                require(user.get('user_level')==(10 if user['id']==phase['admin_user_id']else 1),'SQLRoleLevel')
                for key in ('password','api_key'):require(type(user.get(key))is dict and set(user[key])=={'private_field_sha256'}and hex64(user[key]['private_field_sha256']),'PrivateSQLSecretDigestOnly')
    with tools('notification-fixed')as q:
        result=q.validate(projected,policy);q.audit(summary['default_before']);q.audit(summary['default_after'])
        require(phase.get('qualification')==result,'RecordedNotificationQualifierAgreement')
    require(summary.get('candidate',{}).get('image_id')==IMAGE and summary.get('actual_Main_image_id')==IMAGE and summary.get('actual_Main_Config_User')=='65532:65532','NormalMainImageUID')
    require(summary.get('Source8_is_framework_not_TCP')is True,'Source8FrameworkScope')
    require(summary.get('Source_notification_requests_executed')is False and summary.get('default_workers_activated')is False and summary.get('resource_sampler')is False,'NoSourceWorkerResourceActivation')
    before,after=summary['default_before'],summary['default_after'];changed=[k for k in before['tables']if before['tables'][k]!=after['tables'][k]];seq=[k for k in before['sequences']if before['sequences'][k]!=after['sequences'][k]]
    if negative:
        require(summary.get('passed')is False and summary.get('default_eleven_named_tables_sequences_unchanged')is False and root.get('actual_controller_exit')==1 and root.get('default_audit_passed')is False,'PreserveNotificationFirstFailure')
        require(changed==['core_systemnotification']and seq==[],'ExactRetainedDefaultNegativeScope')
        a,b=before['tables']['core_systemnotification'],after['tables']['core_systemnotification'];require(len(a)==len(b)==1 and [k for k in a[0]if a[0][k]!=b[0][k]]==['updated_at'],'OnlyBuiltinUpdatedAtNegative')
        cleanup(summary['remaining'],root['queries'],summary['project'])
    else:
        require(summary.get('passed')is True and summary.get('default_eleven_named_tables_sequences_unchanged')is True and before==after and root.get('actual_controller_exit')==0 and root.get('passed')is True,'SecondDefaultElevenExact')
        cleanup(summary['remaining'],root['own_cleanup_queries'],summary['project'])
    return {'request_slice_qualified':result['normal_Bearer_Main_notification_slice_qualified'],'statuses':[r['status']for r in phase['cases']],'actual_exit':1 if negative else 0,'default_audit_qualified':not negative,'changed_default_tables':changed,'full_M5_or_Source48_or_performance':False,'Source8_framework_only_and_definition_extras_separate':True}

def once_receipts(receipts,closures):
    require(set(receipts)==set(closures),'AllFiveHistoricalAttempts')
    for name,data in receipts.items():
        cue,claim=data['cue'],data['claim'];a,b=cue['projection'],claim['projection'];require(hex64(cue['raw_sha256'])and hex64(claim['raw_sha256']),'RawReceiptHashes')
        expected_action={'main4s':'image53-main-four-second-physical-once','header-failed':'image53-worker-header-main-branches-once','header-native':'image53-worker-header-native-routed-once','notification-failed':'image53-notification-main-create-repeat-once','notification-fixed':'image53-notification-main-create-repeat-once'}[name]
        require(a.get('authorized')is True and a.get('action')==expected_action and a.get('candidate_image')==IMAGE and a.get('git_revision')==APP_REV and a.get('Root_reviewed_Rust_input_count')==791,'ExactHistoricalActionCandidate')
        require(b['cue_sha256']==cue['raw_sha256']and b['selection_sha256']==a['selection_sha256']==closures[name]['blocked_selection_sha256'],'HistoricalClaimInputBinding')
        require(a['issued_epoch']<=b['claimed_epoch']<a['expires_epoch']and a['boot_id_sha256']==b['boot_id_sha256']and b['no_retry']is True,'HistoricalClockBootOnce')
        require('boot_id'not in a and 'boot_id'not in b,'NoBootValueCopied')
    return {'historical_attempts':5,'consumed_once_provenance_verified':True,'new_authorization_granted':False}

def startup_fix():
    before=(HERE/'inputs/notification-failed/blocked/tools/db.py').read_text()
    after=(HERE/'inputs/notification-fixed/blocked/tools/db.py').read_text()
    old="    import django;django.setup()\n"
    replacement="""    # Use Original's real management-command startup path for EVERY helper stage.
    # The helper action remains argv[1]; append only an exact shell marker.
    if 'shell' not in sys.argv[1:]:sys.argv.append('shell')
    from dispatcharr.app_initialization import should_skip_initialization
    if should_skip_initialization() is not True:raise ValueError('OriginalManagementSkipBeforeSetup')
    import django;django.setup()
    if should_skip_initialization() is not True:raise ValueError('OriginalManagementSkipAfterSetup')
"""
    require(before.count(old)==1 and before.replace(old,replacement)==after,'ExactStartupOnlyFix')
    for rel in ['policy.json','tools/phase.py','tools/qualify.py']:
        require((HERE/'inputs/notification-failed/blocked'/rel).read_bytes()==(HERE/'inputs/notification-fixed/blocked'/rel).read_bytes(),'UnchangedNotificationWorkload:'+rel)
    return {'only_helper_management_startup_changed':True,'timestamp_restore_or_audit_mask':False,'request_phase_qualifier_and_policy_unchanged':True}

def verify():
    authority=HERE/'projection-authority.json';require(sha(authority)==AUTHORITY_SHA,'ExactProjectionAuthority')
    app=read(HERE/'application.json');require(app['image_id']==IMAGE and app['git_revision']==APP_REV and app['runtime_inputs']==791 and app['all_app_inputs']==len(app['source_sha256'])==1288,'Exact53Application')
    require(canonical(app['source_sha256'])==app['source_tree_sha256'],'AppTreeDigest')
    m=load(HERE/'main4s/validate.py','main53_independent_main4s');mr=m.validate(read(HERE/'main4s/retained.json'),read(HERE/'main4s/Root-independent-cleanup-review.json'),read(HERE/'main4s/coverage-authority.json'))
    h=HERE/'worker-header';hr=header(read(h/'source-phase.json'),read(h/'source-failed-summary.json'),read(h/'native-phase.json'),read(h/'native-summary.json'),read(h/'auth-identity-projection.json'),read(h/'native-auth-identity-projection.json'),(h/'source-web.stderr').read_text(),read(h/'Root-independent-review.json'))
    n=HERE/'notifications';nr=[notification(read(n/('attempt'+str(i)+'-phase.json')),read(n/('attempt'+str(i)+'-summary.json')),read(n/('attempt'+str(i)+'-Root-review.json')),negative=i==1)for i in(1,2)]
    startup=startup_fix()
    receipts=once_receipts(read(HERE/'historical-once-receipts.json'),read(HERE/'tool-closures.json'))
    for name,expected in read(authority)['projections'].items():require(sha(HERE/name)==expected,'ExactRetainedProjection:'+name)
    return {'passed':True,'main4s':mr,'worker_header':hr,'notifications':nr,'once_receipts':receipts,'notification_startup_fix':startup,'full_M4':False,'full_M5':False,'full_body_header_timer_route_parity':False,'performance_acceptance':False,'application_rerun':False}

if __name__=='__main__':print(json.dumps(verify(),indent=2))
