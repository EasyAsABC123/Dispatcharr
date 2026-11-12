"""Pure proposed effects comparator. Synthetic controls are never Source results."""
import datetime,hashlib,json
from fixture_guards import database,dataset,TABLES
BASE='/api/core/notifications/'
CASES=(('anonymous-list','anonymous','GET',401),('standard-list','standard','GET',200),('standard-create-denied','standard','POST',403),('standard-dismiss','standard','POST',200),('standard-dismiss-repeat','standard','POST',200),('standard-list-after-dismiss','standard','GET',200),('admin-list-independent','admin','GET',200),('admin-create-positive','admin','POST',201))
NOTICE='core_systemnotification';DISMISS='core_notificationdismissal'
def moment(value):
    parsed=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('TimezoneRequired')
    return parsed
def serialized_notice(row):
    return {k:v for k,v in row.items() if k!='updated_at'}|{'is_dismissed':False}
def equal_notice_json(actual,row):
    expected=serialized_notice(row)
    if set(actual)!=set(expected):return False
    return all(moment(actual[k])==moment(v) if k=='created_at' else actual[k]==v for k,v in expected.items())
def compare(report):
    """Return all observed mismatches, preserving raw caller input unchanged."""
    checks=[]
    def check(ok,label):checks.append({'check':label,'passed':bool(ok)})
    try:
        owned=database(report['database']);seed=report['seed'];rows=report['requests'];notice_id=seed['notification_id'];user_id=seed['standard_user_id'];admin_id=seed['admin_user_id']
        check(report['capture_valid']is True and report['actual_request_count']==8 and len(rows)==8 and not report.get('error_type'),'complete-eight-capture')
        check(report['connections_close_called']is True and not report.get('connection_close_error'),'connections-closed')
        check(seed['standard_user_level']==1 and seed['admin_user_level']==10 and user_id!=admin_id,'real-explicit-role-seed')
        check(report['no_network_setting_mutation']is True,'network-policy-unpatched')
        check(report['resolver']['list']['path']==BASE and report['resolver']['list']['actions'].get('get')=='list' and report['resolver']['list']['actions'].get('post')=='create','list-resolver')
        detail=BASE+str(notice_id)+'/dismiss/'
        check(report['resolver']['dismiss']['path']==detail and report['resolver']['dismiss']['actions'].get('post')=='dismiss','dismiss-resolver')
        wrong=report['wrong_path_control'];check((wrong['resolved']is False and wrong['Resolver404']is True)or(wrong['resolved']is True and wrong['is_notification_view']is False),'wrong-path-control-not-notification')
        seed_rows=report['seed_rows'];check(len(seed_rows['notifications'])==1 and seed_rows['dismissals']==[],'isolated-one-notice')
        original=seed_rows['notifications'][0];check(original['id']==notice_id and original['notification_key']=='test.notif' and original['title']=='Hello' and original['message']=='World','original-test-seed')
        defaults={'notification_type':'info','priority':'normal','source':'system','action_data':{},'is_active':True,'admin_only':False,'expires_at':None}
        check(all(original[k]==v for k,v in defaults.items()),'seed-model-defaults')
        previous=seed_rows;previous_audit=report['setup_after_seed_named_audit']
        dataset(previous_audit,owned)
        for index,(row,expected) in enumerate(zip(rows,CASES)):
            name,role,method,status=expected;tag=name+':'
            check((row['name'],row['role'],row['method'])==(name,role,method),tag+'fixed-request-identity')
            check(row['path']==(detail if index in (3,4) else BASE) and row['follow_redirects']is False,tag+'exact-path-no-redirect')
            check(row['resolver_action']==('dismiss' if index in (3,4) else 'list' if method=='GET' else 'create'),tag+'actual-resolver-action')
            payload=None if method=='GET' else {} if index in (3,4) else {'notification_key':'evil','title':'Phish','message':'Click','is_active':True} if index==2 else {'notification_key':'owned.created.'+owned.removeprefix('rust_probe_notif_'),'title':'Hello','message':'World','is_active':True}
            check(row['request_payload']==payload,tag+'request-payload')
            check(row['actual_status']==status,tag+'predicted-status-compared-to-actual')
            body=bytes.fromhex(row['body_hex']);check(row['body_capture_complete']is True and len(body)==row['body_bytes']<=65536 and hashlib.sha256(body).hexdigest()==row['body_sha256'],tag+'complete-body-hash')
            check(json.loads(body)==row['json'],tag+'parsed-body-bound-to-bytes')
            check(moment(row['began_utc'])<=moment(row['completed_utc']),tag+'own-clock-order')
            before,after=row['before'],row['after'];check(before==previous,tag+'row-chain')
            a,b=row['before_named_audit'],row['after_named_audit'];dataset(a,owned);dataset(b,owned)
            check(all(observed['tables'][table]['rows']==len(state[key]) for observed,state in ((a,before),(b,after)) for table,key in ((NOTICE,'notifications'),(DISMISS,'dismissals'))),tag+'captured-row-counts-match-named-audit')
            check(a==previous_audit,tag+'named-audit-chain')
            check(all(a['tables'][t]==b['tables'][t] for t in TABLES if t not in (NOTICE,DISMISS)),tag+'other-nine-tables-unchanged')
            check(all(a['sequences'][k]==b['sequences'][k] for k in a['sequences'] if k not in ('public.'+NOTICE+'_id_seq','public.'+DISMISS+'_id_seq')),tag+'other-nine-sequences-unchanged')
            if index!=7:check(before['notifications']==after['notifications'],tag+'notice-rows-unchanged')
            if index not in (3,):check(before['dismissals']==after['dismissals'],tag+'dismissal-rows-unchanged')
            allowed=DISMISS if index==3 else NOTICE if index==7 else None
            check(all(a['tables'][t]==b['tables'][t] for t in TABLES if t!=allowed),tag+'all-unassigned-table-effects-unchanged')
            check(all(a['sequences'][k]==b['sequences'][k] for k in a['sequences'] if k!='public.'+str(allowed)+'_id_seq'),tag+'all-unassigned-sequence-effects-unchanged')
            # Sequence values are captured before/after, never invented/reset.
            if index not in (3,7):check(a['sequences']==b['sequences'],tag+'observed-sequences-unchanged')
            if allowed is not None:
                key='public.'+allowed+'_id_seq';prior=a['sequences'][key];observed=b['sequences'][key]
                expected_next=prior['last_value']+(1 if prior['is_called'] else 0)
                generated=after['dismissals'][0]['id'] if index==3 else next(v['id']for v in after['notifications']if v['id']!=notice_id)
                check(observed=={'last_value':expected_next,'is_called':True} and generated==expected_next,tag+'actual-generated-ID-sequence-transition')
            if index in (1,6):
                value=row['json'];check(set(value)=={'notifications','count','unread_count'} and value['count']==value['unread_count']==1 and len(value['notifications'])==1 and equal_notice_json(value['notifications'][0],original),tag+'list-visible-user-not-dismissed')
            if index in (3,4):
                check(row['json']=={'success':True,'message':'Notification dismissed','notification_key':'test.notif'},tag+'dismiss-response')
                check(len(after['dismissals'])==1,tag+'one-dismissal')
                dismissal=after['dismissals'][0]
                check(dismissal['user_id']==user_id and dismissal['notification_id']==notice_id and dismissal['action_taken']is None,tag+'dismissal-owned-identity')
                if index==3:check(moment(row['began_utc'])<=moment(dismissal['dismissed_at'])<=moment(row['completed_utc']),tag+'dismissal-clock')
            if index==5:check(row['json']=={'notifications':[],'count':0,'unread_count':0},tag+'standard-filtered-after-dismiss')
            if index==7:
                added=[v for v in after['notifications'] if v['id']!=notice_id]
                check(len(after['notifications'])==2 and original in after['notifications'] and len(added)==1,tag+'one-created-notice')
                created=added[0]
                check(all(created[k]==v for k,v in defaults.items()) and created['notification_key']==payload['notification_key'] and created['title']=='Hello' and created['message']=='World',tag+'created-actual-model-defaults')
                check(moment(row['began_utc'])<=moment(created['created_at'])<=moment(row['completed_utc']) and moment(row['began_utc'])<=moment(created['updated_at'])<=moment(row['completed_utc']),tag+'created-own-clock')
                check(equal_notice_json(row['json'],created),tag+'create-response-matches-SQL')
            for phase in ('before_owned_cache','after_owned_cache'):
                cache=row[phase];check(isinstance(cache['rows'],list) and len(cache['rows'])<=1000 and 0<=cache['capture_bytes']<=1048576,tag+phase+'-bounded-observation')
                for item in cache['rows']:
                    check(len(bytes.fromhex(item['key_hex']))<=1024 and len(bytes.fromhex(item['dump_hex']))<=65536 and type(item['pttl'])is int,tag+phase+'-raw-entry')
            previous,previous_audit=after,b
    except (KeyError,TypeError,ValueError,IndexError,AttributeError) as error:
        checks.append({'check':'malformed-or-incomplete-observation','passed':False,'error_type':type(error).__name__,'detail':str(error)[:512]})
    return {'passed':bool(checks) and all(c['passed'] for c in checks),'checks':checks,'actual_Source_result':False,'input_origin':'unverified until reviewed controller binds actual capture','new_coverage_registered':False,'scope':'Only eight resolver/APIClient forced-auth Source observations if actual capture qualifies; no credentials/TCP/worker/Native/full-role acceptance','unexpected_outcomes_retained':True}
