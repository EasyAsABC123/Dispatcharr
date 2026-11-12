"""Pure offline48-case SQL/response/effect validator; no Django or Docker imports."""
import copy,datetime,hashlib,json
from seed_binding import validate_seed,expected_target
from fixture_guards import TABLES,dataset
NOTICE='core_systemnotification';DISMISS='core_notificationdismissal'
FIELDS={'id','notification_key','notification_type','priority','title','message','action_data','is_active','admin_only','expires_at','created_at','is_dismissed','source'}
WRITE=FIELDS-{'id','created_at','is_dismissed'}
def instant(v):return datetime.datetime.fromisoformat(v.replace('Z','+00:00')).astimezone(datetime.timezone.utc)
def render(v,prefix,seed):
    if isinstance(v,str):
        if v in ['@A.notification_key','@B.notification_key']:return seed[v[1]]['notification_key']
        return v.replace('@prefix',prefix)
    if isinstance(v,dict):return{k:render(x,prefix,seed)for k,x in v.items()}
    if isinstance(v,list):return[render(x,prefix,seed)for x in v]
    return v

def expected_update(before,payload):
    result=copy.deepcopy(before)
    for k,v in payload.items():
        if k not in WRITE:continue
        if k in ['notification_key','title','message']:v=str(v).strip()
        if k in ['is_active','admin_only']:
            if isinstance(v,str):v=v.lower()in ['true','t','yes','y','on','1']
            else:v=bool(v)
        if k=='expires_at'and v is not None:v=instant(v).isoformat()
        result[k]=v
    return result

def exact(a,b):return json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))

def equal_field(k,a,b):
    if k in ['created_at','expires_at','updated_at']and a is not None and b is not None:return instant(a)==instant(b)
    return type(a)is type(b)and exact(a,b)

def compare(actual,plan):
    checks=[]
    def check(ok,label):checks.append({'label':label,'passed':bool(ok)})
    try:
        check(actual['database'].startswith('rust_probe_notifmut_'),'owned-db-shape')
        from fixture_guards import database
        database(actual['database'])
        check(actual['forced_auth_framework_only']is True and actual['credentials_or_TCP_verified']is False,'forced-framework-scope')
        check(actual['capture_valid']is True and actual['actual_request_count']==48==len(actual['requests'])==len(plan['cases']),'complete48')
        check('fixture_outer_exception'not in actual,'no-fixture-outer-catch')
        check(actual['connections_close_called']is True,'connections-closed')
        check(actual['network_preflight']=={'admin':True,'standard':True,'adminDenied':False},'actual-network-boundaries')
        check(all(v is False for v in actual['client_raise_request_exception'].values()),'explicit-client-exception-policy')
        check(len(actual['actual_dependency_hashes'])==9,'actual-nine-dependencies')
        binding=validate_seed(actual,plan);aliases=binding['aliases'];seed=binding['seed'];actor_ids=binding['actors']
        check(actual['seed_binding_passed']is True and exact(actual['seed_binding'],binding),'exact-reviewed-seed-binding')
        check(actual['owned_rows_after_isolation']=={'notifications':[],'dismissals':[]},'owned-isolation-retained')
        previous=actual['seed_rows']
        for row,case in zip(actual['requests'],plan['cases']):
            tag=str(case['index'])+':'+case['name']+':'
            check(row['index']==case['index']and row['name']==case['name']and row['role']==case['actor']and row['method']==case['method'],'case-binding:'+tag)
            target_expected=expected_target(case,aliases)
            check(type(row['target_id'])is type(target_expected)and row['target_id']==target_expected,'target-bound-to-reviewed-alias:'+tag)
            check(row['path']=='/api/core/notifications/'+str(row['target_id'])+'/'and row['resolver']['kwargs']=={'pk':str(row['target_id'])}and row['resolver']['url_name']=='systemnotification-detail'and row['resolver']['namespaces']==['api','core'],'exact-resolved-path-and-kwargs:'+tag)
            check(row['resolver']['class']=='core.api_views.SystemNotificationViewSet'and row['resolver']['actions'][case['method'].lower()]==plan['candidate_methods'][case['method']],'actual-resolver:'+tag)
            check(row['client_raise_request_exception']is False and row['capture_complete']is True,'complete-explicit-policy:'+tag)
            check(exact(row['before'],previous),'cross-request-SQL-continuity:'+tag);previous=row['after']
            payload=render(case['body'],actual['seed_prefix'],seed)
            check(type(payload)is type(row['request_payload'])and exact(payload,row['request_payload']),'exact-payload:'+tag)
            a=row['before_named_audit'];b=row['after_named_audit'];dataset(a,actual['database']);dataset(b,actual['database'])
            check(a['sequences']==b['sequences'],'all-eleven-sequences-unchanged:'+tag)
            check(exact(row['before_users'],row['after_users']),'raw-user-rows-unchanged:'+tag)
            check(all(a['tables'][t]==b['tables'][t]for t in TABLES if t not in [NOTICE,DISMISS]),'other-nine-tables-unchanged:'+tag)
            for key,t in [('notifications',NOTICE),('dismissals',DISMISS)]:
                check(a['tables'][t]['rows']==len(row['before'][key])and b['tables'][t]['rows']==len(row['after'][key]),'raw-row-count-audit:'+key+':'+tag)
            for side in ['before','after']:
                cache=row[side+'_cache'];keys=[v['key_hex']for v in cache['rows']]
                check(len(keys)==len(set(keys))<=1000 and cache['capture_bytes']<=1048576,'bounded-raw-cache:'+side+':'+tag)
                check(all(len(bytes.fromhex(v['dump_hex']))<=65536 and type(v['pttl'])is int for v in cache['rows']),'cache-raw-DUMP-PTTL:'+side+':'+tag)
                check(set(row[side+'_condition_cache'])=={'H','V','U','D'},'named-cache-observation:'+side+':'+tag)
                runtime=row['runtime_'+side];check(not any(v['present']or v['manager_keys']for v in runtime['runtimes'].values()),'no-proxy-runtime:'+side+':'+tag)
            check(row['runtime_before']==row['runtime_after'],'receiver-state-unchanged:'+tag)
            check(instant(row['began_utc'])<=instant(row['completed_utc']),'request-clock-order:'+tag)
            transport=row['transport_kind'];check(transport in ['http_response','framework_exception'],'literal-transport-kind:'+tag)
            if row.get('framework_exception')is not None:
                exc=row['framework_exception'];check(exc['complete']is True,'complete-literal-exception:'+tag)
                for name,limit in [('message',4096),('traceback',65536)]:
                    e=exc[name+'_evidence'];captured=bytes.fromhex(e['captured_hex'])
                    check(len(captured)==e['captured_bytes']<=limit and hashlib.sha256(captured).hexdigest()==e['captured_sha256'],'exception-captured-byte-hash:'+name+':'+tag)
                    check(e['complete']is True and e['truncated']is False and e['original_bytes']==len(captured)and e['original_sha256']==e['captured_sha256'],'exception-no-silent-truncation:'+name+':'+tag)
                    check(captured.decode('utf-8')==exc[name],'exception-text-binds-raw-bytes:'+name+':'+tag)
            if transport=='http_response':
                raw=bytes.fromhex(row['body_hex']);check(len(raw)==row['body_bytes']<=65536 and hashlib.sha256(raw).hexdigest()==row['body_sha256'],'raw-body-binding:'+tag)
                try:parsed=json.loads(raw)
                except(ValueError,UnicodeDecodeError):parsed=None
                check(exact(parsed,row['json']),'JSON-derived-from-body:'+tag)
                check(row['actual_status']==case['predicted_status'],'status-hypothesis:'+tag)
            else:
                check(row.get('framework_exception')is not None,'literal-raised-exception:'+tag)
                check('actual_status'not in row and 'body_hex'not in row,'no-invented-HTTP-for-exception:'+tag)
                check(False,'returned-HTTP-status-hypothesis-not-observed:'+tag)
            before={x['id']:x for x in row['before']['notifications']};after={x['id']:x for x in row['after']['notifications']};effect=case['effect_class'];target=row['target_id']
            if effect=='none':
                check(exact(row['before'],row['after']),'refusal-SQL-exact:'+tag);check(a['tables']==b['tables'],'refusal-all-eleven-audits-exact:'+tag)
            elif effect in ['delete-cascade','delete-developer']:
                check(target in before and exact(after,{k:v for k,v in before.items()if k!=target}),'delete-only-target:'+tag)
                check(exact(row['after']['dismissals'],[x for x in row['before']['dismissals']if x['notification_id']!=target]),'cascade-only-target-children:'+tag)
                if transport=='http_response':check(row['body_hex']==''and row['body_bytes']==0,'204-empty-body:'+tag)
            else:
                check(set(before)==set(after)and target in before,'update-same-identities:'+tag)
                check(all(exact(v,after[k])for k,v in before.items()if k!=target),'update-other-notices-exact:'+tag)
                check(exact(row['before']['dismissals'],row['after']['dismissals'])and a['tables'][DISMISS]==b['tables'][DISMISS],'update-dismissals-exact:'+tag)
                expected=expected_update(before[target],payload);got=after[target]
                check(set(expected)==set(got),'target-column-set:'+tag)
                check(all(equal_field(k,v,got[k])for k,v in expected.items()if k!='updated_at'),'target-values-and-retained-fields:'+tag)
                check(instant(row['began_utc'])<=instant(got['updated_at'])<=instant(row['completed_utc']),'updated-at-own-request-clock:'+tag)
                if transport=='http_response'and row.get('actual_status')==200:
                    body=row['json'];check(type(body)is dict and set(body)==FIELDS,'serializer-field-surface:'+tag)
                    check(all(equal_field(k,body[k],got[k])for k in FIELDS if k!='is_dismissed'),'response-saved-row:'+tag)
                    dismissed=any(x['notification_id']==target and x['user_id']==actor_ids[case['actor']]for x in row['after']['dismissals'])
                    check(body['is_dismissed']is dismissed,'response-current-actor-dismissal:'+tag)
            if case['name']=='global-developer-shape-error-before-target-lookup':
                exc=row.get('framework_exception');check(exc is not None and exc['type']=='builtins.AttributeError'and 'get'in exc['message'],'literal-developer-shape-error:'+tag)
        check(actual['requests'][-1]['index']==47,'finite-final-index')
    except BaseException as exc:
        check(False,'malformed-or-partial-capture:'+type(exc).__name__+':'+str(exc)[:200])
    return {'passed':all(x['passed']for x in checks),'checks':len(checks),'failures':[x['label']for x in checks if not x['passed']],'details':checks,'scope':'offline48-request framework predictions and eleven named SQL/cache observations; not deployment/credentials/full database','raw_cache_policy':'all raw DUMP/PTTL retained; TTL equality not asserted','automatic_retry':False}
