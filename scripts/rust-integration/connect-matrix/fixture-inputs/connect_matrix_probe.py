"""Capture all representative Connect cells before reporting scoped parity."""
import hashlib
from http.client import HTTPConnection
import json
from urllib.parse import urlsplit

from connect_matrix_contract import (
    ALLOW, DENIALS, MUTATIONS, ROLES, ROUTES, SIDES, TABLES, VARIANTS, cell, payload, phase, route_path, symbolic_path,
)


def checksum(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def credentials(role,accounts):
    if role=='anonymous':
        return {}
    if role=='admin_api_key':
        return {'X-API-Key':accounts['admin']['api_key']}
    slot={'inactive_admin':'inactive','admin_jwt':'admin'}.get(role,role)
    return {'Authorization':'Bearer '+accounts[slot]['access']}


def wire(endpoint,url,method,body,headers):
    base=urlsplit(endpoint)
    if base.scheme!='http' or base.hostname!='127.0.0.1' or not base.port or base.path:
        raise ValueError('Owned Connect HTTP listener required')
    if not url.startswith('/api/connect/') or '..' in url.split('/'):
        raise ValueError('Exact scoped Connect method required')
    conn=HTTPConnection(base.hostname,base.port,timeout=3)
    raw=None if body is None else json.dumps(body,separators=(',',':')).encode()
    fields={'Accept':'application/json',**headers}
    if raw is not None:
        fields['Content-Type']='application/json'
    try:
        conn.request(method,url,body=raw,headers=fields)
        response=conn.getresponse()
        actual=response.read(1024*1024+1)
        if len(actual)>1024*1024:
            raise ValueError('Bounded response size')
        mime=(response.getheader('content-type') or '').split(';',1)[0].lower()
        try:
            decoded=json.loads(actual)
        except (UnicodeError, ValueError):
            decoded=None
        return {'status':response.status,'json':decoded,'mime':mime,'body_bytes':len(actual),
                'body_sha256':hashlib.sha256(actual).hexdigest(),
                'authentication_challenge_present':bool(response.getheader('www-authenticate'))}
    finally:
        conn.close()


def by_id(rows,identifier):
    return next((row for row in rows if row['id']==identifier),None)


def subtree(data,identifier):
    integration=by_id(data['integrations'],identifier)
    subscriptions=[row for row in data['subscriptions'] if row['integration']==identifier]
    ids={row['id'] for row in subscriptions}
    return {'integration':integration,'subscriptions':subscriptions,
            'logs':[row for row in data['logs'] if row['subscription_id'] in ids]}


def expected_subtree(route,before,after,body,allowed):
    desired={**before,'subscriptions':list(before['subscriptions']),'logs':list(before['logs'])}
    if not allowed or route.key not in MUTATIONS:
        return desired
    if route.key=='integration-patch':
        desired['integration']={**before['integration'],**body}
    elif route.key=='subscription-create':
        if len(after['subscriptions'])!=1:
            return None
        desired['subscriptions']=[{'id':after['subscriptions'][0]['id'],**body}]
    elif route.key in ('subscription-put','subscription-patch'):
        [old]=before['subscriptions']
        desired['subscriptions']=[{**old,**body}]
    elif route.key=='subscription-delete':
        desired['subscriptions']=[]
        desired['logs']=[]
    return desired


def response_body(route,seeded,expected):
    if route.key in ('integration-get','integration-patch'):
        return {**expected['integration'],'subscriptions':expected['subscriptions']}
    if route.key=='nested-subscriptions':
        return expected['subscriptions']
    if route.key=='subscriptions-list':
        return seeded['subscriptions']
    if route.key=='log-detail':
        [log]=expected['logs']
        sub=by_id(expected['subscriptions'],log['subscription_id'])
        return {key:value for key,value in log.items() if key!='subscription_id'}|{'subscription':sub}
    if route.key=='subscription-delete':
        return None
    [subscription]=expected['subscriptions']
    return subscription


def controls(requested,seed,after):
    records={}
    desired_subtrees={}
    expected_posts=0
    for route,role,variant,target,prepared,actual in requested:
        before=subtree(seed['seeded'],target['integration'])
        following=subtree(after,target['integration'])
        allow=role in ALLOW
        annotated=route.key=='nested-subscriptions' and variant=='json' and allow
        expected_status=route.status if allow else DENIALS[role]
        desired=expected_subtree(route,before,following,prepared,allow)
        identifier=target['integration']
        desired_subtrees[identifier]=desired
        deleted_logs={row['id'] for row in before['logs']} if route.key=='subscription-delete' and allow else set()
        sql_ok=(desired==following and not any(row['id'] in deleted_logs for row in after['logs']))
        if route.key=='subscription-create' and allow:
            expected_posts+=1
        status=actual['status']
        json_value=actual.get('json')
        if status==204:
            body_ok=(actual['body_bytes']==0)
        elif status==expected_status and allow and desired is not None:
            expected=response_body(route,seed['seeded'],desired)
            if route.key=='subscriptions-list':
                body_ok=(isinstance(json_value,list) and all(isinstance(row,dict) and type(row.get('id')) is int for row in json_value) and
                         sorted(json_value,key=lambda row:row['id'])==sorted(expected,key=lambda row:row['id']))
            else:
                body_ok=(json_value==expected)
        else:
            body_ok=(status==expected_status and isinstance(json_value,dict) and isinstance(json_value.get('detail'),str))
        case=cell(route.key,role)
        data={'method':route.method,'path_template':symbolic_path(route,variant),'role':role,
              'resolver_ordinal':route.bare if variant=='bare' else route.formatted,
              'actual_status':status,'expected_stock_status':expected_status,
              'raw_response_sha256':actual.get('body_sha256'),'mime':actual.get('mime'),
              'authentication_challenge_present':actual.get('authentication_challenge_present'),
              'scope_response_match':bool(body_ok),'owned_sql_rows_match':sql_ok,
              'authenticated_source_format_500_candidate':annotated,
              'denial_json_sha256':checksum(json_value) if not allow and json_value is not None else None,
              'transport_error':actual.get('transport_error'),
              'selected_scope_passed':status==expected_status and bool(body_ok) and sql_ok}
        records[case]=data
    sequences=seed['seeded']['sequences']
    expected={**sequences,'subscriptions':[sequences['subscriptions'][0]+expected_posts,True]}
    untouched=seed['before']
    complete_state=False
    if None not in desired_subtrees.values():
        wanted={table:list(untouched[table]) for table in TABLES}
        for scope in desired_subtrees.values():
            wanted['integrations'].append(scope['integration'])
            wanted['subscriptions'].extend(scope['subscriptions'])
            wanted['logs'].extend(scope['logs'])
        complete_state=all(sorted(wanted[table],key=lambda row:row['id'])==after[table] for table in TABLES)
    return records,after['sequences']==expected,complete_state


def run_physical(endpoint, side, variant, accounts, fixture, request=wire):
    prefix=phase(side,variant)
    count_start=fixture({'action':'connect_create_receiver_count'})['requests']
    seed=fixture({'action':'connect_matrix_prepare','side':side,'variant':variant})
    targets=seed['selected']
    requested=[]
    try:
        for route in ROUTES:
            for role in ROLES:
                target=targets[cell(route.key,role)] if route.key in MUTATIONS else targets['read']
                body=payload(route,role,prefix,target)
                try:
                    actual=request(endpoint,route_path(route,variant,target),route.method,body,credentials(role,accounts))
                except (OSError,ValueError) as error:
                    actual={'status':None,'transport_error':type(error).__name__}
                requested.append((route,role,variant,target,body,actual))
        after=fixture({'action':'connect_matrix_snapshot'})
        cases, sequence_ok, state_ok=controls(requested,seed,after)
    finally:
        cleaned=fixture({'action':'connect_matrix_retire','side':side,'variant':variant})
    count_end=fixture({'action':'connect_create_receiver_count'})['requests']
    preserved=all(cleaned[table]==seed['before'][table] for table in TABLES)
    expected_stored=sum(case['selected_scope_passed'] for case in cases.values())
    return {'cases':cases,'side':side,'variant':variant,'completed_cell_count':len(requested),
            'source_or_native_scope_matches':expected_stored,'preserved_unrelated_SQL_rows':preserved,
            'expected_sequence_transition':sequence_ok,'complete_selected_table_transition':state_ok,'zero_owned_delivery':count_start==count_end==0,
            'selected_scope_passed':expected_stored==len(requested) and preserved and sequence_ok and state_ok and count_start==count_end==0}


def compare_pair(a,b):
    return {key:(a['cases'][key]['selected_scope_passed'] and b['cases'][key]['selected_scope_passed']
                 and all(a['cases'][key][field]==b['cases'][key][field] for field in
                         ('actual_status','mime','authentication_challenge_present','denial_json_sha256')))
            for key in a['cases']}


def run(baseline,candidate,accounts,record,fixture):
    phases={}
    for side,endpoint in zip(SIDES,(baseline,candidate),strict=True):
        for variant in VARIANTS:
            key=side+'-'+variant
            observed=run_physical(endpoint,side,variant,accounts,fixture)
            phases[key]=observed
            record('connect-matrix:'+key,observed['selected_scope_passed'],observed)
    pairs={variant:compare_pair(phases['source-'+variant],phases['native-'+variant]) for variant in VARIANTS}
    matches=sum(sum(case_results.values()) for case_results in pairs.values())
    completed=sum(item['completed_cell_count'] for item in phases.values())
    paired_role_groups={
        variant:{route.key:all(results[cell(route.key,role)] for role in ROLES) for route in ROUTES}
        for variant,results in pairs.items()
    }
    record('connect-matrix:paired',matches==120 and completed==240 and
           all(item['selected_scope_passed'] for item in phases.values()),
           {'physical_http_cells_attempted':completed,'paired_cells_matching':matches,'pairs':pairs,
            'paired_representative_role_groups':paired_role_groups,
            'full_milestone_accepted':False,'register_operations_fully_verified':0})
