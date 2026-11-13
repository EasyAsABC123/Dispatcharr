"""Current46b91 exact producer SQL; runtime values come only from owned seed."""
USER_COLUMNS="id::bigint AS id,username,password,is_active,user_level,stream_limit,octet_length(COALESCE(custom_properties,'{}'::jsonb)::text) AS properties_bytes,CASE WHEN octet_length(COALESCE(custom_properties,'{}'::jsonb)::text)<=$2 THEN COALESCE(custom_properties,'{}'::jsonb)::text END AS properties_text"
USER_SQL='SELECT '+USER_COLUMNS+' FROM accounts_user WHERE api_key=$1 LIMIT 2'
NETWORK_SQL='SELECT value IS NULL AS sql_null,octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=$2 THEN value::text END AS raw FROM core_coresettings WHERE key=$1'
CASES=[(1,'auth_user_ErrorReady_caller_loss'),(2,'auth_user_ErrorReady_logical_deadline'),(3,'UI_network_ErrorReady_logical_deadline'),(4,'admitted_error_responses_and_fifth_capacity'),(5,'bounded_user_properties_capacity'),(6,'bounded_UI_network_capacity')]
def target(number,key):
 if number not in(1,2,3):raise ValueError('FixedErrorProducer')
 return {'sql':NETWORK_SQL if number==3 else USER_SQL,'parameter_hex':[(b'network_access'if number==3 else key.encode()).hex(),'00010000'],'rows':0,'command':None,'errors':['57014'],'tx':'I'}
def warm(snapshot,contract):
 if snapshot['errors']!=[]or snapshot['held']is not None:raise ValueError('NoWarmRelayErrorOrHeldReply')
 rows=[e for e in snapshot['events']if e['kind']=='execute'and e.get('sql')==contract['sql']and e.get('parameter_hex')==contract['parameter_hex']]
 if len(rows)!=1:raise ValueError('OneExactWarmProducerExecute')
 row=rows[0];ready=[e for e in snapshot['events']if e['kind']=='ready-observed'and e['connection']==row['connection']and e.get('sql')==row['sql']and e['monotonic']>=row['monotonic']]
 if len(ready)!=1 or ready[0]['errors']!=[]or row['execute_armed']is not False or row['arm_epoch']!=0 or type(row['execute_generation'])is not int or row['execute_generation']<=0 or type(ready[0]['backend_pid'])is not int or ready[0]['backend_pid']<=0:raise ValueError('ActualSuccessfulWarmReadyBeforeArm')
 return {'connection':row['connection'],'backend_pid':ready[0]['backend_pid'],'execute_generation':row['execute_generation'],'ready_monotonic':ready[0]['monotonic'],'sql':row['sql'],'parameter_hex':row['parameter_hex']}


def cache_binding(configured_kwargs, direct_kwargs):
    """Attest the selected logical DB without copying credentials or endpoints."""
    for value in (configured_kwargs, direct_kwargs):
        if type(value) is not dict or type(value.get('db')) is not int or value['db'] != 1:
            raise ValueError('ActualConfiguredAndDirectCacheDB1')
    return {'Django_default_database': 1, 'direct_database': 1, 'peer_database': 1}
