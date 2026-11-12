"""Read-only Native51/legacy16 report validation; Source force-auth != Native credentials."""
import copy,datetime,hashlib,json,re
TABLES=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
NOTICE='core_systemnotification';DISMISS='core_notificationdismissal'
def require(ok,label):
    if not ok:raise ValueError(label)
def exact(a,b):return json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))
def clock(v):
    value=datetime.datetime.fromisoformat(v.replace('Z','+00:00'));require(value.tzinfo is not None,'AwareClockRequired');return value
def owned(name):require(type(name)is str and re.fullmatch(r'rust_probe_notif_native_[a-f0-9]{10}',name),'OwnedNativeDatabase');return name
def audit(v):
    require(v['database']=='dispatcharr'and set(v['tables'])==set(TABLES),'Default11Tables');require(set(v['sequences'])=={'public.'+t+'_id_seq'for t in TABLES},'Default11Sequences')
    for row in v['tables'].values():require(type(row['rows'])is int and 0<=row['rows']<=10000 and re.fullmatch('[a-f0-9]{64}',row['sha256']),'DefaultTableShape')
    for row in v['sequences'].values():require(type(row['last_value'])is int and row['last_value']>=1 and type(row['is_called'])is bool,'DefaultSequenceShape')
    digest=hashlib.sha256(json.dumps({'tables':v['tables'],'sequences':v['sequences']},sort_keys=True).encode()).hexdigest();require(digest==v['dataset_sha256'],'DefaultAuditDigest');return digest

def body(row,key='json'):
    raw=bytes.fromhex(row['body_hex']);require(len(raw)<=65536,'BoundedRawBody')
    if'body_bytes'in row:require(len(raw)==row['body_bytes'],'RawBodySize')
    if'body_sha256'in row:require(hashlib.sha256(raw).hexdigest()==row['body_sha256'],'RawBodyHash')
    try:parsed=json.loads(raw)
    except(ValueError,UnicodeDecodeError):parsed=None
    require(exact(parsed,row.get(key)),'RawParsedBodyBinding');return raw

def shape(state):
    require(set(state['tables'])==set(state['sequences'])==set(TABLES),'All11NativeTablesAndSequences')
    for t in TABLES:
        require(type(state['tables'][t])is list and len(state['tables'][t])<=10000,'RawTableRowBound')
        s=state['sequences'][t];require(type(s['last_value'])is int and s['last_value']>=1 and type(s['is_called'])is bool,'RawSequenceShape')
    require(len(json.dumps(state).encode())<=8388608,'SQLSnapshotBound')

def compare_values(source,native,case,row,mapping,path=''):
    if isinstance(source,dict):
        require(type(native)is dict and set(source)==set(native),'StableFieldSurface:'+path)
        for k,v in source.items():compare_values(v,native[k],case,row,mapping,path+'/'+k)
    elif isinstance(source,list):
        require(type(native)is list and len(source)==len(native),'StableRowCount:'+path)
        for i,(s,n)in enumerate(zip(source,native)):compare_values(s,n,case,row,mapping,path+'/'+str(i))
    elif isinstance(source,str)and path.rsplit('/',1)[-1]=='expires_at':require(clock(source)==clock(native),'ExactExpiryInstant:'+path)
    elif isinstance(source,str)and path.rsplit('/',1)[-1]in('created_at','updated_at','dismissed_at'):
        s,n=clock(source),clock(native)
        if s in mapping:require(mapping[s]==n,'ClockContinuity:'+path)
        elif clock(case['began_utc'])<=s<=clock(case['completed_utc']):
            require(clock(row['began_utc'])<=n<=clock(row['completed_utc']),'NativeOwnClock:'+path);mapping[s]=n
        else:require(s==n,'UnmappedSeedOrUnchangedClock:'+path)
    else:require(type(source)is type(native)and source==native,'StableValue:'+path)

def raw_cache(value):
    require(value['capture_bytes']<=1048576 and len(value['rows'])<=1000,'BoundedRawCache')
    keys=[]
    for r in value['rows']:
        keys.append(r['key_hex']);require(len(bytes.fromhex(r['key_hex']))<=1024 and len(bytes.fromhex(r['dump_hex']))<=65536 and type(r['pttl'])is int,'RawCacheDUMPPTTL')
    require(len(keys)==len(set(keys)),'UniqueCacheKeys')
    retained=sum(len(bytes.fromhex(r['key_hex']))+len(bytes.fromhex(r['dump_hex']))for r in value['rows']);require(retained<=value['capture_bytes']<=1048576,'RetainedRawCacheByteBound')

def named_cache(cache,source,values,database):
    raw_cache(cache);named=cache['named'];require(named['expected_database']==database,'NamedCacheDatabaseBinding')
    require(set(named['values'])==set(named['entries'])=={'H','V','U','D'}and exact(named['values'],values),'SourceNamedConditionSemantics')
    raw_by_key={row['key_hex']:row for row in cache['rows']}
    def entry(e,key,value,marker=False):
        require(e['key_hex']==key.encode().hex()and e['decoded']is True and exact(e['value'],value),'NamedCacheKeyDecodedValue')
        present=value is not None;require(e['present']is present,'NamedCachePresence')
        if not present:
            require(e['raw_hex']is None and e['raw_bytes']==0 and e['raw_sha256']is None and e['pttl']==-2 and e['key_hex']not in raw_by_key,'NamedAbsentCacheRawBinding');return
        raw=bytes.fromhex(e['raw_hex']);require(len(raw)==e['raw_bytes']<=65536 and hashlib.sha256(raw).hexdigest()==e['raw_sha256'],'NamedBoundedGETBytesSHA')
        require(e['key_hex']in raw_by_key,'NamedGETRequiresRetainedDUMP');dump=raw_by_key[e['key_hex']]
        if marker:require(raw.decode()==database and e['pttl']==dump['pttl']==-1,'OwnedSentinelContinuity')
        else:
            expected=bytes([0x80,4,0x88 if value else 0x89,0x2e]);require(type(value)is bool and raw==expected,'BoundedBoolPickleDecode')
            encoded=bytes.fromhex(dump['dump_hex']);require(len(encoded)==len(raw)+12 and encoded[:2]==bytes([0,len(raw)])and encoded[2:2+len(raw)]==raw,'NamedGETMatchesDUMPBoolPayload')
            require(0<e['pttl']<=300000 and 0<dump['pttl']<=300000,'NamedCacheTTLBoundNotEquality')
    captured=sum(len(bytes.fromhex(e['key_hex']))+e['raw_bytes']for e in [named['sentinel'],*named['entries'].values()]);require(named['capture_bytes']==captured,'NamedCaptureByteCount')
    retained=sum(len(bytes.fromhex(r['key_hex']))+len(bytes.fromhex(r['dump_hex']))for r in cache['rows']);require(retained+captured<=cache['capture_bytes']<=1048576,'AggregateDUMPAndNamedGETByteBound')
    entry(named['sentinel'],'rust-probe-notification-native-owner',database,True)
    admin=source['seed_binding']['actors']['admin']
    for alias in ['H','V','U','D']:entry(named['entries'][alias],':1:dev_notif_condition_'+str(source['seed_aliases'][alias])+'_'+str(admin),values[alias])

def mutations(actual,source):
    owned(actual['database']);require(actual['passed']is True and actual['owned_server_aborted_and_pool_closed']is True,'NativeTestCompletion')
    require(actual['Source48_raw_sha256']=='68542935c46471505dfbc9e1442e45fa9cbe498ef2c587fe0c257fb2ebd8b1bd','SourceCaptureBinding')
    require(actual['final500_renderer_gap_predeclared']is True,'FrameworkRendererGapRetained')
    require(len(actual['cases'])==48 and len(actual['extra_cases'])==3,'Finite48Plus3')
    require(actual['isolated_state']['tables'][NOTICE]==actual['isolated_state']['tables'][DISMISS]==[],'OwnedNoticeIsolation')
    seeded=actual['seeded_state'];shape(seeded);mapping={};seed_context=source['requests'][0]
    for kind,t in [('notifications',NOTICE),('dismissals',DISMISS)]:
        compare_values(source['seed_rows'][kind],seeded['tables'][t],seed_context,actual['cases'][0],mapping,'/seed/'+t)
        require(seeded['sequences'][t]==source['seed_audit']['sequences']['public.'+t+'_id_seq'],'BothSourceSeedSequences')
    require(not mapping,'SeedClocksMustRemainExact')
    cache=actual['cache_after_seed'];named_cache(cache,source,{'H':False,'V':True,'U':None,'D':None},actual['database'])
    admin=source['seed_binding']['actors']['admin']
    for alias in ['H','V']:
        key=(':1:dev_notif_condition_'+str(source['seed_aliases'][alias])+'_'+str(admin)).encode().hex()
        expected=next(r for r in source['cache_after_setup']['rows']if r['key_hex']==key);found=[r for r in cache['rows']if r['key_hex']==key]
        require(len(found)==1 and found[0]['dump_hex']==expected['dump_hex']and 0<found[0]['pttl']<=300000,'RestoredSourceConditionBool:'+alias)
    u=(':1:dev_notif_condition_'+str(source['seed_aliases']['U'])+'_'+str(admin)).encode().hex();require(all(r['key_hex']!=u for r in cache['rows']),'UConditionCacheInitiallyAbsent')
    users=seeded['tables']['accounts_user'];require(len(users)==len(source['seed_users'])==3,'ThreeNativeActors')
    for s in source['seed_users']:
        found=[u for u in users if u['id']==s['id']];require(len(found)==1,'NativeActorIdentity');u=found[0]
        require(all(exact(u[k],s[k])for k in ['id','username','user_level','is_active','custom_properties']),'NativeActorStableRoleNetworkFields')
    previous=seeded
    allrows=actual['cases']+actual['extra_cases']
    for i,row in enumerate(allrows):
        require(row['capture_complete']is True,'FullNativeRequestCapture');before,after=row['before'],row['after'];shape(before);shape(after);require(exact(before,previous),'SQLCrossRequestContinuity');previous=after
        require(clock(row['began_utc'])<=clock(row['completed_utc']),'RequestClockOrder');body(row)
        require(before['sequences']==after['sequences'],'All11SequencesUnchanged')
        require(exact(row['before_credentials'],row['after_credentials']),'NativeCredentialRowsEventsUnchangedNotMasked')
        require(exact(row['before_credentials']['accounts_user'],before['tables']['accounts_user'])and exact(row['after_credentials']['accounts_user'],after['tables']['accounts_user']),'RawCredentialsAccountsBinding')
        for t in TABLES:
            if t not in(NOTICE,DISMISS):require(exact(before['tables'][t],after['tables'][t]),'Other9TablesUnchanged:'+t)
        source_case=source['requests'][i if i<48 else 47]
        before_values=source_case['before_condition_cache']if i<48 else source_case['after_condition_cache']
        named_cache(row['before_cache'],source,before_values,actual['database']);named_cache(row['after_cache'],source,source_case['after_condition_cache'],actual['database'])
        if i<48:
            s=source['requests'][i]
            require(row['index']==i and all(exact(row[k],s[k])for k in ['name','role','method','path','request_payload','request_body_encoding']),'ExactSource48RequestBinding')
            require(row['status']==s['actual_status'],'SourceStatusBinding')
            if i==47:require(row['json']=={'detail':'Notification processing failed.'},'ExistingNative500RendererRetained')
            if row['status']==204:require(row['body_hex']==''and row.get('json')is None,'Empty204Body')
            elif i!=47:compare_values(s['json'],row['json'],s,row,mapping,'/response')
            for kind,t in [('notifications',NOTICE),('dismissals',DISMISS)]:
                compare_values(s['after'][kind],after['tables'][t],s,row,mapping,'/'+t)
                require(after['sequences'][t]==s['after_named_audit']['sequences']['public.'+t+'_id_seq'],'BothSourceSequencesEveryRequest')
            # Mutation own-clock even if a synthetic coincidence equals Source.
            if row['status']==200:
                target=s['target_id'];n=next(x for x in after['tables'][NOTICE]if x['id']==target)
                require(clock(row['began_utc'])<=clock(n['updated_at'])<=clock(row['completed_utc']),'NativeUpdateOwnClock')
        else:
            expected=[('native-poison-before-malformed-pk-body','admin','PATCH',500),('native-standard-before-poison','standard','PATCH',403),('native-anonymous-before-poison','anonymous','DELETE',401)][i-48]
            require((row['name'],row['role'],row['method'],row['status'])==expected and row['path']=='/api/core/notifications/not-an-integer/'and row['request_payload']=='{'and row['request_body_encoding']=='raw-invalid-json','ThreeExplicitNativeOrderControls')
            require(exact(before,after),'NativeOrderControlNoSQLMutation')
    reported=actual['clock_mapping'];require(len(reported)==len(mapping)and len({clock(x['Source'])for x in reported})==len(reported),'ExactGeneratedClockMapSize')
    require(all(clock(x['Source'])in mapping and mapping[clock(x['Source'])]==clock(x['Native'])for x in reported),'ExactRecordedGeneratedClockMap')
    return {'passed':True,'Source48_cases':48,'Native_order_controls':3,'raw_clocks_credentials_cache_retained':True,'renderer_body_parity_claimed':False,'Source_credential_or_TCP_acceptance_claimed':False}

def legacy(actual):
    require(actual['passed']is True and actual['owned_server_aborted_and_pool_closed']is True,'LegacyTestCompletion');require(len(actual['cases'])==len(actual['extra_cases'])==8,'Legacy8Plus8')
    require([r['status']for r in actual['cases']]==[401,200,403,200,200,200,200,201],'LegacySource8Statuses')
    expected=[('duplicate-key-no-sequence',400),('required-fields-no-mutation',400),('standard-permission-before-invalid-body',403),('repeat-truthy-update-only',200),('repeat-falsey-retains-action',200),('mutation-put-requires-fields',400),('mutation-empty-patch-touches-updated-at',200),('mutation-delete-cascades-target',204)]
    previous=actual['cases'][-1]['after']
    for i,(row,(name,status))in enumerate(zip(actual['extra_cases'],expected)):
        require(row['name']==name and row['status']==status,'ExactLegacyExtraDefinitions');body(row,'body');before,after=row['before'],row['after'];shape(before);shape(after);require(exact(before,previous),'LegacyExtraContinuity');previous=after;require(before['sequences']==after['sequences'],'LegacyExtrasAllSequencesUnchanged')
        expected_state=copy.deepcopy(before)
        if i==3:expected_state['tables'][DISMISS][0]['action_taken']='changed'
        if i==6:
            target=after['tables'][NOTICE][0]['updated_at'];require(clock(row['began_utc'])<=clock(target)<=clock(row['completed_utc']),'LegacyPatchOwnClock');expected_state['tables'][NOTICE][0]['updated_at']=target
        if i==7:
            expected_state['tables'][NOTICE]=[r for r in expected_state['tables'][NOTICE]if r['id']!=1];expected_state['tables'][DISMISS]=[r for r in expected_state['tables'][DISMISS]if r['notification_id']!=1];require(row['body_hex']==''and row['body']is None,'LegacyDeleteEmpty204')
        require(exact(expected_state,after),'PreciseLegacyExtraEffect')
    return {'passed':True,'legacy_Source8':8,'legacy_extra_controls':8,'first5_extra_definitions_preserved':True,'last3_501_expectations_replaced_only':True}
