"""DRAFT Original-only actual resolver/forced-auth notification observations.
Loaded ONLY by owned Original manage.py shell; never imported on the host.
No mocked permission/view/network helper and no real token is created.
"""
import datetime,hashlib,json,os,re,sys,traceback
from pathlib import Path
from fixture_guards import database as guarded_database

TABLES=['accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie',
        'vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation',
        'core_coresettings','core_systemnotification','core_notificationdismissal']
ROLE='notifications-shell'
REPORT=Path('/evidence/notification-source-private.json')
MAX_ROW=131072
MAX_AUDIT_BYTES=8388608
MAX_BODY=65536

def save(path,value):
    temporary=path.with_name(path.name+'.tmp')
    encoded=(json.dumps(value,indent=2)+'\n').encode()
    require(len(encoded)<=134217728,'ReportCaptureCapacity')
    temporary.write_bytes(encoded)
    temporary.replace(path)

def require(value,label):
    if not value:raise ValueError(label)

def owned_database(name):
    return guarded_database(name)

def connected_database(connection):
    with connection.cursor()as cursor:
        cursor.execute('SELECT current_database()')
        return cursor.fetchone()[0]

def audit(connection):
    """Read-only eleven-table/sequence fingerprint, with observation caps."""
    tables={};sequences={};used=0
    quote=connection.ops.quote_name
    with connection.cursor()as cursor:
        for name in TABLES:
            # Length is returned before any oversized JSON is transferred.
            cursor.execute('SELECT octet_length(to_jsonb(t)::text), CASE WHEN octet_length(to_jsonb(t)::text)<=%s THEN to_jsonb(t)::text ELSE NULL END FROM '+quote(name)+' t ORDER BY id LIMIT 10001',[MAX_ROW])
            count=0;digest=hashlib.sha256();digest.update(b'[')
            while True:
                row=cursor.fetchone()
                if row is None:break
                size,text=row;require(type(size)is int and size<=MAX_ROW and text is not None,'AuditRowCaptureCapacity')
                encoded=text.encode();used+=len(encoded);count+=1
                require(count<=10000 and used<=MAX_AUDIT_BYTES,'AuditTotalCaptureCapacity')
                if count>1:digest.update(b',')
                digest.update(encoded)
            digest.update(b']');tables[name]={'rows':count,'sha256':digest.hexdigest()}
            cursor.execute('SELECT pg_get_serial_sequence(%s,%s)',[name,'id']);sequence=cursor.fetchone()[0]
            require(type(sequence)is str and sequence.startswith('public.'),'NamedIDSequenceRequired')
            qualified='.'.join(quote(piece)for piece in sequence.split('.'))
            cursor.execute('SELECT last_value,is_called FROM '+qualified);last,called=cursor.fetchone()
            sequences[sequence]={'last_value':last,'is_called':called}
    payload={'tables':tables,'sequences':sequences}
    return {'database':connected_database(connection),**payload,
            'dataset_sha256':hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest(),
            'capture_bytes':used,'scope':'eleven named tables and sequences, not full database'}

def notice_rows(SystemNotification,NotificationDismissal):
    notices=list(SystemNotification.objects.order_by('id').values('id','notification_key','notification_type','priority','source','title','message','action_data','is_active','admin_only','expires_at','created_at','updated_at'))
    dismissed=list(NotificationDismissal.objects.order_by('id').values('id','user_id','notification_id','dismissed_at','action_taken'))
    def literal(value):
        if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
        if isinstance(value,list):return[literal(item)for item in value]
        if isinstance(value,dict):return{key:literal(item)for key,item in value.items()}
        return value
    return literal({'notifications':notices,'dismissals':dismissed})

def cache_snapshot():
    """Bounded raw owned Django-cache DB inventory; TTL activity is observation."""
    from django_redis import get_redis_connection
    client=get_redis_connection('default');cursor=0;rows=[];used=0
    for _ in range(128):
        cursor,keys=client.scan(cursor=cursor,count=100)
        for key in keys:
            require(len(key)<=1024 and len(rows)<1000,'CacheKeyCapacity')
            value=client.dump(key)
            if value is None:continue
            used+=len(key)+len(value);require(len(value)<=65536 and used<=1048576,'CacheValueCapacity')
            rows.append({'key_hex':key.hex(),'dump_hex':value.hex(),'pttl':client.pttl(key)})
        if cursor==0:break
    else:raise ValueError('CacheScanCapacity')
    unique={row['key_hex']:row for row in rows}
    return {'rows':sorted(unique.values(),key=lambda r:r['key_hex']),'capture_bytes':used,'scope':'owned Django Redis cache database only; TTL and unrelated timezone/group activity retained; no Channels/Celery delivery assertion'}

def literal(value):
    if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
    if isinstance(value,list):return [literal(x)for x in value]
    if isinstance(value,dict):return {k:literal(v)for k,v in value.items()}
    return value

def exception_record(info):
    if not info:return None
    typ,value,tb=info
    def evidence(text,limit):
        raw=text.encode('utf-8');captured=raw[:limit]
        return captured.decode('utf-8',errors='replace'),{'original_bytes':len(raw),'original_sha256':hashlib.sha256(raw).hexdigest(),
            'captured_bytes':len(captured),'captured_sha256':hashlib.sha256(captured).hexdigest(),'captured_hex':captured.hex(),
            'complete':len(raw)<=limit,'truncated':len(raw)>limit}
    message,m=evidence(str(value),4096);trace,t=evidence(''.join(traceback.format_exception(typ,value,tb)),65536)
    return {'type':typ.__module__+'.'+typ.__qualname__,'message':message,'traceback':trace,
            'message_evidence':m,'traceback_evidence':t,'complete':m['complete']and t['complete']}

def runtime_snapshot():
    import weakref
    from django.apps import apps
    from django.db.models import signals
    proxy=apps.get_app_config('proxy');runtimes={}
    for attr in ['live_proxy','hls_proxy']:
        obj=getattr(proxy,attr,None);managers=getattr(obj,'stream_managers',{})if obj is not None else {}
        runtimes[attr]={'present':obj is not None,'manager_keys':[str(x)for x in managers]}
    require(not any(v['present']or v['manager_keys']for v in runtimes.values()),'NoProxyRuntimeBeforeOrAfterMutation')
    result={}
    for name in ['pre_save','post_save','pre_delete','post_delete']:
        result[name]=[]
        for row in getattr(signals,name).receivers:
            receiver=row[1]()if isinstance(row[1],weakref.ReferenceType)else row[1]
            result[name].append({'lookup_key':repr(row[0]),'receiver':None if receiver is None else receiver.__module__+'.'+getattr(receiver,'__qualname__',type(receiver).__qualname__),'async':row[2]if len(row)>2 else None})
    return {'runtimes':runtimes,'receivers':result,'proxy_signals_loaded':'apps.proxy.signals'in sys.modules}

def main():
    require(os.environ.get('NOTIFICATION_FIXTURE_AUTHORITY')=='owned-source-only','FixtureAuthority')
    stage=os.environ['NOTIFICATION_FIXTURE_STAGE'];expected=os.environ['NOTIFICATION_FIXTURE_DATABASE']
    require(stage in ['audit','exercise'],'KnownStage')
    if stage=='exercise':owned_database(expected)
    else:require(expected=='dispatcharr','DefaultAuditReadOnlyOnly')
    from django.db import connection,connections,transaction
    from django.urls import resolve,reverse
    from django.utils import timezone
    from django.contrib.auth import get_user_model
    from django.test import RequestFactory
    from django.core.cache import cache
    from rest_framework.test import APIClient
    from dispatcharr.app_initialization import should_skip_initialization
    from dispatcharr.utils import network_access_allowed
    from core.api_views import SystemNotificationViewSet
    from core.models import CoreSettings,SystemNotification,NotificationDismissal
    plan=json.loads(Path('/notification-inputs/plan.json').read_text())
    report={'database':expected,'stage':stage,'passed':False,'capture_valid':False,'actual_request_count':0,'requests':[],
            'forced_auth_framework_only':True,'credentials_or_TCP_verified':False,'exception_policy':plan['exception_policy'],
            'fixture_phase':'startup','Original_revision':plan['Source_revision']}
    def flush():
        if stage=='exercise':save(REPORT,report)
    try:
        require('shell'in sys.argv and should_skip_initialization()is True,'OriginalManagementStartupScope')
        require(connected_database(connection)==expected,'ConnectedDatabaseBinding')
        if stage=='audit':
            with transaction.atomic():
                with connection.cursor()as cur:cur.execute('SET TRANSACTION READ ONLY')
                report.update(audit=audit(connection),passed=True,capture_valid=True)
            report['audit_only']=True;return
        report['fixture_phase']='preflight';report['runtime_before_isolation']=runtime_snapshot();flush()
        import django,rest_framework,inspect,importlib
        report['dependency_versions']={'Django':django.get_version(),'DRF':rest_framework.VERSION}
        proof=json.loads(Path('/notification-inputs/dependency-proof.json').read_text())
        report['actual_dependency_hashes']=[]
        for row in proof['files']:
            rel=row['path'].removeprefix('inputs/actual-image/');p=Path('/dispatcharrpy/lib/python3.13/site-packages')/rel
            got=hashlib.sha256(p.read_bytes()).hexdigest();require(got==row['sha256']==row['upstream_tag_sha256'],'DependencyBytesUnchanged')
            report['actual_dependency_hashes'].append({'path':str(p),'sha256':got})
        report['serializer_definition']=repr(SystemNotificationViewSet.serializer_class())
        with connection.cursor()as cur:
            cur.execute("SELECT conrelid::regclass::text,conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid IN ('core_systemnotification'::regclass,'core_notificationdismissal'::regclass) ORDER BY conrelid::regclass::text,conname")
            report['notification_constraints']=cur.fetchall()
        report['owned_rows_before_isolation']=notice_rows(SystemNotification,NotificationDismissal);report['owned_audit_before_setup']=audit(connection)
        report['network_settings_before_seed']=CoreSettings.get_network_access_settings();report['after_settings_read_audit']=audit(connection)
        suffix=expected.removeprefix('rust_probe_notifmut_');prefix='owned.notifmut.'+suffix
        report['fixture_phase']='owned-isolation-and-seed';flush()
        def guard():require(connected_database(connection)==owned_database(expected),'OwnedMutationDatabaseBinding')
        User=get_user_model();actors={};notices={};seedtime=timezone.now();report['seed_started_utc']=seedtime.isoformat()
        with transaction.atomic():
            guard();NotificationDismissal.objects.all().delete();guard();SystemNotification.objects.all().delete()
            report['owned_rows_after_isolation']=notice_rows(SystemNotification,NotificationDismissal)
            for role,attrs in plan['seed']['users'].items():
                if attrs is None:continue
                guard();actors[role]=User.objects.create_user(username='notifmut_'+role+'_'+suffix,password='owned-local-only',user_level=attrs['user_level'],is_active=attrs['is_active'],custom_properties={'allowed_networks':{'UI':attrs['UI']}})
            for alias,attrs in plan['seed']['notifications'].items():
                data=dict(attrs);data.pop('id');data['notification_key']=data['notification_key'].replace('@prefix',prefix)
                data['expires_at']=seedtime+datetime.timedelta(days=-1 if data['expires_at']=='@seed.past'else 1)
                guard();notices[alias]=SystemNotification.objects.create(**data)
            for role,alias,action in plan['seed']['dismissals']:
                guard();NotificationDismissal.objects.create(user=actors[role],notification=notices[alias],action_taken=action)
        report['seed_completed_utc']=timezone.now().isoformat()
        report['seed_anchor_utc']=seedtime.isoformat();report['seed_aliases']={k:v.pk for k,v in notices.items()};report['seed_prefix']=prefix
        report['seed_rows']=notice_rows(SystemNotification,NotificationDismissal)
        report['seed_users']=literal(list(User.objects.filter(pk__in=[u.pk for u in actors.values()]).order_by('id').values()))
        report['seed_audit']=audit(connection)
        from seed_binding import validate_seed,expected_target
        report['seed_binding']=validate_seed(report,plan);report['seed_binding_passed']=True;flush()
        beforekeys=cache_snapshot();report['cache_before_setup']=beforekeys
        for alias,value in [('H',False),('V',True)]:cache.set(f'dev_notif_condition_{notices[alias].pk}_{actors["admin"].pk}',value,timeout=300)
        require(cache.get(f'dev_notif_condition_{notices["U"].pk}_{actors["admin"].pk}')is None,'UnknownConditionKeyInitiallyAbsent')
        report['cache_after_setup']=cache_snapshot();report['runtime_after_seed']=runtime_snapshot()
        def named_cache():return {alias:cache.get(f'dev_notif_condition_{notices[alias].pk}_{actors["admin"].pk}')for alias in ['H','V','U','D']}
        report['named_cache_after_setup']=named_cache()
        clients={};network={};rf=RequestFactory()
        for role in plan['seed']['users']:
            client=APIClient();client.raise_request_exception=False
            if role!='anonymous':
                client.force_authenticate(user=actors[role]);request=rf.get('/',REMOTE_ADDR='127.0.0.1')
                network[role]=network_access_allowed(request,'UI',actors[role])
            clients[role]=client
        require(network=={'admin':True,'standard':True,'adminDenied':False},'ActualNetworkBoundary')
        report['network_preflight']=network;report['client_raise_request_exception']={k:v.raise_request_exception for k,v in clients.items()}
        missing=max(v.pk for v in notices.values())+100000;require(not SystemNotification.objects.filter(pk=missing).exists(),'MissingPKAbsent')
        def render(v):
            if isinstance(v,str):
                if v in ['@A.notification_key','@B.notification_key']:return notices[v[1]].notification_key
                return v.replace('@prefix',prefix)
            if isinstance(v,dict):return{k:render(x)for k,x in v.items()}
            if isinstance(v,list):return[render(x)for x in v]
            return v
        report['fixture_phase']='requests';flush()
        for case in plan['cases']:
            guard();report['current_case_index']=case['index'];alias=case['target']
            pk='not-an-integer'if alias=='malformed'else missing if alias=='missing'else notices[alias].pk
            require(type(pk)is type(expected_target(case,report['seed_aliases']))and pk==expected_target(case,report['seed_aliases']),'ReviewedCaseTargetBinding')
            path='/api/core/notifications/'+str(pk)+'/'
            require(reverse('api:core:systemnotification-detail',kwargs={'pk':pk})==path,'DetailReverse')
            matched=resolve(path);method=case['method'].lower();action=plan['candidate_methods'][case['method']]
            require(matched.func.cls is SystemNotificationViewSet and matched.func.actions[method]==action,'CorrectDetailResolvedAction')
            body=render(case['body']);row={'index':case['index'],'name':case['name'],'role':case['actor'],'method':case['method'],'path':path,'target_id':pk,'request_payload':body,'request_body_encoding':case.get('body_encoding','json'),'resolver':{'class':matched.func.cls.__module__+'.'+matched.func.cls.__qualname__,'actions':matched.func.actions,'url_name':matched.url_name,'namespaces':matched.namespaces,'kwargs':matched.kwargs},'client_raise_request_exception':False,'before':notice_rows(SystemNotification,NotificationDismissal),'before_named_audit':audit(connection),'before_cache':cache_snapshot(),'before_condition_cache':named_cache(),'before_users':literal(list(User.objects.filter(pk__in=[u.pk for u in actors.values()]).order_by('id').values())),'runtime_before':runtime_snapshot()}
            report['requests'].append(row);flush()
            row['began_utc']=timezone.now().isoformat()
            try:
                kwargs={'data':body,'content_type':'application/json'}if case.get('body_encoding')=='raw-invalid-json'else{'data':body,'format':'json'}
                response=getattr(clients[case['actor']],method)(path,**kwargs,HTTP_ACCEPT='application/json',REMOTE_ADDR='127.0.0.1',follow=False)
                raw=bytes(response.content);require(len(raw)<=MAX_BODY,'FullResponseCaptureCapacity')
                try:parsed=json.loads(raw)
                except(ValueError,UnicodeDecodeError):parsed=None
                row.update(transport_kind='http_response',actual_status=response.status_code,response_headers=dict(response.items()),body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),body_bytes=len(raw),json=parsed,framework_exception=exception_record(getattr(response,'exc_info',None)))
            except BaseException:
                row.update(transport_kind='framework_exception',framework_exception=exception_record(sys.exc_info()))
                # Capacity/ownership errors are fixture errors, never normal request outcomes.
                if sys.exc_info()[0]is ValueError and str(sys.exc_info()[1])=='FullResponseCaptureCapacity':raise
            finally:
                row['completed_utc']=timezone.now().isoformat()
            row.update(after=notice_rows(SystemNotification,NotificationDismissal),after_named_audit=audit(connection),after_cache=cache_snapshot(),after_condition_cache=named_cache(),after_users=literal(list(User.objects.filter(pk__in=[u.pk for u in actors.values()]).order_by('id').values())),runtime_after=runtime_snapshot(),capture_complete=row.get('framework_exception')is None or row['framework_exception']['complete'])
            report['actual_request_count']+=1;flush()
            require(row['capture_complete'],'ExceptionCaptureCapacity')
        report.update(capture_valid=True,fixture_phase='complete',effect_qualification='PENDING_OFFLINE_VALIDATOR')
    except BaseException:
        report['fixture_outer_exception']={'phase':report['fixture_phase'],'index':report.get('current_case_index'),'exception':exception_record(sys.exc_info())}
    finally:
        try:connections.close_all();report['connections_close_called']=True
        except BaseException:report['connection_close_exception']=exception_record(sys.exc_info());report['capture_valid']=False
        if stage=='exercise':save(REPORT,report)
        else:
            save(Path('/evidence/audit-'+os.environ['NOTIFICATION_AUDIT_LABEL']+'-private.json'),report)
            if not report.get('capture_valid'):raise RuntimeError('DefaultAuditInvalid')
    if stage=='exercise':
        print(json.dumps({'capture_valid':report['capture_valid'],'actual_requests':report['actual_request_count'],'effect_qualification':'PENDING_OFFLINE_VALIDATOR'}))
        if not report['capture_valid']or report['actual_request_count']!=48:raise RuntimeError('NotificationMutationFixtureCaptureInvalid')
if __name__=='__main__':main()
