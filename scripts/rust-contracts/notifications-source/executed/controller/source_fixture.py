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
    require(len(encoded)<=67108864,'ReportCaptureCapacity')
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

def main():
    require(os.environ.get('NOTIFICATION_FIXTURE_AUTHORITY')=='owned-source-only','FixtureAuthority')
    stage=os.environ['NOTIFICATION_FIXTURE_STAGE'];expected=os.environ['NOTIFICATION_FIXTURE_DATABASE']
    require(stage in ['audit','exercise'],'KnownStage')
    if stage=='exercise':owned_database(expected)
    else:require(expected=='dispatcharr','DefaultAuditReadOnlyOnly')
    # Django was initialized by Original manage.py shell, with Original settings.
    from django.db import connection,connections,transaction
    from django.urls import resolve,reverse,Resolver404
    from django.utils import timezone
    from django.contrib.auth import get_user_model
    from rest_framework.test import APIClient
    from dispatcharr.app_initialization import should_skip_initialization
    from dispatcharr.utils import network_access_allowed
    from core.api_views import SystemNotificationViewSet
    from core.models import CoreSettings,SystemNotification,NotificationDismissal
    report={'database':expected,'passed':False,'stage':stage,'capture_valid':False,'actual_request_count':0,
            'predictions_only_until_compared_with_actual':True,'role':'Original framework client forced auth, not credential/TCP proof'}
    try:
        require('shell'in sys.argv and should_skip_initialization()is True,'OriginalManagementStartupScope')
        require(connected_database(connection)==expected,'ConnectedDatabaseBinding')
        if stage=='audit':
            with transaction.atomic():
                with connection.cursor()as cursor:cursor.execute('SET TRANSACTION READ ONLY')
                report.update(audit=audit(connection),passed=True,capture_valid=True)
            report['audit_only']=True
            return
        base='/api/core/notifications/';detail=lambda pk:base+str(pk)+'/dismiss/'
        require(reverse('api:core:systemnotification-list')==base,'ActualListReverse')
        resolver=resolve(base);require(resolver.func.cls is SystemNotificationViewSet and resolver.func.actions['get']=='list'and resolver.func.actions['post']=='create','ActualListResolveActions')
        report['resolver']={'list':{'path':base,'url_name':resolver.url_name,'namespaces':resolver.namespaces,'actions':resolver.func.actions}}
        try:
            wrong=resolve('/api/system/notifications/')
            report['wrong_path_control']={'resolved':True,'url_name':wrong.url_name,'handler':wrong.func.__module__+'.'+getattr(wrong.func,'__name__',type(wrong.func).__name__),'is_notification_view':getattr(wrong.func,'cls',None)is SystemNotificationViewSet}
            require(report['wrong_path_control']['is_notification_view']is False,'WrongPathMustNotBeNotificationRoute')
        except Resolver404:report['wrong_path_control']={'resolved':False,'Resolver404':True}
        # Observe network settings BEFORE isolating the owned seed; no policy mutation.
        setup_before=audit(connection)
        report['network_access_settings_observed']=CoreSettings.get_network_access_settings()
        report['setup_before_named_audit']=setup_before
        report['setup_after_settings_named_audit']=audit(connection)
        # This is an owned-copy-only seed. Do not reset any sequence.
        require(connected_database(connection)==owned_database(expected),'SeedDatabaseBinding')
        report['owned_rows_before_isolation']=notice_rows(SystemNotification,NotificationDismissal)
        NotificationDismissal.objects.all().delete();SystemNotification.objects.all().delete()
        User=get_user_model();suffix=expected.removeprefix('rust_probe_notif_')
        standard=User.objects.create_user(username='notif_user_'+suffix,password='x',user_level=User.UserLevel.STANDARD)
        admin=User.objects.create_user(username='notif_admin_'+suffix,password='x',user_level=User.UserLevel.ADMIN)
        notice=SystemNotification.objects.create(notification_key='test.notif',title='Hello',message='World',is_active=True,admin_only=False,source=SystemNotification.Source.SYSTEM)
        require(reverse('api:core:systemnotification-dismiss',kwargs={'pk':notice.pk})==detail(notice.pk),'ActualDismissReverse')
        resolved=resolve(detail(notice.pk));require(resolved.func.cls is SystemNotificationViewSet and resolved.func.actions['post']=='dismiss','ActualDismissResolveAction')
        report['resolver']['dismiss']={'path':detail(notice.pk),'url_name':resolved.url_name,'namespaces':resolved.namespaces,'actions':resolved.func.actions}
        report['seed_users']=[{'id':u.pk,'username':u.username,'user_level':u.user_level,'is_active':u.is_active,'is_superuser':u.is_superuser,'custom_properties':u.custom_properties}for u in (standard,admin)]
        report['seed']={'notification_id':notice.pk,'standard_user_id':standard.pk,'admin_user_id':admin.pk,'standard_user_level':standard.user_level,'admin_user_level':admin.user_level,'source':'system','condition':None,'default_source_model_code_unchanged':True}
        clients={name:APIClient(raise_request_exception=False)for name in ['anonymous','standard','admin']}
        clients['standard'].force_authenticate(user=standard);clients['admin'].force_authenticate(user=admin)
        # No network setting is mutated. Fail setup rather than patch policy if
        # the inherited owned fixture does not allow the actual admin loopback.
        from django.test import RequestFactory
        network_request=RequestFactory().post(base,REMOTE_ADDR='127.0.0.1')
        require(network_access_allowed(network_request,'UI',admin),'OwnedLoopbackUIAccessRequired')
        report['setup_after_seed_named_audit']=audit(connection)
        report['seed_rows']=notice_rows(SystemNotification,NotificationDismissal)
        report['no_network_setting_mutation']=True
        calls=[('anonymous-list','anonymous','get',base,None,401),
               ('standard-list','standard','get',base,None,200),
               ('standard-create-denied','standard','post',base,{'notification_key':'evil','title':'Phish','message':'Click','is_active':True},403),
               ('standard-dismiss','standard','post',detail(notice.pk),{},200),
               ('standard-dismiss-repeat','standard','post',detail(notice.pk),{},200),
               ('standard-list-after-dismiss','standard','get',base,None,200),
               ('admin-list-independent','admin','get',base,None,200),
               ('admin-create-positive','admin','post',base,{'notification_key':'owned.created.'+suffix,'title':'Hello','message':'World','is_active':True},201)]
        report['requests']=[]
        for name,role,method,path,payload,predicted in calls:
            require(connected_database(connection)==expected,'RequestDatabaseBinding')
            matched=resolve(path)
            require(matched.func.cls is SystemNotificationViewSet and matched.func.actions.get(method)==('dismiss'if '/dismiss/'in path else 'list'if method=='get'else 'create'),'PerRequestResolverBinding')
            before=notice_rows(SystemNotification,NotificationDismissal);before_audit=audit(connection);before_cache=cache_snapshot();began=timezone.now()
            response=getattr(clients[role],method)(path,**({'data':payload,'format':'json'}if method=='post'else{}),HTTP_ACCEPT='application/json',REMOTE_ADDR='127.0.0.1',follow=False)
            completed=timezone.now();body=bytes(response.content);complete=len(body)<=MAX_BODY
            try:parsed=json.loads(body)if complete else None
            except (ValueError,UnicodeDecodeError):parsed=None
            after=notice_rows(SystemNotification,NotificationDismissal);after_audit=audit(connection);after_cache=cache_snapshot()
            report['requests'].append({'name':name,'role':role,'method':method.upper(),'path':path,'request_payload':payload,'resolver_action':matched.func.actions[method],'follow_redirects':False,'predicted_status':predicted,'actual_status':response.status_code,'status_prediction_matched':response.status_code==predicted,'response_headers':dict(response.items()),'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'body_hex':body[:MAX_BODY].hex(),'body_capture_complete':complete,'json':parsed,'began_utc':began.isoformat(),'completed_utc':completed.isoformat(),'before':before,'after':after,'before_owned_cache':before_cache,'after_owned_cache':after_cache,'before_named_audit':before_audit,'after_named_audit':after_audit})
            report['actual_request_count']+=1
        # A separate offline effect validator must recompute all proposed SQL
        # effects. Captured status matches alone do NOT mean fixture acceptance.
        report.update(capture_valid=all(row['body_capture_complete']for row in report['requests']),status_predictions_matched=all(row['status_prediction_matched']for row in report['requests']),effect_qualification='PENDING_OFFLINE_VALIDATOR',passed=False)
    except BaseException as error:
        report['error_type']=type(error).__name__
        report['error_message']=str(error)[:4096]
        (REPORT.parent/'notification-source-failure-private.log').write_text(traceback.format_exc()[-65536:])
    finally:
        try:connections.close_all();report['connections_close_called']=True
        except BaseException as error:report['connection_close_error']=type(error).__name__;report['capture_valid']=False
        if stage=='exercise':save(REPORT,report)
        else:
            save(Path('/evidence/audit-'+os.environ['NOTIFICATION_AUDIT_LABEL']+'-private.json'),report)
            print(json.dumps(report))
            if not report.get('capture_valid'):raise RuntimeError('DefaultAuditInvalid')
    if stage=='exercise':
        print(json.dumps({'capture_valid':report['capture_valid'],'actual_requests':report['actual_request_count'],'accepted':False,'effect_qualification':'PENDING_OFFLINE_VALIDATOR'}))
        if not report['capture_valid']or report['actual_request_count']!=8:raise RuntimeError('NotificationFixtureCaptureInvalid')

if __name__=='__main__':main()
