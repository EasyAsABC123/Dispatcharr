"""Borrowed Source48 bounded audit/receiver/literal helpers; no auth or entrypoint.
Original Dispatcharr contributors retain application/algorithm credit.
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
    from django.core.cache import cache
    history=cache.get('throttle_api_key_auth_127.0.0.1',None)
    require(history is None or(type(history)is list and len(history)<=10 and all(type(x)in(int,float)for x in history)),'BoundedActualAPIKeyHistory')
    return {'rows':sorted(unique.values(),key=lambda r:r['key_hex']),'capture_bytes':used,'api_key_failed_auth_history':history,'scope':'owned Django Redis cache database only; TTL and unrelated timezone/group activity retained; no Channels/Celery delivery assertion'}

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

def signal_slot(value,depth=0):
    """Typed JSON-safe literal/identity metadata; never calls a receiver or repr hook."""
    import types,weakref
    require(depth<=16,'SignalSlotDepthCapacity')
    cls=type(value);kind=type.__getattribute__(cls,'__module__')+'.'+type.__getattribute__(cls,'__qualname__')
    out={'type':kind}
    if value is None or cls in (bool,int,str):out.update(kind='literal',value=value)
    elif cls is float:out.update(kind='float-hex',value=float.hex(value))
    elif cls is bytes:out.update(kind='bytes-hex',value=value.hex())
    elif cls in (tuple,list):out.update(kind='ordered-slots',length=len(value),items=[signal_slot(v,depth+1)for v in value])
    elif isinstance(value,weakref.ReferenceType):
        # Invoke only the builtin weak-reference dereference, never its referent.
        referent=weakref.ReferenceType.__call__(value)
        if isinstance(value,weakref.WeakMethod):
            function=weakref.ReferenceType.__call__(object.__getattribute__(value,'_func_ref'))
            out['bound_method_function']=None if function is None else signal_entity(function)
        out.update(kind='weak-reference',identity=id(value),alive=referent is not None,
                   referent=None if referent is None else signal_entity(referent))
    elif cls in (types.FunctionType,types.BuiltinFunctionType,types.MethodType)or isinstance(value,type):
        out.update(kind='callable-or-type-metadata',entity=signal_entity(value))
    else:out.update(kind='opaque-object-metadata',identity=id(value),object_type=kind)
    return out

def signal_entity(value):
    """Resolve callable/type labels without invoking callable or custom repr."""
    import types
    cls=type(value);result={'identity':id(value),'type':type.__getattribute__(cls,'__module__')+'.'+type.__getattribute__(cls,'__qualname__')}
    if isinstance(value,type):
        result.update(module=type.__getattribute__(value,'__module__'),qualname=type.__getattribute__(value,'__qualname__'))
    elif cls is types.MethodType:
        function=object.__getattribute__(value,'__func__');owner=object.__getattribute__(value,'__self__')
        result.update(module=object.__getattribute__(function,'__module__'),qualname=object.__getattribute__(function,'__qualname__'),bound_self_identity=id(owner))
    elif cls in (types.FunctionType,types.BuiltinFunctionType):
        result.update(module=object.__getattribute__(value,'__module__'),qualname=object.__getattribute__(value,'__qualname__'))
    return result

def signal_registration(row):
    require(type(row)in (tuple,list),'SignalRegistrationOrderedTupleOrList')
    return {'container_type':type(row).__name__,'slot_count':len(row),
            'slots':[{'index':i,**signal_slot(value)}for i,value in enumerate(row)]}

def runtime_snapshot():
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
            result[name].append(signal_registration(row))
    return {'runtimes':runtimes,'receivers':result,'proxy_signals_loaded':'apps.proxy.signals'in sys.modules}

