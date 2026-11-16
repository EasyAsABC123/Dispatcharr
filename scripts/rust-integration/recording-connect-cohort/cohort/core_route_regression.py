"""Actual distinct-DB regression inside the existing Native Core milestone cell.
No application, fixture, Source task or build starts on import/invocation here.
Original Dispatcharr/Celery authors retain all application/algorithm credit.
"""
import hashlib,json,time,uuid
from urllib.parse import urlsplit


def cache_queue(fixture):
    return json.loads(fixture.redis(0,"local n=redis.call('LLEN','celery');if n>64 then return redis.error_reply('unexpected cache task queue capacity')end;local out={};for _,raw in ipairs(redis.call('LRANGE','celery',0,-1))do local v=cjson.decode(raw);out[#out+1]={task=v.headers.task,id=v.headers.id}end;return cjson.encode({count=n,headers=out})"))


def run(fixture,core_module,m6_module,callback):
    if fixture.backend!='Native'or fixture.case!='core-produced':raise ValueError('ExistingNativeCoreContextOnly')
    if {'web','celery','dvr','beat'}&set(fixture.roles):raise ValueError('NoOriginalCacheConsumerAllowedInNativeContext')
    raw=fixture.command([fixture.docker,'inspect','--format','{{json .Config.Env}}',fixture.native],secret=True)
    values=dict(item.split('=',1)for item in json.loads(raw)if '='in item)
    databases={key:int(urlsplit(values[key]).path.lstrip('/')or'0')for key in ['REDIS_URL','CELERY_BROKER_URL']}
    if databases!={'REDIS_URL':0,'CELERY_BROKER_URL':1}:raise ValueError('ActualDistinctCache0Broker1Required')
    before=cache_queue(fixture)
    if before['count']!=0:raise AssertionError(('CacheQueueAlreadyContainsTasks',before))
    key='native:core:route-regression:'+uuid.uuid4().hex;token=uuid.uuid4().hex;owned=False
    output={'actual_database_numbers':databases,'cache_queue_before':before,'scope':'actual Main HTTP Core producer -> existing shared brokerDB1 -> actual worker; cache/locks remainDB0','began_epoch':time.time()}
    try:
        owned=fixture.redis(0,"return redis.call('SET',ARGV[1],ARGV[2],'EX',1890,'NX')or''",key,token)=='OK'
        if not owned:raise RuntimeError('OwnRouteMarkerNotAcquired')
        observation=callback(fixture,m6_module)
        publication=observation['task']['publication'];meta=observation['task']['backend']['metadata']
        if publication['queue']!='celery'or meta['task_id']!=publication['headers']['id']or meta['status']!='SUCCESS':raise AssertionError('ActualBrokerConsumerResultMissing')
        after=cache_queue(fixture);marker=fixture.redis(0,"return redis.call('GET',ARGV[1])or''",key)
        if after['count']!=0 or marker!=token:raise AssertionError(('CachePublicationOrMarkerMutation',after))
        output.update(status='passed',cache_queue_after=after,own_cache_marker_unchanged=True,actual_broker1_parent_id=publication['headers']['id'],actual_parent_status=meta['status'],observation=observation)
        observation['actual_distinct_db_route']={key:value for key,value in output.items()if key!='observation'}
        return observation
    finally:
        if owned:
            released=fixture.redis(0,"if redis.call('GET',ARGV[1])==ARGV[2]then return redis.call('DEL',ARGV[1])end;return 0",key,token)
            output['actual_own_marker_release']=released
            if 'observation'in output:output['observation']['actual_distinct_db_route']['actual_own_marker_release']=released
            if released!=1:raise RuntimeError('RouteMarkerOwnedReleaseNotConfirmed')
        output['ended_epoch']=time.time()
        m6_module.save(m6_module.directory(fixture)/('route-'+callback.__name__+'.json'),output)
