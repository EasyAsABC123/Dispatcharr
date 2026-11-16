"""Bounded actual queue/backend observation; metadata existence is not claim."""
import json

def observe(fixture,key,raw):
    queued=fixture.redis(1,"return redis.call('LRANGE','dvr',0,-1)")
    value=fixture.redis(2,"local k='celery-tasks:celery-task-meta-'..ARGV[1];if redis.call('STRLEN',k)>262144 then return redis.error_reply('RevokeMetadataBudget')end;return redis.call('GET',k)or false",key)
    metadata=None if value is None or value is False else json.loads(value)
    result={'exact_raw_queued':raw in queued,'backend_absent':value is None or value is False,'backend_raw':value,'metadata':metadata}
    expected={'status','result','traceback','children','date_done','task_id'}
    result['same_task_sixfield_REVOKED_before_claim']=isinstance(metadata,dict)and set(metadata)==expected and metadata['status']=='REVOKED'and metadata['task_id']==key and metadata['children']is None and metadata['traceback']is None and metadata['result']=={'exc_type':'TaskRevokedError','exc_message':['revoked'],'exc_module':'celery.exceptions'}
    return result
