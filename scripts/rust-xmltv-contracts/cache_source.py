"""Actual original single-flight helper with real Redis and owned fault keys.

Original Dispatcharr authors and contributors retain full application credit.
"""
import threading,time,uuid
from django_redis import get_redis_connection
from apps.output.streaming_chunk_cache import stream_cached_response

def capture():
    redis=get_redis_connection('default');prefix='epg_content:owned-helper-'+uuid.uuid4().hex;keys=[]
    def state(key):
        return {'chunks':[part.decode() for part in redis.lrange(key+':chunks',0,-1)],'status':(redis.get(key+':status') or b'').decode(),'ready':bool(redis.get(key+':ready')),'lock':bool(redis.get(key+':lock')),'ttls':{tail:redis.ttl(key+':'+tail)for tail in ['chunks','status','ready','lock']}}
    def response(key,source):
        keys.append(key);return stream_cached_response(key,source,redis=redis)
    report={}
    try:
        key=prefix+':concurrent';count=[0];lock=threading.Lock();barrier=threading.Barrier(8);outputs=[];errors=[]
        def source():
            with lock:count[0]+=1
            for part in ['A\n','B\n','C\n']:time.sleep(.01);yield part
        def worker():
            try:barrier.wait(timeout=2);body=b''.join(response(key,source).streaming_content)
            except Exception as error:errors.append(type(error).__name__)
            else:outputs.append(body.decode())
        threads=[threading.Thread(target=worker)for _ in range(8)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=5);assert not thread.is_alive()
        report['singleflight']={'source_invocations':count[0],'outputs':outputs,'errors':errors,'state':state(key)}
        assert count[0]==1 and len(outputs)==8 and outputs==['A\nB\nC\n']*8 and not errors
        key=prefix+':error'
        def failed():yield 'partial';raise ValueError('owned source failure')
        iterator=iter(response(key,failed).streaming_content);first=next(iterator).decode()
        try:next(iterator)
        except ValueError:report['source_error']={'first':first,'state':state(key)}
        else:raise AssertionError('source error swallowed')
        assert not report['source_error']['state']['chunks'] and report['source_error']['state']['status']=='error'
        report['error_rebuild']={'body':b''.join(response(key,lambda:iter(['replacement'])).streaming_content).decode(),'state':state(key)}
        key=prefix+':cancel';resp=response(key,lambda:iter(['first','second']));iterator=iter(resp.streaming_content);first=next(iterator).decode();resp.close()
        report['leader_closed']={'first':first,'state':state(key)}
        # Reacquire after a deliberately expired/deleted owned lock: characterize
        # whether a stale source can overwrite a completed successor cache.
        key=prefix+':stale';old=response(key,lambda:iter(['old-first','old-last']));iterator=iter(old.streaming_content);next(iterator);redis.delete(key+':lock')
        newer=b''.join(response(key,lambda:iter(['new-only'])).streaming_content).decode();before=state(key);remaining=b''.join(iterator).decode();old.close()
        report['stale_same_generation']={'new_body':newer,'before_old_resumes':before,'old_remaining':remaining,'after_old_resumes':state(key)}
        # Real generation suffix isolation, without clearing an in-flight build.
        key=prefix+':generation';generation=prefix+':counter';redis.set(generation,'7');keys.append(generation)
        old=stream_cached_response(key,lambda:iter(['old-first','old-last']),redis=redis,generation_key=generation);iterator=iter(old.streaming_content);next(iterator);keys.append(key+':g=7');redis.incr(generation)
        current=b''.join(stream_cached_response(key,lambda:iter(['new-only']),redis=redis,generation_key=generation).streaming_content).decode();keys.append(key+':g=8');remaining=b''.join(iterator).decode();old.close()
        report['generation_isolation']={'new_body':current,'old_remaining':remaining,'old':state(key+':g=7'),'new':state(key+':g=8')}
    finally:
        for key in set(keys):redis.delete(key,*[key+':'+suffix for suffix in ['chunks','status','ready','lock']])
    report['owned_keys_removed']=not any(redis.exists(key)for key in set(keys));return report
