"""Guarded actual original simultaneous ordinary VOD reads, not performance.
All application credit remains with original Dispatcharr authors/contributors.
"""
import os,sys,json,time,hashlib,http.client,threading,concurrent.futures
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture guards required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import AccessToken
user=User.objects.get(username='rust-probe-vod-rest-standard')
assert user.user_level==1 and user.is_active
headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(user))}
barrier=threading.Barrier(11);epoch=time.monotonic()
def reader(index):
    barrier.wait(timeout=5);began=time.monotonic();connection=http.client.HTTPConnection('127.0.0.1',5656,timeout=10)
    try:
        connection.request('GET','/api/vod/movies/',headers=headers);response=connection.getresponse();body=response.read(8*1024*1024+1)
        if len(body)>8*1024*1024:raise RuntimeError('Response byte bound')
        return {'index':index,'started_seconds':began-epoch,'status':response.status,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest()}
    finally:connection.close()
with concurrent.futures.ThreadPoolExecutor(max_workers=10)as pool:
    futures=[pool.submit(reader,index)for index in range(10)];barrier.wait(timeout=5);records=[future.result(timeout=15)for future in futures]
print(json.dumps({'scope':'Actual original simultaneous10 ordinary MovieList requests; not performance, no staggering','passed':all(row['status']==200 for row in records)and len({row['body_sha256']for row in records})==1,'records':records,'start_spread_seconds':max(row['started_seconds']for row in records)-min(row['started_seconds']for row in records),'native_verified':False}))
