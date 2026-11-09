"""Original byte-progress samples at final100th-chunk boundaries; actual provider.
Original Dispatcharr authors/contributors retain all application credit.
"""
import os,sys,json,time,hashlib
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import RequestFactory
from apps.vod.models import Movie
from apps.m3u.models import M3UAccountProfile
from apps.proxy.vod_proxy.multi_worker_connection_manager import MultiWorkerVODConnectionManager
from core.utils import RedisClient
seed=json.loads(Path('/evidence/vod-seed.json').read_text());redis=RedisClient.get_client()
if list(redis.scan_iter(match='vod_persistent_connection:*')):raise RuntimeError('VOD fixture mustbeidle')
manager=MultiWorkerVODConnectionManager.get_instance();movie=Movie.objects.get(uuid=seed['movie_uuid']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id'])
cases=[]
for index,length in enumerate([819199,819200,819201]):
    sid='fixture-packaged-vod-progress-'+str(index);range_value='bytes=0-'+str(length-1)
    response=manager.stream_content_with_session(sid,movie,'http://origin:8080/vod.mp4',profile,'192.0.2.24','fixture-progress-edge',RequestFactory().get('/proxy/vod/fixture'),range_header=range_value)
    try:
        digest=hashlib.sha256();count=0
        for chunk in response.streaming_content:digest.update(chunk);count+=len(chunk)
        row=redis.hmget('vod_persistent_connection:'+sid,['bytes_sent','active_streams'])
        if response.status_code!=206 or count!=length or row[0] is None:raise RuntimeError('Source progress boundary fixture incomplete')
        cases.append({'case':'range-'+str(length),'session_id':sid,'range':range_value,'status':response.status_code,'bytes':count,'sha256':digest.hexdigest(),'bytes_sent':int(row[0])})
    finally:response.close()
    deadline=time.monotonic()+4
    while redis.exists('vod_persistent_connection:'+sid):
        if time.monotonic()>deadline:raise RuntimeError('Source progress fixture didnotcleanup')
        time.sleep(.05)
print(json.dumps({'format_version':1,'scope':'Original actualRangebody final100thpartial/full/proceeding-chunk byteprogress','capture_complete':len(cases)==3,'cases':cases,'application_parity_verified':False,'performance_verified':False}))
