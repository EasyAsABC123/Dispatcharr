"""Actual Original unstaggered All/Movie/Category HTTP correctness, no samples.
Original Dispatcharr authors/contributors retain credit and AGPL licensing.
"""
import concurrent.futures,hashlib,http.client,json,os,sys,threading,time
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings
from rest_framework_simplejwt.tokens import AccessToken
prefix='rust-probe-vod-rest-';user=User.objects.get(username=prefix+'standard');assert user.is_active and user.user_level==1
assert M3UAccount.objects.filter(name__startswith=prefix).count()==3 and not M3UAccount.objects.filter(name__startswith=prefix).exclude(server_url__startswith='http://127.0.0.1:').exists()
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=DjangoJSONEncoder).encode()).hexdigest()for m in models}
before=fingerprint();token=str(AccessToken.for_user(user));epoch=time.monotonic();barrier=threading.Barrier(11)
paths=['/api/vod/all/','/api/vod/movies/','/api/vod/categories/']
def request(index):
 path=paths[index%3];client=http.client.HTTPConnection('127.0.0.1',5656,timeout=8)
 barrier.wait();began=time.monotonic()-epoch
 try:
  client.request('GET',path,headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+token});response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024
  return {'index':index,'path':path,'started_seconds':began,'completed_seconds':time.monotonic()-epoch,'status':response.status,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest()}
 finally:client.close()
with concurrent.futures.ThreadPoolExecutor(max_workers=10)as executor:
 futures=[executor.submit(request,i)for i in range(10)];barrier.wait();records=[v.result()for v in futures]
assert all(v['status']==200 for v in records)
for path in paths:assert len({v['body_sha256']for v in records if v['path']==path})==1
assert fingerprint()==before
print(json.dumps({'passed':True,'scope':'Actual Original unstaggered10TCP mixed All/Movie/Category requests; barrier concurrency correctness only, no CPU/memory/latency performance acceptance','paths':paths,'records':records,'all13table_fingerprints_before':before,'all13table_fingerprints_after':fingerprint(),'owned_rows_unchanged':True,'provider_refresh_called':False,'credentials_recorded':False}))
