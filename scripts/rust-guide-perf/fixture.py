"""Guarded original ORM fixture for stored-guide smoke/planned performance only.
Original Dispatcharr authors retain full application credit. Replies include private JWT/UUIDs.
"""
import hashlib,json,os,sys
from datetime import timedelta
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('GUIDE_PERF_DISPOSABLE')!='playback-fixture-only':raise RuntimeError('Explicit owned guide fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.utils import timezone
from django.db import connection
from rest_framework_simplejwt.tokens import RefreshToken
from apps.accounts.models import User
from apps.channels.models import Channel,Logo
from apps.epg.models import EPGSource,EPGData,ProgramData
from django_redis import get_redis_connection
request=json.load(sys.stdin);op=request['op']
if op=='seed':
 prefix='rust-probe-guide-perf-';
 if User.objects.filter(username__startswith=prefix).exists():raise RuntimeError('Fresh guide fixture required')
 user=User.objects.create(username=prefix+'admin',password='!',user_level=10,is_active=True,is_staff=True,is_superuser=True)
 Channel.objects.all().update(hidden_from_output=True)
 source=EPGSource(name=prefix+'stored',source_type='xmltv',url='https://never-requested.invalid/guide.xml',is_active=False);EPGSource.objects.bulk_create([source])
 guides=[EPGData(name=prefix+str(i),tvg_id='guide.perf.'+str(i),epg_source=source)for i in range(50)];EPGData.objects.bulk_create(guides)
 logos=[Logo(name=prefix+str(i),url='https://never-requested.invalid/logo-'+str(i)+'.png')for i in range(50)];Logo.objects.bulk_create(logos)
 channels=[Channel(name=prefix+str(i),channel_number=100000+i,epg_data=guides[i%50],logo=logos[i%50],hidden_from_output=False)for i in range(500)];Channel.objects.bulk_create(channels)
 at=timezone.now().replace(microsecond=0);rows=[]
 for i,guide in enumerate(guides):
  for slot in range(45):
   start=at-timedelta(hours=2)if slot==0 else at+timedelta(hours=2,minutes=30*(slot-1));end=at+timedelta(hours=2)if slot==0 else start+timedelta(minutes=30)
   rows.append(ProgramData(epg=guide,start_time=start,end_time=end,title=f'Guide {i} programme {slot} café',sub_title='Stored guide fixture',description='A & B < C 東京 '+('bounded fixture ' *8),custom_properties={'categories':['News','Sport & café'],'language':'en','season':'2','episode':'3'},**({'tvg_id':guide.tvg_id}if request.get('workload_family')=='grid'else{})))
 ProgramData.objects.bulk_create(rows)
 with connection.cursor()as cursor:
  for table in [ProgramData._meta.db_table,EPGData._meta.db_table,Channel._meta.db_table]:cursor.execute('ANALYZE '+connection.ops.quote_name(table))
 result={'clock':at.isoformat(),'source_current_until':(at+timedelta(hours=2)).isoformat(),'channels':500,'guides':50,'stored_programmes':2250,'channel_uuids':[str(c.uuid)for c in channels],'token':str(RefreshToken.for_user(user).access_token)}
elif op in ['clear_cache','cache_state']:
 client=get_redis_connection('default');generation=client.get('epg_content_generation')or b'0'
 if isinstance(generation,bytes):generation=generation.decode('ascii')
 if len(generation)>32 or not generation.isdigit():raise RuntimeError('Unexpected owned cache generation')
 base='epg_content:all:anonymous:d=1:p=0:logos=True:tvgid=channel_number:origin=http://guide-perf.invalid:8088:g='+generation
 keys=[base+':'+kind for kind in ['chunks','ready','status','lock']]
 if op=='clear_cache':
  removed=client.delete(*keys);result={'owned_xml_keys_removed':removed,'generation':generation,'key_sha256':[hashlib.sha256(key.encode()).hexdigest()for key in keys]}
 else:
  # Bounded server-side sizes only; never transfer/cache the13MiB XML in this helper.
  state=client.eval("""local n=redis.call('LLEN',KEYS[1]); if n>100000 then return redis.error_reply('guide chunks bound') end; local total=0; for i=0,n-1,256 do local batch=redis.call('LRANGE',KEYS[1],i,math.min(i+255,n-1)); for _,v in ipairs(batch)do total=total+string.len(v); if total>33554432 then return redis.error_reply('guide bytes bound') end end end; return {n,total,redis.call('GET',KEYS[2])or '',redis.call('GET',KEYS[3])or '',redis.call('EXISTS',KEYS[4]),redis.call('TTL',KEYS[2])}""",4,*keys)
  result={'generation':generation,'chunks':state[0],'bytes':state[1],'ready':state[2].decode(),'status':state[3].decode(),'locked':bool(state[4]),'ready_ttl_seconds':state[5]}
elif op=='phase':
 user=User.objects.get(username='rust-probe-guide-perf-admin')
 from django.db.models import Min,Max
 rows=ProgramData.objects.filter(epg__epg_source__name='rust-probe-guide-perf-stored')
 times=rows.aggregate(first=Min('start_time'),last=Max('end_time'))
 from core.models import CoreSettings
 fingerprint_rows={'channels':list(Channel.objects.filter(name__startswith='rust-probe-guide-perf-').order_by('id').values()),'guides':list(EPGData.objects.filter(epg_source__name='rust-probe-guide-perf-stored').order_by('id').values()),'programmes':list(rows.order_by('id').values()),'settings':list(CoreSettings.objects.order_by('id').values())}
 fingerprint=hashlib.sha256(json.dumps(fingerprint_rows,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
 with connection.cursor()as cursor:
  cursor.execute("SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position")
  schema=hashlib.sha256(json.dumps(cursor.fetchall(),separators=(',',':'),default=str).encode()).hexdigest()
 result={'token':str(RefreshToken.for_user(user).access_token),'fixture_sha256':fingerprint,'schema_sha256':schema,'clock':timezone.now().isoformat(),'current_until':(times['first']+timedelta(hours=4)).isoformat(),'stored_programmes':rows.count(),'first':times['first'].isoformat(),'last':times['last'].isoformat(),'channels':Channel.objects.filter(name__startswith='rust-probe-guide-perf-').count()}
else:raise RuntimeError('Unknown guarded guide fixture operation')
print(json.dumps(result,separators=(',',':')))
