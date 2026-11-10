"""Guarded category experiment fixture; metadata only, no provider/job signals.
Original Dispatcharr authors retain application credit and AGPL attribution.
Output token is private evidence, never public documentation.
"""
import datetime,hashlib,json,os,sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_CATEGORY_PERF_DISPOSABLE')!='playback-fixture-only':raise RuntimeError('Explicit disposable category fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection,transaction
from django.utils import timezone
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import VODCategory,M3UVODCategoryRelation,Movie,Series,Episode,VODLogo
from core.models import CoreSettings
request=json.load(sys.stdin)
if set(request)!={'op'}or request['op']not in ['seed','phase']:raise RuntimeError('Only fixed seed/phase operations')
prefix='rust-probe-vod-category-perf-';at=datetime.datetime(2026,11,10,tzinfo=datetime.timezone.utc)
if request['op']=='seed':
 if User.objects.filter(username__startswith=prefix).exists()or VODCategory.objects.exists()or M3UVODCategoryRelation.objects.exists():raise RuntimeError('Fresh category namespace/tables required')
 if any(model.objects.exists()for model in [Movie,Series,Episode,VODLogo]):raise RuntimeError('No content/posters in category experiment')
 if M3UAccount.objects.filter(account_type='XC',is_active=True).exists():raise RuntimeError('No pre-existing active XC accounts allowed')
 with transaction.atomic():
  User.objects.bulk_create([User(username=prefix+'standard',password='!',user_level=1,is_active=True,custom_properties={'vod_movies_enabled':True,'vod_series_enabled':True})])
  accounts=M3UAccount.objects.bulk_create([M3UAccount(name=prefix+str(index),account_type='XC'if index<4 else'STD',is_active=index!=3,priority=index,server_url='http://origin:8080/category-never-contact',custom_properties={'enable_vod':True,'auto_enable_new_groups_vod':index!=2})for index in range(5)])
  M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name=prefix+'default',is_default=True,is_active=True,search_pattern='^(.*)$',replace_pattern='$1',max_streams=0)for account in accounts])
  categories=VODCategory.objects.bulk_create([VODCategory(name=prefix+('movie-'if index<120 else'series-')+f'{index%120:04}',category_type='movie'if index<120 else'series')for index in range(240)])
  defaults=VODCategory.objects.bulk_create([VODCategory(name='Uncategorized',category_type=kind)for kind in ['movie','series']])
  rows=[]
  for index,category in enumerate(categories):
   for position in [index%4,(index+1)%4]:
    props={'fixture':index,'provider':position,'language':'café東京','nil':None};assert len(json.dumps(props,ensure_ascii=False).encode())<=256
    rows.append(M3UVODCategoryRelation(category=category,m3u_account=accounts[position],enabled=(index+position)%3!=0,custom_properties=props))
   if index%16==0:rows.append(M3UVODCategoryRelation(category=category,m3u_account=accounts[4],enabled=False,custom_properties={'fixture':index,'kind':'STD'}))
  rows.extend(M3UVODCategoryRelation(category=category,m3u_account=account,enabled=account.custom_properties['auto_enable_new_groups_vod'],custom_properties={})for category in defaults for account in accounts[:3])
  assert len(rows)==501;M3UVODCategoryRelation.objects.bulk_create(rows,batch_size=500)
  for model,query in [(User,User.objects.filter(username__startswith=prefix)),(M3UAccount,M3UAccount.objects.filter(name__startswith=prefix)),(M3UAccountProfile,M3UAccountProfile.objects.filter(m3u_account__name__startswith=prefix)),(VODCategory,VODCategory.objects.all()),(M3UVODCategoryRelation,M3UVODCategoryRelation.objects.all())]:
   fields={field.name for field in model._meta.fields};updates={name:at for name in ['created_at','updated_at']if name in fields}
   if updates:query.update(**updates)
 with connection.cursor()as cursor:
  for model in [User,M3UAccount,M3UAccountProfile,VODCategory,M3UVODCategoryRelation]:cursor.execute('ANALYZE '+connection.ops.quote_name(model._meta.db_table))
user=User.objects.get(username=prefix+'standard');assert user.user_level==1 and user.is_active
assert VODCategory.objects.count()==242 and M3UVODCategoryRelation.objects.count()==501
assert M3UAccount.objects.filter(account_type='XC',is_active=True).count()==3
assert M3UAccount.objects.filter(name__startswith=prefix).count()==5
assert M3UAccountProfile.objects.filter(m3u_account__name__startswith=prefix).count()==5
assert all(not model.objects.exists()for model in [Movie,Series,Episode,VODLogo])
digest=hashlib.sha256();table_counts={}
# These complete tables are stable in this dedicated fixture. This includes
# the baseline custom STD account/profiles and baseline admin user, if present.
for model in [User,M3UAccount,M3UAccountProfile,VODCategory,M3UVODCategoryRelation,CoreSettings]:
 query=model.objects.all();table_counts[model._meta.db_table]=query.count();digest.update(model._meta.db_table.encode())
 for row in query.order_by('id').values().iterator(chunk_size=512):digest.update(json.dumps(row,sort_keys=True,separators=(',',':'),default=str).encode()+b'\n')
with connection.cursor()as cursor:
 cursor.execute("SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position");schema=hashlib.sha256(json.dumps(cursor.fetchall(),separators=(',',':'),default=str).encode()).hexdigest()
print(json.dumps({'categories':242,'nested_links':501,'owned_accounts':5,'active_XC_accounts':3,'owned_profiles':5,'content_tables_empty':True,'fingerprinted_table_counts':table_counts,'fixture_sha256':digest.hexdigest(),'schema_sha256':schema,'token':str(AccessToken.for_user(user)),'clock':timezone.now().isoformat()},separators=(',',':')))
