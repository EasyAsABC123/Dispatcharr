"""Actual original network category default coercions and fault bodies.
Original Dispatcharr authors retain application/algorithm credit; AGPL retained.
"""
import os,sys,json,http.client,hashlib
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount
from apps.vod.models import VODCategory,M3UVODCategoryRelation
prefix='rust-probe-vod-rest-';user=User.objects.get(username=prefix+'both_off');account=M3UAccount.objects.get(name=prefix+'second');categories=list(VODCategory.objects.filter(name='Uncategorized').order_by('category_type'));assert len(categories)==2
saved_props=account.custom_properties;saved=list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).values());assert len(saved)==2
records=[]
def restore_links():
 M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).delete()
 M3UVODCategoryRelation.objects.bulk_create([M3UVODCategoryRelation(**row)for row in saved])
 for row in saved:M3UVODCategoryRelation.objects.filter(pk=row['id']).update(**{key:value for key,value in row.items()if key!='id'})
def invoke():
 client=http.client.HTTPConnection('127.0.0.1',5656,timeout=10)
 try:
  client.request('GET','/api/vod/categories/',headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(user))});response=client.getresponse();body=response.read(65537);assert len(body)<=65536
  return {'status':response.status,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'body':body.decode('utf-8'),'content_type':response.getheader('Content-Type'),'selected_headers':{name:response.getheader(name)for name in ['Content-Length','Allow','Vary','WWW-Authenticate']}}
 finally:client.close()

try:
 for value in [True,False,0,1,'0','1','true','True','false','False','t','f',None,'',2,[],{}]:
  M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).delete();M3UAccount.objects.filter(pk=account.pk).update(custom_properties={'enable_vod':True,'auto_enable_new_groups_vod':value})
  outcome=invoke();rows=list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).order_by('category__category_type').values('category__category_type','enabled','custom_properties'));records.append({'default':value,'missing':True,'outcome':outcome,'committed_links':rows})
 # Existing get_or_create rows bypass invalid defaults entirely.
 restore_links();M3UAccount.objects.filter(pk=account.pk).update(custom_properties={'enable_vod':True,'auto_enable_new_groups_vod':2})
 records.append({'default':2,'missing':False,'outcome':invoke(),'committed_links':list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).order_by('category__category_type').values('category__category_type','enabled','custom_properties'))})
finally:
 M3UAccount.objects.filter(pk=account.pk).update(custom_properties=saved_props);restore_links()
 assert list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).values())==saved
print(json.dumps({'passed':True,'scope':'Actual original HTTP default coercions/exception bodies and committed effects, exact restored category-link rows; no native acceptance','records':records,'normal_fixture_props_and_exact_links_restored':True}))
