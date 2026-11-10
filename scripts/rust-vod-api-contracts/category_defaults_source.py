"""Guarded original category get_or_create default coercions and fault prefixes.
Original Dispatcharr authors retain application/algorithm credit; Factory only.
"""
import os,sys,json
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount
from apps.vod.models import VODCategory,M3UVODCategoryRelation
prefix='rust-probe-vod-rest-';user=User.objects.get(username=prefix+'both_off');account=M3UAccount.objects.get(name=prefix+'second');categories=list(VODCategory.objects.filter(name='Uncategorized').order_by('category_type'));assert len(categories)==2
saved_props=account.custom_properties;saved=list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).values('category_id','enabled','custom_properties'));assert len(saved)==2
records=[]
def invoke():
 req=APIRequestFactory().get('/api/vod/categories/',HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION='Bearer '+str(AccessToken.for_user(user)),REMOTE_ADDR='127.0.0.1');match=resolve('/api/vod/categories/');req.resolver_match=match
 try:
  response=match.func(req,*match.args,**match.kwargs);response.render();return {'status':response.status_code,'json':json.loads(response.content)}
 except Exception as error:return {'exception':type(error).__name__,'message':str(error)[:1024],'sqlstate':getattr(error.__cause__,'sqlstate',None)}
try:
 for value in [True,False,0,1,'0','1','true','True','false','False','t','f',None,'',2,[],{}]:
  M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).delete();M3UAccount.objects.filter(pk=account.pk).update(custom_properties={'enable_vod':True,'auto_enable_new_groups_vod':value})
  outcome=invoke();rows=list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).order_by('category__category_type').values('category__category_type','enabled','custom_properties'));records.append({'default':value,'missing':True,'outcome':outcome,'committed_links':rows})
 # Existing get_or_create rows bypass invalid defaults entirely.
 M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).delete();M3UVODCategoryRelation.objects.bulk_create([M3UVODCategoryRelation(m3u_account=account,**row)for row in saved]);M3UAccount.objects.filter(pk=account.pk).update(custom_properties={'enable_vod':True,'auto_enable_new_groups_vod':2})
 records.append({'default':2,'missing':False,'outcome':invoke(),'committed_links':list(M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).order_by('category__category_type').values('category__category_type','enabled','custom_properties'))})
finally:
 M3UAccount.objects.filter(pk=account.pk).update(custom_properties=saved_props);M3UVODCategoryRelation.objects.filter(m3u_account=account,category__in=categories).delete();M3UVODCategoryRelation.objects.bulk_create([M3UVODCategoryRelation(m3u_account=account,**row)for row in saved])
print(json.dumps({'passed':True,'scope':'Actual original APIRequestFactory plus DB committed effects; not SourceNetwork exception rendering or native acceptance','records':records,'normal_fixture_props_and_semantic_links_restored':True}))
