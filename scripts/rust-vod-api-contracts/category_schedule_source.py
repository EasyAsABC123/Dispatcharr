"""Guarded actual original category SQL and nested-FK serializer schedule.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import os,sys,json,hashlib
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test.utils import CaptureQueriesContext
from django.db import connection
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount
from apps.vod.models import VODCategory,M3UVODCategoryRelation
from apps.vod.serializers import M3UVODCategoryRelationSerializer
prefix='rust-probe-vod-rest-';user=User.objects.get(username=prefix+'standard')
last_raw=None
def request():
 global last_raw
 req=APIRequestFactory().get('/api/vod/categories/',HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION='Bearer '+str(AccessToken.for_user(user)),REMOTE_ADDR='127.0.0.1');match=resolve('/api/vod/categories/');req.resolver_match=match
 response=match.func(req,*match.args,**match.kwargs);response.render();assert response.status_code==200;last_raw=response.content;return json.loads(response.content)
with CaptureQueriesContext(connection)as captured:baseline=request()
base=[item['sql']for item in captured.captured_queries if item['sql'].startswith('SELECT ')and'FROM "vod_vodcategory"'in item['sql']and'ORDER BY'in item['sql']]
assert len(base)==1 and';'not in base[0]
category=VODCategory.objects.get(name=prefix+'Drama',category_type='movie')
shadow_name=prefix+'category-shadow-parent'
assert not M3UAccount.objects.filter(name=shadow_name).exists()
account=M3UAccount.objects.bulk_create([M3UAccount(name=shadow_name,account_type='XC',is_active=False,server_url='http://127.0.0.1/never-provider',custom_properties={})])[0]
relation=M3UVODCategoryRelation.objects.create(category=category,m3u_account=account,enabled=True,custom_properties={'schedule':'old-selected'})
original=M3UVODCategoryRelationSerializer.to_representation;calls=[]
def scheduled(self,instance):
 if instance.pk==relation.pk:
  calls.append(instance.pk);M3UVODCategoryRelation.objects.filter(pk=relation.pk).update(custom_properties={'schedule':'new-unselected'})
  M3UAccount.objects.filter(pk=account.pk,name=shadow_name).delete()
 return original(self,instance)
try:
 with patch.object(M3UVODCategoryRelationSerializer,'to_representation',scheduled):payload=request()
 assert len(calls)==1
 item=next(row for row in payload if row['id']==category.pk);tail=next(row for row in item['m3u_accounts']if row['custom_properties']=={'schedule':'old-selected'})
 assert tail['category']==category.pk and tail['enabled']is True and tail['m3u_account']is None
 result={'passed':True,'scope':'Actual original APIRequestFactory, full serializer method executes after controlled FK deletion; no SourceNetwork claim','category_sql':base[0],'baseline_ids':[row['id']for row in baseline],'baseline':baseline,'category_id':category.pk,'body':payload,'removed_parent_item':tail,'selected_relation_properties_old':True,'fresh_removed_parent_value':tail['m3u_account'],'body_sha256':hashlib.sha256(last_raw).hexdigest()}
finally:
 M3UAccount.objects.filter(pk=account.pk,name=shadow_name).delete();M3UVODCategoryRelation.objects.filter(pk=relation.pk).delete()

if os.environ.get('VOD_CATEGORY_NATIVE_PREPARE')=='1':
 account=M3UAccount.objects.bulk_create([M3UAccount(name=shadow_name,account_type='XC',is_active=False,server_url='http://127.0.0.1/never-provider',custom_properties={})])[0]
 relation=M3UVODCategoryRelation.objects.create(category=category,m3u_account=account,enabled=True,custom_properties={'schedule':'old-selected'})
 result['native_shadow_account_id']=account.pk;result['native_shadow_relation_id']=relation.pk
print(json.dumps(result,ensure_ascii=False))
