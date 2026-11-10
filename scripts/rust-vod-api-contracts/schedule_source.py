"""Guarded actual original page/serializer interleaving witness.
All application/algorithm credit belongs to original Dispatcharr authors.
Factory timing hook changes schedule, never projected field/query results.
"""
import os
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':
    raise RuntimeError('Explicit fresh owned Source VOD fixture required')
import sys,json,hashlib
from unittest.mock import patch
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.vod.models import Movie,VODLogo
from apps.vod.serializers import VODLogoSerializer
prefix='rust-probe-vod-rest-'
assert Movie.objects.filter(name__startswith=prefix).count()==27
user=User.objects.get(username=prefix+'standard')
logo=VODLogo.objects.get(name=prefix+'shared-logo');old_url=logo.url
shadow_name=prefix+'Schedule-hidden-no-provider'
assert not Movie.objects.filter(name=shadow_name).exists()
mutated=[]
original=VODLogoSerializer.get_movie_count
new_url='https://fixture.invalid/changed-after-page.jpg'
def after_page(serializer,obj):
    if not mutated:
        # Original queryset and select_related logo have already materialized.
        VODLogo.objects.filter(pk=logo.pk).update(url=new_url)
        shadow=Movie.objects.create(name=shadow_name,logo_id=logo.pk,custom_properties={})
        mutated.append(shadow.pk)
    return original(serializer,obj)
try:
    path='/api/vod/movies/?page_size=1'
    req=APIRequestFactory().get(path,HTTP_HOST='localhost',HTTP_ACCEPT='application/json',
        HTTP_AUTHORIZATION='Bearer '+str(AccessToken.for_user(user)),REMOTE_ADDR='127.0.0.1')
    match=resolve('/api/vod/movies/');req.resolver_match=match
    with patch.object(VODLogoSerializer,'get_movie_count',after_page):
        response=match.func(req,*match.args,**match.kwargs);response.render();body=response.content
    assert response.status_code==200 and len(mutated)==1
    payload=json.loads(body);item=payload['results'][0]
    assert item['logo']['url']==old_url and item['logo']['movie_count']==3
    assert any(shadow_name in name for name in item['logo']['item_names'])
    print(json.dumps({'scope':'Actual original APIRequestFactory page/metadata interleaving; notSourceNetwork',
        'passed':True,'status':response.status_code,'json':payload,'body_sha256':hashlib.sha256(body).hexdigest(),
        'body_bytes':len(body),'old_logo_url':old_url,'new_logo_url':new_url,'logo_id':logo.pk,
        'shadow_name':shadow_name,'selected_logo_scalar_snapshot_retained':True,'fresh_count_and_names_after_page':True,
        'provider_refresh_attempted':False,'original_method_runs_after_schedule_hook':True}))
finally:
    VODLogo.objects.filter(pk=logo.pk).update(url=old_url)
    Movie.objects.filter(pk__in=mutated,name=shadow_name).delete()
