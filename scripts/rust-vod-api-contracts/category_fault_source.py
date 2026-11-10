"""Actual original lazy category partial-failure witness in disposable DB.
All credit remains with original Dispatcharr authors. Factory exception/row
prefix is observed; this is not an outer Source-network500 wire claim.
"""
import os
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture guards required')
import sys,json
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.vod.models import Movie,VODCategory,M3UMovieRelation,M3USeriesRelation
prefix='rust-probe-vod-rest-'
assert Movie.objects.filter(name__startswith=prefix).count()==27
user=User.objects.get(username=prefix+'both_off')
def request():
    req=APIRequestFactory().get('/api/vod/categories/',HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION='Bearer '+str(AccessToken.for_user(user)),REMOTE_ADDR='127.0.0.1')
    match=resolve('/api/vod/categories/');req.resolver_match=match
    response=match.func(req,*match.args,**match.kwargs);response.render();return response

def reset():
    categories=VODCategory.objects.filter(name='Uncategorized')
    assert not M3UMovieRelation.objects.filter(category__in=categories).exists()
    assert not M3USeriesRelation.objects.filter(category__in=categories).exists()
    categories.delete()
reset();caught=None
try:
    with connection.cursor()as cursor:
        cursor.execute("SELECT to_regprocedure('public.rust_vod_fixture_fail_series()')")
        assert cursor.fetchone()[0]is None
        cursor.execute("CREATE FUNCTION public.rust_vod_fixture_fail_series() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='Uncategorized' AND NEW.category_type='series' THEN RAISE EXCEPTION 'owned-vod-series-failure'; END IF; RETURN NEW; END$$")
        cursor.execute("CREATE TRIGGER rust_vod_fixture_fail_series BEFORE INSERT ON vod_vodcategory FOR EACH ROW EXECUTE FUNCTION public.rust_vod_fixture_fail_series()")
    try:request()
    except Exception as error:caught={'class':type(error).__name__,'sqlstate':getattr(error.__cause__,'sqlstate',None),'message':str(error)[:512]}
    rows=list(VODCategory.objects.filter(name='Uncategorized').order_by('category_type').values_list('category_type',flat=True))
    assert caught and rows==['movie']
    captured={'scope':'Actual originalAPIRequestFactory injectedPGfailure, partial committed row prefix; notSourceHTTPerror parity',
        'passed':True,'failure':caught,'retained_category_types':rows,'disabled_flags_do_not_skip_lazy_writes':True,
        'earlier_get_or_create_survives_later_failure':True}
finally:
    with connection.cursor()as cursor:
        cursor.execute('DROP TRIGGER IF EXISTS rust_vod_fixture_fail_series ON vod_vodcategory')
        cursor.execute('DROP FUNCTION IF EXISTS public.rust_vod_fixture_fail_series()')
    reset();restored=request();assert restored.status_code==200 and json.loads(restored.content)==[]
print(json.dumps(captured))
