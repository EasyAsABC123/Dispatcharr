"""Owned Source/native-network lazy category creation/failure-prefix witness.
Original Dispatcharr authors retain application/algorithm credit; AGPL retained.
The database is disposable. All prior category/relationship rows are restored.
"""
import os,sys,json,http.client,hashlib
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from apps.accounts.models import User
from apps.vod.models import VODCategory,M3UVODCategoryRelation,M3UMovieRelation,M3USeriesRelation,Movie
from rest_framework_simplejwt.tokens import AccessToken
assert Movie.objects.filter(name__startswith='rust-probe-vod-rest-').count()==27
user=User.objects.get(username='rust-probe-vod-rest-both_off');assert user.user_level==1 and user.is_active
saved=list(VODCategory.objects.filter(name='Uncategorized').order_by('id').values());assert len(saved)==2
identifiers=[row['id']for row in saved];links=list(M3UVODCategoryRelation.objects.filter(category_id__in=identifiers).order_by('id').values())
assert not M3UMovieRelation.objects.filter(category_id__in=identifiers).exists()and not M3USeriesRelation.objects.filter(category_id__in=identifiers).exists()
name='rust_vod_category_network_fail_series'
with connection.cursor()as cursor:cursor.execute('SELECT to_regprocedure(%s)',('public.'+name+'()',));assert cursor.fetchone()[0]is None
target=os.environ.get('VOD_CATEGORY_INITIAL_TARGET','original')
if target not in ['original','candidate']:raise RuntimeError('Only owned original/candidate service targets allowed')
host,port=('127.0.0.1',5656)if target=='original'else('candidate',9192)
headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(user))}
result=None
try:
    VODCategory.objects.filter(id__in=identifiers).delete()
    with connection.cursor()as cursor:
        cursor.execute("CREATE FUNCTION public.rust_vod_category_network_fail_series() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='Uncategorized' AND NEW.category_type='series' THEN RAISE EXCEPTION 'owned-vod-network-series-failure'; END IF; RETURN NEW; END$$")
        cursor.execute('CREATE TRIGGER rust_vod_category_network_fail_series BEFORE INSERT ON vod_vodcategory FOR EACH ROW EXECUTE FUNCTION public.rust_vod_category_network_fail_series()')
    client=http.client.HTTPConnection(host,port,timeout=10)
    try:
        client.request('GET','/api/vod/categories/',headers=headers);response=client.getresponse();body=response.read(1024*1024+1);assert len(body)<=1024*1024
        observed={'status':response.status,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'body':body.decode('utf-8'),'content_type':response.getheader('Content-Type'),'selected_headers':{name:response.getheader(name)for name in ['Content-Length','Allow','Vary','WWW-Authenticate']}}
        application_headers={name:response.getheader(name)for name in ['X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']}
    finally:client.close()
    prefix=list(VODCategory.objects.filter(name='Uncategorized').order_by('category_type').values_list('category_type',flat=True))
    assert observed['status']==500 and prefix==['movie']
    result={'passed':True,'scope':'Actual '+target+' network with owned PostgreSQL trigger fault; disabled flags do not skip writes; earlier category survives later failure','target':target,'application_headers':application_headers,'response':observed,'committed_category_type_prefix':prefix,'provider_refresh_verified':False,'native_verified':False}
finally:
    with connection.cursor()as cursor:
        cursor.execute('DROP TRIGGER IF EXISTS rust_vod_category_network_fail_series ON vod_vodcategory')
        cursor.execute('DROP FUNCTION IF EXISTS public.rust_vod_category_network_fail_series()')
    VODCategory.objects.filter(name='Uncategorized').delete()
    VODCategory.objects.bulk_create([VODCategory(**row)for row in saved])
    M3UVODCategoryRelation.objects.bulk_create([M3UVODCategoryRelation(**row)for row in links])
    # bulk_create executes auto_now/auto_now_add pre_save. Restore the captured
    # timestamps explicitly rather than claiming exact restoration of new dates.
    for row in saved:VODCategory.objects.filter(pk=row['id']).update(**{key:value for key,value in row.items()if key!='id'})
    for row in links:M3UVODCategoryRelation.objects.filter(pk=row['id']).update(**{key:value for key,value in row.items()if key!='id'})
    assert list(VODCategory.objects.filter(id__in=identifiers).order_by('id').values())==saved
    assert list(M3UVODCategoryRelation.objects.filter(category_id__in=identifiers).order_by('id').values())==links
if result is None:raise RuntimeError('Missing source observation')
result['original_category_and_relationship_rows_exactly_restored']=True
print(json.dumps(result))
