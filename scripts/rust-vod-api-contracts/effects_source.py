"""Read only the owned VOD fixture effects after packaged native reads.
All application credit remains with original Dispatcharr authors/contributors.
"""
import os,sys,json
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Explicit disposable fixture guards required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.m3u.models import M3UAccount
from apps.vod.models import Movie,Series,Episode,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UVODCategoryRelation
prefix='rust-probe-vod-rest-';accounts=M3UAccount.objects.filter(name__startswith=prefix)
if accounts.count()!=3:raise RuntimeError('Wrong owned namespace')
result={'uncategorized':[list(row)for row in VODCategory.objects.filter(name='Uncategorized').order_by('id').values_list('id','category_type')],
'owned_category_relations':[list(row)for row in M3UVODCategoryRelation.objects.filter(m3u_account__in=accounts).order_by('id').values_list('id','category_id','m3u_account_id','enabled')],
'movie_refresh_rows':[dict(row,last_advanced_refresh=row['last_advanced_refresh'].isoformat()if row['last_advanced_refresh']else None)for row in M3UMovieRelation.objects.filter(m3u_account__in=accounts).order_by('id').values('id','custom_properties','last_advanced_refresh')],
'series_refresh_rows':[dict(row,last_episode_refresh=row['last_episode_refresh'].isoformat()if row['last_episode_refresh']else None)for row in M3USeriesRelation.objects.filter(m3u_account__in=accounts).order_by('id').values('id','custom_properties','last_episode_refresh')],
'owned_counts':{'movies':Movie.objects.filter(name__startswith=prefix).count(),'series':Series.objects.filter(name__startswith=prefix).count(),'episodes':Episode.objects.filter(name__startswith=prefix).count()}}
print(json.dumps(result))
