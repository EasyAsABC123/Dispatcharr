"""Seed actual VOD ORM records only in the isolated playback fixture.
Original application credit: upstream Dispatcharr authors and contributors.
"""
import json
import os
import sys
import uuid
from pathlib import Path

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('VOD seed is restricted to the disposable playback fixture')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.m3u.models import M3UAccount
from apps.vod.models import Movie, M3UMovieRelation, Series, Episode, M3UEpisodeRelation
from apps.accounts.models import User
from core.models import CoreSettings, StreamProfile

account, _ = M3UAccount.objects.update_or_create(name='Rust VOD fixture XC', defaults={
    'account_type': 'XC', 'server_url': 'http://origin:8080/player_api.php',
    'username': 'fixture', 'password': 'fixture', 'is_active': True,
    'max_streams': 2, 'priority': 100})
provider = account.profiles.get(is_default=True)
provider.search_pattern = '^(.*)$'
provider.replace_pattern = '$1'
provider.max_streams = 2
provider.save()
movie, _ = Movie.objects.update_or_create(uuid=uuid.UUID('a4d78cd1-bf9f-4a63-a746-f40566825cf9'),
    defaults={'name': 'Offline native VOD fixture'})
relation, _ = M3UMovieRelation.objects.update_or_create(m3u_account=account, stream_id='fixture',
    defaults={'movie': movie, 'container_extension': 'mp4'})
series, _ = Series.objects.update_or_create(uuid=uuid.UUID('2fb292d4-1236-4ed4-bf70-98d6308a3773'), defaults={'name': 'Offline VOD series'})
episode, _ = Episode.objects.update_or_create(uuid=uuid.UUID('3c884edf-a48f-4e91-adf3-f876c8094814'), defaults={'name': 'Offline VOD episode', 'series': series, 'season_number': 1, 'episode_number': 1})
M3UEpisodeRelation.objects.update_or_create(m3u_account=account, stream_id='fixture', defaults={'episode': episode, 'container_extension': 'mp4'})
for name, properties, cap in [('flag-disabled', {'vod_movies_enabled': False}, 0), ('capped', {}, 1)]:
    User.objects.update_or_create(username='rust-vod-fixture-'+name, defaults={'is_active': True, 'user_level': 0, 'stream_limit': cap, 'password': '!', 'custom_properties': properties})
settings = CoreSettings.get_stream_settings().copy()
settings['default_stream_profile'] = StreamProfile.get_locked('Proxy').pk
CoreSettings.objects.update_or_create(key='stream_settings', defaults={'name': 'Fixture stream settings', 'value': settings})
result = {'movie_uuid': str(movie.uuid), 'episode_uuid': str(episode.uuid), 'series_uuid': str(series.uuid), 'provider_profile_id': provider.pk,
          'provider_account_id': account.pk, 'relation_id': relation.pk,
          'native_path': f'/proxy/vod/movie/{movie.uuid}',
          'scope': 'real ORM native XC file playback; not full VOD parity'}
Path('/evidence/vod-seed.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
