"""Ephemeral SQL and Redis observations for the Channel set-epg action."""
import base64
import hashlib
import json

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone
import redis

from apps.channels.models import Channel, ChannelGroup, ChannelProfile
from apps.epg.models import EPGData, EPGSource
from core.utils import RedisClient

PREFIX = 'rust-probe-channel-batch-epg-'
TASK = 'apps.epg.tasks.parse_programs_for_tvg_id'


def dispatch(data):
    if data['action'] == 'epg_batch_seed':
        with transaction.atomic():
            if Channel.objects.filter(name__startswith=PREFIX).exists():
                raise RuntimeError('Set EPG fixture already ran')
            group = ChannelGroup.objects.create_manual(name=PREFIX + 'group')
            profile, = ChannelProfile.objects.bulk_create([ChannelProfile(name=PREFIX + 'profile')])
            names = ('dummy', 'xmltv')
            sources = EPGSource.objects.bulk_create([
                EPGSource(name=PREFIX + name, source_type=name, is_active=False)
                for name in names
            ])
            guides = EPGData.objects.bulk_create([
                EPGData(name=PREFIX + name, tvg_id=PREFIX + name, epg_source=source)
                for name, source in zip(names, sources, strict=True)
            ] + [EPGData(name=PREFIX + 'orphan', tvg_id=PREFIX + 'orphan')])
            channels = Channel.objects.bulk_create([
                Channel(name=PREFIX + backend, channel_group=group, channel_number=900 + i,
                        user_level=10, is_adult=True, hidden_from_output=True)
                for i, backend in enumerate(f'{side}-{number}' for side in ('source', 'native')
                                            for number in range(103))
            ])
        return {'source': [row.pk for row in channels[:103]],
                'native': [row.pk for row in channels[103:]], 'dummy': guides[0].pk,
                'xmltv': guides[1].pk, 'orphan': guides[2].pk, 'profile': profile.pk}
    if data['action'] != 'epg_batch_snapshot':
        raise RuntimeError('Unexpected Set EPG fixture operation')

    with transaction.atomic(), connection.cursor() as cursor:
        tables = ('channel', 'channelgroup', 'channeloverride', 'channelgroupm3uaccount', 'channelprofilemembership', 'channelstream')
        rows, sequences = {}, {}
        for table in tables:
            cursor.execute(f'SELECT to_jsonb(t)::text FROM dispatcharr_channels_{table} t ORDER BY id')
            rows[table] = [json.loads(row[0]) for row in cursor.fetchall()]
            cursor.execute(f'SELECT last_value,is_called FROM dispatcharr_channels_{table}_id_seq')
            sequences[table] = list(cursor.fetchone())
    broker = redis.Redis.from_url(settings.CELERY_BROKER_URL)
    if broker.llen('celery') > 64:
        raise RuntimeError('Owned task queue exceeded observation bound')
    jobs = []
    raw_frames = broker.lrange('celery', 0, 63)
    for raw in raw_frames:
        frame = json.loads(raw)
        if frame['headers']['task'] == TASK:
            args, kwargs, _ = json.loads(base64.b64decode(frame['body'], validate=True))
            jobs.append({'args': args, 'kwargs': kwargs, 'id': frame['headers']['id']})
    generation = RedisClient.get_client().get('epg_content_generation')
    return {'tables': rows, 'sequences': sequences, 'generation': int(generation or 0), 'jobs': jobs,
            'queue_frame_digests': [hashlib.sha256(raw).hexdigest() for raw in raw_frames]}
