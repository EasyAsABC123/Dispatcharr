"""Owned Native resolver boundary fixture; never invokes Source build_command."""
import json,os,sys
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('OwnedBenchmarkOnly')
count=int(os.environ['PROFILE_BOUND_COUNT'])
if count not in (32,33):raise RuntimeError('OnlyDeclaredBoundaryCounts')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import transaction
from apps.channels.models import Channel,ChannelStream,Stream
from apps.m3u.models import M3UAccount
from core.models import StreamProfile
row=json.loads(Path('/evidence/seed.json').read_text())['channels'][9]
with transaction.atomic():
 channel=Channel.objects.get(pk=row['channel_pk'],uuid=row['channel_uuid'],name='Offline benchmark channel 9')
 profile=StreamProfile.objects.get(name='rust-m3-owned-input-source18',command='ffmpeg',locked=False,is_active=True)
 account=M3UAccount.get_custom_account()
 if account.server_group_id is not None or not account.is_active:raise RuntimeError('OwnedPlainProviderOnly')
 existing=list(channel.streams.all())
 if any(not s.is_custom or not s.url.startswith('http://origin:8080/live/')for s in existing):raise RuntimeError('OwnedOriginOnly')
 ChannelStream.objects.filter(channel=channel).delete()
 streams=[Stream(name='Offline input capacity '+str(i),url='http://origin:8080/live/9.ts',m3u_account=account,stream_profile=profile,is_custom=True)for i in range(count)]
 Stream.objects.bulk_create(streams)
 ChannelStream.objects.bulk_create([ChannelStream(channel=channel,stream=s,order=i)for i,s in enumerate(streams)])
 actual=ChannelStream.objects.filter(channel=channel).count()
 if actual!=count:raise RuntimeError('ActualBoundaryRows')
print(json.dumps({'prepared':True,'actual_candidates':actual,'channel_UUID':str(channel.uuid),'profile_pk':profile.pk,'channel_pk':channel.pk,'Native_policy_boundary_only':True,'Source_model_method_replayed':False}))
