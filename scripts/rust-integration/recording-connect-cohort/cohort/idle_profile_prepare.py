"""Prepare ONLY owned benchmark channels for new real input-profile media cases.
No model method replacement, command execution or previous Source18 replay.
"""
import json,os,re,sys
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('OwnedBenchmarkOnly')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import transaction
from apps.channels.models import Channel,ChannelOverride,Stream
from core.models import StreamProfile
seed=json.loads(Path('/evidence/seed.json').read_text());rows=seed['channels']
if len(rows)!=10 or seed['profile']!='Proxy' or seed['profile_locked']is not True:raise RuntimeError('ExactExistingOwnedBenchmarkSeed')
template=StreamProfile.get_locked('FFmpeg')
if template.command!='ffmpeg' or '{streamUrl}'not in template.parameters:raise RuntimeError('PinnedOriginalFFmpegTemplateRequired')
parameters=template.parameters
with transaction.atomic():
 profile=StreamProfile.objects.create(name='rust-m3-owned-input-source18',command='ffmpeg',parameters=parameters,locked=False,is_active=True,user_agent=template.user_agent)
 observed=[]
 for index,row in enumerate(rows):
  channel=Channel.objects.get(pk=row['channel_pk'],uuid=row['channel_uuid'],name='Offline benchmark channel '+str(index))
  if ChannelOverride.objects.filter(channel=channel).exists():raise RuntimeError('NoOwnedChannelOverridesExpected')
  streams=list(Stream.objects.filter(pk__in=[row['stream_pk'],row['backup_stream_pk']]).order_by('pk'))
  if len(streams)!=2 or any(not s.is_custom or not s.url.startswith('http://origin:8080/live/') for s in streams):raise RuntimeError('ExactOwnedOriginStreams')
  Channel.objects.filter(pk=channel.pk).update(stream_profile=profile)
  Stream.objects.filter(pk__in=[s.pk for s in streams]).update(stream_profile=profile)
  observed.append({'channel_pk':channel.pk,'channel_UUID':str(channel.uuid),'stream_pks':[s.pk for s in streams],'profile_pk':profile.pk})
print(json.dumps({'prepared':True,'scope':'New input-profile media fixture only','command':template.command,'parameters':parameters,'Original_template_profile_pk':template.pk,'locked':False,'source18_argument_replayed':False,'model_rows':observed,'active_OutputProfile_or_provider_transform':False}))
