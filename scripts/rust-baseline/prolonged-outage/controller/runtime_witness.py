"""Owned Source-process helper: actual effective settings/code/profile evidence.
No Django or Redis import until explicitly invoked. CLI blocked before review.
"""
import ast,hashlib,inspect,json,os,textwrap
from pathlib import Path
EXECUTION_BLOCKED=True

def capture(channel_uuid,redis_client):
 if os.environ.get('LIVE_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedFixtureRequired')
 from apps.channels.models import Channel,ChannelStream
 from apps.proxy.live_proxy.config_helper import ConfigHelper as C
 from apps.proxy.live_proxy.input.http_streamer import HTTPStreamReader
 from core.models import StreamProfile
 channel=Channel.objects.get(uuid=channel_uuid);profile=channel.get_stream_profile()
 links=list(ChannelStream.objects.filter(channel=channel).select_related('stream').order_by('order','pk'))
 from urllib.parse import urlsplit
 candidates=[]
 for link in links:
  url=urlsplit(link.stream.url)
  if url.hostname!='origin'or url.port!=8080 or url.query or url.fragment:raise ValueError('NonOwnedCandidate')
  candidates.append({'id':link.stream_id,'path':url.path})
 raw=redis_client.hgetall('live:channel:'+str(channel_uuid)+':metadata');metadata={k.decode():v.decode()for k,v in raw.items()}
 value=metadata.get('stream_profile');resolved=None
 if value is not None:
  if value.isdigit():resolved=StreamProfile.objects.get(pk=int(value))
  elif value==profile.name:resolved=StreamProfile.objects.get(pk=profile.pk,name=value)
 native=not profile.is_proxy()
 code=ast.parse(textwrap.dedent(inspect.getsource(HTTPStreamReader._read_stream)))
 timeouts=[];retries=[]
 for node in ast.walk(code):
  if not isinstance(node,ast.Call):continue
  if isinstance(node.func,ast.Attribute)and node.func.attr=='get':timeouts +=[ast.literal_eval(x.value)for x in node.keywords if x.arg=='timeout']
  if isinstance(node.func,ast.Name)and node.func.id=='HTTPAdapter':retries +=[ast.literal_eval(x.value)for x in node.keywords if x.arg=='max_retries']
 if len(timeouts)!=1 or len(retries)!=1 or len(timeouts[0])!=2:raise ValueError('ActualHTTPReaderPathAmbiguous')
 settings={'max_retries':C.max_retries(),'rotation_cooldown':C.failover_rotation_cooldown(),'switch_timeout':C.url_switch_timeout(),'buffering_timeout':C.buffering_timeout(),'stream_timeout':C.stream_timeout(),'failover_grace':C.failover_grace_period(),'connection_timeout':C.connection_timeout(),'health_interval':C.get('HEALTH_CHECK_INTERVAL'),'startup_timeout':C.stream_startup_timeout(),'HTTP_connect_timeout':timeouts[0][0],'HTTP_read_timeout':timeouts[0][1],'HTTP_adapter_retries':retries[0],'candidate_count':len(candidates)}
 authority=json.loads(Path(__file__).with_name('source-authority.json').read_text())
 hashes={name:hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()for name in authority['loaded_file_sha256_required']}
 witness={'profile_id':profile.pk,'profile_name':profile.name,'profile_locked':profile.locked,'metadata_profile_raw':value,'metadata_profile_resolved_id':None if resolved is None else resolved.pk,'metadata_profile_resolves_to_owned_locked_Proxy':resolved is not None and resolved.pk==profile.pk and resolved.is_proxy(),'initial_owner_transcode':native,'context_not_exhausted':{'retry_window_seconds':C.retry_window_seconds(),'max_stream_switches':C.max_stream_switches(),'stable_connection_threshold':C.stable_connection_threshold(),'max_keepalive_duration':C.get('MAX_KEEPALIVE_DURATION')},'actual_HTTP_reader_code_file':HTTPStreamReader._read_stream.__code__.co_filename,'loaded_Source_sha256':hashes,'metadata_owner_present':bool(metadata.get('owner')),'metadata_stream_id':metadata.get('stream_id'),'metadata_input_url':metadata.get('url'),'owned_candidates':candidates,'proof_scope':'initial-owner native Proxy branch from actual selected profile/metadata and pinned url_utils/Core/server/manager; later reconstructed/follower path not assumed'}
 if metadata.get('url')not in{'http://origin:8080'+x['path']for x in candidates}:raise ValueError('ActualInputURLNotOwnedCandidate')
 if not witness['metadata_owner_present']or metadata.get('stream_id')not in{str(x['id'])for x in candidates}:raise ValueError('OwnedActiveInputWitnessMissing')
 return settings,witness
if __name__=='__main__':raise RuntimeError('RuntimeWitnessExecutionBlockedPendingRootReview')
