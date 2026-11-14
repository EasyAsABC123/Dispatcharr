"""Finite Source-only normal future-clocked case; no app imports or execution."""
TITLE='Owned Parented Artwork'
CHANNEL='Owned Parented Channel'
ARTWORK='apps.channels.tasks.prefetch_recording_artwork'
RECORD='apps.channels.tasks.run_recording'
QUEUE_ARTWORK='celery'
QUEUE_RECORD='dvr'
EXPECTED_ROUTES={RECORD:{'queue':'dvr'}}
LEAD_SECONDS=60
DURATION_SECONDS=30
INPUT_SECONDS=120
TASK_DEADLINE_SECONDS=180
ORIGIN_MAX_REQUESTS=4
ORIGIN_BODY_LIMIT=16*1024*1024
SOURCE_FILE_LIMIT=32*1024*1024
SOURCE_FILES_TOTAL=64*1024*1024
SOURCE_FILE_COUNT=128
PNG_BASE64='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jWZkAAAAASUVORK5CYII='
def actual_argv(role):
 base=['/dispatcharrpy/bin/celery','-A','observe:app']
 if role=='beat':return base+['beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile','/evidence/owned-beat.pid','--schedule','/evidence/owned-beat-local','--loglevel=WARNING']
 if role=='artwork':return base+['worker','--pool=prefork','--concurrency=1','-Q',QUEUE_ARTWORK,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-owned-clocked-artwork@fixture']
 if role=='dvr':return base+['worker','--pool=threads','--concurrency=1','-Q',QUEUE_RECORD,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-owned-clocked-dvr@fixture']
 raise ValueError('OnlyThreeOwnedActualRoles')
def expected_url_path(channel_uuid):
 import uuid
 if str(uuid.UUID(channel_uuid))!=channel_uuid:raise ValueError('ActualCanonicalChannelUUID')
 return '/proxy/ts/stream/'+channel_uuid
