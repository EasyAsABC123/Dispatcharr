"""Pure fixed owned fixture specification. No application import or execution."""
TITLE='Owned Parented Artwork'
TVG='rust.owned.parented.artwork'
SOURCE='rust-owned-parented-artwork-source'
CHANNEL='Owned Parented Channel'
REFRESH='apps.epg.tasks.refresh_epg_data'
SERIES='apps.channels.tasks.evaluate_series_rules'
ARTWORK='apps.channels.tasks.prefetch_recording_artwork'
INDEX='apps.epg.tasks.build_programme_index_task'
SETUP='apps.epg.tasks.parse_programs_for_tvg_id'
QUEUES={'refresh':'rust-owned-parented-refresh','series':'rust-owned-parented-series','artwork':'rust-owned-parented-artwork'}
ROUTES={REFRESH:{'queue':QUEUES['refresh']},SERIES:{'queue':QUEUES['series']},ARTWORK:{'queue':QUEUES['artwork']}}
CASES=['refresh-created-source','parented-series-positive','artwork-two-real-children','refresh-repeat','parented-series-repeat']
EXPECTED_TASKS=[REFRESH,SERIES,ARTWORK,ARTWORK,REFRESH,SERIES]
PNG_BASE64='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jWZkAAAAASUVORK5CYII='
def queue_for(role):
 if role not in QUEUES:raise ValueError('OnlyFixedOwnedRole')
 return QUEUES[role]
def xmltv(start,end):
 # Source times are the actual future fixture instants, not captured clock reuse.
 from xml.sax.saxutils import escape
 return ('<?xml version="1.0" encoding="UTF-8"?>\n<tv><channel id="'+TVG+'"><display-name>'+CHANNEL+'</display-name></channel><programme start="'+start.strftime('%Y%m%d%H%M%S %z')+'" stop="'+end.strftime('%Y%m%d%H%M%S %z')+'" channel="'+TVG+'"><title>'+escape(TITLE)+'</title><sub-title>Owned Episode One</sub-title><episode-num system="xmltv_ns">0.0.</episode-num></programme></tv>\n').encode()
def validate_role_frames(raw_frames,role,ids):
 """Known owned Source-role safety preflight only, not production intake."""
 import base64,json,uuid
 queue=queue_for(role);expected={'refresh':REFRESH,'series':SERIES,'artwork':ARTWORK}[role]
 if type(ids)is not list or len(ids)!=(2 if role=='artwork'else 1)or len(set(ids))!=len(ids):raise ValueError('ExactRoleTaskIDs')
 for ident in ids:
  parsed=uuid.UUID(ident)
  if str(parsed)!=ident or parsed.version!=4 or parsed.variant!=uuid.RFC_4122:raise ValueError('CanonicalActualSourceTaskID')
 frames=[json.loads(base64.b64decode(v,validate=True))for v in raw_frames]
 if len(frames)!=len(ids):raise ValueError('ExactOwnedQueueMultiplicity')
 if [f['headers']['id']for f in reversed(frames)]!=ids:raise ValueError('ActualOwnedFIFOIDs')
 for frame in frames:
  delivery=frame['properties']['delivery_info']
  if frame['headers']['task']!=expected or delivery['routing_key']!=queue or delivery['exchange']!='':raise ValueError('ExactOwnedRoleFamilyExchangeRoute')
 return True
