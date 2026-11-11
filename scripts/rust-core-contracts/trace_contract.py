"""Strict ordered actual Source/native core projection; no prospective runtime."""
import json
def equal(a,b):return json.dumps(a,sort_keys=True,separators=(',',':'),allow_nan=False)==json.dumps(b,sort_keys=True,separators=(',',':'),allow_nan=False)
EXPECTED_LABELS=['empty', 'partial100', 'published_two_packets', 'cursor_adapter', 'pending_complete_and_partial', 'mark_flushes_complete_drops_partial', 'post_mark_stamping_first_public_chunk', 'second_mark_flushes_previously_stamped_pending_PID', 'reset_discards_local_leftovers', 'real_ttl_expired', 'fresh_after_expiry', 'stop_preserves_redis_reads', 'stopped_write_rejected', 'new_writer_reads_existing_index', 'new_writer_publishes_next_existing_index', 'new_writer_reads_persistent_index_after_ttl', 'new_writer_continues_after_ttl', 'initial_mark_before_first_publication', 'initial_mark_first_publication', 'stop_retains_only_partial_packet_source_boundary', 'stop_discards_complete_and_partial', 'reset_discards_only_partial_packet', 'marker_expires_while_media_retained']
FIELDS=['index','write_buffer_bytes','partial_packet_bytes','discontinuities','oldest_last_consumed','age0_last_consumed','age60_last_consumed','reader_discontinuity','passed']
def compare(source,native):
 if source.get('passed')is not True or native.get('passed')is not True:raise ValueError('Actual peer failed')
 if [r.get('label')for r in source['records']]!=EXPECTED_LABELS or [r.get('label')for r in native['records']]!=EXPECTED_LABELS:raise ValueError('Missing/extra/reordered actual trace')
 differences=[]
 for original,rust in zip(source['records'],native['records']):
  if original['label']=='cursor_adapter':
   if original.get('full_bytes_verified')is not True:raise ValueError('Original byte verification missing/failed')
   if any(type(v)is not int for v in [original['Source_last_consumed_before'],original['Source_last_consumed_after'],rust.get('native_next_before'),rust.get('native_next_after')]):raise ValueError('Cursor must be actual integer observation')
   if not(rust.get('native_execution_observed')is True and rust.get('full_bytes_verified')is True and rust.get('native_next_before')==original['Source_last_consumed_before']+1 and rust.get('native_next_after')==original['Source_last_consumed_after']+1):raise ValueError('Actual cursor/full-byte observation missing')
   continue
  for key in FIELDS:
   if (key in original)!=(key in rust):
    differences.append({'label':original['label'],'field':key,'issue':'field presence mismatch'});continue
   if not equal(original.get(key),rust.get(key)):differences.append({'label':original['label'],'field':key,'source':original.get(key),'native':rust.get(key)})
  if ('chunks'in original)!=('chunks'in rust):
   differences.append({'label':original['label'],'field':'chunks','issue':'field presence mismatch'});continue
  for key in ['sequence','bytes','hex']:
   a=[r[key]for r in original.get('chunks',[])];b=[r[key]for r in rust.get('chunks',[])]
   if not equal(a,b):differences.append({'label':original['label'],'field':'chunks.'+key,'source':a,'native':b})
 return differences

PENDING_LABELS=['mark1','publish1','mark2','publish2','pending_mark3_at_full_payload_cap']
def compare_pending(source,native):
 if source.get('passed')is not True or native.get('passed')is not True:raise ValueError('Actual pending-marker peer failed')
 if [r.get('label')for r in source['records']]!=PENDING_LABELS or [r.get('label')for r in native['records']]!=PENDING_LABELS:raise ValueError('Pending-marker record order/count changed')
 # Predeclared common domain ends before a third payload: Source itself has no
 # native byte cap. Do not relabel its over-budget third publication as parity.
 over=source['source_only_over_budget_third_publication']
 if over['index']!=3 or sum(x['bytes']for x in over['chunks'])!=1128:raise ValueError('Source over-budget witness missing')
 if source['records'][-1]['index']!=2 or source['records'][-1]['payload_bytes']!=752:raise ValueError('Common payload domain changed')
 differences=[]
 for a,b in zip(source['records'],native['records']):
  for key in FIELDS+['mark_result','mark_error','payload_bytes','chunks']:
   if (key in a)!=(key in b):differences.append({'label':a['label'],'field':key,'issue':'field presence mismatch'});continue
   if key=='chunks':
    # SHA is redundant; compare every synthetic byte and exact sequence/type.
    av=[{k:v[k]for k in ['sequence','bytes','hex']}for v in a.get(key,[])];bv=[{k:v[k]for k in ['sequence','bytes','hex']}for v in b.get(key,[])]
   else:av=a.get(key);bv=b.get(key)
   if not equal(av,bv):differences.append({'label':a['label'],'field':key,'source':av,'native':bv})
 return differences
