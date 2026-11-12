"""Fail-closed offline command-reply to original caller-frame attribution."""
def join(events):
 calls={e['frame']:e for e in events if e['kind']=='call'}
 exits={e['frame']:e for e in events if e['kind']in['normal_exit','unwind']}
 links=[e for e in events if e['kind']=='wire_link'];opened={e['connection']:e for e in events if e['kind']=='wire_connection_open'};closed={e['connection']:e for e in events if e['kind']=='wire_connection_close'};matched=[];unmatched=[]
 for effect in[e for e in events if e['kind']=='actual_counter_commit']:
  connection=effect['connection'];wire=opened.get(connection);end=closed.get(connection);candidates={}
  if wire is not None and end is not None:
   for link in links:
    call=calls.get(link['frame']);returned=exits.get(link['frame']);writer=calls.get(link.get('writer_frame'));writer_return=exits.get(link.get('writer_frame'))
    if any(x is None for x in[call,returned,writer,writer_return]):continue
    if link['local_port']!=wire['peer_port']or link['remote_port']!=wire['local_port']:continue
    sent=effect['command_at'];at=effect['at']
    if not wire['at']<=sent<=at<=end['at']:continue
    if not call['at']<=link['at']<=sent<=at<=returned['at']:continue
    if not writer['at']<=call['at']<=returned['at']<=writer_return['at']:continue
    if writer.get('rule_kind')!='counter':continue
    candidates[link['frame']]={'effect_sequence':effect['sequence'],'wire_connection':connection,'wire_call_frame':link['frame'],'writer_frame':writer['frame'],'role':writer.get('role'),'action':writer.get('action'),'sid_sha256':writer.get('sid_sha256'),'generator':writer.get('generator'),'callback':writer.get('callback'),'creator_generator':writer.get('creator_generator'),'writer_code':writer['code'],'writer_firstline':writer['firstline'],'value':effect['value'],'via':effect['via']}
  if len(candidates)==1:matched.append(next(iter(candidates.values())))
  else:unmatched.append({'effect_sequence':effect['sequence'],'candidate_count':len(candidates),'reason':'missing-or-overlapping-caller-wire-interval'})
 return{'all_actual_counter_writes_attributed':not unmatched,'matched':matched,'unmatched':unmatched,'scope':'only complete exact TCP incarnation/command/caller windows; no owner guessing'}
