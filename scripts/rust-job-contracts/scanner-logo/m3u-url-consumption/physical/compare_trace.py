"""Retain fields and check declared temporal semantics, not wall-date equality."""
from datetime import datetime,timezone
import json
TABLES=('stream_settings_rows','accounts','profiles','streams','groups','group_links','channels','channel_streams','system_events')
TIME={'created_at','updated_at','last_seen','timestamp','stream_stats_updated_at'}
def capture_snapshot(value):
 # Exactly the default=str JSON boundary used by retained Source/native data.
 # No field selection, date truncation or value replacement.
 return json.loads(json.dumps(value,default=str))
def instant(v):
 if not isinstance(v,str):raise ValueError('Timestamp must remain a string')
 t=datetime.fromisoformat(v.replace('Z','+00:00'))
 if t.tzinfo is None:t=t.replace(tzinfo=timezone.utc)
 return t.timestamp()
def compare(reference,before,after,source_windows,native_windows):
 differences=[];temporal=[]
 for table in TABLES:
  originals=reference['after'][table];actual=after[table]
  if len(originals)!=len(actual):differences.append({'path':table,'kind':'row-count','Source':len(originals),'native':len(actual)});continue
  rb={v['id']:v for v in reference['before'][table]};nb={v['id']:v for v in before[table]}
  for index,(expected,observed)in enumerate(zip(originals,actual)):
   if set(expected)!=set(observed):differences.append({'path':f'{table}/{index}','kind':'field-presence','Source':sorted(expected),'native':sorted(observed)});continue
   for key,value in expected.items():
    path=f'{table}/{index}/{key}';got=observed[key]
    if key not in TIME:
     if value!=got:differences.append({'path':path,'kind':'value','Source':value,'native':got})
     continue
    old=rb.get(expected['id'],{});prior=nb.get(observed['id'],{})
    if value is None:
     if got is not None:differences.append({'path':path,'kind':'null','Source':value,'native':got})
     temporal.append({'path':path,'kind':'null-retained','Source':value,'native':got});continue
    preserved=key in old and old[key]==value
    if preserved:
     if key not in prior or got!=prior[key]:differences.append({'path':path,'kind':'preserved-timestamp-changed','Source_before':old.get(key),'Source_after':value,'native_before':prior.get(key),'native_after':got})
     temporal.append({'path':path,'kind':'preserved','Source_before':old.get(key),'Source_after':value,'native_before':prior.get(key),'native_after':got});continue
    try:
     source_time=instant(value);native_time=instant(got)
     phases=[name for name,window in source_windows.items()if window[0]<=source_time<=window[1]]
     if len(phases)!=1:
      differences.append({'path':path,'kind':'Source-timestamp-origin-unresolved','Source':value,'phases':phases});continue
     phase=phases[0];sw=source_windows[phase];nw=native_windows[phase]
     if not sw[0]<=source_time<=sw[1]:differences.append({'path':path,'kind':'Source-outside-observed-command','value':value,'window':sw})
     if not nw[0]<=native_time<=nw[1]:differences.append({'path':path,'kind':'native-outside-command','value':got,'window':nw})
     if key in old and key in prior and old[key]is not None and prior[key]is not None:
      if value!=old[key] and got==prior[key]:differences.append({'path':path,'kind':'changed-timestamp-not-changed','Source_before':old[key],'Source_after':value,'native_before':prior[key],'native_after':got})
      if source_time>=instant(old[key])and native_time<instant(prior[key]):differences.append({'path':path,'kind':'timestamp-regression'})
     temporal.append({'path':path,'kind':'new-or-changed','Source_before':old.get(key),'Source_after':value,'native_before':prior.get(key),'native_after':got,'phase':phase,'Source_window':sw,'native_window':nw})
    except (TypeError,ValueError)as error:differences.append({'path':path,'kind':'timestamp-type','Source':value,'native':got,'error':str(error)})
 if reference['after']['events']!=after['events']:differences.append({'path':'events','kind':'ordered-typed-events','Source':reference['after']['events'],'native':after['events']})
 if reference['after']['files']!=after['files']:differences.append({'path':'files','kind':'exact-input-stat-and-bytes','Source':reference['after']['files'],'native':after['files']})
 return {'passed':not differences,'differences':differences,'temporal_fields_retained':temporal,'timestamp_fields_dropped':0}
