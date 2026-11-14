"""Two real, fixed production requests expose known profile501 cells.
The global batch fixture owns SQL/Bearer/media trap/bootstrap/cleanup. No app or
process starts here; no redirects/retries and no arbitrary URL/request interface.
"""
import argparse,hashlib,json,re,time
from pathlib import Path
from absolute_http import Observation

def requests(fixture,node='Main/active-profile-status-probes'):
 if node=='Main/input-profile-and-provider-probes':
  rows=[]
  for name,key in [('custom-input-profile','owned_custom_input_channel_UUID'),('transformed-provider','owned_transformed_provider_channel_UUID'),('credential-provider-pool','owned_credential_pool_channel_UUID')]:
   channel=fixture[key]
   if not re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',channel):raise ValueError('ActualOwnedInputProviderChannel')
   rows.append((name,'/proxy/ts/stream/'+channel,200))
  return rows
 channel=fixture['owned_channel_UUID'];profile=fixture['owned_active_OutputProfile_id']
 if not re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',channel)or type(profile)is not int or profile<=0:raise ValueError('ActualOwnedRowIDsRequired')
 path='/proxy/ts/stream/'+channel+'?output_profile='+str(profile)
 return [('active-profile-ts',path,200),('active-profile-hls',path+'&output_format=hls',302)]

def run(context):
 from source_workflow import validate_inputs
 validate_inputs()
 if context['batch']!='M2-M3-M5-wide-1'or context['node_id']not in ['Main/active-profile-status-probes','Main/input-profile-and-provider-probes']:raise ValueError('GlobalBatchNodeIdentity')
 fp=Path(context['fixture']);raw=fp.read_bytes()
 if hashlib.sha256(raw).hexdigest()!=context['fixture_sha256']:raise ValueError('ExactGlobalFixtureBinding')
 fixture=json.loads(raw)
 if fixture['candidate_image_id']!=context['candidate_image_id']or fixture['owned_SQL_rows_attested']is not True:raise ValueError('ActualCurrentCandidateAndOwnedRows')
 out=Path(context['output']);out.mkdir(parents=True,exist_ok=True);dest=out/'profile-status-probes-private.json'
 if dest.exists():raise ValueError('OneAttemptNoAdaptiveRetry')
 report={'passed':False,'scope':'Literal known-required-route status probes only; not fullprofile/media/Sourceparity','actual_attempted':0,'actual_completed':0,'requests':[],'candidate_image_id':context['candidate_image_id'],'Main_profile_delivery_or_fullM3':False}
 def save():dest.write_text(json.dumps(report,indent=2)+'\n')
 declared=requests(fixture,context['node_id'])
 report['declared_requests']=len(declared);save()
 for label,path,expected in declared:
  record={'name':label,'path':path,'method':'GET','Source_code_expected_status':expected,'Source_reference':fixture.get('source_reference',{}).get(label),'began_monotonic':time.monotonic(),'actual_attempted':False};report['requests'].append(record);save();observation=None
  try:
   observation=Observation(fixture['candidate_base'],seconds=10,io_seconds=8,address=fixture.get('candidate_address','127.0.0.1'))
   record['actual_attempted']=True;report['actual_attempted']+=1
   response=observation.request(path,fixture['private_auth_headers']);prefix=response.raw.read(8192)
   record.update(status=response.status_code,body_prefix_bytes=len(prefix),body_prefix_hex=prefix.hex(),body_prefix_sha256=hashlib.sha256(prefix).hexdigest(),header_names=sorted(response.headers),content_type=next((v for k,v in response.headers.items()if k.lower()=='content-type'),None),status_matches_expected=response.status_code==expected,complete=True)
   report['actual_completed']+=1;observation.complete()
  except BaseException as e:record['exception']={'type':type(e).__name__,'message':str(e)[:1024]}
  finally:
   if observation is not None:
    try:record['physical_close']=observation.cancel()
    except BaseException as e:record['close_error']={'type':type(e).__name__,'message':str(e)[:1024]}
   record['completed_monotonic']=time.monotonic();save()
 report['passed']=report['actual_attempted']==report['actual_completed']==len(declared) and all(r.get('status_matches_expected')is True and 'exception'not in r and 'close_error'not in r for r in report['requests']);save()
 return {'actual_exit':0 if report['passed']else 1,'report':str(dest),'status':'passed'if report['passed']else'failed','note':'Actual501 is a failure and is retained; decode/pipeline/shutdown are separate required cells'}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--context',type=Path,required=True);a=p.parse_args();r=run(json.loads(a.context.read_text()));print(json.dumps(r));raise SystemExit(r['actual_exit'])
