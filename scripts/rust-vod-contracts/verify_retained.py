"""Independent read-only fixture cleanup/evidence evaluation; never launches apps."""
import argparse,json,os,re,subprocess,time
from pathlib import Path

def verify(output,docker,cleanup_only=False):
 output=Path(output);marker=json.loads((output/'owned-project.json').read_text());project=marker['project']
 if not re.fullmatch(r'dispatcharr-vod-contract-[a-f0-9]{10}',project):raise ValueError('Not an owned VOD fixture project')
 deadline=time.monotonic()+30
 def command(args):
  left=deadline-time.monotonic()
  if left<=0:raise TimeoutError('Independent read-only cleanup deadline')
  result=subprocess.run([docker,*args],capture_output=True,text=True,timeout=min(5,left),check=True)
  if len(result.stdout)+len(result.stderr)>65536:raise ValueError('Cleanup metadata capacity')
  return result.stdout.split()
 inventory={kind:command([kind,'ls','-q','--filter','label=com.docker.compose.project='+project])for kind in('container','network','volume')}
 history=json.loads((output/'owned-container-history.json').read_text())if(output/'owned-container-history.json').is_file()else[]
 ids={item['Id']:command(['container','ls','-aq','--no-trunc','--filter','id='+item['Id']])for item in history}
 generations=[]
 for item in history:
  try:current=Path('/proc',str(item['pid']),'stat').read_text().rsplit(')',1)[1].split()[19]
  except(FileNotFoundError,ProcessLookupError):current=None
  generations.append({'Id':item['Id'],'actual_recorded_generation_absent':not item['generation']or current!=item['generation']})
 cleanup=not any(inventory.values())and not any(ids.values())and all(row['actual_recorded_generation_absent']for row in generations)
 result={'scope':'Independent actual named-project/CID/recorded-host-generation absence; read-only','project':project,'inventories':inventory,'exact_ids':ids,'generations':generations,'passed':cleanup,'Source_replayed':False,'full_M4_passed':False}
 if not cleanup_only:
  report=json.loads((output/'vod-contracts.json').read_text())
  requirements={
   'actual_native_counterpart_passed':report.get('passed')is True,
   'no_Source_business_HTTP_replay':report.get('Source_business_HTTP_requests_executed')==0,
   'retained_authority_declared':report.get('retained_Source_authority',{}).get('Source_replayed')is False,
   'actual_source_fixture_matches_retained':report.get('retained_Source_origin_fixture',{}).get('fixture_sha256')is not None,
   'actual_Main_ordinary_exit0':report.get('actual_Main_normal_stop',{}).get('passed')is True,
   'actual_current_source_bound':report.get('current_candidate_bundle')==marker['candidate_bundle'],
   'actual_data_only_mount':report.get('actual_native_data_only_mount',{}).get('destination')=='/data',
   'all_seven_selected_wire_cases_match':report.get('wire_comparison',{}).get('captured_wire_matches')is True and len(report.get('candidate_wire',{}).get('events',[]))==7,
   'actual_native_A_V_decoder':any(row.get('backend')=='candidate'and row.get('strict_decoder_passed')is True and row.get('audio_and_video_mapped')is True for row in report.get('decoders',[])),
   'controller_cleanup_passed':report.get('cleanup_passed')is True and report.get('independent_owned_cleanup',{}).get('passed')is True,
  }
  result['requirements']=requirements;result['passed']=cleanup and all(requirements.values())
 return result
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--docker',default='docker');parser.add_argument('--cleanup-only',action='store_true');args=parser.parse_args();result=verify(args.output,args.docker,args.cleanup_only);(args.output/('independent-cleanup.json'if args.cleanup_only else'independent-qualification.json')).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':result['passed'],'full_M4_passed':False}));raise SystemExit(0 if result['passed']else 1)
