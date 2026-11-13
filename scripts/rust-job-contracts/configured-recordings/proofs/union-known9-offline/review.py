"""Read-only retained actual Normal7 review; no app/database/network execution."""
import base64,copy,hashlib,importlib.util,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3];RUN=ROOT/'artifacts/rust/configured-recordings-native-known-insert-union-1';PREP=ROOT/'artifacts/rust/configured-recordings-known-insert-union-prep-1';OUT=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
r=json.loads((RUN/'integration.json').read_text());actual=json.loads((RUN/'source-configured-recordings-private.json').read_text());reference=json.loads((RUN/'job-tools/reference.json').read_text());checks=[]
def check(name,value):
 checks.append({'name':name,'passed':bool(value)})
 if not value:raise ValueError(name)
check('actual_host_guest0',r['actual_host_exit']==0 and r['actual_controller_exit']==0 and r['actual_controller_settled']is True)
check('actual_nine_cases_eleven_checks',actual['passed']is True and len(actual['cases'])==9 and len(actual['checks'])==11 and all(x['passed']is True for x in actual['checks']))
indices=(1,2,3,5,6,7,8,9,10);rows=[]
for index,case in zip(indices,actual['cases']):
 original=reference['cases'][index];raw=base64.b64decode(case['result']['raw_base64'],validate=True);metadata=json.loads(raw)
 check(case['name']+'-raw_backend_projection',metadata==case['result']['metadata'])
 check(case['name']+'-Source_normal_value_and_Original_get',type(metadata['result'])is type(original['result']['metadata']['result'])and metadata['result']==original['result']['metadata']['result']==case['actual_get_value'])
 check(case['name']+'-identity_success_no_traceback',metadata['status']=='SUCCESS'and metadata['task_id']==case['id']and metadata['traceback']is None)
 delta=lambda key:len(case['after'][key])-len(case['before'][key])
 rows.append({'name':case['name'],'recording_delta':delta('recordings'),'clocked_delta':delta('clocked'),'default_frame_delta':delta('frames'),'Channels_delta':delta('Channels_decoded')})
check('recording_deltas_exact',[x['recording_delta']for x in rows]==[1,0,1,0,0,1,0,0,0])
check('artwork_deltas_exact',[x['default_frame_delta']for x in rows]==[2,0,2,0,0,1,0,0,0])
check('event_deltas_exact',[x['Channels_delta']for x in rows]==[1,1,1,0,1,1,0,0,0])
last=actual['cases'][-1]['after'];frames=[json.loads(base64.b64decode(v['raw_base64'],validate=True))for v in last['frames']]
check('all_six_actual_default_frames_retained',len(frames)==6 and sum(f['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'for f in frames)==5 and sum(f['headers']['task']=='apps.epg.tasks.parse_programs_for_tvg_id'for f in frames)==1)
check('three_Recordings_two_Clocked',len(last['recordings'])==3 and len(last['clocked'])==2)
# Independently invoke the exact sealed contract again from empty alias/clock
# bindings; this is clearly declared comparator reuse, not a second execution.
sys.path.insert(0,str(RUN/'job-tools'))
path=RUN/'job-tools/normal_contract.py';spec=importlib.util.spec_from_file_location('retained_known9_contract',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
context=copy.deepcopy(actual)
for key in ('row_id_bindings','execution_clock_bindings','temporal_observations'):context.pop(key,None)
for i,case in zip(indices,context['cases']):module.validate_case(reference['cases'][i],case,context)
check('full_contract_recomputed_165_unstripped_temporal_observations',len(context['temporal_observations'])==len(actual['temporal_observations'])==165 and sorted(json.dumps(v,sort_keys=True)for v in context['temporal_observations'])==sorted(json.dumps(v,sort_keys=True)for v in actual['temporal_observations']))
check('default11_byte_equal',(RUN/'default-before-private.json').read_bytes()==(RUN/'default-after-private.json').read_bytes())
inventory=json.loads((RUN/'independent-cleanup.json').read_text());check('retained_all_project_inventories_empty',all(v['empty']is True and v['exit']==0 for v in inventory.values()))
readiness="import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"
owned=set(r['owned_containers']);nonzero=[]
for command in r['processes']:
 argv=command['argv']
 if command['exit']==0:continue
 health=command['exit']==1 and argv[-6:]==['exec','-T','baseline','/dispatcharrpy/bin/python','-c',readiness]
 absence=command['exit']==1 and len(argv)==3 and argv[1]=='inspect'and argv[2]in owned
 check('nonzero_exact_qualified_health_or_owned_absence:'+str(len(nonzero)),health or absence)
 nonzero.append({'argv':argv,'exit':command['exit'],'classification':'fixed bounded initial Source readiness probe'if health else'exact owned former-CID absence'})
check('all_actual_command_waits_and_pipes_settled',all(x['physically_settled']is True and x['pipes_settled']is True and x['collection_invalid']is None for x in r['processes']))
check('cleanup_within_original90',r['cleanup_budget']['within_budget']is True and r['cleanup_budget']['budget_seconds']==90)
record={'passed':True,'scope':'Offline retained-data review; actual source producer/native worker/Original result decoder Known9 only; reused sealed comparator separately declared, no new fixture/Main/default FIFO/child consumption/performance','checks':checks,'retained_nonzero_auxiliary_commands':nonzero,'cases':rows,'temporal_observations':165,'fixture_input_day_correspondence':actual['fixture_date_delta_days'],'full_source_fields_preserved':True,'historical_Source11_host_exit':1,'historical_SQLSTATE_raw':None,'Source_SQLSTATE_captured':False,'Normal1_failure_preserved':True,'physical_cleanup_review_scope':'Retained receipts only; Root owns independent daemon queries','bindings':{str(p.relative_to(ROOT)):sha(p)for p in [RUN/'integration.json',RUN/'source-configured-recordings-private.json',RUN/'job-tools/reference.json',path,RUN/'independent-cleanup.json',RUN/'execution-inputs.json',PREP/'seal.json']}}
(OUT/'review.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps({'passed':True,'checks':len(checks),'cases':9,'temporal_observations':165},indent=2))
