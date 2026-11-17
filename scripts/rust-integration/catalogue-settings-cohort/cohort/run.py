#!/usr/bin/env python3
"""One Root-dispatched normal DVR cohort; no local pool or implicit Source replay."""
import argparse,hashlib,importlib.util,json,re,sys,time
from budget import LeaseBudget
from pathlib import Path
from workflow import Workflow,save

def load(path):return json.loads(Path(path).read_text())
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def paired_exceptional_ranges(output,binding):
 """Compare actual fresh Source transport, never invent HTTP416 or retry."""
 observed={}
 for backend in ('Original','Native'):
  path=Path(binding['retained_Source_playback'])if backend=='Original'else output/backend/'overlap-completed-playback.json'
  if backend=='Native'and not path.exists():
   retained=binding.get('retained_native_contexts',{}).get('overlap',{})
   if retained.get('paired_playback_reference'):path=Path(retained['paired_playback_reference'])
  if not path.exists():return {'status':'blocked','reason':'both actual completed playback captures required'}
  cells=load(path)['actual_cells'];items={}
  for cell in cells:
   if cell['cell']not in ('range-reversed-transport','range-outside-transport'):continue
   text=cell.get('actual_header_bytes','');parts=text.split('\r\n');status=parts[0].split()if parts else[]
   headers={k.lower():v.strip()for line in parts[1:]if ':'in line for k,v in [line.split(':',1)]}
   match=re.fullmatch(r'bytes ([+-]?\d+)-([+-]?\d+)/(\d+)',headers.get('content-range',''))
   ranged=None
   if match:
    start,end,size=map(int,match.groups());ranged=[start,end]if cell['cell']=='range-reversed-transport'else[start-size,end-size]
   items[cell['cell']]={'status_code':status[1]if len(status)>1 else None,'content_length':headers.get('content-length'),'content_range_offsets':ranged,'transfer_encoding':headers.get('transfer-encoding'),'accept_ranges':headers.get('accept-ranges'),'content_type':headers.get('content-type'),'peer_EOF':cell.get('actual_peer_EOF')is True,'transport_failure':cell.get('transport_failure'),'body_bytes':cell['actual_body_bytes'],'wire_bytes_if_no_HTTP':cell.get('actual_wire_bytes')if len(status)<2 else None}
   # Wire hash/bytes are retained in the backend's raw packet; do not compare
   # Date/server identifiers or distinct recording file lengths as parity.
  observed[backend]=items
 matched=bool(observed['Original'])and len(observed['Original'])==2 and observed['Original']==observed['Native']and all(v['peer_EOF']and not v['transport_failure']and v['status_code']is not None for v in observed['Original'].values())
 source_closed_without_status=any(v['peer_EOF']and not v['transport_failure']and v['status_code']is None for v in observed['Original'].values())
 return {'status':'passed'if matched else'observed_unqualified'if source_closed_without_status else'failed','Source_reference_shape':'settled peer EOF without HTTP status observed; no automatic pass'if source_closed_without_status else'HTTP status observed','actual':observed,'scope':'only this paired actual exceptional-range transport; no broader HTTP/error-domain acceptance'}

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--adapter',type=Path,required=True);p.add_argument('--binding',type=Path,required=True);p.add_argument('--global-context',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 binding=load(a.binding);context=load(a.global_context);budget=LeaseBudget(context['deadline_monotonic']);budget.remaining(1)
 if budget.deadline-time.monotonic()>3600:raise ValueError('MaximumWholeCohort3600s')
 if a.output.exists():raise ValueError('FreshOutputRequired')
 if not(context['heavy_exclusive']is True and context['active_functional_fixtures']==1 and context['attempt']==1 and context['claim_persisted']is True):raise ValueError('ActualGlobalHeavyExclusiveLeaseRequired')
 if binding.get('performance')is not False:raise ValueError('NoBenchmarkAuthority')
 if binding['adapter_sha256']!=sha(a.adapter):raise ValueError('ExactRealFixtureAdapterRequired')
 for ref in binding['inputs']:
  budget.remaining(1)
  if sha(ref['path'])!=ref['sha256']:raise ValueError('ExactInput:'+ref['path'])
 spec=importlib.util.spec_from_file_location('owned_normal_dvr_fixture',a.adapter);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 a.output.mkdir(parents=True,exist_ok=False)
 backends=binding.get('backend_order',['Original','Native'])
 if not backends or len(backends)!=len(set(backends)) or set(backends)-{'Original','Native'}:raise ValueError('ExplicitUniqueOwnedBackendSelectionRequired')
 results=[];cleanup=[]
 try:
  # Serial backends within the SAME one global heavy lease. Each factory owns
  # one Compose namespace and exact teardown, using existing bootstrap helpers.
  for backend in backends:
   if budget.expired():break
   with module.open_fixture(binding,backend,context)as fixture:
    runner=Workflow(fixture,a.output/backend);results.append(runner.run())
   proof=fixture.cleanup_proof();cleanup.append(proof)
   if results[-1].get('infrastructure_failure'):break
   if proof.get('exact_owned_absence')is not True or proof.get('all_original_process_generations_absent')is not True:break
 finally:
  a.output.mkdir(exist_ok=True,parents=True)
  exceptional=paired_exceptional_ranges(a.output,binding);save(a.output/'paired-exceptional-ranges.json',exceptional)
  report={'scope':binding.get('execution_scope','Source-first affected cohort; see frozen binding for actual selected contexts'),'historical_native20_contexts':binding.get('historical_native20_contexts',{}),'backends':results,'expected_backend_order':backends,'expected_cleanup_count':len(backends),'cleanup':cleanup,'selected_application_cells_passed':len(results)==len(backends) and all(r.get('executed_slice_passed')is True for r in results),'exact_cleanup_passed':len(cleanup)==len(backends) and all(r.get('exact_owned_absence')and r.get('all_original_process_generations_absent')and r.get('host_CLI_groups_settled')and not r.get('errors')for r in cleanup),'full_milestone_passed':False,'performance_verified':False,'paired_exceptional_ranges':exceptional,'retained_known_required_gaps':['Source exceptional-range transport','Source historical warm/crash periodic previousrow/delete/orphan transition','unqualified enabled-plugin/eager edges, custom guides and held-search Main cancellation','remote-owner wider profile/fault domains']};report['combined_selected_workflow_passed']=report['selected_application_cells_passed']and report['exact_cleanup_passed']
  save(a.output/'report.json',report)
 return 0 if report['combined_selected_workflow_passed']else 1
if __name__=='__main__':raise SystemExit(main())
