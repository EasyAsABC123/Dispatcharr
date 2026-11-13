#!/usr/bin/env python3
"""Portable offline actual-data comparison. No fixture execution or private input.
Historical failed attempts stay failures; only normal2/known9/remaining7 are accepted.
"""
import argparse,base64,copy,hashlib,importlib.util,json,sys
from materialize_evidence import validate_projected_tree
from pathlib import Path
HERE=Path(__file__).resolve().parent

def load_module(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def sql_comparison_view(case):
 # The unchanged historical comparator decodes ALL frontend payloads before
 # filtering Parse/Query. Authentication bytes are intentionally unavailable.
 # Only its SQL-derivation clone omits redacted auth frames; stored projected
 # records retain every frame/order/epoch/receipt and remain hash-bound.
 if 'native_pg'not in case:return 0
 frames=case['native_pg']['frames'];kept=[];omitted=0
 for frame in frames:
  if 'authentication_payload_redacted'not in frame:kept.append(frame);continue
  receipt=frame['authentication_payload_redacted'];kind=receipt['kind']
  if frame['payload_base64']is not None or (frame['direction'],frame['kind_hex'],kind)not in [('frontend','70','frontend-PasswordMessage'),('backend','52','backend-SASLContinue'),('backend','52','backend-SASLFinal')]:raise ValueError('ExactProjectedAuthReceiptRequired')
  omitted+=1
 case['native_pg']['frames']=kept
 return omitted

def review(evidence):
 validate_projected_tree(evidence)
 results={}
 for label,fixture,indices,count in [('normal2','normal7',[1,2,3,5,7,8,9],9),('known9','known9',[1,2,3,5,6,7,8,9,10],11),('union-known9','known9',[1,2,3,5,6,7,8,9,10],11)]:
  folder=evidence/label;report=json.loads((folder/'integration.json').read_text());raw=json.loads((folder/'source-configured-recordings-private.json').read_text());reference=json.loads((HERE/'fixtures'/fixture/'tools/reference.json').read_text())
  if report.get('passed')is not True or report.get('Native_verified')is not True or raw.get('passed')is not True or raw.get('Native_verified')is not True or report['actual_host_exit']!=0 or report['actual_controller_exit']!=0 or report['actual_controller_settled']is not True or len(raw['cases'])!=len(indices)or len(raw['checks'])!=count or not all(v['passed']is True for v in raw['checks']):raise ValueError('ActualSuccessfulScopeRequired:'+label)
  if(folder/'default-before-private.json').read_bytes()!=(folder/'default-after-private.json').read_bytes():raise ValueError('ActualManifestChanged')
  tools=HERE/'fixtures'/fixture/'tools';sys.path.insert(0,str(tools));module=load_module(tools/'normal_contract.py','configured_public_'+label);context=copy.deepcopy(raw)
  for key in ('row_id_bindings','execution_clock_bindings','temporal_observations'):context.pop(key,None)
  omitted_auth_frames=0
  for index,case in zip(indices,context['cases']):
   metadata=json.loads(base64.b64decode(case['result']['raw_base64'],validate=True))
   if metadata!=case['result']['metadata']or metadata['result']!=case['actual_get_value']:raise ValueError('RawBackendOriginalGetMismatch')
   omitted_auth_frames+=sql_comparison_view(case)
   module.validate_case(reference['cases'][index],case,context)
  if sorted(json.dumps(v,sort_keys=True)for v in raw['temporal_observations'])!=sorted(json.dumps(v,sort_keys=True)for v in context['temporal_observations']):raise ValueError('TemporalFieldOrWindowChanged')
  results[label]={'actual_host_exit':0,'actual_guest_exit':0,'cases':len(indices),'checks':count,'complete_temporal_records':len(raw['temporal_observations']),'all_application_fields_and_original_get_compared':True,'projected_auth_frames_omitted_only_from_legacy_SQL_derivation_clone':omitted_auth_frames,'authentication_replay_qualified':False}
 folder=evidence/'physical-channels7';report=json.loads((folder/'integration.json').read_text());raw=json.loads((folder/'source-configured-recordings-private.json').read_text());module=load_module(HERE/'fixtures/physical-channels7/tools/physical_contract.py','configured_public_physical');results['physical-channels7']=module.validate_report(report['actual_controller_exit'],raw,'physical-channels-onward')
 if report['actual_host_exit']!=0:raise ValueError('ActualPhysicalHostExit0')
 return {'passed':True,'scope':'Offline exact public retained-data comparisons only, not new fixture/defaultgraph/Main/performance acceptance','results':results,'failed_attempts_relabelled':False,'PG_authentication_bytes_replay_qualified':False,'input_binding':'Exact projected-proof-map only; original private raw hashes are provenance, NEVER accepted alternatives'}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--evidence',type=Path,required=True);p.add_argument('--record',type=Path,required=True);a=p.parse_args();result=review(a.evidence.resolve());a.record.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
if __name__=='__main__':main()
