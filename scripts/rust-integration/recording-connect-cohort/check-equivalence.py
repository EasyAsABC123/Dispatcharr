#!/usr/bin/env python3
"""Pure private-reference/public-shape controls. No payload values are printed.
Original Dispatcharr/dependency authors retain all application/model/task credit.
"""
import argparse,copy,hashlib,importlib.util,json,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
DYNAMIC={'recording_id','channel_id','stream_id','start_time','end_time','file_path','file_name','file_url','file_size','bytes_written'}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--private-reference',type=Path,required=True);p.add_argument('--output',type=Path);args=p.parse_args();sys.dont_write_bytecode=True;sys.path.insert(0,str(HERE/'cohort'))
 spec=importlib.util.spec_from_file_location('public_shape_controls',HERE/'cohort/recording_connect_cases.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 private=json.loads(args.private_reference.read_text());public=json.loads((HERE/'public-shapes.json').read_text());assert len(private['events'])==len(public['events'])==4;cases=[]
 for index,(old,shape)in enumerate(zip(private['events'],public['events'])):
  payload=old['actual_payload'];wanted={k:shape[k]for k in('keys','semantic_values','semantic_types')};assert m.public_shape_projection(payload)==wanted
  # Reconstruct the OLD full shape using locally owned values, not published
  # constants for URLs/provider/profile names. All four projections are exact.
  reconstructed={key:'<dynamic '+key+'>'if key in DYNAMIC else shape['semantic_values'][key]if key in shape['semantic_values']else payload[key]for key in shape['keys']}
  assert reconstructed==m.source_contract_shape(payload)
  broken=copy.deepcopy(payload);broken.pop(next(iter(broken)));assert m.public_shape_projection(broken)!=wanted
  for key in shape['semantic_values']:
   broken=copy.deepcopy(payload);broken[key]=not broken[key]if isinstance(broken[key],bool)else'unqualified';assert m.public_shape_projection(broken)!=wanted
  # The real Native validator still compares every normalized value against
  # fresh LOCAL Source authority. Mutating an owned invariant must fail it.
  owned=next(key for key in payload if key not in DYNAMIC and key not in shape['semantic_values'])
  broken=copy.deepcopy(payload);broken[owned]='changed-owned-invariant';assert m.source_contract_shape(broken)!=m.source_contract_shape(payload)
  cases.append({'event_index':index,'event':old['event'],'old_full_projection_reconstructed_exactly':True,'public_keyshape_and_semantics_match':True,'missing_key_rejected':True,'semantic_drift_rejected':True,'fresh_local_owned_value_drift_rejected':True})
 report={'state':'PURE_SHAPE_EQUIVALENCE_CONTROLS_PASSED','private_reference_sha256':hashlib.sha256(args.private_reference.read_bytes()).hexdigest(),'public_shapes_sha256':hashlib.sha256((HERE/'public-shapes.json').read_bytes()).hexdigest(),'cases':cases,'Source_payload_values_published':False,'actual_Source_Native_or_Docker_execution':False,'public_export_runtime_qualified':False,'scope':'Exactly four old positive projections and declared negative controls. Full Native values still use fresh locally collected Source authority; no historical payload constants shipped.'}
 raw=json.dumps(report,indent=2)+'\n'
 if args.output:args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(raw)
 else:print(raw,end='')
 return 0
if __name__=='__main__':raise SystemExit(main())
