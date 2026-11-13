"""Verify copy-only bytes and separately recompute retained offline comparison."""
import argparse,hashlib,importlib.util,json
from pathlib import Path
from materialize import decode
ROOT=Path(__file__).resolve().parent;sha=lambda x:hashlib.sha256(x).hexdigest()
def verify(root=ROOT):
 root=Path(root);manifest=json.loads((root/'evidence-manifest.json').read_text());restored={name:decode((root/row['payload']).read_bytes(),row)for name,row in manifest.items()}
 def raw(folder,name):return json.loads(restored[folder+'/'+name])
 states=[]
 for folder in ['clocked-recordings-native-normal-1','clocked-recordings-native-normal-2','clocked-recordings-native-normal-3']:
  host=raw(folder,'integration.json');assert host['actual_host_exit']==1 and host['passed']is False;states.append({'folder':folder,'actual_host_exit':host['actual_host_exit'],'actual_guest_exit':host['actual_controller_exit'],'executed_error':host['error']})
 host=raw('clocked-recording-sql-schema-1','integration.json');assert host['actual_host_exit']==host['actual_controller_exit']==0 and host['schema_verified']is True
 spec=importlib.util.spec_from_file_location('portable_normal_comparator',root/'proofs/offline-correction/tools/native_contract.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 acceptance=module.validate_native_capture(0,raw('clocked-recordings-native-normal-3','native-clocked-recording-private.json'),root/'proofs/offline-correction/tools/Source3-reference.json');assert acceptance['passed']is True
 peer=raw('clocked-recordings-native-normal-3','native-dvr-private.json');assert peer['actual_run']['kind']=='Completed';assert acceptance['actual_Native_video_duration_seconds']==21.36 and acceptance['actual_Source3_video_duration_seconds']==26.48 and acceptance['Native_duration_equals_Source_qualified']is False
 return {'passed':True,'bound_evidence_media_files':len(restored),'actual_historical_host_states':states,'schema_only29':True,'separate_full_offline_comparison_passed':True,'actual_native_video_seconds':21.36,'actual_Source_video_seconds':26.48,'duration_or_byte_equality_qualified':False,'all_date_fields_retained':acceptance['all_temporal_fields_retained'],'temporal_observations':len(acceptance['temporal_inventory']),'application_build_decoder_executed':False,'historical_HOST1_not_relabelled':True}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
