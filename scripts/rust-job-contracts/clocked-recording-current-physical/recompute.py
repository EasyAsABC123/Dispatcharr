"""Recompute strict retained scopes; no application or workload execution."""
import argparse,importlib,importlib.util,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def module(folder,name):
 sys.path.insert(0,str(folder/'tools'));sys.path.insert(0,str(folder))
 for n in ['physical_contract','known_failure_gate','result_stage']:sys.modules.pop(n,None)
 spec=importlib.util.spec_from_file_location('isolated_'+name,folder/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);a=p.parse_args();load=lambda p:json.loads(p.read_text());rows=[]
 for suffix,prep in [('1-1','4'),('2-1','4'),('4-1','5'),('6-2','7'),('7-1','7')]:
  R=a.evidence/('clocked-recordings-native-physical-'+suffix);h=load(R/'integration.json');r=load(R/'native-clocked-recording-private.json');assert type(h['actual_host_exit'])is int and h['actual_host_exit']==0 and h['actual_controller_exit']==0 and h['actual_controller_settled']is True
  m=module(ROOT/'proofs'/('physical-prep-'+prep)/'tools','physical_contract');v=m.validate_capture(h['actual_controller_exit'],r,r['physical']['mode']);rows.append({'case':suffix,'actual_host_exit':0,'actual_guest_exit':0,'effects':v})
 for suffix,folder in [('3-1','physical-case3-offline-correction-2'),('5-1','physical-case5-offline-correction-1')]:
  R=a.evidence/('clocked-recordings-native-physical-'+suffix);m=module(ROOT/'proofs'/folder,'known_failure_gate');v=m.review(load(R/'integration.json'),load(R/'native-clocked-recording-private.json'));rows.append({'case':suffix,'actual_host_exit_preserved':1,'actual_guest_exit_preserved':1,'pure_effects_only':v})
 R=a.evidence/'clocked-recordings-native-physical-6-1';h=load(R/'integration.json');r=load(R/'native-clocked-recording-private.json');assert h['actual_host_exit']==h['actual_controller_exit']==1 and r['error']=='ActualPhysicalTarget15s'and r['physical']['Redis_close']['target_replies']==0 and not r['physical'].get('capture_complete');rows.append({'case':'6-1','target_qualified':False,'actual_host_exit_preserved':1,'actual_guest_exit_preserved':1})
 print(json.dumps({'passed':True,'five_actual_host0':5,'known_error_body_only_host1':2,'missing_target_unqualified_host1':1,'retained_rows':rows}))
