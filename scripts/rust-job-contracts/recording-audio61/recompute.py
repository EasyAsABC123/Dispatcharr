"""Offline strict retained-data comparisons only; no application or network."""
import argparse,hashlib,importlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def compare(evidence,mode):
 names={'source':'clocked-recordings-original-audio61-1','normal':'clocked-recordings-native-audio61-1-3','refusals':'clocked-recordings-native-audio61-3-2','held':'clocked-recordings-native-audio61-2-4'}
 run=evidence/'runs'/names[mode];sys.path.insert(0,str(run/'job-tools'))
 host=json.loads((run/'integration.json').read_text())
 if host['actual_host_exit']!=0 or host['actual_controller_exit']!=0 or host['passed']is not True:raise ValueError('ActualHOSTAndGuest0')
 if (run/'default-before-private.json').read_bytes()!=(run/'default-after-private.json').read_bytes():raise ValueError('DefaultBytes')
 if host['default_audit_scope']!={'tables':62,'sequences':59,'columns':501,'constraints':198,'indexes':255}:raise ValueError('ExactDefaultSchemaScope')
 if mode=='source':
  from contract import validate_source_capture
  r=json.loads((run/'source-clocked-recording-private.json').read_text());return validate_source_capture(host['actual_controller_exit'],r)
 from audio_contract import validate_capture
 r=json.loads((run/'native-clocked-recording-private.json').read_text())
 if mode=='held':
  from stderr_sidecars import load_report
  from physical_contract import exact
  encoded,expanded,proof=load_report(run/'native-dvr-physical-private.json',run)
  c=r['physical']
  if not exact(encoded,c['actual_native_encoded'])or not exact(expanded,c['actual_native'])or not exact(expanded['commands'],[s['reply']for s in c['samples']]):raise ValueError('WholeLosslessReportAndAllRawIPC')
 return validate_capture(host['actual_controller_exit'],r,{'normal':'normal61','held':'result61','refusals':'refusals61'}[mode])

def negatives(evidence):
 names=['1-1','2-1','3-1','1-2','2-2','2-3'];rows=[]
 for suffix in names:
  r=evidence/'runs'/('clocked-recordings-native-audio61-'+suffix);h=json.loads((r/'integration.json').read_text())
  if type(h['actual_host_exit'])is not int or h['actual_host_exit']!=1 or h['actual_controller_exit']!=1:raise ValueError('PreservedNegativeActualExit')
  if (r/'default-before-private.json').read_bytes()!=(r/'default-after-private.json').read_bytes():raise ValueError('NegativeDefaultBytes')
  rows.append({'suffix':suffix,'actual_host_exit':h['actual_host_exit'],'actual_controller_exit':h['actual_controller_exit'],'not_relabelled':True})
 return rows
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',required=True,type=Path);p.add_argument('--mode',choices=['source','normal','refusals','held','negatives']);a=p.parse_args()
 if a.mode:print(json.dumps(negatives(a.evidence)if a.mode=='negatives'else compare(a.evidence,a.mode)))
 else:
  import subprocess
  for mode in ['source','normal','refusals','held','negatives']:
   r=subprocess.run([sys.executable,str(Path(__file__).resolve()),'--evidence',str(a.evidence),'--mode',mode],check=True,capture_output=True,text=True,timeout=60);print(r.stdout.strip())
