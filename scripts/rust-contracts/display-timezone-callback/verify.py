"""Portable retained observation validation; no application/compiler/network."""
import argparse,copy,gzip,hashlib,importlib.util,io,json,sys,tarfile,tempfile
from pathlib import Path
import reconstruct
ROOT=Path(__file__).resolve().parent
def need(ok,label):
 if not ok:raise ValueError(label)
def evidence(raw):return {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
def restore_case0(projected):
 """Recreate only deterministic closed-fixture keys after exact digest checks.
 The published projection is a NEW reference binding; original raw byte SHA is
 retained separately. This does not claim original JSON byte reconstruction.
 """
 value=copy.deepcopy(projected);nonce=value['database'].removeprefix('rust_probe_notifforms_native_');need(len(nonce)==10 and all(c in'0123456789abcdef'for c in nonce),'OwnedFixtureNonce');keys=['native-forms-api-'+nonce+'-'+role for role in ['admin','standard','admin_denied']];known={(evidence(k.encode())['sha256'],len(k.encode())):k for k in keys};changes=0
 def recover(row):
  need(type(row)is dict and set(row)=={'sha256','bytes'}and type(row['bytes'])is int,'ExactCredentialEvidenceShape');key=(row['sha256'],row['bytes']);need(key in known,'ExactOwnFixtureCredentialEvidence');return known[key]
 def walk(node):
  nonlocal changes
  if type(node)is dict:
   for key,item in list(node.items()):
    if key=='api_key'and type(item)is dict:node[key]=recover(item);changes+=1
    elif key=='raw_headers_private':
     need(type(item)is dict and set(item)<= {'HTTP_X_API_KEY'},'OnlyDeclaredAPIKeyHeader')
     for header,row in item.items():item[header]=recover(row).encode().hex();changes+=1
    else:walk(item)
  elif type(node)is list:
   for item in node:walk(item)
 walk(value);need(changes>0,'ProjectedCredentialChangesPresent');return value

def lifecycle(case,destination):
 root=ROOT/f'actual/case{case}';inventory=json.loads((root/'lifecycle-inventory.json').read_text());expected={r['name']:r for r in inventory if r['selection']=='byte-exact-gzip'};destination.mkdir()
 with tarfile.open(fileobj=io.BytesIO(gzip.decompress((root/'lifecycle.tar.gz').read_bytes())),mode='r:')as tar:
  members=tar.getmembers();need({m.name.removesuffix('.gz')for m in members}==set(expected)and len(members)==len(expected),'CompleteLifecycleArchive')
  for m in members:
   need(m.isfile()and Path(m.name).name==m.name and m.name.endswith('.gz'),'RegularLifecycleMember');raw=gzip.decompress(tar.extractfile(m).read());row=expected[m.name[:-3]];need(len(raw)==row['bytes']and hashlib.sha256(raw).hexdigest()==row['sha256'],'ExactLifecycleRawBytes');(destination/m.name[:-3]).write_bytes(raw)
 return destination

def verify(repo):
 policy=json.loads((ROOT/'projection-policy.json').read_text());binding=policy['projections'][0];path=ROOT/'actual/case0/Native19-projection.json';need(reconstruct.sha(path)==binding['projection_sha256'],'StrictProjectedReferenceOnlyNoRawFallback');summary=json.loads((ROOT/'actual-summary.json').read_text());results=[]
 with tempfile.TemporaryDirectory(prefix='display-callback-offline-',dir=repo/'artifacts/rust')as tmp:
  tmp=Path(tmp);closure=reconstruct.inputs(tmp/'execution',repo);sys.path.insert(0,str(tmp/'execution/controller'));spec=importlib.util.spec_from_file_location('validate_native',tmp/'execution/controller/validate_native.py');native=importlib.util.module_from_spec(spec);sys.modules['validate_native']=native;spec.loader.exec_module(native)
  for case in range(7):
   actual=summary['actual'][case];need(actual['case']==case and actual['controller_exit']==0,'SevenActualControllerExits0');directory=lifecycle(case,tmp/f'case{case}');report=json.loads((directory/'execution-report.json').read_text());need(report['actual_controller_exit']==0 and report['all_command_processes_settled']is True and report['cleanup_within_declared_90_seconds']is True,'ActualSettledExecution');need((directory/'default-before.json').read_bytes()==(directory/'default-after.json').read_bytes(),'LiteralFullDefault6259Bytes')
   if case==0:value=restore_case0(json.loads(path.read_text()));result=native.validate(value);need(value['passed']is False,'CaptureOnlyRecorderFalsePreserved')
   else:
    raw=gzip.decompress((ROOT/f'actual/case{case}/callback-private.json.gz').read_bytes());need(hashlib.sha256(raw).hexdigest()==actual['raw_capture_sha256'],'ExactRawPhysicalBytes');value=json.loads(raw)
    if case==1:
     spec=importlib.util.spec_from_file_location('callback_physical',tmp/'execution/controller/validate_physical.py');physical=importlib.util.module_from_spec(spec);spec.loader.exec_module(physical)
    result=physical.validate(value,case,directory)
   results.append({'case':case,'recomputed':result,'historical845_only':True})
 return {'passed':True,'cases':results,'exact_input_closure':closure,'Source_or_Main_runs':0,'IANA_catalogue_or_serialization_byte_equivalence':False,'current842_union_physical_transfer':False,'projection_reference_not_executed_original_bytes':True}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);a=p.parse_args();print(json.dumps(verify(a.repo),indent=2))
