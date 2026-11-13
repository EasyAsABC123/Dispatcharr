"""NEW projected-only OFFLINE qualification, original host1 results stay unchanged."""
import hashlib,importlib.util,io,json,re,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PROJECTED={5:'a234de8bdac2219971cf2f4622336c261af0dfa0c5f01f77515c5e8a067b8a9a',6:'8d666459802f1445ccd9fa97cd06c8220989f6647a9bffe291d519a6d8a49489'}
ORIGINAL={5:'4d6f1080473b1be59a66f15fc077a4822a86959dff4a126ce039ad33c7a4af8d',6:'c3f40ebe1f11230e75cd5002cdbb4e4e1eebfea9b262886d7b35e18756b65add'}
def require(ok,label):
 if not ok:raise ValueError(label)
def load_module(name,file):
 spec=importlib.util.spec_from_file_location(name,file);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
CORRECTED=load_module('public_backup_physical_corrected',ROOT/'validation/corrected-validator.py')
ORACLE=load_module('public_backup_physical_executed',ROOT/'validation/executed-validator.py')

def load_projection(case):
 data=(ROOT/f'case{case}-projection.json').read_bytes();require(hashlib.sha256(data).hexdigest()==PROJECTED[case],'ExactlyOneProjectedBindingNoRawFallback')
 policy=json.loads((ROOT/'projection-policy.json').read_text());entry=next(row for row in policy['cases']if row['case']==case);require(entry['Original_raw_sha256']==ORIGINAL[case]and entry['projection_sha256']==PROJECTED[case]and entry['changed_fields']==6,'ExactlySixScopedCredentialProjections')
 return json.loads(data)

def members():
 spec=json.loads((ROOT/'lifecycle-inventory.json').read_text());raw=(ROOT/'lifecycle.tar.gz').read_bytes();require(hashlib.sha256(raw).hexdigest()==spec['archive_sha256'],'ExactLifecycleArchiveSHA');expected={row['path']:row for row in spec['members']};out={}
 with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz')as tar:
  entries=tar.getmembers();require(len(entries)==len(expected)and{m.name for m in entries}==set(expected),'All410ExactMembers')
  for entry in entries:
   require(entry.isfile(),'OnlyRegularEvidenceFiles');data=tar.extractfile(entry).read();row=expected[entry.name];require(len(data)==row['bytes']and hashlib.sha256(data).hexdigest()==row['sha256'],'LiteralLifecycleBytes');out[entry.name]=data
 return out

def source_proof():
 mapping=json.loads((ROOT/'compiled830-map.json').read_text());require(len(mapping)==830,'Historical830SourceMap')
 for name in('rust/crates/job-transport/src/backup_job_physical_tests.rs','rust/crates/job-transport/src/backup_application.rs'):
  require(hashlib.sha256((ROOT/'runtime'/name).read_bytes()).hexdigest()==mapping[name],'SameCompiledPeerSource')
 for p in(ROOT/'compiled-source').rglob('*'):
  if p.is_file():require(hashlib.sha256(p.read_bytes()).hexdigest()==mapping[str(p.relative_to(ROOT/'compiled-source'))],'SameCompiledSourceSubset')
 peer=(ROOT/'runtime/rust/crates/job-transport/src/backup_job_physical_tests.rs').read_text();require(re.findall(r'let store = Store::connect\(&url,\s*([0-9]+)\)',peer)==['4'],'ActualSameCompiledPoolMax4')
 store=(ROOT/'compiled-source/rust/crates/store/src/lib.rs').read_text();require('.max_connections(max_connections)'in store,'ActualSQLxBuilderMax')
 inner=(ROOT/'upstream/src/pool/inner.rs').read_text();close=inner[inner.index('    pub(super) fn close<'):inner.index('    pub(crate) fn close_event')];require('self.idle_conns.pop()'in close and'num_idle'not in close,'SourceCloseDoesNotDecrementIdleCounter')
 return {'sqlx_core':'0.8.6','same_compiled_pool_max':4,'preclose_idle_or_queue_length':'UNOBSERVED','while_alive_external_backend_observer':'UNOBSERVED'}

def qualify_projected(case,value,data):
 prefix=f'case{case}/attempt2/'
 # Run the byte-identical prior validator on this NEW projected reference: original idle0 still fails.
 import tempfile
 with tempfile.TemporaryDirectory(prefix='backup-offline-wire-')as d:
  output=Path(d)
  for name in('proxy-events.jsonl','proxy-process.json'):(output/name).write_bytes(data[prefix+name])
  try:ORACLE.validate(value,case,output)
  except ValueError as error:require(str(error)=='ActualSinglePoolCloseSizeIdle0','OriginalFinalOracleStillUnmet')
  else:raise ValueError('NeverRelabelOriginalControllerPass')
  result=CORRECTED.validate(value,case,output)
 require(type(value['closed_pool_idle'])is int and value['closed_pool_idle']==2,'LiteralIdle2Retained')
 execution=json.loads(data[prefix+'execution-report.json']);require(execution['actual_controller_exit']==1 and execution['passed']is False and execution['error']['message']=='ActualSinglePoolCloseSizeIdle0','OriginalHost1Preserved')
 processes=execution['processes'];elf=[row for row in processes if row['kind']==f'actual-backup-physical-case-{case}-once'];require(len(elf)==1 and all(type(row['actual_exit'])is int and row['actual_exit']==0 and row['physically_settled']is True and row['pipes_settled']is True for row in processes),'ActualELFAndAllCommands0Settled')
 require(data[prefix+'default-before.json']==data[prefix+'default-after.json']and execution['default11_full62tables59seq_exact']is True,'LiteralDefault11AndFull6259')
 observed=json.loads(data[prefix+'backend-after-ELF-exit.json']);require(observed and all(row['actual_backends']==[]for row in observed),'ActualExternalBackendAbsentAfterELFOnly')
 require(execution['all_command_processes_settled']is True and execution['cleanup_within_declared_90_seconds']is True,'ActualCleanupAndPipes')
 for attempt in(1,):
  first=json.loads(data[f'case{case}/attempt{attempt}/execution-report.json']);require(first['actual_controller_exit']==1 and not any(row.get('kind','')and row['kind'].startswith('actual-backup-physical-case-')for row in first['processes']),'EarlierMissingSchemaPreELF1Preserved')
 return {**result,'original_host_exit':1,'actual_ELF_exit':0,'literal_idle_diagnostic':2,'new_input_binding':'projected-only-not-executed-raw','after_ELF_external_backends':[],'while_alive_external_backends':'UNOBSERVED'}

def validate():
 inventory=json.loads((ROOT/'packet-inventory.json').read_text());actual={str(p.relative_to(ROOT))for p in ROOT.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-inventory.json'};require(actual==set(inventory['files']),'ExactPacketInventory')
 for name,row in inventory['files'].items():
  raw=(ROOT/name).read_bytes();require(len(raw)==row['bytes']and hashlib.sha256(raw).hexdigest()==row['sha256'],'ExactPacketFile:'+name)
 proof=source_proof();data=members();results=[qualify_projected(case,load_projection(case),data)for case in(5,6)]
 return {'state':'SEPARATE_PROJECTED_OFFLINE_ORACLE_CORRECTION','qualified_cases':results,'source_proof':proof,'original_plan1_host_exits':[1,1],'original_plan2_host_exits':[1,1],'actual_plan2_ELF_exits':[0,0],'all_original_results_and_idle2_retained':True,'new_app_or_request_runs':0,'Main_currentimage_fullM8_or_earlier_max1_failure_transfer':False}
if __name__=='__main__':print(json.dumps(validate(),sort_keys=True))
