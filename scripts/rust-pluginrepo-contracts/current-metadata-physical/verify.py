"""Read-only portable retained qualification. No Docker/ELF/Source execution."""
from pathlib import Path
import gzip,hashlib,json,sys,tempfile
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'portable'))
from public_inputs import load_source
from validate_current23 import validate as validate_current
from validate_physical import validate as validate_physical
from validate_external import qualify,validate_import

def need(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_bytes())
def load_projection(name):
 policy=read(ROOT/'projection-policy.json');need(name in policy['projections'],'ExactFiveEpochNames');item=policy['projections'][name];p=ROOT/item['path'];need(sha(p)==item['selected_compressed_sha256'],'SelectedRawCompressedSHA');raw=gzip.decompress(p.read_bytes());need(len(raw)==item['selected_JSON_bytes']and hashlib.sha256(raw).hexdigest()==item['selected_JSON_sha256'],'SelectedRawJSONSHA');return json.loads(raw)
def lifecycle(name,filename):
 item=read(ROOT/'lifecycle.json')[name][filename];p=ROOT/'lifecycle'/name/(filename+'.gz');need(sha(p)==item['selected_sha256'],'SelectedLifecycleCompressedSHA');raw=gzip.decompress(p.read_bytes());need(hashlib.sha256(raw).hexdigest()==item['selected_JSON_or_raw_sha256'],'SelectedLifecycleJSONorRawSHA');return json.loads(raw)if item['kind']=='JSON_PROJECTION'else raw

def run():
 files=read(ROOT/'packet-files.json');actual={str(p.relative_to(ROOT))for p in ROOT.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-files.json'};need(actual==set(files),'ExactPublicInventory')
 for name,h in files.items():need(sha(ROOT/name)==h,'ExactPublicFile:'+name)
 source=load_source();current=load_projection('Current23');q=validate_current(source,current);need(q==lifecycle('Current23','effect-validation.json'),'Current23ExactRecomputation');need(current['passed']is False and q['group_serialization_byte_equal_all']is False,'OriginalCaptureAndSerializationFalsePreserved')
 results={'Current23':q};life=read(ROOT/'lifecycle.json')
 for case in(1,2,3,4):
  name='Metadata'+str(case);v=load_projection(name);execution=lifecycle(name,'execution-report.json')
  with tempfile.TemporaryDirectory(prefix='pluginrepo-portable-')as temp:
   for filename in life[name]:
    if filename.endswith('.json'):
     (Path(temp)/filename).write_text(json.dumps(lifecycle(name,filename)))
   result=validate_physical(v,case,temp)
  need(v['passed']is True and execution['actual_ELF_exit']==0,'ActualRust0RawTrue')
  if case==1:
   need(execution['actual_controller_exit']==1 and execution['error']['message']=='ExactImportedSourceSeed:plugins_pluginconfig','OriginalCase1Host1Preserved');need('owned-state-after.json'not in life[name],'Case1FullOwnedAfterUnexecuted');results[name]={'offline_physical':result,'original_host_exit':1,'owned_full6259_after':'UNEXECUTED','all4Redis_after':'UNEXECUTED','limited13tables_credentials_DB1Scope':True}
  else:
   before=lifecycle(name,'imported-state-before-peer.json');after=lifecycle(name,'owned-state-after.json');base=validate_import(source,before);external=qualify(source,before,after,case,v);need(base==lifecycle(name,'base-import-validation-before-peer.json'),'BeforePeerExactSourceGuard');need(external==lifecycle(name,'independent-storage-validation.json')and result==lifecycle(name,'effect-validation.json'),'ExactPhysicalAndIndependentPostStateRecomputation');need(execution['actual_controller_exit']==0,'OriginalHost0');results[name]={'physical':result,'external':external}
  need(lifecycle(name,'default-before.json')==lifecycle(name,'default-after.json'),'ActualDefault6259Equality');need(lifecycle(name,'owned-backends-after-ELF.json')==[],'AfterELFBackendEmpty')
  rows=execution['processes'];need(all(type(r['actual_exit'])is int and r['collection_invalid']is None and r['physically_settled']is True and r['readers_settled']is True and r['writer_settled']is True for r in rows),'ActualLifecycleWaitPipes');need(execution['cleanup_within90']is True and not any(x.strip()for x in execution['remaining'].values()),'RecordedCleanup')
 for case in(2,3,4):
  name='PreELF-mode2-case'+str(case);v=lifecycle(name,'execution-report.json');need(v['actual_controller_exit']==1 and v['passed']is False and v.get('actual_ELF_exit')is None,'ThreeLiteralPreELFNegatives');need('metadata-physical-private.json'not in life[name]and'actual-ELF-process.json'not in life[name],'NoRustCaptureForPreELFNegatives');need(lifecycle(name,'default-before.json')==lifecycle(name,'default-after.json')and v['cleanup_within90']is True,'NegativeDefaultCleanup')
 for name,closure in read(ROOT/'manifest.json')['closures'].items():
  records=read(ROOT/closure['input_records']);need(len(records)==closure['original_local_count'],'CompleteExecutedInputRecords')
  for path,record in records.items():
   if record['kind']=='ELF_REBUILD_REQUIRED':continue
   blob=(ROOT/record['blob']).read_bytes()
   if record.get('blob_encoding')=='gzip':need(hashlib.sha256(blob).hexdigest()==record['selected_blob_compressed_sha256'],'ExactCompressedInputBlob');blob=gzip.decompress(blob)
   need(hashlib.sha256(blob).hexdigest()==record['selected_sha256'],'ExactClosedInputBlob')
 return {'passed':True,'Current23':23,'physical_cases2_3_4':3,'case1_original_host1_offline_only_limited':True,'preELF_negatives_preserved':3,'source_replay':False,'Main_or_fullM8':False,'qualifications':results}
if __name__=='__main__':print(json.dumps(run(),indent=2))
