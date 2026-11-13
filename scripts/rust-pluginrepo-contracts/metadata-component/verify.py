"""Portable projected-reference qualification; no app/Docker/SQL execution."""
from pathlib import Path
import gzip,hashlib,importlib.util,json,sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'portable'))
import source24,source2,native24,native2

def need(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load_projection(name,path=None):
 policy=json.loads((ROOT/'projection-policy.json').read_text());row=policy['projections'][name];p=ROOT/row['selected_path']if path is None else Path(path);need(sha(p)==row['selected_compressed_sha256'],'ExactSelectedPublicProjectionOnlyNoFallback');raw=gzip.decompress(p.read_bytes());need(len(raw)==row['selected_JSON_bytes']<=134217728 and hashlib.sha256(raw).hexdigest()==row['selected_JSON_sha256'],'ExactPublicProjectionJSONBytes');return json.loads(raw)
def load_source_projection():return load_projection('Source24')
def load_lifecycle(epoch,name):
 manifest=json.loads((ROOT/'manifest.json').read_text());row=manifest['lifecycle'][epoch][name];p=ROOT/'lifecycle'/epoch/(name+'.gz');need(sha(p)==row['selected_sha256'],'ExactLifecycleProjection');raw=gzip.decompress(p.read_bytes());need(len(raw)<=16777216,'Lifecycle16MiBBound');return json.loads(raw)if name.endswith('.json')else raw

def qualify(a,b,c,d):
 schema=json.loads((ROOT/'authority/schema62-authority.json').read_text());s24=source24.validate(a,json.loads((ROOT/'executed/Source24/inputs/plan.json').read_text()),schema);n24=native24.validate(a,b);s2=source2.validate(c,json.loads((ROOT/'executed/Source2/inputs/plan.json').read_text()),schema);n2=native2.validate(c,d)
 need(a['passed']is b['passed']is c['passed']is d['passed']is False,'AllActualCaptureOnlyRecorderFalsePreserved')
 need(n24['literal_Source_framework_exception_Native500_gap']is True and n24['group_serialization_byte_equal_all']is False and n2['group_serialization_byte_equal_all']is False,'OldGapAndBothSerializationNegativesRemainLiteral')
 need(a['requests'][-1].get('status')is None and a['requests'][-1]['transport']=='framework_exception'and b['requests'][-1]['status']==500 and dict((k.lower(),v)for k,v in b['requests'][-1]['headers'])['content-type'].startswith('application/json'),'DoNotRelabelHistoricalFinalJSONGap')
 return {'Source24':s24,'Native24':n24,'Source2':s2,'Native2':n2}
def verify():
 manifest=json.loads((ROOT/'manifest.json').read_text());policy=json.loads((ROOT/'projection-policy.json').read_text());need(sha(ROOT/'projection-policy.json')==manifest['public_projection_policy_sha256'],'ExactPublicProjectionPolicy')
 refs={name:load_projection(name)for name in('Source24','Native24','Source2','Native2')};result=qualify(*refs.values())
 lifecycle=[]
 for epoch in refs:
  report=load_lifecycle(epoch,'execution-report.json');need(report['passed']is True and report['actual_controller_exit']==0,'ActualFourControllerExits0NotRecorderFlag')
  for process in report['processes']:need(process['actual_exit']==0 and process['collection_invalid']is None and process['physically_settled']is True and process['readers_settled']is True,'ActualHostWaitPipeRecords')
  if epoch.startswith('Source'):before='default-before-private.json';after='default-after-private.json';backends='owned-backends-after-Source.json'
  else:before='default-before.json';after='default-after.json';backends='owned-backends-after-ELF.json'
  need(manifest['lifecycle'][epoch][before]['original_sha256']==manifest['lifecycle'][epoch][after]['original_sha256'],'OriginalFullDefault6259LiteralByteEquality');need(load_lifecycle(epoch,before)==load_lifecycle(epoch,after),'ProjectedDefault6259StillEqual');need(load_lifecycle(epoch,backends)==[],'ActualPostProcessBackendObserverEmpty')
  lifecycle.append({'epoch':epoch,'actual_commands':len(report['processes']),'actual_controller_exit':0,'post_process_backends':[],'while_alive_backend_claim':False})
 for epoch,closure in json.loads((ROOT/'closure-materialization.json').read_text()).items():
  need(sha(ROOT/'executed'/epoch/'seal.json')==closure['executed_seal_sha256'],'ExactOriginalExecutedSeal')
  for rel,row in closure['original_files'].items():
   if'exact_public_blob'in row:need(sha(ROOT/row['exact_public_blob'])==row['original_sha256'],'EveryDeduplicatedOriginalInputBlob')
   else:need(row['omitted_kind']in('immutableOriginalGitArchive','newBuildRequiresActualELFProof','explicitProjectedReference_NEW_binding')and row['no_silent_hash_fallback']is True,'OnlyDeclaredReconstructionOmissions')
 for row in json.loads((ROOT/'authority/original-inputs.json').read_text())['files']:need(sha(ROOT/'authority/Original'/row['path'])==row['sha256'],'PinnedOriginalAuthorityFile')
 for row in json.loads((ROOT/'authority/actual-dependencies67.json').read_text()):need(sha(ROOT/'authority'/row['path'].removeprefix('inputs/'))==row['sha256'],'ActualPinned55Plus12Dependencies')
 need(sha(ROOT/'authority/source500-body.html')==manifest['body500_sha256']=='fd62a53afdd49595ccd111b6ac06466a1690e7c8da4aeb0e884b5e0f4e937624','ActualSource145ByteHTML')
 return {'passed':True,'scope':'four distinct projected captureepochs; newpublicrefbinding, no app/fullM8/Main/performance transfer','qualifications':result,'lifecycle':lifecycle,'original_APIkeys_consumed_by_original_Native_inputs':True,'public_reproduction_requires_NEW_inputseal':True}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
