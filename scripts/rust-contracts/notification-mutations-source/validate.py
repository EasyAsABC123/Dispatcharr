#!/usr/bin/env python3
"""Portable read-only Source48 projection, exact closure and negative-evidence checks."""
import argparse,gzip,hashlib,importlib,json,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
MANIFEST='e716cd1aff1805f11f185eefc789d4f51fb4aea2a859a9ee30e70ca563278d96'
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def require(ok,label):
    if not ok:raise ValueError(label)
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text())
def validate_manifest(root=HERE):
    require(digest(root/'raw-manifest.json')==MANIFEST,'ExactManifest')
    rows=read(root/'raw-manifest.json')['files'];seen=set()
    for row in rows:
        p=Path(row['path']);require(not p.is_absolute()and'..'not in p.parts and row['path']not in seen,'UniqueRelativeEvidencePath');seen.add(row['path'])
        require((root/p).stat().st_size==row['bytes']and digest(root/p)==row['sha256'],'ExactEvidence:'+row['path'])
    return rows

def load_capture(root=HERE):
    row=read(root/'closures.json')['gzip_raw'][0]
    with gzip.open(root/row['path'],'rb')as f:data=f.read(134217729);require(not f.read(1),'GzipEOF')
    require(len(data)==row['uncompressed_bytes']<=134217728 and hashlib.sha256(data).hexdigest()==row['uncompressed_sha256'],'ExactUncompressedSourceCapture')
    return json.loads(data)

def effects(root,actual,plan):
    names=['fixture_guards','seed_binding','effects'];saved={n:sys.modules.pop(n,None)for n in names};sys.path.insert(0,str(root/'executed/executor3/controller'))
    try:return importlib.import_module('effects').compare(actual,plan)
    finally:
        sys.path.pop(0)
        for n in names:
            sys.modules.pop(n,None)
            if saved[n]is not None:sys.modules[n]=saved[n]

def closures(root=HERE):
    meta=read(root/'closures.json');require(meta['Original_revision']==REV and meta['Source_replays']==meta['operation_registrations']==0,'PinnedUnchangedScope')
    for version,row in meta['executors'].items():
        base=root/row['path'];require(digest(base/'seal.json')==row['seal_sha256'],'ExecutedSeal:'+version);seal=read(base/'seal.json');require(len(seal['files'])==row['input_count'],'ExactClosureCount:'+version)
        for item in seal['files']:
            path=Path(item['path']);require(not path.is_absolute()and'..'not in path.parts,'RelativeInput')
            if item['path']=='inputs/original-source.tar':require(item['bytes']==meta['archive']['bytes']and item['sha256']==meta['archive']['sha256'],'ExactArchiveRecipe');continue
            require((base/path).stat().st_size==item['bytes']and digest(base/path)==item['sha256'],'ExactExecutedInput:'+version+':'+item['path'])
    require(digest(root/'definition/seal.json')==meta['definition_seal_sha256'],'OriginalDefinitionSeal')
    for row in read(root/'definition/seal.json')['inputs']:require(digest(root/'definition'/row['path'])==row['sha256'],'ExactDefinitionInput')
    return meta

def derive(root,actual):
    plan=read(root/'executed/executor3/inputs/plan.json');result=effects(root,actual,plan);require(result['passed']is True and result['checks']==1449 and result['failures']==[],'Recomputed1449SourceChecks')
    retained=read(root/'raw/Source2/effect-validation.json');require(all(retained[k]==v for k,v in result.items()),'ExactSavedEffectProjection')
    a=read(root/'raw/Source2/execution.json');negative=read(root/'raw/Source1/execution.json')
    require(a['passed']is True and a['controller_exit']==a['Source_process_result']['exit']==0 and a['actual_request_count']==48,'ActualSource2Exit0')
    require(negative['passed']is False and negative['controller_exit']==negative['Source_process_result']['exit']==1,'Source1NegativeExitPreserved')
    require('TypeError: Object of type ReferenceType is not JSON serializable'in(root/'raw/Source1/command-011.stderr').read_text(),'LiteralSource1PreflightFailure')
    statuses={}
    for row in actual['requests']:key=str(row['actual_status']);statuses[key]=statuses.get(key,0)+1
    require(statuses=={'401':3,'403':6,'404':9,'200':12,'400':15,'204':2,'500':1},'ActualStatusCounts')
    for version in [1,2]:
        raw=root/('raw/Source'+str(version));before=(raw/'default-before-private.json').read_bytes();after=(raw/'default-after-private.json').read_bytes();require(before==after,'BothDefault11Exact:'+str(version))
        cleanup=read(raw/'independent-cleanup-query-receipt.json');require(all(cleanup[k]is True for k in ['all_project_lists_empty','exact_CID_absent','all_known_host_PIDs_absent','default_before_after_exact']),'IndependentAllResourcesCIDPIDCleanup:'+str(version))
        require(cleanup['queries'][0]['argv'][2:4]==['ls','--all'],'StoppedContainersIncluded')
        require(read(raw/'cleanup.json')=={'container':[],'network':[],'volume':[]},'OriginalProjectCleanup:'+str(version))
        receipt=read(raw/'settlement/Source-once-settlement.json');require(receipt['actual_exit']==(1 if version==1 else 0)and receipt['wait_completed']is True and receipt['process_poll']==receipt['actual_exit'],'HostWaitSettlement:'+str(version))
    final=actual['requests'][-1];require(final['transport_kind']=='http_response'and final['actual_status']==500 and final['framework_exception']['complete']is True,'LiteralFinalFrameworkHTTPException')
    return {'passed':True,'Source2_requests':48,'recomputed_effect_checks':1449,'Source1_prefailure_exit_preserved':1,'Source2_shell_and_controller_exit':0,'status_counts':statuses,'both_runs_default11_and_owned_cleanup_verified':True,'raw_Source2_capture_sha256':read(root/'closures.json')['gzip_raw'][0]['uncompressed_sha256'],'framework_forced_active_actor_scope_only':True,'Source_reruns':0,'Native_runs':0,'operation_registrations':0,'whole_endpoint_TCP_or_credential_or_performance_acceptance':False,'Original_credit':'Original Dispatcharr contributors retain full application and algorithm credit.'}

def validate(root=HERE):
    validate_manifest(root);closures(root);projection=derive(root,load_capture(root))
    require(read(root/'projection.json')==projection,'ExactPortableProjection')
    return projection
if __name__=='__main__':print(json.dumps(validate(),indent=2))
