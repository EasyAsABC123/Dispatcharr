"""Offline exact Source3 rows/results/clock/process/media receipts; no app/decoder."""
import argparse,hashlib,json,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE/'tools'))
from contract import validate_source_capture
RAW_SOURCE='323c396c00b88fa48459a61a63799c5f2529919554b529778aa3b7a221b0a7b8'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def need(ok,label):
 if ok is not True:raise ValueError(label)
def verify(root):
 root=Path(root);source=root/'source-clocked-recording-private.json';need(sha(source)==RAW_SOURCE,'ExactUnprojectedSource3Report');g=json.loads(source.read_text());h=json.loads((root/'integration.json').read_text());acceptance=validate_source_capture(h['actual_controller_exit'],g);need(acceptance==h['source_acceptance']and h['actual_host_exit']==0 and h['passed']is True,'LiteralSource3ActualAcceptance')
 need((root/'default-before-private.json').read_bytes()==(root/'default-after-private.json').read_bytes(),'DefaultFullBytesEqual');need(h['default_audit_scope']=={'columns':501,'constraints':198,'indexes':255,'sequences':59,'tables':62},'ActualDefault6259Schema');need(all(p['physically_settled']is True and p['pipes_settled']is True and p['collection_invalid']is None for p in h['processes']),'ActualHostWaitPipeReceipts');need(h['cleanup_budget']['within_budget']is True,'ActualCleanup90Receipt')
 for row in g['retained_media']['files']:
  path=root/Path(row['retained_copy']).relative_to('/evidence');need(path.stat().st_size==row['bytes']and sha(path)==row['sha256'],'RetainedUnprojectedMedia')
 need(sha(root/'owned-origin-input.ts')==g['origin_ready']['sha256'],'ActualOwnedTSInput')
 for name,digest in json.loads((root/'execution-inputs.json').read_text())['tools_sha256'].items():need(sha(HERE/'tools'/name)==digest,'ExactExecutedTool:'+name)
 for name,row in json.loads((HERE/'pinned-source-proof.json').read_text())['modules'].items():need(sha(HERE/'original'/name)==row['sha256'],'ExactOriginalModule:'+name)
 need(g['Native_verified']is False and g['performance_verified']is False,'FalseScopeFlagsRemainFalse');need(len(g['all_prerun'])==len(g['all_postrun'])==2,'OnlyTwoActualSourceTasks')
 return {'passed':True,'scope':'Actual Source3 normal case17/2tasks3roles, offline preserved receipts only; no Native/Main/default graph/performance/decoder rerun','actual_Source_acceptance':acceptance,'source_raw_sha256':RAW_SOURCE,'actual_DVR_Original_result':g['recording_task']['Original_AsyncResult_get'],'queued_static_siblings':len(g['unconsumed_Beat_siblings']['ids']),'siblings_executed':False,'default_tables':62,'default_sequences':59,'raw_timestamps_removed':0,'Source1_Source2_remain_failed':True,'fresh_Docker_queries_here':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--evidence-root',type=Path,required=True);a=p.parse_args();print(json.dumps(verify(a.evidence_root),indent=2))
