"""Replay captured selected-M6 predicates in a temporary copy; no app or Docker.
All Original Dispatcharr application/algorithm credit remains with its authors.
"""
import argparse,hashlib,json,shutil,subprocess,sys,tempfile
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('directory',type=Path);a=p.parse_args();root=a.directory.resolve();data=root/'capture';report=json.loads((data/'execution-private.json').read_bytes());dispatch=json.loads((root/'dispatch/report.json').read_bytes());before={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()for p in root.rglob('*')if p.is_file()}
 with tempfile.TemporaryDirectory(prefix='dispatcharr-m6-readonly-')as temporary:
  copied=Path(temporary)/'capture';shutil.copytree(data,copied)
  run=subprocess.run([sys.executable,'-B',str(root/'executed-tools/qualify.py'),str(copied)],capture_output=True,text=True,timeout=15)
  if run.returncode:print(run.stdout);print(run.stderr,file=sys.stderr);return run.returncode
  q=json.loads((copied/'qualification.json').read_bytes());bound=json.loads((copied/'selected-ETA-read-bound.json').read_bytes());print(run.stdout.strip())
 checks={'actual_execution_evaluation_cleanup0':all(v['actual_exit']==0 and v['process_group_settled']and v['stdout_EOF']and v['stderr_EOF']for v in dispatch['phases'].values()),'actual222_predicates':q['qualified']and len(q['checks'])==222 and all(v['passed']for v in q['checks']),'startup2_observed':q['qualified_startup_roots']==2,'futureETA20reads_bound47':len(bound['pending_ETA_head_intervals'])==1 and bound['pending_ETA_head_intervals'][0]['actual_peeks']==20 and bound['pending_ETA_head_intervals'][0]['maximum_peeks']==47 and bound['qualified'],'cleanup10_default_unchanged':len(report['cleanup_steps'])==10 and all(v['passed']for v in report['cleanup_steps'])and all(report['default_SQL_and_cache_isolation'].values()),'Original_tree_exposure_not_hidden':any(v['Destination']=='/evidence'for v in report['native_mounts'])and not any(v['Destination']=='/app'for v in report['native_mounts']),'no_fullM6_or_perf_claim':q['fullM6']is False and q['performance_verified']is False}
 cleanup=json.loads((data/'selected-independent-cleanup.json').read_bytes());checks['actual_independent6inventories_3CIDs_57CLIs']=cleanup['passed']and cleanup['independent_inventory_checks']==6 and cleanup['actual_CIDs_absent']==3 and cleanup['known_original_host_CLIs_absent']==57
 after={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()for p in root.rglob('*')if p.is_file()};checks['publication_bytes_unchanged']=before==after
 result={'passed':all(checks.values()),'checks':checks,'apps_or_Docker_or_Source_calls':0,'full_M6':False,'Native_mount_scope':'No /app Original bind. Broad /evidence bind exposes retained Original tree, so no-source-access is NOT established.','performance_claims':False};print(json.dumps(result));return 0 if result['passed']else 1
if __name__=='__main__':raise SystemExit(main())
