#!/usr/bin/env python3
"""Publish sanitized copied-data workflow evidence; Original authors retain AGPL credit."""
import argparse,hashlib,json
from pathlib import Path

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();rp=a.input/'integration.json';raw=a.input/'source-refresh-workflow-private.json';r=json.loads(rp.read_text());d=json.loads(raw.read_text());assert r['passed']and d['passed']and r['default_database_unchanged']and r['cleanup']=='project removed'and d['queue_and_results_after_cleanup']==[0,0];assert len(d['imports'])==2
 rows=[]
 for v in d['imports']:
  assert len(v['completed'])==3 and all(x['state']=='SUCCESS'for x in v['completed']);assert len(v['metadata']['children'])==2
  rows.append({'iteration':v['iteration'],'http':v['http'],'tasks':sorted(x['task']for x in v['completed']),'children':2,'current_titles':[x['title']for x in v['current']],'guide_titles':[x['title']for x in v['programmes']],'index_channel_count':len(v['index']['channels']),'update_count':len(v['events']),'file_sha256':v['file_sha256'],'file_mode':v['file_mode'],'source_status':v['source_status']})
 result={'passed':True,'scope':d['scope'],'original_revision':r['original_revision'],'checks':r['checks'],'imports':rows,'copied_nonempty_tables':r['copied_nonempty_tables'],'original_archive_sha256':r['original_archive_sha256'],'integration_sha256':digest(rp),'private_report_sha256':digest(raw),'tool_source_sha256':{k:v for k,v in r['tool_source_sha256'].items()if k.startswith('scripts/rust-job-contracts/')and any(s in k for s in ['workflow','projection'])},'default_database_unchanged':True,'cleanup':'project removed','queue_and_results_after_cleanup':[0,0],'descendants_quarantined':False,'task_functions_replaced':False,'native_application_verified':bool(r.get('paired_complete_workflow_verified',False)),'performance_verified':False,'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL licensing; framework authors retain respective licenses.'}
 if r.get('paired_complete_workflow_verified'):
  assert r['paired_projection']==r['original_projection'] and r['candidate_shutdown']=={'exit_code':0,'oom_killed':False,'running':False}
  audit=a.input/'candidate-path-audit.txt'
  if audit.exists():
   text=audit.read_text();assert text.startswith('uid=65532 ')and '/app/media -> /data'in text
   assert r['candidate_path_audit_sha256']==digest(audit)
   result['candidate_path_audit_sha256']=digest(audit);result['actual_uid']=65532
  result.update(scope='Canonical image36 native enabled refresh→Index+Series0→second import→Current/programmes and actual ChannelsRedis, paired with pinned Original',candidate_image=r['candidate_image'],candidate_manifest_sha256=r['candidate_manifest_sha256'],candidate_shutdown=r['candidate_shutdown'],reference_integration_sha256=r['reference_integration_sha256'],exact_projection_verified=True,projection_exclusions=['Generated task identifiers/date_done excluded after separate parent/result checks.','Event updated_at must parse as UTC and is replaced by explicit runtime-time placeholder.'],failed_attempts_retained=[{'paths':['artifacts/rust/m6-complete-refresh-workflow-image36-1','artifacts/rust/m6-complete-refresh-workflow-image36-2'],'cause':'Owned guide directory mkdir was masked to0700 by host umask; UID65532 unreadable. Explicit0755 dedicated guide-only directory fixed fixture; no native/deadline change.'},{'path':'artifacts/rust/m6-complete-refresh-workflow-image36-3','cause':'Actual40 workflow checks passed; post-run controller helper name shadowed Compose project string. Offline exact projection equal; alias-only fix, clean4/5 replays.'}])
 a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':True,'checks':r['checks'],'imports':2}))
if __name__=='__main__':main()
