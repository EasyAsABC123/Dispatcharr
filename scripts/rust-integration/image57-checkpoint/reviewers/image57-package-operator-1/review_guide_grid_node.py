"""Read-only retained guide comparison and fresh exact-project inventory review."""
import datetime
import hashlib
import json
import subprocess
from pathlib import Path
from collect_cleanup import collect
R=Path.cwd(); M=R/'artifacts/rust/image57-packaged-matrix-1'; S=R/'artifacts/rust/image57-matrix-scheduler-1'
name='guide-grid-packaged'; O=M/name
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
res=M/('controller-logs/'+name+'.result.json'); r=json.loads(res.read_text())
plan=json.loads((S/'inputs/plan.json').read_text()); node=next(n for n in plan['groups'] if n['name']==name)
a=json.loads((O/'integration.json').read_text()); b=json.loads((O/'packaged-grid.json').read_text()); source=json.loads((O/'guide-wire-source.json').read_text())
assert r['argv']==node['argv'] and r['cwd']==node['cwd'] and r['returncode']==0 and r['process_settled'] is True
assert a==json.loads((O/'result.json').read_text())
assert a['passed'] is True and a['resources_retained'] is False and a['cleanup']=='project removed'
assert a['native_verified'] is False and a['native_guide_http_verified'] is False and a['performance_verified'] is False
assert a['source_cases']==104 and a['original_guide_http_cases']==104 and a['original_grid_exploratory_negotiation_cases']==0
assert a['native_grid_packaged_image_verified'] is True and a['candidate_shutdown']=={'ExitCode':0,'OOMKilled':False}
c=plan['candidate']; assert a['candidate_provenance']=={'image_id':c['image_id'],'source_tree_sha256':c['tree_sha256'],'build_manifest_sha256':c['manifest_sha256']}
assert b['passed'] is True and b['differences']==[] and b['normalization'] is None
assert b['application_parity_verified'] is False and b['performance_verified'] is False
assert len(b['cases'])==len(source['cases'])==43
fields=['status','body_bytes','body_sha256','content_type']
anchor=datetime.datetime.fromisoformat(source['anchor']); hour=datetime.datetime.fromisoformat(source['utc_hour'])
for i,(actual,expected) in enumerate(zip(b['cases'],source['cases'])):
 assert actual['index']==i and actual['method']==expected['method'] and actual['role']==expected['role']
 assert {k:actual[k] for k in fields}=={k:expected['expected'][k] for k in fields}
 assert actual['selected_headers']=={k:expected['expected']['selected_headers'][k] for k in b['selected_header_names']}
 before=datetime.datetime.fromisoformat(actual['before']); after=datetime.datetime.fromisoformat(actual['after'])
 assert anchor<=before<=after and (after-anchor).total_seconds()<300
 assert before.replace(minute=0,second=0,microsecond=0)==hour==after.replace(minute=0,second=0,microsecond=0)
assert len(json.loads((O/'guide-source.json').read_text())['cases'])==104
assert len(json.loads((O/'source-reads.json').read_text())['cases'])==104
cp=M/('reviews/'+name+'-cleanup.json'); clean=collect([a['project']],cp); assert clean['passed']
reports=['integration.json','result.json','packaged-grid.json','guide-wire-source.json','guide-source.json','source-reads.json']
p=M/('reviews/'+name+'-review.json')
v={'name':name,'controller_result_sha256':sha(res),'actual_process_exit':0,'actual_process_settled':True,'passed':True,'semantic_review_complete':True,'actual_reports':[{'path':str(O/n),'sha256':sha(O/n)} for n in reports],'observed_default_audits':[],'audit_scope':'Owned guide fixture and exact retained HTTP comparisons; no unexposed default audit inferred.','observed_counts_and_candidate_Source_scope':{'packaged_HTTP_comparisons':43,'separate_Original_reference_cases':104,'differences':0,'retained_response_fields_and_selected_headers_recomputed':True,'clock_window_recomputed':True,'native_verified_literal':False,'native_guide_http_verified_literal':False,'application_parity_verified':False,'performance_verified':False,'candidate_provenance':a['candidate_provenance']},'cleanup':{'scope':'independent-owned-empty','verified':True,'project_ids':clean['projects'],'inventories':clean['inventories'],'query_receipt':{'path':str(cp),'sha256':sha(cp)}}}
with p.open('x') as f: f.write(json.dumps(v,indent=2)+'\n')
subprocess.run(['python3',str(S/'scheduler.py'),'review','--review',str(p),'--review-sha256',sha(p)],check=True)
print(json.dumps({'name':name,'packaged_comparisons':43,'Original_cases':104,'passed_clean':True}))
