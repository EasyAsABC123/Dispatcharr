"""Root actual wire and fresh-reference review for the fixed provider-action suite."""
import hashlib,json,subprocess
from pathlib import Path
from collect_cleanup import collect
R=Path.cwd();M=R/'artifacts/rust/image57-packaged-matrix-1';S=R/'artifacts/rust/image57-matrix-scheduler-1';name='packaged-cached-provider-info';O=M/name/'http'
load=lambda n:json.loads((O/n).read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();canon=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False)
res=M/('controller-logs/'+name+'.result.json');r=json.loads(res.read_text());a=load('source-run.json');plan=json.loads((S/'inputs/plan.json').read_text())
assert r['returncode']==0 and r['process_settled']and a['passed']is True and a['resources_retained']is False and a['provider_trap_attempts']==0 and a['provider_refresh_rows_unchanged']is True
assert a['build_manifest_sha256']==plan['candidate']['manifest_sha256']and a['candidate_binding']['image_id']==plan['candidate']['image_id']
o=load('packaged-http.json');s=load('source-corpus.json');assert o['passed']and o['compared']==len(o['records'])==393 and o['differences']==[]and s['capture_complete']
for v in o['records']:
 e=s['cases'][v['index']];assert all(v[k]==e[k]for k in ['path','method','role']);assert canon(v['actual'])==canon({k:e['expected'][k]for k in v['actual']})
p=load('packaged-cached-info.json');assert p['passed']and p['compared']==len(p['records'])==90 and p['differences']==[]and p['owned_metadata_restored']is True and canon(p['full18_exact_before'])==canon(p['full18_exact_after'])
for row in p['records']:
 assert row['differences']==[]and canon(row['expected'])==canon(row['actual'])
 for side in ['expected','actual']:
  wire=row[side];body=wire['body_text'].encode();assert hashlib.sha256(body).hexdigest()==wire['body_sha256'];assert (row['method']=='HEAD' and len(body)==0) or wire['headers'].get('Content-Length')is None or len(body)==int(wire['headers']['Content-Length'])
for key in ['Source_unstaggered10','candidate_unstaggered10']:
 assert len(p[key])==10 and all(v['matched']is True for v in p[key])
 for v in p[key]:assert hashlib.sha256(v['actual']['body_text'].encode()).hexdigest()==v['actual']['body_sha256']
cpath=M/('reviews/'+name+'-cleanup.json');c=collect([a['project']],cpath);assert c['passed']
v={'name':name,'controller_result_sha256':sha(res),'actual_process_exit':0,'actual_process_settled':True,'passed':True,'semantic_review_complete':True,'actual_reports':[{'path':str(O/n),'sha256':sha(O/n)}for n in ['source-run.json','packaged-http.json','packaged-cached-info.json']],'observed_default_audits':[],'audit_scope':'Actual ordinary393 and cached-info90 wire comparisons and Source10/candidate10 bursts, full18 restoration and owned cleanup only; no complete default database, fullVOD or performance acceptance.','observed_counts_and_candidate_Source_scope':{'ordinary':393,'cached_info':90,'burst_readers_each_backend':10,'performance_verified':False},'cleanup':{'scope':'independent-owned-empty','verified':True,'project_ids':c['projects'],'inventories':c['inventories'],'query_receipt':{'path':str(cpath),'sha256':sha(cpath)}}}
p=M/('reviews/'+name+'-review.json');p.write_text(json.dumps(v,indent=2)+'\n');subprocess.run(['python3',str(S/'scheduler.py'),'review','--review',str(p),'--review-sha256',sha(p)],check=True);print('cached info90 +ordinary393 passed; independent cleanup empty')
