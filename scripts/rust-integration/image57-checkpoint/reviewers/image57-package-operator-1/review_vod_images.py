"""Root actual wire and fresh-reference review for the fixed provider-action suite."""
import base64,hashlib,json,subprocess
from pathlib import Path
from collect_cleanup import collect
R=Path.cwd();M=R/'artifacts/rust/image57-packaged-matrix-1';S=R/'artifacts/rust/image57-matrix-scheduler-1';name='packaged-vod-images';O=M/name/'http'
load=lambda n:json.loads((O/n).read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();canon=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False)
res=M/('controller-logs/'+name+'.result.json');r=json.loads(res.read_text());a=load('source-run.json');plan=json.loads((S/'inputs/plan.json').read_text())
assert r['returncode']==0 and r['process_settled']and a['passed']is True and a['resources_retained']is False and a['provider_trap_attempts']==0 and a['provider_refresh_rows_unchanged']is True
assert a['build_manifest_sha256']==plan['candidate']['manifest_sha256']and a['candidate_binding']['image_id']==plan['candidate']['image_id']
o=load('packaged-http.json');s=load('source-corpus.json');assert o['passed']and o['compared']==len(o['records'])==393 and o['differences']==[]and s['capture_complete']
for v in o['records']:
 e=s['cases'][v['index']];assert all(v[k]==e[k]for k in ['path','method','role']);assert canon(v['actual'])==canon({k:e['expected'][k]for k in v['actual']})
p=load('packaged-vod-images.json');assert p['passed']and len(p['records'])==240 and p['owned_metadata_restored']is True and p['owned_local_file_removed']is True and p['provider_refresh_requests']==0 and canon(p['full18_exact_before'])==canon(p['full18_exact_after'])
for row in p['records']:
 assert row['differences']==[]and canon(row['expected'])==canon(row['actual_native'])
 for side in ['expected','actual_native']:
  wire=row[side];body=base64.b64decode(wire['body_base64'],validate=True);assert len(body)==wire['body_bytes']and hashlib.sha256(body).hexdigest()==wire['body_sha256']
assert len(p['FIFO_cancellation_audit'])==2
for v in p['FIFO_cancellation_audit']:assert all(v[k]is True for k in ['passed','HTTP_caller_aborted_before_writer','physical_opener_survived_until_owned_writer','reader_closed_after_writer'])
m=p['mixed'];assert m['passed']and m['full18_unchanged']
for key in ['Source_unstaggered10','native_unstaggered10']:
 assert len(m[key])==10 and all(v['matched']is True for v in m[key])
cpath=M/('reviews/'+name+'-cleanup.json');c=collect([a['project']],cpath);assert c['passed']
v={'name':name,'controller_result_sha256':sha(res),'actual_process_exit':0,'actual_process_settled':True,'passed':True,'semantic_review_complete':True,'actual_reports':[{'path':str(O/n),'sha256':sha(O/n)}for n in ['source-run.json','packaged-http.json','packaged-vod-images.json']],'observed_default_audits':[],'audit_scope':'Actual ordinary393 and image240 wire comparisons, two FIFO cancellation observations and two10-reader bursts, full18 restoration and owned cleanup only; no complete default database, fullVOD or performance acceptance.','observed_counts_and_candidate_Source_scope':{'ordinary':393,'images':240,'FIFO_observations':2,'burst_readers_each_backend':10,'performance_verified':False},'cleanup':{'scope':'independent-owned-empty','verified':True,'project_ids':c['projects'],'inventories':c['inventories'],'query_receipt':{'path':str(cpath),'sha256':sha(cpath)}}}
p=M/('reviews/'+name+'-review.json');p.write_text(json.dumps(v,indent=2)+'\n');subprocess.run(['python3',str(S/'scheduler.py'),'review','--review',str(p),'--review-sha256',sha(p)],check=True);print('images240 +ordinary393 passed; independent cleanup empty')
