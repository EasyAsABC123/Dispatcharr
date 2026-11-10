#!/usr/bin/env python3
"""Publish exact reviewed orphan integration component selection, not app parity.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
Django/django-celery-beat authors retain framework credit/licenses.
"""
import argparse,hashlib,json,re
from pathlib import Path
from publish_epg_orphan import project_case
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text())
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',required=True,type=Path);a=p.parse_args();source=a.source.resolve();i=load(source/'integration.json');raw=load(source/'source-native-epg-orphan-worker-private.json');reg=load(source/'source-native-refresh-worker-orphans-private.json')
 assert i['passed']and i['default_database_unchanged']and i['cleanup']=='project removed'
 assert raw['passed']and len(raw['checks'])==161 and len(raw['native_worker_cases'])==9
 assert reg['passed']and len(reg['checks'])==80 and all(c['passed']for c in raw['checks']+reg['checks'])
 held=[c for c in raw['native_worker_cases']if c['audit']['held_orphan_marker']]
 assert len(held)==2 and all(c['audit']['logical_waiter_aborted']and c['audit']['outcome']=='None'for c in held)
 out=ROOT/'scripts/rust-job-contracts';data=out/'source-native-epg-orphan-worker-1.json'
 dataset={'format_version':1,'scope':'Actual Original producer/native FileJob orphan helper+normal writer/Original AsyncResult; no app or default consumer',
          'checks':raw['checks'],'direct_source_cases':[project_case(c)for c in raw['cases']], 'direct_native_cases':raw['native_cases'],
          'original_worker_cases':[project_case(c,True)for c in raw['worker_cases']],'native_worker_cases':raw['native_worker_cases'],
          'helper_resources':raw['native_resources'],'targeted_regression_checks':reg['checks'],
          'targeted_regression_cases':[{k:c[k]for k in ['name','scope','source_id','audit','metadata','metadata_sha256','repeat']}for c in reg['cases']],
          'normalizations':['Identical restored owned row IDs, all ordinary row/FK values exact.',
                            'Beat marker wall time compared as changed or unchanged; direct helper uses fixed native clock.',
                            'Source/native publications have explicitly different task UUIDs; each result task_id must match its own publication. date_done is volatile.',
                            'Result TTL checked in [3590,3600], not exact wall-clock equality.'],
          'aborted_waiter_return_observed':False,'aborted_waiter_completion_evidence':'Actual bounded native result read and Original AsyncResult; audit return is None, not invented Completed.'}
 data.write_text(json.dumps(dataset,indent=2)+'\n')
 selected={}
 for relative in ['rust/crates/ingest-epg/src/orphan_cleanup.rs','rust/crates/ingest-epg/src/file_job.rs','rust/crates/job-transport/src/refresh_worker.rs','rust/crates/job-transport/src/refresh_worker_peer.rs']:
  path=source/'frozen'/relative;assert sha(path)==i['rust_source_sha256'][relative];selected[relative]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 lib=source/'frozen/rust/crates/ingest-epg/src/lib.rs';append='\n// Source orphan79/native125 helper; no application/default consumer authority.\npub mod orphan_cleanup;\n';assert lib.read_text().endswith(append)
 tools={}
 for name in ['run_native_epg_orphan_worker.py','source_native_epg_orphan_worker.py','orphan_native_compare.py','orphan_worker_compare.py','source_native_refresh_worker_orphans.py','epg_orphan_worker_observer.py']:
  rel='scripts/rust-job-contracts/'+name;path=source/'tool-source'/rel;assert sha(path)==i['tool_source_sha256'][rel];tools[rel]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 assert 'Finished `dev` profile' in (source/'clippy.log').read_text()
 assert '3 passed; 0 failed' in (source/'native-refresh-worker-physical.log').read_text()
 units={}
 for name in ['ingest-unit.log','job-unit.log']:
  match=re.search(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored', (source/name).read_text());assert match
  units[name]={'passed':int(match[1]),'external_ignored':int(match[2])}
 summary={'passed':True,'scope':dataset['scope'],'orphan_checks':161,'targeted_regression_checks':80,'actual_source_native_worker_cases':9,'held_sql_worker_cases':2,'direct_helper_resource_cases':2,'controlled_worker_redis_resource_tests':3,
          'original_revision':i['original_revision'],'frozen_default_units':units,'selected_runtime':selected,'lib_registration':{'source':str(lib.relative_to(ROOT)),'sha256':sha(lib),'append_exact':append,'copy_entire_lib':False},'cargo_graph_changes':False,
          'selected_tools':tools,'dataset':str(data.relative_to(ROOT)),'dataset_sha256':sha(data),'rust_source_archive':str((source/'rust-source.tar').relative_to(ROOT)),'rust_source_archive_sha256':i['rust_source_archive_sha256'],
          'raw_evidence_sha256':{name:sha(source/name)for name in ['integration.json','source-native-epg-orphan-worker-private.json','source-native-refresh-worker-orphans-private.json','native-orphan-worker-private.log','native-refresh-worker-physical.log','clippy.log','ingest-unit.log','job-unit.log']},
          'strict_ingest_job_all_targets_clippy':True,'default_database_unchanged':True,'cleanup':'project removed','wholeworkspace_acceptance':False,'app_default_consumer_or_202_verified':False,'refresh_eta_consumer_verified':False,'performance_verified':False,
          'prior_attempts':[{'path':'artifacts/rust/m6-source-native-epg-orphan-worker-1','aggregate_passed':False,'cause':'Report filename mismatch, after actual161+80 checks passed; preserved/cleaned.'},{'path':'artifacts/rust/m6-source-native-epg-orphan-worker-2','aggregate_passed':True,'limitation':'Held-waiter audit synthesized a Completed label despite logical waiter abort; actual result independently read. Reviewed final peer records unobserved None. Runtime unchanged.'}],
          'limitations':['Unknown SQL/transport and ambiguous outcomes remain operational; only observed settled P0001 maps helper false.','No generic collector/concurrent graph or marker writer parity.','No deliberately paused physical ROLLBACK reply.','Unclassified FileJobOutcome::Failed remains operational; no broad success promotion.','Unrelated full-source sibling inputs receive no acceptance; selected closure is narrow.'],
          'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain all framework credit/licenses.'}
 path=out/'verified-source-native-epg-orphan-worker-1.json';path.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(path),'sha256':sha(path)}))
if __name__=='__main__':main()
