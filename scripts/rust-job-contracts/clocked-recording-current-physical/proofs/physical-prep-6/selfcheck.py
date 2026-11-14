"""Read-only exact executable closure, no Docker/process/socket/workload."""
import ast,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def verify(root=ROOT):
 root=Path(root);tools=root/'tools';required=next(ast.literal_eval(n.value)for n in ast.parse((tools/'run_source.py').read_text()).body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='REQUIRED'for t in n.targets));assert len(required)==24
 for path in tools.glob('*.py'):ast.parse(path.read_text(),filename=str(path))
 once=json.loads((root/'once-plan.json').read_text());assert len(once['commands'])==2;outputs=[]
 for row in once['commands']:
  inputs=json.loads(Path(row['proposed_inputs']).read_text());assert inputs['authorized']is False and row['mode']==inputs['mode']and sha(row['proposed_inputs'])==row['proposed_inputs_sha256'];assert inputs['case_plan_sha256']==sha(root/'case-plan.json')and inputs['pinned_source_proof_sha256']==sha(root/'pinned-source-proof.json');assert set(inputs['tools_sha256'])==required and all(sha(tools/n)==h for n,h in inputs['tools_sha256'].items());outputs.append(inputs['output']);assert not Path(inputs['output']).exists()
  for suffix in ['-host-preflight.json','-host-signal.json']:assert not Path(inputs['output']+suffix).exists()
  assert row['argv'][-1]==str((root/('Root-execution-inputs-'+str(row['case_index'])+'.json')).resolve());assert row['argv'][row['argv'].index('--mode')+1]==row['mode'];assert row['argv'][row['argv'].index('--output')+1]==inputs['output']
 assert len(set(outputs))==2
 native=inputs['native'];mapping=json.loads(Path(native['map_path']).read_text());assert len(mapping)==878 and sha(native['map_path'])==native['map_sha256'] and all(sha(Path(native['source_root'])/n)==h for n,h in mapping.items());assert sha(native['elf_path'])==native['elf_sha256'];build=json.loads(Path(native['build_receipts']).read_text());assert len(build['commands'])==5 and all(r['actual_wait_exit']==0 for r in build['commands'])and build['source_map_sha256']==native['map_sha256']and sha(native['build_receipts'])==native['build_receipts_sha256']
 source=json.loads((root/'pinned-source-proof.json').read_text());assert len(source['modules'])==14 and all(sha(root/'original'/n)==r['sha256']for n,r in source['modules'].items());full=json.loads((root/'support-full-inputs.json').read_text());assert len(full['files'])==6148 and sha(root/'support-full-inputs.json')==inputs['support_full_manifest_sha256'];assert all(sha(Path(inputs['support_root'])/n)==h for n,h in full['files'].items())
 for name in ['Original_image_inspect','tooling_environment_script','compose_executable']:assert sha(inputs[name]['path'])==inputs[name]['sha256']
 for filename in ['pg-policy.json','redis-policy.json']:
  p=json.loads((tools/filename).read_text());assert p['schema15_source_map_sha256']==sha(ROOT.parent/'frozen-schema-qa-15/inputs.json')
  rows=[p]if'source_path'in p else[p[k]for k in ['cache_read','result_write','channels_publish']]
  for row in rows:assert sha(Path(native['source_root'])/row['source_path'])==row['source_sha256']
 policy=json.loads((tools/'redis-policy.json').read_text());assert policy['actual_SETEX_seconds']==3600 and "redis.call('SETEX',KEYS[1],3600,ARGV[1])"in policy['result_write']['script']
 # Exact current root composition, not a false whole-current==historical map.
 proof=json.loads((root/'current-union-authority/QA2-outside-proof.json').read_text());assert proof['map_sha256']==native['map_sha256']and proof['ELF_sha256']==native['elf_sha256']and proof['production_unchanged_from_full804_744']is True
 old=json.loads((root/'current-union-authority/preflight-source-inputs.json').read_text());assert set(mapping)==set(old)and {n for n in mapping if mapping[n]!=old[n]}=={'rust/crates/job-transport/src/recording_dvr_physical_peer.rs'}
 qa16=json.loads((ROOT.parent/'frozen-physical-qa-16/inputs.json').read_text());assert mapping['rust/crates/job-transport/src/recording_dvr_physical_peer.rs']==qa16['rust/crates/job-transport/src/recording_dvr_physical_peer.rs']
 return {'passed':True,'tools':24,'Native_full_inputs':878,'Original_modules':14,'support_files':6148,'fixed_remaining_modes':2,'actual_compile_waits0':5,'Root_current_production_preflight_bound':True,'full_defaults_QA2_repeated':False,'new_physical_TTL':3600,'app_dispatched':False}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
