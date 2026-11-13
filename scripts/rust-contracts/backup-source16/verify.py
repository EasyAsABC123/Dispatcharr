#!/usr/bin/env python3
"""Offline Source16 projected evidence validation; no builds/apps/Docker."""
import copy,hashlib,json,sys,tarfile,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from projection_effects import compare
from projection import evidence
ORIGINAL_SHA="daa2835e76df10e6dece88473d4b08d2ce1e4ad82680de3296de99604b54a288"
PROJECTED_SHA='5f0ce76f0d308da14cce1c0f57899577dd14732e1bc818ee916864d2d1902760'
SEAL_SHA="1f7bace09a0c787d70a821fd48109e7ee6ad5b8a83fc54d3843c3fd5d78cd735"
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def require(v,label):
 if not v:raise ValueError(label)
def load_plan():return json.loads((ROOT/'plan.json').read_text())
def load_source_projection():
 reference=json.loads((ROOT/'reference.json').read_text());manifest=json.loads((ROOT/'Source16-projection-manifest.json').read_text());require(manifest['Original_raw_sha256']==reference['Original_raw_sha256']==ORIGINAL_SHA and manifest['projected_sha256']==reference['Source16_projection_sha256']==sha(ROOT/'Source16-projection.json')==PROJECTED_SHA,'StrictOriginalAndProjectedReferenceHashes');require(manifest['executed_original_bytes']is False and manifest['projection_does_not_reconstruct_original_archive']is True,'ExplicitProjectionOnlyScope');return json.loads((ROOT/'Source16-projection.json').read_text())
def unpack(output):
 inventory=json.loads((ROOT/'lifecycle-projection-inventory.json').read_text());seen=set();total=0
 with tarfile.open(ROOT/'lifecycle-projection.tar.gz')as tar:
  for m in tar:
   require(m.isfile()and m.name in inventory and m.name not in seen and not Path(m.name).is_absolute()and'..'not in Path(m.name).parts and m.size<=33554432,'ExactBoundedLifecycleMember');data=tar.extractfile(m).read();total+=len(data);require(total<=134217728 and inventory[m.name]['projected_bytes']==len(data)and inventory[m.name]['projected_sha256']==hashlib.sha256(data).hexdigest(),'LifecycleProjectedBytes');p=output/m.name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data);seen.add(m.name)
 require(seen==set(inventory),'CompleteLifecycleInventory');return len(seen)
def validate_observations(source,folder):
 projected=copy.deepcopy(source);require(json.loads((folder/'backup-job-private.json').read_text())==source,'ProjectedLifecycleSourceBinding');expected_queue='rust_probe_backupjob_'+source['nonce'];before=source['before_Celery_configuration'];after=source['after_Celery_configuration'];require(before['Original_routes']==after['Original_routes']=={'apps.channels.tasks.run_recording':{'queue':'dvr'}}and before['producer_only_owned_route_added']is True and after['producer_only_owned_route_added']is False and before['owned_queue']==after['owned_queue']==before['actual_create_route_queue']==expected_queue and after['actual_create_route_queue']=='celery','ExactDeclaredProducerOnlyRouting');
 for row,spec in zip(source['requests'],load_plan()['cases']):
  expected={}
  if 'x_api_key'in spec['auth']:expected['HTTP_X_API_KEY']=evidence(('backup-fixture-'+source['nonce']+'-'+spec['auth']['x_api_key']).encode())
  if 'bearer_literal'in spec['auth']:expected['HTTP_AUTHORIZATION']=evidence(('Bearer '+spec['auth']['bearer_literal']).encode())
  require(row['raw_headers_private']==expected,'ExactProjectedRealCredentialMaterial')
 projected['_portable_archive_proof']=json.loads((ROOT/'archive-proof.json').read_text());result=compare(projected,load_plan(),*[json.loads((folder/name).read_text())for name in('worker-ready.json','worker-prerun.json','worker-postrun.json','worker-returned.json','post-worker-snapshot.json')]);execution=json.loads((folder/'execution.json').read_text());require(execution['controller_exit']==0 and execution['passed']is True and execution['reviewed_seal_sha256']==SEAL_SHA and execution['owned_cleanup_independently_verified']is True,'ActualControllerAndScope');require((folder/'default-before-private.json').read_bytes()==(folder/'default-after-private.json').read_bytes(),'LiteralDefault11Bytes');require(all(r['physically_settled']and r['pipes_settled']and r['exit']==0 for r in execution['host_children']),'AllActualHostWaitsPipes0');require(json.loads((folder/'owned-backends-after-worker.json').read_text())['actual_backends']==[],'ActualSQLBackendAbsence');cleanup=json.loads((folder/'independent/cleanup.json').read_text());require(cleanup['passed']is True and len(cleanup['queries'])==4 and all(x['original_process_absent']for x in cleanup['PIDs']),'IndependentExactCleanup');return result
def validate():
 inventory=json.loads((ROOT/'packet-inventory.json').read_text());actual={str(p.relative_to(ROOT)):sha(p)for p in ROOT.rglob('*')if p.is_file()and p.name!='packet-inventory.json'and'__pycache__'not in p.parts};require(actual==inventory,'ExactCompletePacketInventory');source=load_source_projection();definition=ROOT/'executed-definition';require(sha(definition/'seal.json')==SEAL_SHA,'ExactExecutedSeal');seal=json.loads((definition/'seal.json').read_text())
 for row in seal['files']:
  if row['path']=='inputs/original-source.tar':require(row['sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','OnlyOriginalGitArchiveOmitted');continue
  p=definition/row['path'];require(p.stat().st_size==row['bytes']and sha(p)==row['sha256'],'ExactExecutedInput:'+row['path'])
 with tempfile.TemporaryDirectory(prefix='backup-source16-proof-',dir=ROOT.parent)as t:
  folder=Path(t);count=unpack(folder);effects=validate_observations(source,folder)
 host=json.loads((ROOT/'Root-actual-exit.json').read_text());require(host['exit']==0 and host['wait_completed']is True,'ActualHostWait0');review=json.loads((ROOT/'Root-actual-review.json').read_text());require(review['passed']is True and review['requests']==16 and review['pure_effect_checks']==144,'RetainedIndependentPrivate144Review');return {'passed':True,'actual_Source_requests':16,'original_private_effect_checks':144,'projected_scope_validation':effects,'projected_lifecycle_files':count,'exact_executed_inputs':len(seal['files']),'omitted_original_archive_recipe_required':True,'raw_PG_archive_reconstructed_or_publicly_redecoded':False,'Source_TCP_Native_fullstartup_restore_schedule_claim':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
