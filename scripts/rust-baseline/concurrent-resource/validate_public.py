#!/usr/bin/env python3
"""Portable static verification of blocked observer qualification, not raw replay."""
from pathlib import Path
import hashlib,json
HERE=Path(__file__).resolve().parent
EXPECTED={'verified-source-observer-setup-negative-1.json':'3a11ab9e1f34653222ab8dc81541398e001e9e4dc2d7d3d0cc0a155565563a7b','verified-source-observer-contract-2.json':'eee06ca9606f447d5a3249c6c5a0fe1d925ea9f8a6da32645af835b3320e6f33','captured-observer-plan-2.json':'716f98643a7ed0c0202da05a6fbe33a3c591554c488c57beea18f2bbcecfa32a','captured-resource7-seal.json':'d5eb23ca1f2623f49c4751d627a2790c2796fa879c8ad373d0efdba62fc51b88'}
EXPECTED.update({'captured-resource7-qualification.json': 'ccf0a9776be3985bfc10c0214623302d1d62fc6627f43fdfda51ee4323a42618', 'captured-policy-audit6.json': '3ac0dab99c9caf4bafc9c3674526963bd5ed28b14a67121fdf845f2937942679'})
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def require(value,why):
 if not value:raise ValueError(why)
def validate(directory=HERE):
 directory=Path(directory);documents={}
 for name,expected in EXPECTED.items():require(sha(directory/name)==expected,'PublicEvidenceHash:'+name);documents[name]=json.loads((directory/name).read_text())
 plan=documents['captured-observer-plan-2.json'];positive=documents['verified-source-observer-contract-2.json'];negative=documents['verified-source-observer-setup-negative-1.json'];seal=documents['captured-resource7-seal.json']
 files={p.name:sha(p)for p in(directory/'controller').iterdir()if p.is_file()};require(files==plan['controller_sha256']and len(files)==36,'Controller36Bytes')
 require(all(files[x['name']]==x['sha256']for x in seal['controller_files'])and len(seal['controller_files'])==34,'Resource7Seal34')
 require(files['policy.json']==plan['Resource7_policy_sha256']and seal['policy_bytes_unchanged']is True,'UnchangedReviewedPolicy')
 source=(directory/'controller/run.py').read_text();require('RESOURCE_EXECUTION_BLOCKED=True'in source,'BlockedResourceEntry')
 require(plan['full_resource_window_enabled']is False and plan['Native_enabled']is False,'NoFullWindow')
 require(positive['passed']is True and positive['resource_acceptance']is False and positive['M0_complete']is False and positive['performance_verified']is False,'ScopedPositive')
 require(positive['counts']=={'resource_rows':10,'CPU_intervals':9,'complete_censuses':5}and positive['owned_cleanup_independently_empty']is True,'ActualCountsCleanup')
 fault=positive['physical_fault'];require(fault['actual_collection_failure']=='CollectionDeadline'and fault['owned_stop']['container_count']==4 and fault['owned_stop']['all_stopped_PID_zero']is True and fault['host_settlement']['drained']is True and fault['host_settlement']['pipes_alive']==0,'PhysicalHelperScope')
 require(negative['passed']is False and negative['first_fatal']['exit_code']==2 and negative['secondary_diagnostics'][1]['effective_status']=='NOT_RUN'and negative['physical_helper_fault_reached']is False,'RetainedNegative')
 return {'passed':True,'validation_scope':'static published attestation and exact blocked controller hashes, not private raw replay or new execution','controller_files':36,'Resource7_files':34,'full_resource_entries_blocked':True,'Source1_setup_failure_retained':True,'Source2_functional_observer_pass':True,'Native_enabled':False,'resource_acceptance':False,'M0_complete':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
