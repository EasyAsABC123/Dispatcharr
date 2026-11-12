"""Verify public component identity and counts; not a private fixture reexecution."""
import json,re,hashlib
from pathlib import Path
from reconstruct import prepare

def verify(root):
    root=Path(root);base,final,_,patches=prepare(root);data=json.loads((root/'verified.json').read_text())
    assert all(data[k] is False for k in ['Main_validated','new_application_image_validated','full_M4_accepted','performance_accepted'])
    for name,key in [('inputs.json','input_map_sha256'),('runtime-cargo.diff','runtime_cargo_diff_sha256'),('cleanup.json','independent_cleanup_sha256'),('run_native.py','native_runner_sha256'),('source-header-primitives.json','Source_header_primitive_prior1_sha256')]:
        assert hashlib.sha256((root/name).read_bytes()).hexdigest()==data[key]
    assert data['passed'] is True and data['registry_packages_added']==0
    assert data['non_test_build_passed'] is True and data['strict_vod_http_all_targets_Clippy_passed'] is True
    assert data['package_default_test_executions']==80 and data['ignored_fixture_tests']==238 and data['test_doc_result_suites']==14
    expected=[('http','',15),('retained_reserve','retained_',6),('retained_reserve','ordinary_two_second_',1),('lease_physical','',13),('idle_entry','pending_follower_',4),('idle_entry','idle_entry_',5),('generation_scope','local_scope_',20),('generation_scope_physical','local_physical_',9),('pending_follower_http','pending_http_',9),('http_recovery','actual_http_',13)]
    rows=data['real_batches'];assert [(x['target'],x['filter'],x['passed_count'])for x in rows]==expected
    assert all(type(x['passed_count'])is int and type(x['returncode'])is int for x in rows)
    assert sum(x['passed_count']for x in rows)==data['real_Redis_media_checks']==95 and all(x['returncode']==0 and re.fullmatch('[0-9a-f]{64}',x['log_sha256'])for x in rows)
    binaries=data['outside_ELF_sha256'];assert len(binaries)==15 and {x[0]for x in expected}<=set(binaries)
    assert all(type(x['bytes'])is int and x['bytes']>0 and re.fullmatch('[0-9a-f]{64}',x['sha256'])for x in binaries.values())
    cleanup=json.loads((root/'cleanup.json').read_text());assert cleanup['passed'] is True and len(cleanup['exact_owned_container_inventory'])==2 and all(x['remaining_containers']==[]for x in cleanup['exact_owned_container_inventory'])
    assert data['raw_media_or_binaries_published'] is False
    vectors=json.loads((root/'source-header-primitives.json').read_text())['rows'];assert len(vectors)==9 and sum(x['success']for x in vectors)==7
    assert sum(x.get('error_type')=='BadHeaderError'for x in vectors)==2
    assert len(base)==788 and len(final)==789 and len(patches)==5
    return {'passed':True,'runtime_paths':5,'qualified_Rust_inputs':789,'package_test_executions':80,'ignored':238,'actual_native_Redis_media_checks':95,'Main_or_new_image_qualification':False,'full_M4_or_performance_claim':False}

if __name__=='__main__':print(json.dumps(verify(Path(__file__).resolve().parent)))
