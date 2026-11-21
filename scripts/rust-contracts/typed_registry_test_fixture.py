"""Pinned slice fixtures for historical typed Source validation."""
from contextlib import contextmanager
import copy
from hashlib import sha256
from importlib import import_module
import json
from pathlib import Path
import shutil
import tempfile

import source_contracts


MANIFEST_PATH = 'scripts/rust-contracts/executable-source-inputs.json'
REGISTER_PATH = 'docs/rust-contract-register.json'
HISTORICAL_TYPED_AUTHORITY_MODULES = {
    'pinned_original_account_settings_tests': 'account_settings_contracts',
    'captured_original_scanner_task_observations': 'scanner_task_contracts',
    'pinned_original_provider_output_consumer_tests': 'provider_output_contracts',
    'retained_original_task_operator7_observations': 'retained_task_operator_contracts',
    'retained_original_comskip_task_observations': 'retained_comskip_task_contracts',
    'retained_original_clocked_recording_tasks': 'retained_clocked_recording_contracts',
    'retained_original_C105_guide_startup_tasks': 'retained_c105_guide_startup_contracts',
}
# Exact authority-ref vectors at 0183abb312db38259b758d3dd7fba475a610f878
# and 10d85261a35f27689b226b13613f10b6e6ea9479.
HISTORICAL_TYPED_REFERENCES = {
    18: 'a31ac40e1c69cb5bf99a1872d46df56baf249a8403f9d5053642e4700c0c2c54',
    21: 'e39f92b8f709a9f1cd25fb6b2b9ac848acfa35f4728d76fbf1a572dfc120e172',
}


@contextmanager
def historical_typed_registry(root, candidate_count):
    manifest = json.loads((root / MANIFEST_PATH).read_text())
    modules = dict(HISTORICAL_TYPED_AUTHORITY_MODULES)
    if candidate_count == 18:
        del modules['retained_original_C105_guide_startup_tasks']
    historical = [ref for ref in manifest['inputs'] if ref['kind'] in modules]
    canonical = json.dumps(historical, sort_keys=True, separators=(',', ':')).encode()
    assert sha256(canonical).hexdigest() == HISTORICAL_TYPED_REFERENCES[candidate_count]
    authority_paths = {REGISTER_PATH}
    for ref in historical:
        authority_paths.add(ref['path'])
        if ref['kind'] == 'pinned_original_account_settings_tests':
            data = json.loads((root / ref['path']).read_text())
            authority_paths.update(item['path'] for item in data['authorities'])
            authority_paths.update(item['path'] for item in data['original_test_files'])
        else:
            validator = import_module(modules[ref['kind']])
            authority_paths.update(validator.dependencies())

    with tempfile.TemporaryDirectory() as temp:
        fixture = Path(temp)
        for name in sorted(authority_paths):
            target = fixture / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / name, target)
        (fixture / MANIFEST_PATH).write_text(json.dumps({**manifest, 'inputs': historical}))
        yield fixture


def assert_typed_idempotence(test, root, candidate_count):
    before = json.loads((root / REGISTER_PATH).read_text())['non_HTTP_candidates']
    test.assertEqual(len(before), 250)
    test.assertTrue(all(row['fully_verified'] is False for row in before))
    historical = copy.deepcopy(before)
    with historical_typed_registry(root, candidate_count) as fixture:
        mapping = source_contracts.apply_non_HTTP(fixture, historical)
    test.assertEqual(mapping['candidates'], candidate_count)
    test.assertEqual(mapping['fully_verified_added'], 0)
    test.assertEqual(before, historical)
