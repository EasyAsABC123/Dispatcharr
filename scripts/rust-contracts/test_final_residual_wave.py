"""Portable controls for the final composed Original evidence manifest."""
import copy
from hashlib import sha256
from pathlib import Path
import json
import unittest

import retained_autosync28_task
import retained_backup_create_task
import retained_backup_lifecycle_tasks
import retained_matching51_tasks
import retained_migration_recorder_contracts as migrations
import retained_model_registry_contracts as model_registry
import retained_task_registry_contracts as task_registry
import retained_management_registration_contracts as command_registry
import retained_residual_source_contracts as portable
import retained_root7_contract
import retained_worker_wire_contracts
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


TYPED = (
    retained_backup_create_task,
    retained_backup_lifecycle_tasks,
    retained_matching51_tasks,
    retained_autosync28_task,
    retained_worker_wire_contracts,
)
HTTP_COUNT = 177
TYPED_COUNT = 11


def dependencies(root):
    return portable.dependencies(root).union(retained_root7_contract.dependencies(), migrations.dependencies(),
                                             model_registry.dependencies(), task_registry.dependencies(), command_registry.dependencies(),
                                             *(module.dependencies() for module in TYPED))


def restore_pre_final_wave(root, register):
    """Reverse the exact scoped additions before checking historical row digests."""
    inputs = register['executable_Source_mapping']['inputs']
    command_input = inputs.pop()
    assert (command_input['kind'], command_input['path']) == (command_registry.KIND, command_registry.INPUT)
    command_registry.validate(root, source_contracts.checked(root, command_input))
    command_registry.restore_before(root, register['non_HTTP_candidates'])
    task_input = inputs.pop()
    assert (task_input['kind'], task_input['path']) == (task_registry.KIND, task_registry.INPUT)
    task_registry.validate(root, source_contracts.checked(root, task_input))
    task_registry.restore_before(root, register['non_HTTP_candidates'])
    model_input = inputs.pop()
    assert (model_input['kind'], model_input['path']) == (model_registry.KIND, model_registry.INPUT)
    model_registry.validate(root, source_contracts.checked(root, model_input))
    model_registry.restore_before(root, register['non_HTTP_candidates'])
    migration_input = inputs.pop()
    assert (migration_input['kind'], migration_input['path']) == (migrations.KIND, migrations.INPUT)
    migrations.validate(root, source_contracts.checked(root, migration_input))
    migrations.restore_before(root, register['non_HTTP_candidates'])
    root7 = inputs.pop()
    assert root7['kind'] == retained_root7_contract.KIND and root7['path'] == retained_root7_contract.INPUT
    retained_root7_contract.validate(root, source_contracts.checked(root, root7))
    retained_root7_contract.restore_before(root, register['runtime_routes'])
    tail = inputs[-(len(TYPED) + 1):]
    assert [ref['kind'] for ref in tail] == [module.KIND for module in (*TYPED, portable)]
    captured_http = source_contracts.checked(root, tail[-1])
    portable.validate(root, captured_http)
    http = {contract['contract_id']: contract for contract in captured_http['contracts']}
    typed = {}
    for ref, module in zip(tail[:-1], TYPED):
        captured = source_contracts.checked(root, ref)
        module.validate(root, captured)
        for contract in captured['non_HTTP_contracts']:
            identifier = contract['candidate_id']
            assert identifier not in typed
            typed[identifier] = contract
    assert (len(http), len(typed)) == (HTTP_COUNT, TYPED_COUNT)

    operations = {op['contract_id']: op for row in register['runtime_routes'] for op in row['operations']}
    for identifier, contract in http.items():
        operation = operations[identifier]
        assert operation.pop('executable_Source_contract') == contract
        operation.update(
            setup_fixture=None,
            assertions=[],
            result=None,
            coverage_state='scoped_evidence_needs_case_mapping' if operation['related_evidence'] else 'not_yet_mapped',
        )
        for field in ('observed_fixture_definitions', 'complete', 'observed_roles'):
            operation['role_matrix'].pop(field)
        operation['role_matrix']['state'] = 'unmapped'

    candidates = {entry['candidate_id']: entry for entry in register['non_HTTP_candidates']}
    for identifier, contract in typed.items():
        entry = candidates[identifier]
        assert entry.pop('executable_Source_contract') == contract
        for field in ('setup_fixture', 'owning_milestone', 'assertions'):
            entry.pop(field)
        entry.update(state='static_candidate_requires_runtime_contract', result=None)


def restore_before_residual_wave(root, projection):
    """Recover the pinned 619-operation register prior to the final additive wave."""
    result = projection['executable_Source_mapping']
    restore_pre_final_wave(root, projection)
    del result['inputs'][-(len(TYPED) + 1):]
    result['operations'] -= HTTP_COUNT + 1
    # Canonical field bytes from 0c2b9920b180318f591506b31d555bc1aba7780d.
    before = {
        'runtime_routes': '4b2a083a493df7be212b52f01e06993c810a5b3a1099cb83efe3bcb5bc13966f',
        'non_HTTP_candidates': '3ba566ff6cddc0076f4e0ccd4c69b6fd914e53a8a3d7d2de96f4d80878ac5403',
        'executable_Source_mapping': 'bfb0837cb0f49ecf7b2fa24aeecf7a61ad92653ca6bc3f7004efb6a8cbf56b20',
    }
    for key, digest in before.items():
        canonical = (json.dumps(projection[key], indent=2, ensure_ascii=False) + '\n').encode()
        assert sha256(canonical).hexdigest() == digest, key


def apply_before_residual_wave(root, rows, bindings):
    """Replay live validators, then recover the exact historical output scope."""
    projection = json.loads((root / 'docs/rust-contract-register.json').read_bytes())
    projection['runtime_routes'] = rows
    projection['executable_Source_mapping'] = source_contracts.apply(root, rows, bindings)
    restore_before_residual_wave(root, projection)
    return projection['executable_Source_mapping']


class ResidualWave(unittest.TestCase):
    def test_both_outer_observation_modes_and_projection_tamper(self):
        captured = [json.loads((ROOT / name).read_text()) for name in sorted(portable.dependencies(ROOT))]
        self.assertTrue(captured)
        entries = [capture for binding in captured for capture in binding["captures"]]
        self.assertTrue(any("outer_execution_sha256" in capture for capture in entries))
        scoped = {capture["stage"]: capture for capture in entries if "reviewed_outer_wait" in capture}
        self.assertEqual(set(scoped), set(portable.REVIEWED_WAIT))
        for binding in captured:
            portable.validate(ROOT, binding)
        for capture in scoped.values():
            self.assertIs(capture["reviewed_outer_wait"]["separate_outer_EOF_process_group_witness"], False)
            self.assertTrue(all(phase["EOF_and_process_group_settled"] for phase in capture["phase_exits"].values()))
            for mutate in (
                lambda c: c.update(outer_execution_sha256="0"*64),
                lambda c: c["reviewed_outer_wait"].update(actual_exit=1),
                lambda c: c["phase_exits"]["probe"].update(EOF_and_process_group_settled=False),
            ):
                damaged = copy.deepcopy(capture)
                mutate(damaged)
                with self.assertRaises(AssertionError):
                    portable.materialize([damaged])


if __name__ == "__main__":
    unittest.main()
