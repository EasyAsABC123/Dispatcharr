"""Portable controls for the final composed Original evidence manifest."""
import copy
from pathlib import Path
import json
import unittest

import retained_autosync28_task
import retained_backup_create_task
import retained_backup_lifecycle_tasks
import retained_matching51_tasks
import retained_residual_source_contracts as portable
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
    return portable.dependencies(root).union(*(module.dependencies() for module in TYPED))


def restore_pre_final_wave(root, register):
    """Reverse the exact scoped additions before checking historical row digests."""
    tail = register['executable_Source_mapping']['inputs'][-(len(TYPED) + 1):]
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
