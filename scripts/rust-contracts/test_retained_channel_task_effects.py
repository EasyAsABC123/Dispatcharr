"""Check the exact behavioral supplements alongside existing registration evidence."""
from copy import deepcopy
import json
from pathlib import Path
import unittest

import retained_channel_task_effects as subject
import retained_task_registry_contracts as registration

ROOT = Path(__file__).resolve().parents[2]


class OrdinaryChannelTaskEffects(unittest.TestCase):
    def setUp(self):
        stored = json.loads((ROOT / "docs/rust-contract-register.json").read_text())
        self.entries = stored["non_HTTP_candidates"]
        self.observed = [entry for entry in self.entries if subject.FIELD in entry]
        self.primary = deepcopy(self.entries)
        for entry in self.primary:
            entry.pop(subject.FIELD, None)

    def test_attach_only_four_exact_ordinary_task_observations(self):
        self.assertEqual(len(self.observed), 4)
        primary = registration.derive(ROOT)["non_HTTP_contracts"]
        registrations = {one["candidate_id"]: one for one in primary}
        for entry in self.observed:
            self.assertEqual(entry["executable_Source_contract"], registrations[entry["candidate_id"]])
            self.assertIs(entry["result"]["task_execution_verified"], False)
            self.assertIs(entry["fully_verified"], False)
        subject.apply(ROOT, self.primary)
        self.assertEqual(self.primary, self.entries)

    def test_reject_duplicate_attachment(self):
        with self.assertRaisesRegex(AssertionError, "duplicate Original task observation"):
            subject.apply(ROOT, self.entries)

    def test_reject_bad_identity_and_observed_missing_effects(self):
        cases = [json.loads(subject.exact_input(ROOT, path))
                 for path in (subject.SOURCE_REPORT, subject.CONTROLLER, subject.CLEANUP)]
        mutations = {
            "changed task": lambda capture, _, cleanup: capture["cases"][0].update(task="wrong"),
            "controlled exception": lambda capture, _, cleanup: capture["cases"][0].update(controlled_owned_PG_error={}),
            "wrong backend task": lambda capture, _, cleanup: capture["cases"][0].update(
                actual_task_id=capture["cases"][1]["actual_task_id"]),
            "missing progress": lambda capture, _, cleanup: capture["cases"][2]["Channels"]["decoded"].pop(),
            "missing durable logo": lambda capture, _, cleanup: capture["cases"][2]["after"]["dispatcharr_channels_logo"].pop(),
            "missing relation": lambda capture, _, cleanup: capture["cases"][3]["after"]["dispatcharr_channels_channelstream"].pop(),
            "nonzero controller": lambda _, controller, cleanup: controller.update(actual_controller_exit=1),
            "uncertain retirement": lambda _, controller, cleanup: cleanup.update(exact_owned_absence=False),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                altered = deepcopy(cases)
                mutate(*altered)
                with self.assertRaises(AssertionError):
                    subject.observations(self.primary, *altered)


if __name__ == "__main__":
    unittest.main()
