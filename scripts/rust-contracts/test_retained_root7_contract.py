"""Portable Root-7 scope and receipt controls."""
import copy
import json
from pathlib import Path
import unittest

import retained_root7_contract as subject

ROOT = Path(__file__).resolve().parents[2]


class Root7(unittest.TestCase):
    def test_exact_unnamed_get_and_reject_altered_physical_scope(self):
        receipt = json.loads((ROOT / subject.RECEIPT).read_bytes())
        binding = json.loads((ROOT / subject.INPUT).read_bytes())
        subject.validate(ROOT, binding)
        register = json.loads((ROOT / "docs/rust-contract-register.json").read_bytes())
        route, = (route for route in register["runtime_routes"] if route["resolver_ordinal"] == 589)
        operation, = route["operations"]
        self.assertEqual((route["name"], route["pattern_exact"], route["source_handler"]), ("", "", subject.HANDLER))
        self.assertIsNone(operation["method"])
        self.assertIsNone(operation["source_action"])
        self.assertFalse(operation["fully_verified"])
        self.assertFalse(operation["role_matrix"]["complete"])
        self.assertEqual(operation["executable_Source_contract"], binding["contracts"][0])
        self.assertEqual(operation["executable_Source_contract"]["observed_methods"], ["GET"])
        self.assertEqual(operation["role_matrix"]["observed_roles"], ["anonymous"])

        def change_wire(value, field, new):
            value["actual"]["root_wire_and_resolver"][field] = new

        def change_resolver(value, field, new):
            value["actual"]["root_wire_and_resolver"]["actual_resolver"][field] = new

        corruptions = {
            "changed locale": lambda value: value["login_fixture"].update(single_added_entry="LANG=C"),
            "mounted app": lambda value: value["Original"].update(unmounted_application_and_assets=False),
            "failed phase": lambda value: value["actual"]["phase_results"]["probe"].update(actual_exit=1),
            "unsettled role": lambda value: value["actual"].update(exact_owned_Original_container_and_generation_retired=False),
            "remaining resource": lambda value: value["actual"]["owned_resources_after_cleanup"].update(container=1),
            "wrong response": lambda value: change_wire(value, "actual_html_sha256", "0" * 64),
            "missing asset": lambda value: value["actual"]["root_wire_and_resolver"]["exact_related_assets"].pop(),
            "redirected asset": lambda value: value["actual"]["root_wire_and_resolver"]["exact_related_assets"][0].update(redirect_present=True),
            "named URL": lambda value: change_wire(value, "resolver_url_name", "root"),
            "wrong ordinal": lambda value: change_resolver(value, "ordinal", 590),
            "declared method inferred": lambda value: change_wire(value, "resolver_method_declaration", "GET"),
            "wrong observed method": lambda value: change_wire(value, "method", "HEAD"),
        }
        for name, corrupt in corruptions.items():
            with self.subTest(name=name):
                damaged = copy.deepcopy(receipt)
                corrupt(damaged)
                with self.assertRaises(AssertionError):
                    subject.materialize(damaged)


if __name__ == "__main__":
    unittest.main()
