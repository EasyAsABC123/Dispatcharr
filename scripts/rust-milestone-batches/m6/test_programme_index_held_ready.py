"""Offline controls for the exact held Index reply phase."""
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
import unittest

import programme_index_held_ready as phase


class HeldIndexReady(unittest.TestCase):
    def test_protocol_requires_exact_executed_write_and_backend_error_before_ready(self):
        repo = Path(phase.__file__).resolve().parents[3]
        writer = repo / "rust/crates/ingest-epg/src/index_service.rs"
        self.assertIn('sqlx::query("' + phase.WRITE + '")', writer.read_text())
        namespace = {"__name__": "test_pg_gate"}
        exec(compile(phase.guest_source(repo), "index_pg_gate.py", "exec"), namespace)
        def exchange(query, error_code, message, execute=True):
            gate = namespace["Exchanges"]("update-error-Ready")
            gate.frontend(b"P", b"statement\0" + query.encode() + b"\0\0\0")
            gate.frontend(b"S", b"")
            self.assertIsNone(gate.backend(b"Z", b"T"))
            gate.forwarded_ready()
            gate.frontend(b"B", b"portal\0statement\0")
            if execute:
                gate.frontend(b"E", b"portal\0\0\0\0\0")
            gate.frontend(b"S", b"")
            error = b"SERROR\0C" + error_code.encode() + b"\0M" + message.encode() + b"\0\0"
            self.assertIsNone(gate.backend(b"E", error))
            return gate.backend(b"Z", b"E")
        self.assertEqual(exchange(phase.WRITE, "P0001", phase.MESSAGE)["identity"], phase.WRITE)
        for args in (("SELECT 1", "P0001", phase.MESSAGE),
                     (phase.WRITE, "42501", phase.MESSAGE),
                     (phase.WRITE, "P0001", "other"),
                     (phase.WRITE, "P0001", phase.MESSAGE, False)):
            with self.subTest(args=args):
                self.assertIsNone(exchange(*args))

    def test_premature_result_lost_lease_or_changed_owner_do_not_pass(self):
        event = {"stage": "actual-matching-Ready-held", "identity": phase.WRITE, "actual_error": True,
                 "executions": 1, "subject_complete_occurrence": 1, "frontend_Sync_or_simpleQuery_observed": True,
                 "matching_rows": 0, "matching_commands_hex": [], "backend_pid": 41, "monotonic": 1,
                 "sqlstate": "P0001", "ready_status": "E",
                 "message_sha256": sha256(phase.MESSAGE.encode()).hexdigest()}
        held = {"timeline": [event], "errors": []}
        samples = [{"started": begin, "finished": begin + .1, "result": ["result", 0],
                    "lease": ["lock", "a" * 40, 10000]} for begin in (2, 3)]
        phase.verify_hold(held, samples, "result", "lock")
        for key, replacement in (("result", ["result", 1]), ("lease", ["lock", False, -2]),
                                 ("lease", ["lock", "b" * 40, 10000])):
            with self.subTest(key=key, replacement=replacement):
                bad = deepcopy(samples)
                bad[1][key] = replacement
                with self.assertRaisesRegex(ValueError, "Redis file lease"):
                    phase.verify_hold(held, bad, "result", "lock")


if __name__ == "__main__":
    unittest.main()
