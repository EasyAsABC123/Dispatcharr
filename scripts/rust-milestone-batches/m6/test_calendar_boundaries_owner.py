"""Offline controls for the Original fall-fold witness and scoped Native preflight."""
from base64 import b64encode
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import calendar_boundaries_owner as owner

FOLD = owner.common.load_module(Path(__file__).resolve().parents[2] / "rust-job-contracts/source_calendar_fall_fold.py",
                                "offline_original_fall_fold")
ARTIFACTS = os.environ.get("M6_REPLAY_ARTIFACTS")
CANDIDATE = os.environ.get("M6_CALENDAR_COLD_CANDIDATE")
TEMPLATE = os.environ.get("M6_CALENDAR_NATIVE_TEMPLATE")


def controlled_witness():
    identity = "249629d8-3b12-4e24-b46a-a2baf710850e"
    frame = {"headers": {"id": identity, "root_id": identity, "task": FOLD.TASK,
                         "periodic_task_name": FOLD.NAME, "parent_id": None, "eta": None},
             "properties": {"correlation_id": identity, "delivery_info": {"exchange": "", "routing_key": "celery"}},
             "body": b64encode(json.dumps([[3], {}, {}]).encode()).decode()}
    before = {"id": 2, "name": FOLD.NAME, "enabled": True, "one_off": False,
              "count": 0, "last_run_at": "2026-10-31T05:30:00+00:00",
              "timezone": "America/New_York", "minute": "30", "hour": "1"}
    stages = [{"clock": clock, "local_fold": fold, "frames": payload, "clock_reads": 2}
              for clock, fold, payload in ((FOLD.BEFORE, 0, []), (FOLD.FIRST, 0, [frame]), (FOLD.SECOND, 1, [frame]))]
    return {"source_id": 3, "new_york_tzif_sha256": FOLD.TZIF_SHA, "stages": stages,
            "before": before, "after": before | {"count": 1, "last_run_at": FOLD.FIRST},
            "beat_actual_exit": 0, "beat_generation_retired": True, "queue_and_results_after_cleanup": [0, 0]}


class FoldWitness(unittest.TestCase):
    def test_refuses_second_publication_and_false_exit(self):
        witness = controlled_witness()
        self.assertEqual(FOLD.qualify(witness)["durable_count"], 1)
        duplicate = deepcopy(witness)
        duplicate["stages"][2]["frames"].append(duplicate["stages"][1]["frames"][0])
        with self.assertRaisesRegex(ValueError, "only publish at the first"):
            FOLD.qualify(duplicate)
        with self.assertRaisesRegex(ValueError, "literal Beat exit"):
            FOLD.qualify(witness | {"beat_actual_exit": False})

    def test_requires_live_process_dependency_reads_for_second_fold(self):
        witness = controlled_witness()
        with TemporaryDirectory() as folder:
            path = Path(folder) / "clock.jsonl"
            entries = [{"pid": 71, "clock": step["clock"], "dependency": "entry-now"}
                       for step in witness["stages"] for _ in range(2)]
            raw = "".join(json.dumps(entry) + "\n" for entry in entries).encode()
            path.write_bytes(raw)
            witness.update(beat_pid=71, beat_starttime="1993", clock_log_sha256=sha256(raw).hexdigest())
            self.assertEqual(FOLD.clock_log(witness, path), witness["clock_log_sha256"])
            raw = "".join(json.dumps(entry) + "\n" for entry in entries[:-1]).encode()
            path.write_bytes(raw)
            witness["clock_log_sha256"] = sha256(raw).hexdigest()
            with self.assertRaisesRegex(ValueError, "clock calls missing"):
                FOLD.clock_log(witness, path)

    @unittest.skipUnless(ARTIFACTS, "set the retained Original evidence root")
    def test_retained_spring_and_eager_fall_keep_missing_process_scope_open(self):
        artifact = Path(ARTIFACTS).resolve() / "m6-source-native-calendar-vectors-final-1/source-scheduler-calendar-private.json"
        proof = owner.original_boundaries(artifact)
        self.assertEqual(proof["spring_real_beat"]["controlled_original_beat_first_publication_offset_seconds"], 1800)
        self.assertTrue(proof["fall_eager_sql_and_publication"])
        self.assertIsNone(proof["fall_real_beat"])
        self.assertFalse(proof["canonical_calendar_full_scope_accepted"])

    @unittest.skipUnless(CANDIDATE and TEMPLATE, "set the ordinary package context and exact calendar template")
    def test_current_saved_zone_preflight_has_no_dst_or_physical_credit(self):
        candidate = owner.read(Path(CANDIDATE))
        with TemporaryDirectory() as folder:
            stage = Path(folder) / "calendar"
            binding = owner.prepare_native(Path(TEMPLATE), candidate, stage)
            self.assertEqual([row["scope"] for row in binding["scoped_native_preflights"]], list(owner.SCOPES))
            self.assertTrue(all(row["engine_calls"] == 0 for row in binding["scoped_native_preflights"]))
            self.assertFalse(binding["real_dst_transition_verified"])
            with self.assertRaises(FileNotFoundError):
                owner.native_partial(stage, binding["seal"], candidate)
