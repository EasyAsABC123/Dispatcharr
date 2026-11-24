"""Focused cold binding and fail-closed controls for the first-scan owner."""
from base64 import b64encode
from copy import deepcopy
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import UUID

import scanner_first_owner as owner
import scanner_first_scenario as scenario

ROOT = os.environ.get("M6_SCANNER_REFERENCE_ROOT")
TEMPLATE = os.environ.get("M6_SCANNER_PACKAGE_TEMPLATE")
CANDIDATE = os.environ.get("M6_SCANNER_COLD_CANDIDATE")


def witness():
    ids = list(range(10, 17))
    seed = {"database": "normal_dvr_0123456789abcdef", "ids": ids, "stock": [], "schedule_id": 6}
    before = {"database": seed["database"], "read_only": True, "files": {"m3us": [], "epgs": [], "logos": []},
              "sources": [], "indexes": [], "programmes": [], "playlists": [], "logos": [], "parsers": []}
    for ident, (label, kind, active, state) in zip(ids, scenario.SOURCES, strict=True):
        before["sources"].append({"id": ident, "name": "rust-owned-first-scan-" + label, "source_type": kind,
                                  "is_active": active, "file_path": f"/data/epgs/rust-owned-first-scan-{label}.xml",
                                  "extracted_file_path": None})
        if state != "absent":
            before["indexes"].append({"source_id": ident, "data": {} if state == "empty" else None,
                                      "sql_null": state == "sql-null", "updated_at": "2026-11-24T01:00:00+00:00"})
    schedule = {"id": 6, "name": scenario.NAME, "task": scenario.SCAN, "enabled": False, "count": 0,
                "every": 20, "period": "seconds", "one_off": False, "args": [], "kwargs": {}, "last_run_at": None}
    before["schedule"] = schedule
    after = before | {"schedule": schedule | {"count": 1, "last_run_at": "2026-11-24T01:01:00+00:00"}}
    identifiers = [str(UUID(int=i + (4 << 76) + (2 << 62))) for i in range(3)]
    root = identifiers[0]
    entries = []
    for index, ident in enumerate(identifiers):
        task = scenario.SCAN if not index else scenario.INDEX
        frame = {"headers": {"id": ident, "task": task, "eta": None, "periodic_task_name": scenario.NAME if not index else None,
                              "parent_id": root if index else None, "root_id": root},
                 "properties": {"correlation_id": ident, "delivery_info": {"exchange": "", "routing_key": "celery"}},
                 "body": b64encode(json.dumps([[ids[index - 1]] if index else [], {}, {}]).encode()).decode()}
        entries.append({"queue": "celery", "frame": frame, "actual_redis_time": 100 + index})
    terminals = {ident: {"task_id": ident, "status": "SUCCESS", "result": None,
                         "parent_id": None if ident == root else root, "children": []} for ident in identifiers}
    terminals[root]["children"] = [[[value, None], None] for value in identifiers[1:]]
    report = {"seed": seed, "before": before, "after": after, "jobs": scenario.publications(entries, set(ids[:2])),
              "terminals": terminals, "arm": {"id": 6, "count": 0, "enabled": True,
                                              "last_run_at": "2026-11-24T01:00:00+00:00"},
              "arm_started": 99, "arm_finished": 101, "receiver": "test",
              "events": {"actual_receiver": "test", "events": [], "raw_events_base64": []}, "ready_or_reserved": 0}
    return report, entries


class ScannerFirst(unittest.TestCase):
    def test_sql_null_json_null_and_excluded_actual_job_controls(self):
        report, wire = witness()
        self.assertEqual(scenario.validate(report, wire)["eligible_source_count"], 2)
        report["before"]["indexes"][1]["sql_null"] = True
        with self.assertRaisesRegex(ValueError, "SQL NULL, JSON null"):
            scenario.validate(report, wire)
        report, wire = witness()
        wire[2]["frame"]["body"] = b64encode(b"[[12],{},{}]").decode()
        with self.assertRaisesRegex(ValueError, "excluded or unrelated"):
            scenario.validate(report, wire)

    def test_event_order_sql_drift_and_extra_job_are_refused(self):
        report, wire = witness()
        earlier = deepcopy(wire)
        earlier[1]["actual_redis_time"] = 99
        with self.assertRaisesRegex(ValueError, "must precede"):
            scenario.validate(report, earlier)
        with self.assertRaisesRegex(ValueError, "duplicate first-scan"):
            scenario.validate(report, wire + [wire[2]])
        after = deepcopy(report)
        after["after"]["logos"] = [29]
        with self.assertRaisesRegex(ValueError, "scanner changed"):
            scenario.validate(after, wire)
        report["events"]["events"].append("unexpected")
        with self.assertRaisesRegex(ValueError, "emit no file summary"):
            scenario.validate(report, wire)

    @unittest.skipUnless(ROOT and TEMPLATE and CANDIDATE, "set exact retained scanner and package inputs")
    def test_prepare_checks_original_and_native_without_starting_an_engine(self):
        repo = Path(ROOT)
        candidate = owner.read(Path(CANDIDATE))
        with TemporaryDirectory() as folder:
            root = Path(folder)
            stage = root / "stage"
            prepared = owner.prepare(repo, Path(TEMPLATE), candidate, stage)
            self.assertEqual(prepared["cold_preflight"]["engine_calls"], 0)
            binding = owner.dispatch_spec(repo, stage, prepared["seal"], candidate, "scanner", root / "out")
            self.assertIn("run", binding["argv"])
            self.assertIn("evaluate", binding["evaluator_argv"])
            self.assertFalse((stage / "actual-1").exists())
            with self.assertRaisesRegex(ValueError, "frozen ordinary package"):
                owner.dispatch_spec(repo, stage, prepared["seal"], dict(candidate, git="0" * 40), "scanner", root / "out")
