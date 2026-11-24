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


def lifecycle_witness():
    wire = []
    passes = []
    for number, children in ((1, 2), (2, 0), (3, 2)):
        part, entries = witness()
        entries = entries[:1 + children]
        metas = {}
        root_id = None
        for i, entry in enumerate(entries):
            head = entry["frame"]["headers"]
            ident = str(UUID(int=(number << 32) + i + (4 << 76) + (2 << 62)))
            old = head["id"]
            if not i:
                root_id = ident
            head.update(id=ident, parent_id=root_id if i else None, root_id=root_id)
            entry["frame"]["properties"]["correlation_id"] = ident
            entry["actual_redis_time"] += number * 100
            metas[ident] = part["terminals"][old] | {"task_id": ident, "parent_id": head["parent_id"], "children": []}
        metas[root_id]["children"] = [[[value, None], None] for value in list(metas)[1:]]
        if passes:
            part["before"] = passes[-1]["after"]
        part["after"]["schedule"].update(count=number, last_run_at=f"2026-11-24T01:0{number}:00+00:00")
        part["arm"]["count"] = number - 1
        part.update(journal_cursor=len(wire), arm_started=99 + number * 100, arm_finished=101 + number * 100,
                    terminals=metas, main="new" if number == 3 else "old",
                    jobs=scenario.publications(entries, set(part["seed"]["ids"][:children])))
        if number == 2:
            state = {"held": [42, "2026-11-24T01:00:00+00:00"], "waiters": []}
            part.update(lock_before_arm=state, lock_after_root=deepcopy(state))
        if number == 3:
            state = {"held": [43, "2026-11-24T01:03:00+00:00"], "waiters": []}
            part.update(lock_before_arm=state, blocked_first_helper=state | {"waiters": [44]},
                        blocked_root_result=["celery-tasks:celery-task-meta-" + root_id, 0, ["400", "500000"]])
        passes.append(part)
        wire.extend(entries)
    first, repeat, restarted = passes
    return {"seed": first["seed"], "before": first["before"], "after": restarted["after"], "passes": passes,
            "retained_results": first["terminals"] | repeat["terminals"],
            "restart": {"old": "old", "new": "new", "exit": {"actual_exit": 0, "oom_killed": False, "expected_exit": 0},
                        "after_stop": repeat["after"], "after_start": repeat["after"]}}, wire


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

    def test_same_main_repeat_and_cold_restart_evidence(self):
        report, wire = lifecycle_witness()
        self.assertEqual(scenario.validate_lifecycle(report, wire)["index_children"], [2, 0, 2])

    def test_missing_lock_restart_and_retained_results_are_refused(self):
        for key, expected, replacement in (
            ("lock_after_root", "same actual Index lock", {"held": None, "waiters": []}),
            ("main", "same process repeat", "different"),
        ):
            with self.subTest(key=key):
                report, wire = lifecycle_witness()
                report["passes"][1][key] = replacement
                with self.assertRaisesRegex(ValueError, expected):
                    scenario.validate_lifecycle(report, wire)
        report, wire = lifecycle_witness()
        report["retained_results"] = {}
        with self.assertRaisesRegex(ValueError, "DB2 retained"):
            scenario.validate_lifecycle(report, wire)
        report, wire = lifecycle_witness()
        report["passes"][2]["blocked_root_result"][1] = 1
        with self.assertRaisesRegex(ValueError, "lack its own DB2"):
            scenario.validate_lifecycle(report, wire)
        report, wire = lifecycle_witness()
        wire[3] = wire[1]
        with self.assertRaisesRegex(ValueError, "excluded or unrelated"):
            scenario.validate_lifecycle(report, wire)

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

    @unittest.skipUnless(ROOT and TEMPLATE and CANDIDATE, "set exact retained scanner and package inputs")
    def test_relocated_scanner_import_and_custody(self):
        repo = Path(ROOT)
        candidate = owner.read(Path(CANDIDATE))
        with TemporaryDirectory() as folder:
            root = Path(folder)
            stage = root / "stage"
            coordinator = root / "scripts/rust-milestone-batches/coordinator.py"
            coordinator.parent.mkdir(parents=True)
            coordinator.write_bytes((Path(owner.__file__).parents[1] / "coordinator.py").read_bytes())
            prepared = owner.prepare(repo, Path(TEMPLATE), candidate, stage, portable=True)
            binding = owner.dispatch_spec(root, stage, prepared["seal"], candidate, "scanner",
                                          root / "out", reference=repo)
            self.assertIn("--reference", binding["argv"])
            self.assertFalse((stage / "actual-1").exists())
            with self.assertRaises(ValueError):
                owner.dispatch_spec(root, stage, prepared["seal"], dict(candidate, git="0" * 40),
                                    "scanner", root / "out", reference=repo)
            wrong = coordinator.with_name(".dispatcher.lock")
            wrong.symlink_to(root / "unused")
            with self.assertRaises(owner.subprocess.CalledProcessError):
                owner.offline_layout(stage, coordinator, owner.digest(coordinator), repo / "artifacts/rust")
            wrong.unlink()
            script = stage / "run.py"
            script.write_text(script.read_text() + "\n# changed\n")
            sums = stage / "SHA256SUMS"
            sums.write_text("".join(f"{owner.digest(path)}  {path.name}\n" for path in
                                    sorted(stage.iterdir()) if path.name != "SHA256SUMS"))
            with self.assertRaisesRegex(ValueError, "exact reproducible"):
                owner.dispatch_spec(root, stage, owner.digest(sums), candidate, "scanner",
                                    root / "out", reference=repo)

    def test_portable_physical_fence_contends_on_legacy_inode(self):
        import fcntl

        with TemporaryDirectory() as folder:
            root = Path(folder)
            artifacts = root / "artifacts/rust"
            legacy = root / "scripts/rust-milestone-batches/.dispatcher.lock"
            private = root / "clean/scripts/rust-milestone-batches/.dispatcher.lock"
            artifacts.mkdir(parents=True)
            legacy.parent.mkdir(parents=True)
            private.parent.mkdir(parents=True)
            private.touch()
            with legacy.open("xb") as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                with self.assertRaisesRegex(ValueError, "already leased"):
                    with owner.lease.physical_fence(artifacts, private):
                        self.fail("running legacy context must exclude scanner")
            with owner.lease.physical_fence(artifacts, private):
                self.assertTrue(legacy.is_file())
            (artifacts / ".api202-positive-fifo-cleanup-required.json").touch()
            with self.assertRaisesRegex(ValueError, "retirement unresolved"):
                owner.lease.first_fence(artifacts, private)
