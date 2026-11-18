"""Real temporary-directory kernel locks; no applications or canonical leases."""
import hashlib
import importlib.util
import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("coordinator_under_lease_test", Path(__file__).with_name("coordinator.py"))
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)
leases = c._functional_leases
RECEIPTS = []
CHILD = r'''
import importlib.util,json,os,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location('coordinator_child',sys.argv[1]);c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
# Only temporary OS-lock tests bypass resource sizing; separate tests cover the
# production guard. No app or fixture can be started by this child.
c._functional_leases._resource_headroom=lambda:None
backend=None if sys.argv[3]=='exclusive'else sys.argv[3]
owner={'pid':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
try:
 with c.fixture_lease(Path(sys.argv[2]),backend)as admission:
  if sys.argv[5]!='-':
   with c.fixture_lease(Path(sys.argv[5]),backend)as support:
    if support is not None:support['cleanup_verified']=True
  if admission is not None:admission['cleanup_verified']=sys.argv[4]not in('dirty','hold-dirty','dirty-linger')
  print(json.dumps({'state':'acquired','owner':owner}),flush=True)
  if sys.argv[4]=='hard-exit':os._exit(17)
  if sys.argv[4]in('hold-dirty','hold-clean'):sys.stdin.readline()
 if sys.argv[4]=='dirty-linger':
  print(json.dumps({'state':'uncertain_record_written','owner':owner}),flush=True);sys.stdin.readline()
 code=0
except BlockingIOError:
 print(json.dumps({'state':'blocked_by_flock','owner':owner}),flush=True);code=3
except RuntimeError as error:
 print(json.dumps({'state':'blocked_by_interlock','owner':owner,'error':str(error)}),flush=True);code=4
if sys.argv[4]=='linger':sys.stdin.readline();code=0
sys.exit(code)
'''


def generation_absent(owner):
    path = Path("/proc") / str(owner["pid"]) / "stat"
    return not path.exists() or path.read_text().rsplit(")", 1)[1].split()[19] != owner["starttime"]


def durable(path, value):
    with path.open("x") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class FunctionalLeases(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="dispatcharr-kernel-lease-only-")
        self.root = Path(self.temporary.name)
        self.primary = self.root / ".dispatcher.lock"
        self.support = self.root / "support-PRIMARY"
        self.headroom = mock.patch.object(leases, "_resource_headroom", return_value=None)
        self.headroom.start()
        self.addCleanup(self.headroom.stop)
        self.addCleanup(self.temporary.cleanup)

    def argv(self, backend, mode="clean", support=None):
        return [sys.executable, "-B", "-c", CHILD, str(Path(c.__file__).resolve()),
                str(self.primary), backend or "exclusive", mode, str(support) if support else "-"]

    def attempt(self, label, backend, expected, mode="clean", support=None):
        argv = self.argv(backend, mode, support)
        began = time.monotonic()
        result = subprocess.run(argv, capture_output=True, text=True, timeout=8)
        RECEIPTS.append({"label": label, "argv": argv, "actual_exit": result.returncode,
                         "stdout": result.stdout, "stderr": result.stderr,
                         "seconds": time.monotonic() - began, "actual_wait_and_pipe_EOF": True,
                         "resource_guard_mocked_only_for_temporary_kernel_test": True})
        self.assertEqual(result.returncode, expected, RECEIPTS[-1])
        self.assertEqual(result.stderr, "")
        return json.loads(result.stdout)

    def coordinator_main(self, schedule):
        binding = self.root / "bindings.json"
        if not binding.exists():
            binding.write_text(json.dumps({"candidate": {}, "nodes": {},
                                           "functional_parallel": leases.PAIR_MODE}))
        with mock.patch.object(c, "HERE", self.root), \
             mock.patch.object(c, "inventory", return_value=[]), \
             mock.patch.object(c, "module", return_value=SimpleNamespace(candidate_ready=lambda value: True)), \
             mock.patch.object(c, "owner_readiness", return_value={}), \
             mock.patch.object(c, "schedule", schedule), \
             mock.patch.object(sys, "argv", ["coordinator.py", "--bindings", str(binding),
                                             "--output", str(self.root / "output")]):
            return c.main()

    def test_old_exclusive_blocks_both_shared_lanes(self):
        with c.fixture_lease(self.primary):
            self.attempt("old EX blocks Original SH", "Original", 3)
            self.attempt("old EX blocks Native SH", "Native", 3)

    def test_source_native_coexist_with_distinct_root_support_locks(self):
        with c.fixture_lease(self.primary, "Original") as root:
            pending = leases.uncertainty_path(self.primary, "Original").read_bytes()
            self.assertEqual(json.loads(pending)["state"], "pending")
            with c.fixture_lease(self.support, "Original") as support:
                self.attempt("Source Native coexist across Root/support", "Native", 0, support=self.support)
                self.assertEqual(leases.uncertainty_path(self.primary, "Original").read_bytes(), pending)
                self.assertFalse(leases.uncertainty_path(self.primary, "Native").exists())
                self.assertTrue(leases.uncertainty_path(self.support, "Original").exists())
                self.assertFalse(leases.uncertainty_path(self.support, "Native").exists())
                root["cleanup_verified"] = support["cleanup_verified"] = True
        self.assertFalse(any(leases.uncertainty_path(path, backend).exists()
                             for path in (self.primary, self.support) for backend in leases.BACKENDS))

    def test_same_backend_refuses(self):
        with c.fixture_lease(self.primary, "Original") as admission:
            self.attempt("same backend refuses", "Original", 3)
            admission["cleanup_verified"] = True

    def test_partial_acquisition_unwinds_before_failed_child_exits(self):
        child = None
        argv = self.argv("Original", "linger")
        began = time.monotonic()
        try:
            with c.fixture_lease(self.primary, "Original") as admission:
                child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, text=True)
                # The child only reports this after its failed lane attempt has
                # unwound the shared PRIMARY acquisition; keep it alive.
                first = child.stdout.readline()
                self.assertEqual(json.loads(first)["state"], "blocked_by_flock")
                self.assertIsNone(child.poll())
                admission["cleanup_verified"] = True
            self.attempt("partial failure releases PRIMARY with failed child alive", None, 0)
            stdout, stderr = child.communicate("finish\n", timeout=8)
            RECEIPTS.append({"label": "failed acquirer remains alive until release control", "argv": argv,
                             "actual_exit": child.returncode, "stdout": first + stdout, "stderr": stderr,
                             "seconds": time.monotonic() - began, "actual_wait_and_pipe_EOF": True})
            self.assertEqual(child.returncode, 0)
            self.assertEqual(stderr, "")
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=8)

    def test_dirty_owner_exit_blocks_reuse_and_coordinator_until_independent_cleanup(self):
        self.attempt("uncertain cleanup owner exits", "Original", 0, mode="dirty")
        marker = leases.uncertainty_path(self.primary, "Original")
        raw = marker.read_bytes()
        owner = json.loads(raw)["owner_generation"]
        self.assertTrue(generation_absent(owner))
        self.assertEqual(owner["boot_id"], leases._generation()["boot_id"])
        self.assertEqual(marker.stat().st_mode & 0o777, 0o600)
        self.attempt("dirty owner blocks shared reuse after exit", "Native", 4)
        schedule = mock.Mock(return_value={"full_batch_passed": True})
        with self.assertRaisesRegex(RuntimeError, "independently established"):
            self.coordinator_main(schedule)
        schedule.assert_not_called()
        receipt = {"passed": True, "scope": "temporary kernel locks only; no application resources existed",
                   "owner_generation": owner, "original_generation_absent": generation_absent(owner),
                   "marker_sha256": hashlib.sha256(raw).hexdigest()}
        durable(self.root / "independent-cleanup-receipt.json", receipt)
        marker.unlink()
        self.assertEqual(self.coordinator_main(schedule), 0)
        schedule.assert_called_once()

    def test_default_legacy_coordinator_stays_exclusive_even_with_optin_document(self):
        schedule = mock.Mock(return_value={"full_batch_passed": True})
        with c.fixture_lease(self.primary, "Native") as admission:
            with self.assertRaises(BlockingIOError):
                self.coordinator_main(schedule)
            admission["cleanup_verified"] = True
        schedule.assert_not_called()
        self.assertEqual(self.coordinator_main(schedule), 0)
        schedule.assert_called_once()

    def test_default_lease_does_not_require_pair_resources_or_create_lanes(self):
        with mock.patch.object(leases, "_resource_headroom", side_effect=AssertionError("legacy must not size pair")):
            with c.fixture_lease(self.primary) as admission:
                self.assertIsNone(admission)
        self.assertEqual([path.name for path in self.root.iterdir()], [".dispatcher.lock"])

    def test_default_off_and_reject_mixed_or_performance_plans(self):
        for value in (None, False):
            self.assertIsNone(c.functional_backend({"functional_parallel": value}))
        for backend, source, native in (("Original", ["new"], []), ("Native", [], ["affected"])):
            self.assertEqual(c.functional_backend({"functional_parallel": leases.PAIR_MODE,
                                                  "source_contexts": source, "native_contexts": native,
                                                  "base_binding": {"performance": False}}), backend)
        for source, native, performance in ((["x"], ["y"], False), ([], [], False), (["x"], [], True)):
            with self.assertRaises(ValueError):
                c.functional_backend({"functional_parallel": leases.PAIR_MODE,
                                      "source_contexts": source, "native_contexts": native,
                                      "base_binding": {"performance": performance}})

        with self.assertRaises(ValueError):
            c.functional_backend({"functional_parallel": leases.PAIR_MODE,
                                  "source_contexts": ["x"], "native_contexts": [], "base_binding": None})
        with self.assertRaises(ValueError):
            c.functional_backend({"functional_parallel": leases.PAIR_MODE,
                                  "source_contexts": "x", "native_contexts": [],
                                  "base_binding": {"performance": False}})

    def test_both_uncertain_backends_keep_separate_durable_owner_records(self):
        children = []
        try:
            for backend in leases.BACKENDS:
                argv = self.argv(backend, "hold-dirty")
                child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, text=True)
                first = child.stdout.readline()
                children.append((backend, child, argv, first))
                self.assertEqual(json.loads(first)["state"], "acquired")
            for backend, child, argv, first in children:
                stdout, stderr = child.communicate("finish\n", timeout=8)
                RECEIPTS.append({"label": "both active owners independently retain dirty marker " + backend,
                                 "argv": argv, "actual_exit": child.returncode,
                                 "stdout": first + stdout, "stderr": stderr, "actual_wait_and_pipe_EOF": True})
                self.assertEqual(child.returncode, 0)
                self.assertEqual(stderr, "")
            records = [json.loads(leases.uncertainty_path(self.primary, backend).read_text())
                       for backend in leases.BACKENDS]
            self.assertEqual({row["backend"] for row in records}, set(leases.BACKENDS))
            self.assertEqual(len({row["owner_generation"]["pid"] for row in records}), 2)
            self.assertTrue(all(generation_absent(row["owner_generation"]) for row in records))
            self.attempt("both dirty records block exclusive admission", None, 4)
        finally:
            for _, child, _, _ in children:
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=8)

    def test_sigkill_pending_owner_blocks_shared_and_coordinator_after_kernel_unlock(self):
        argv = self.argv("Original", "hold-clean")
        child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        first = ""
        try:
            first = child.stdout.readline()
            owner = json.loads(first)["owner"]
            self.assertEqual(owner["pid"], child.pid)
            self.assertFalse(generation_absent(owner))
            marker = leases.uncertainty_path(self.primary, "Original")
            pending = json.loads(marker.read_text())
            self.assertEqual(pending["state"], "pending")
            self.assertEqual(pending["owner_generation"], owner)
            child.kill()
            stdout, stderr = child.communicate(timeout=8)
            RECEIPTS.append({"label": "SIGKILL after durable pending before finally", "argv": argv,
                             "actual_exit": child.returncode, "stdout": first + stdout, "stderr": stderr,
                             "actual_wait_and_pipe_EOF": True, "signal": signal.SIGKILL,
                             "owned_generation_matched_before_signal": True})
            self.assertEqual(child.returncode, -signal.SIGKILL)
            self.assertEqual(stderr, "")
            self.assertTrue(generation_absent(owner))
            # Kernel flock really was released. Only durable pending state now
            # prevents reuse through the actual coordinator/helper path.
            with self.primary.open("a") as raw:
                leases.fcntl.flock(raw, leases.fcntl.LOCK_EX | leases.fcntl.LOCK_NB)
            self.assertEqual(json.loads(marker.read_text()), pending)
            self.attempt("dead pending SIGKILL owner blocks Native", "Native", 4)
            self.attempt("dead pending SIGKILL owner blocks same backend", "Original", 4)
            schedule = mock.Mock(return_value={"full_batch_passed": True})
            with self.assertRaisesRegex(RuntimeError, "independently established"):
                self.coordinator_main(schedule)
            schedule.assert_not_called()
            durable(self.root / "SIGKILL-independent-cleanup-receipt.json", {
                "passed": True, "scope": "temporary kernel locks only; no application resources existed",
                "owner_generation": owner, "original_generation_absent": True,
                "pending_marker_sha256": hashlib.sha256(marker.read_bytes()).hexdigest()})
            marker.unlink()
            leases._fsync_directory(marker.parent)
            self.assertEqual(self.coordinator_main(schedule), 0)
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=8)

    def test_os_exit_cannot_erase_pending_owner_without_finally(self):
        self.attempt("os._exit bypasses finally with cleanup flag already True", "Original", 17, mode="hard-exit")
        marker = leases.uncertainty_path(self.primary, "Original")
        pending = json.loads(marker.read_text())
        self.assertEqual(pending["state"], "pending")
        self.assertTrue(generation_absent(pending["owner_generation"]))
        self.attempt("os._exit pending blocks Native", "Native", 4)
        self.attempt("os._exit pending blocks exclusive", None, 4)

    def test_explicit_uncertainty_blocks_even_while_its_owner_still_lives(self):
        argv = self.argv("Original", "dirty-linger")
        child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            first = child.stdout.readline()
            second = child.stdout.readline()
            owner = json.loads(second)["owner"]
            self.assertEqual(json.loads(second)["state"], "uncertain_record_written")
            self.assertFalse(generation_absent(owner))
            self.assertEqual(json.loads(leases.uncertainty_path(self.primary, "Original").read_text())["state"], "uncertain")
            self.attempt("explicit uncertainty blocks Native despite live owner", "Native", 4)
            self.attempt("explicit uncertainty blocks own backend despite live owner", "Original", 4)
            self.attempt("explicit uncertainty blocks exclusive despite live owner", None, 4)
            stdout, stderr = child.communicate("finish\n", timeout=8)
            RECEIPTS.append({"label": "uncertain owner kept alive after releasing kernel lease", "argv": argv,
                             "actual_exit": child.returncode, "stdout": first + second + stdout,
                             "stderr": stderr, "actual_wait_and_pipe_EOF": True})
            self.assertEqual(child.returncode, 0)
            self.assertEqual(stderr, "")
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=8)

    def test_clean_exit_never_removes_a_marker_with_changed_lease_identity(self):
        marker = leases.uncertainty_path(self.primary, "Original")
        with self.assertRaisesRegex(RuntimeError, "marker changed"):
            with c.fixture_lease(self.primary, "Original") as admission:
                changed = json.loads(marker.read_text())
                changed["lease_id"] = "different-owner-lease"
                marker.write_text(json.dumps(changed))
                admission["cleanup_verified"] = True
        self.assertEqual(json.loads(marker.read_text()), changed)
        self.assertFalse(generation_absent(changed["owner_generation"]))
        # A live PID without its old kernel lane is stale, not joinable.
        self.attempt("live stale pending without owned lane blocks Native", "Native", 4)
        self.attempt("changed pending owner blocks exclusive", None, 4)

    def test_live_pending_with_mismatched_process_generation_is_not_joinable(self):
        marker = leases.uncertainty_path(self.primary, "Original")
        with c.fixture_lease(self.primary, "Original") as admission:
            original = marker.read_bytes()
            changed = json.loads(original)
            changed["owner_generation"]["starttime"] = "0"
            marker.write_text(json.dumps(changed))
            try:
                self.attempt("PID alive but generation mismatched blocks Native", "Native", 4)
            finally:
                marker.write_bytes(original)
                admission["cleanup_verified"] = True

    def test_earlier_single_uncertainty_marker_still_blocks_exclusive(self):
        self.primary.with_name(self.primary.name + ".functional-cleanup-required.json").write_text("{}")
        self.attempt("earlier single uncertainty marker blocks quiet", None, 4)


class ResourceGuard(unittest.TestCase):
    def test_pair_headroom_guard_accepts_boundary_and_refuses_low_memory_or_cpu(self):
        for available, cpus, okay in ((leases.MIN_AVAILABLE_BYTES, 8, True),
                                       (leases.MIN_AVAILABLE_BYTES - 1024, 8, False),
                                       (leases.MIN_AVAILABLE_BYTES, 7, False)):
            text = "MemAvailable: " + str(available // 1024) + " kB\n"
            with mock.patch.object(Path, "read_text", return_value=text), \
                 mock.patch.object(os, "sched_getaffinity", return_value=set(range(cpus))):
                if okay:
                    leases._resource_headroom()
                else:
                    with self.assertRaises(RuntimeError):
                        leases._resource_headroom()


def tearDownModule():
    if os.environ.get("LEASE_TEST_RECEIPT"):
        Path(os.environ["LEASE_TEST_RECEIPT"]).write_text(json.dumps({
            "actual_child_commands": RECEIPTS, "canonical_PRIMARY_touched": False,
            "resource_guard_mocked_for_kernel_tests_and_separately_unit_checked": True,
            "real_fixture_isolation_qualified": False}, indent=2) + "\n")


if __name__ == "__main__":
    result = unittest.main(exit=False)
    if os.environ.get("LEASE_TEST_RECEIPT"):
        Path(os.environ["LEASE_TEST_RECEIPT"]).write_text(json.dumps({
            "passed": result.result.wasSuccessful(), "tests_run": result.result.testsRun,
            "actual_child_commands": RECEIPTS, "canonical_PRIMARY_touched": False,
            "resource_guard_mocked_for_kernel_tests_and_separately_unit_checked": True,
            "real_fixture_isolation_qualified": False}, indent=2) + "\n")
    sys.exit(0 if result.result.wasSuccessful() else 1)
