"""Exercise global scheduling failures and resource boundaries without apps."""
import importlib.util
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock
from types import SimpleNamespace

SPEC = importlib.util.spec_from_file_location("coordinator", Path(__file__).with_name("coordinator.py"))
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)


class Scheduling(unittest.TestCase):
    def run_graph(self, nodes, failures=(), uncertain=()):
        active, observed = [], []
        lock = threading.Lock()

        def work(node, bound, directory, candidate):
            with lock:
                active.append(node)
                observed.append(list(active))
            time.sleep(0.02)
            with lock:
                active.remove(node)
            return {"id": node["id"], "status": "failed" if node["id"] in failures else "passed",
                    "cleanup_verified": node["id"] not in uncertain}

        # Real bindings are checked independently; these tests exercise the
        # scheduler with a mock worker, never a shell or production evaluator.
        digest = c.hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        binding = {"argv": ["unused"], "evaluator_argv": ["unused"], "cleanup_argv": ["unused"],
                   "seconds": 1, "cleanup_seconds": 1, "output_bytes": 1024,
                   "input_sha256": {str(Path(__file__)): digest}}
        with tempfile.TemporaryDirectory() as directory:
            report = c.schedule(nodes, {n["id"]: binding for n in nodes}, Path(directory) / "run", {}, work)
        return report, observed

    def node(self, key, deps=(), heavy=False, resources=()):
        return {"id": key, "depends_on": list(deps), "heavy": heavy, "resources": list(resources)}

    def test_global_three_and_exclusive_heavy(self):
        nodes = [self.node(str(i)) for i in range(8)] + [self.node("heavy", heavy=True)]
        report, observed = self.run_graph(nodes)
        self.assertTrue(report["full_batch_passed"])
        self.assertEqual(max(map(len, observed)), 3)
        self.assertTrue(all(len(group) == 1 for group in observed if any(n["heavy"] for n in group)))

    def test_shared_fixture_serializes(self):
        _, observed = self.run_graph([self.node(str(i), resources=["db"]) for i in range(4)])
        self.assertTrue(all(len(group) == 1 for group in observed))

    def test_failure_blocks_only_descendant(self):
        report, _ = self.run_graph([self.node("a"), self.node("child", ["a"]), self.node("independent")], failures=["a"])
        self.assertEqual(report["results"]["child"]["status"], "blocked")
        self.assertEqual(report["results"]["independent"]["status"], "passed")
        self.assertFalse(report["full_batch_passed"])

    def test_uncertain_cleanup_stops_new_dispatch(self):
        report, _ = self.run_graph([self.node("a", heavy=True), self.node("b")], uncertain=["a"])
        self.assertTrue(report["cleanup_uncertain"])
        self.assertEqual(report["results"]["b"]["status"], "blocked")

    def test_external_unaccepted_dependency_cannot_pass(self):
        report, _ = self.run_graph([self.node("a", ["external/missing"])])
        self.assertEqual(report["results"]["a"]["status"], "blocked")

    def test_cycle_rejected(self):
        with self.assertRaisesRegex(ValueError, "cyclic"):
            self.run_graph([self.node("a", ["b"]), self.node("b", ["a"])])

    def test_retained_reference_satisfies_dependency_without_execution(self):
        reference = self.node("reference")
        child = self.node("child", ["reference"])
        digest = c.hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        bound = {"argv": ["unused"], "evaluator_argv": ["unused"], "cleanup_argv": ["unused"],
                 "seconds": 1, "cleanup_seconds": 1, "output_bytes": 1024,
                 "input_sha256": {str(Path(__file__)): digest}}
        called = []
        def work(node, *args):
            called.append(node["id"])
            return {"id": node["id"], "status": "passed", "cleanup_verified": True}
        with tempfile.TemporaryDirectory() as directory:
            result = c.schedule([reference, child], {"child": bound}, Path(directory) / "run", {}, work,
                                authorities={"reference": {"id": "reference", "status": "retained_source_authority", "satisfies_dependency": True}})
        self.assertEqual(called, ["child"])
        self.assertEqual(result["results"]["reference"]["status"], "retained_source_authority")

    def test_arbitrary_argv_cannot_bypass_blocked_owner(self):
        owner = SimpleNamespace(resolve_node=lambda *args: {"ready": False, "reasons": ["unimplemented"]},
                                load=lambda: ({}, {}, {}))
        with mock.patch.object(c, "module", return_value=owner):
            with self.assertRaisesRegex(ValueError, "owner blocked"):
                c.owner_readiness([{"id": "package/a", "owner": "m235"}], {"nodes": {"package/a": {"argv": ["pretend-pass"]}}})

    def test_retained_source_cannot_also_be_dispatched(self):
        owner = SimpleNamespace(resolve_node=lambda *args: {"status": "retained_source_authority"},
                                load=lambda: ({}, {}, {}))
        with mock.patch.object(c, "module", return_value=owner):
            with self.assertRaisesRegex(ValueError, "cannot also be dispatched"):
                c.owner_readiness([{"id": "package/source", "owner": "m235"}], {"nodes": {"package/source": {"argv": ["replay"]}}})

    def run_pair(self, age=0, failures=(), missing_native=False, extra=(), native_deps=()):
        import datetime
        calls, active, observed = [], [], []
        lock = threading.Lock()
        digest = c.hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        source = self.node('source', resources=['source-db'])
        native = self.node('native', ['source', *native_deps], resources=['native-db'])
        native.update(same_global_lease_as='source', reference_max_age_seconds=900)
        nodes = [source, *extra, native]
        bound = {"argv": ["unused"], "evaluator_argv": ["unused"], "cleanup_argv": ["unused"],
                 "seconds": 1, "cleanup_seconds": 1, "output_bytes": 1024,
                 "input_sha256": {str(Path(__file__)): digest}}
        with tempfile.TemporaryDirectory() as directory:
            reference = Path(directory) / 'reference.json'
            bindings = {n['id']: dict(bound) for n in nodes if not (missing_native and n['id'] == 'native')}
            if not missing_native:bindings['native']['reference_clock_file'] = str(reference)
            def work(node, *args):
                with lock:
                    calls.append(node['id']);active.append(node['id']);observed.append(list(active))
                if node['id'] == 'source':
                    reference.write_text(c.json.dumps({'fixture_clock': datetime.datetime.fromtimestamp(1000, datetime.timezone.utc).isoformat()}))
                time.sleep(0.02)
                with lock:active.remove(node['id'])
                return {'id':node['id'], 'status':'failed' if node['id'] in failures else 'passed', 'cleanup_verified':True}
            report = c.schedule(nodes, bindings, Path(directory) / 'run', {}, work, wall_clock=lambda:1000+age)
        report['_observed_active'] = observed
        return report, calls

    def test_paired_reference_and_other_callers_keep_global_three(self):
        report, _ = self.run_pair(extra=[self.node('other'+str(i)) for i in range(8)])
        self.assertEqual(max(map(len,report['_observed_active'])),3)
        self.assertTrue(all(not ('source' in group and 'native' in group) for group in report['_observed_active']))
        self.assertTrue(report['full_batch_passed'])

    def test_paired_reference_keeps_union_of_resources_until_native_finishes(self):
        competing = self.node('competing', resources=['native-db'])
        report, calls = self.run_pair(extra=[competing])
        self.assertEqual(calls, ['source', 'native', 'competing'])
        self.assertTrue(report['full_batch_passed'])
        self.assertEqual(report['results']['source']['paired_lease'], report['results']['native']['paired_lease'])

    def test_paired_reference_waits_for_native_other_dependencies_before_source(self):
        report, calls = self.run_pair(extra=[self.node('setup')], native_deps=['setup'])
        self.assertEqual(calls, ['setup', 'source', 'native'])
        self.assertTrue(report['full_batch_passed'])

    def test_paired_reference_missing_native_never_runs_source(self):
        report, calls = self.run_pair(missing_native=True)
        self.assertEqual(calls, [])
        self.assertEqual(report['results']['source']['status'], 'blocked')
        self.assertEqual(report['results']['native']['status'], 'blocked')

    def test_paired_reference_failure_never_runs_native_or_retries_source(self):
        report, calls = self.run_pair(failures=['source'])
        self.assertEqual(calls, ['source'])
        self.assertEqual(report['results']['native']['status'], 'blocked')

    def test_paired_reference_checks_actual_clock_age_before_native(self):
        for age, expected in [(0,'passed'), (900,'passed'), (900.01,'blocked'), (-1,'blocked')]:
            report, calls = self.run_pair(age=age)
            self.assertEqual(report['results']['native']['status'], expected, age)
            self.assertEqual(calls.count('source'), 1)
            self.assertEqual('native' in calls, expected=='passed')

    def test_utc_guard_runs_before_node_output_or_worker_at_boundary(self):
        digest = c.hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        bound = {"argv": ["unused"], "evaluator_argv": ["unused"], "cleanup_argv": ["unused"],
                 "seconds": 1, "cleanup_seconds": 1, "output_bytes": 1024,
                 "input_sha256": {str(Path(__file__)): digest}}
        for minute in [0,35,36,59]:
            node = self.node('guide');node['UTC_minute_window']=[0,35];calls=[]
            with tempfile.TemporaryDirectory() as directory:
                def work(node,bound,path,candidate):
                    calls.append(node['id']);path.mkdir();return {'id':node['id'],'status':'passed','cleanup_verified':True}
                output=Path(directory)/'run'
                report=c.schedule([node],{'guide':bound},output,{},work,utc_minute=lambda:minute)
                self.assertEqual(bool(calls), minute<=35)
                self.assertEqual(bool(list(output.glob('guide-*'))), minute<=35)
                self.assertEqual(report['results']['guide']['status'], 'passed' if minute<=35 else 'blocked')

    def test_inventory_retains_actual_owner_clock_and_pair_constraints(self):
        rows={n['id']:n for n in c.inventory()}
        self.assertEqual(rows['package/guide-grid-packaged']['UTC_minute_window'],[0,35])
        self.assertEqual(rows['package/complete-refresh']['same_global_lease_as'],'package/complete-refresh-original')
        self.assertEqual(rows['package/complete-refresh']['reference_max_age_seconds'],900)

    def test_exit_zero_is_not_semantic_acceptance(self):
        import sys
        with tempfile.TemporaryDirectory() as directory:
            bound = {"argv": [sys.executable, "-c", "pass"],
                     "cleanup_argv": [sys.executable, "-c", 'print(\'{"exact_owned_absence":true}\')'],
                     "evaluator_argv": [sys.executable, "-c", 'print("{}")'],
                     "seconds": 2, "cleanup_seconds": 2, "output_bytes": 1024}
            result = c.execute(self.node("a"), bound, Path(directory) / "run", {})
            self.assertEqual(result["status"], "failed")
            self.assertTrue(result["cleanup_verified"])

    def test_timeout_still_runs_cleanup(self):
        import sys
        with tempfile.TemporaryDirectory() as directory:
            bound = {"argv": [sys.executable, "-c", "import time; time.sleep(10)"],
                     "cleanup_argv": [sys.executable, "-c", 'print(\'{"exact_owned_absence":true}\')'],
                     "evaluator_argv": [sys.executable, "-c", 'print("{}")'],
                     "seconds": 1, "cleanup_seconds": 2, "output_bytes": 1024}
            result = c.execute(self.node("a"), bound, Path(directory) / "run", {})
            self.assertEqual(result["execution"]["failure"], "timeout")
            self.assertLess(result["execution"]["actual_exit"], 0)
            self.assertTrue(result["cleanup_verified"])
            self.assertEqual(result["status"], "failed")

    def test_successful_controller_cannot_leave_its_process_group_running(self):
        import os
        import signal
        import sys
        with tempfile.TemporaryDirectory() as directory:
            bound = {"argv": [sys.executable, "-c",
                "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(20)'], stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); print(p.pid,flush=True)"],
                "seconds": 2, "output_bytes": 1024}
            child = None
            try:
                result = c.command(bound["argv"], bound, Path(directory), "orphan", 2)
                child = int(Path(result["stdout"]).read_text())
                status = Path('/proc') / str(child) / 'stat'
                running = status.exists() and status.read_text().rsplit(')', 1)[1].split()[0] != 'Z'
                self.assertFalse(running, "resource lease cannot retire while controller's original process group is still running")
                self.assertEqual(result['failure'], 'controller left process-group descendants')
                self.assertTrue(result['process_group_settled'])
                self.assertTrue(any(row['pid'] == child for row in result['reaped_descendants']))
            finally:
                if child is not None:
                    try: os.kill(child, signal.SIGKILL)
                    except ProcessLookupError: pass

    def test_descendant_ignoring_term_is_killed_and_reaped(self):
        import os
        import signal
        import sys
        child_code = "import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print(os.getpid(),flush=True); time.sleep(20)"
        leader_code = "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]],stdout=subprocess.PIPE); print(p.stdout.readline().decode().strip(),flush=True)"
        with tempfile.TemporaryDirectory() as directory:
            bound = {"argv": [sys.executable, '-c', leader_code, child_code], "seconds": 2, "output_bytes": 1024}
            child = None
            try:
                result = c.command(bound['argv'], bound, Path(directory), 'ignores-term', 2)
                child = int(Path(result['stdout']).read_text())
                self.assertEqual(result['failure'], 'controller left process-group descendants')
                self.assertTrue(result['process_group_settled'])
                self.assertGreaterEqual(result['seconds'], 5)
                self.assertLess(result['seconds'], 8)
                self.assertTrue(any(row['pid'] == child and os.WIFSIGNALED(row['wait_status']) and os.WTERMSIG(row['wait_status']) == signal.SIGKILL for row in result['reaped_descendants']))
            finally:
                if child is not None:
                    try: os.kill(child, signal.SIGKILL)
                    except ProcessLookupError: pass

    def test_fast_success_collects_eof_and_bounded_output(self):
        import sys
        with tempfile.TemporaryDirectory() as directory:
            bound = {"argv": [sys.executable, "-c", "print('ok')"], "seconds": 2, "output_bytes": 1024}
            result = c.command(bound['argv'], bound, Path(directory), 'success', 2)
            self.assertIsNone(result['failure'])
            self.assertEqual(result['actual_exit'], 0)
            self.assertTrue(result['process_group_settled'])
            self.assertTrue(result['stdout_EOF'] and result['stderr_EOF'])
            self.assertEqual(Path(result['stdout']).read_text(), 'ok\n')

    def test_inventory_keeps_m6_performance_required(self):
        self.assertTrue(any(n['id'].startswith('performance/') and 'M6' in n['id'] for n in c.inventory()),
                        "M6 owner declares a performance requirement that cannot disappear from global acceptance")

    def test_output_budget_is_checked_after_process_exit(self):
        import sys
        with tempfile.TemporaryDirectory() as directory:
            bound = {"argv": [sys.executable, "-c", "print('x'*4096)"],
                     "seconds": 2, "output_bytes": 32}
            result = c.command(bound["argv"], bound, Path(directory), "output", 2)
            self.assertEqual(result["failure"], "output budget exceeded")
            self.assertLessEqual(Path(result["stdout"]).stat().st_size + Path(result["stderr"]).stat().st_size, 32)
            self.assertTrue(result["process_group_settled"])


if __name__ == "__main__":
    unittest.main()
