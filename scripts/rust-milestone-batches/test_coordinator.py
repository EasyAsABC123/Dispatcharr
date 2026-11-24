"""Exercise global scheduling failures and resource boundaries without apps."""
import importlib.util
import json
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


class RetainedM235Custody(unittest.TestCase):
    def test_uses_existing_default_without_an_explicit_repo(self):
        default = Path("/reviewed/repo")
        self.assertIs(c.retained_m235_repo({}, default), default)

    def test_explicit_repo_must_own_the_bound_package_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            repo = root / "repo"
            repo.mkdir()
            manifest = repo / "manifest.json"
            manifest.write_text("{}")
            archive = repo / "source.tar"
            archive.touch()
            binding = {"repo": str(repo), "candidate": {
                "manifest": {"path": str(manifest)}, "archive": {"path": str(archive)}}}
            self.assertEqual(c.retained_m235_repo(binding, root), repo)
            alias = repo / "alias.json"
            alias.symlink_to(manifest)
            for target in (str(alias), str(root)):
                binding["candidate"]["manifest"]["path"] = target
                with self.assertRaisesRegex(ValueError, "differs from bound package custody"):
                    c.retained_m235_repo(binding, root)
            with self.assertRaisesRegex(ValueError, "absolute existing"):
                c.retained_m235_repo(binding | {"repo": "."}, root)


class BindingFragments(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.output = self.root / "results"
        self.candidate = {"git": "fixed", "image_id": "current"}
        self.nodes = [{"id": name} for name in ("first", "second", "source/retained")]
        self.owner = SimpleNamespace(candidate_ready=mock.Mock(return_value=True))
        self.load_owner = mock.patch.object(c, "module", return_value=self.owner)
        self.validate = mock.patch.object(c, "validate")
        self.readiness = mock.patch.object(
            c, "owner_readiness", side_effect=lambda nodes, document, output: document.get("authorities", {}))
        for patcher in (self.load_owner, self.validate, self.readiness):
            patcher.start()
            self.addCleanup(patcher.stop)

    def write(self, name, nodes, **fields):
        path = self.root / name
        path.write_text(json.dumps({"candidate": self.candidate, "nodes": nodes, **fields}))
        return path

    def test_handoff_contexts_keep_each_owner_and_shared_execution(self):
        first = self.write("first.json", {"first": {"argv": ["one"]}}, owner_bindings={"m235": {"output_root": "one"}})
        second = self.write("second.json", {"first": {"argv": ["one"]}, "second": {"argv": ["two"]}},
                            owner_bindings={"m235": {"output_root": "two"}},
                            authorities={"source/retained": {"status": "retained_source_authority"}})
        candidate, merged, authorities = c.load_binding_fragments([first, second], self.nodes, self.output)
        self.assertEqual(candidate, self.candidate)
        self.assertEqual(merged, {"first": {"argv": ["one"]}, "second": {"argv": ["two"]}})
        self.assertEqual(list(authorities), ["source/retained"])
        self.assertEqual([call.args[1]["owner_bindings"]["m235"]["output_root"]
                          for call in c.owner_readiness.call_args_list], ["one", "two"])
        self.assertFalse(self.output.exists())

    def test_reject_stale_candidate_before_validating_any_owner(self):
        first = self.write("first.json", {"first": {}})
        second = self.write("second.json", {"second": {}}, candidate={"git": "previous"})
        with self.assertRaisesRegex(ValueError, "differs from the common candidate"):
            c.load_binding_fragments([first, second], self.nodes, self.output)
        c.owner_readiness.assert_not_called()

    def test_reject_conflicting_duplicate_execution(self):
        first = self.write("first.json", {"first": {"argv": ["one"]}})
        second = self.write("second.json", {"first": {"argv": ["elsewhere"]}})
        with self.assertRaisesRegex(ValueError, "conflicting execution bindings: first"):
            c.load_binding_fragments([first, second], self.nodes, self.output)

    def test_reject_source_dispatched_by_one_owner_and_retained_by_another(self):
        first = self.write("first.json", {"source/retained": {}})
        second = self.write("second.json", {}, authorities={"source/retained": {}})
        with self.assertRaisesRegex(ValueError, "retained Source cannot also be dispatched"):
            c.load_binding_fragments([first, second], self.nodes, self.output)

    def test_readiness_keeps_partial_probes_out_of_full_workflows(self):
        nodes = [*self.nodes, {"id": "m6/default-job-graph"}]
        self.assertEqual(c.binding_coverage(nodes, {"first": {}, "m6/default-job-graph": {}}, {"source/retained": {}}), {
            "required_rows": 4,
            "execution_bindings": ["first"],
            "retained_source_authority": ["source/retained"],
            "partial_probes": ["m6/default-job-graph"],
            "unbound_rows": ["second"],
        })


class Scheduling(unittest.TestCase):
    def run_graph(self, nodes, failures=(), uncertain=(), overrides=None):
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
            report = c.schedule(nodes, {n["id"]: {**binding, **(overrides or {}).get(n["id"], {})} for n in nodes}, Path(directory) / "run", {}, work)
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

    def test_binding_can_upgrade_heavy_but_cannot_downgrade(self):
        nodes = [self.node("ordinary"), self.node("upgrade"), self.node("already-heavy", heavy=True)]
        _, observed = self.run_graph(nodes, overrides={"upgrade": {"exclusive_heavy": True}, "already-heavy": {"exclusive_heavy": False}})
        for group in observed:
            if any(n["id"] in ("upgrade", "already-heavy") for n in group):
                self.assertEqual(len(group), 1)
                self.assertTrue(group[0]["heavy"])

    def test_binding_resource_upgrade_serializes_original_resource_user(self):
        _, observed = self.run_graph([self.node("a", resources=["db"]), self.node("b")],
                                     overrides={"b": {"exclusive_resources": ["db"]}})
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

    def test_release_workflow_waits_for_datastore_scope(self):
        nodes = c.inventory()
        workflow = "root/M9-workflow-parity"
        datastore = "external/M9/datastore-scope"
        # Model the other completed release prerequisites using the actual
        # inventory. The scheduler must still reject an unqualified datastore.
        prior = {
            node["id"]: {"status": "passed"}
            for node in nodes
            if node["id"] not in (workflow, datastore)
        }
        binding = {
            "argv": ["unused"], "evaluator_argv": ["unused"], "cleanup_argv": ["unused"],
            "seconds": 1, "cleanup_seconds": 1, "output_bytes": 1024,
            "input_sha256": {str(Path(__file__)): c.hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        }
        for status in ("blocked", "failed", "passed"):
            with self.subTest(datastore=status), tempfile.TemporaryDirectory() as directory:
                prior[datastore] = {"status": status}
                dispatched = []

                def work(node, *_):
                    dispatched.append(node["id"])
                    return {"id": node["id"], "status": "passed", "cleanup_verified": True}

                report = c.schedule(nodes, {workflow: binding}, Path(directory) / "run", {},
                                    worker=work, authorities=prior)
                self.assertEqual(dispatched, [workflow] if status == "passed" else [])
                self.assertEqual(report["results"][workflow]["status"], "passed" if status == "passed" else "blocked")

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
