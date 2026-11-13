"""Socket-free controller/startup/closure controls. No Docker subprocess calls."""
import ast
import json
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

class ControllerControls(unittest.TestCase):
    def setUp(self):
        self.code=(ROOT/'controller/run.py').read_text();self.tree=ast.parse(self.code)
        self.fixture=(ROOT/'controller/source_fixture.py').read_text();self.ftree=ast.parse(self.fixture)
    def test_fresh_namespace(self):
        self.assertIn("db=database('rust_probe_settings32_'+nonce)",self.code)
        self.assertIn("project='dispatcharr-settings32-source-'+nonce",self.code)
        self.assertNotIn('rust_probe_notifforms_',self.code)
    def test_reviewed_once_before_mutation(self):
        main=next(n for n in self.tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
        text=ast.get_source_segment(self.code,main)
        self.assertLess(text.index('ReviewedSealHashBinding'),text.index('out.mkdir('))
        self.assertIn('exist_ok=False',text)
        self.assertLess(text.index('args.execute_reviewed_seal is None'),text.index('out.mkdir('))
    def test_internal_network_no_ports(self):
        self.assertIn("'internal':True",self.code)
        self.assertNotIn("'ports':",self.code)
        self.assertNotIn("'network_mode':",self.code)
    def test_exact_pinned_image(self):self.assertIn('sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f',self.code)
    def test_exact_mount_root(self):
        self.assertIn("str(HERE.parent)+':/settings-root:ro'",self.code)
        self.assertIn("ROOT = Path('/settings-root')",self.fixture)
    def test_source_readonly_mount(self):self.assertIn("str(source)+':/app:ro'",self.code)
    def test_start_shell_real_marker(self):
        self.assertIn("'manage.py','shell','-c'",self.code)
        self.assertIn("should_skip_initialization() is True",self.fixture)
        self.assertNotIn('django.setup(',self.fixture)
    def test_migrate_original_command(self):
        start=(ROOT/'controller/start.sh').read_text()
        self.assertIn('python manage.py migrate --noinput',start)
        self.assertNotIn('celery -A',start)
        self.assertNotIn('uwsgi --',start)
    def test_fixed_budgets(self):
        self.assertIn('1200-(time.monotonic()-began)',self.code)
        self.assertIn('cleanup_started+90',self.code)
        self.assertIn('timeout=360)',self.code)
        self.assertIn("<=90",self.code)
    def test_cleanup_precedes_postwork(self):
        self.assertLess(self.code.index('        begin_cleanup()\n        actual='),self.code.index("qualified=compare(actual,PLAN)"))
    def test_bounded_neutral_stop(self):
        stop=(ROOT/'controller/bounded_stop.py').read_text()
        self.assertIn('lambda:None',stop.replace(' ',''))
        self.assertIn('bounded_stop(',self.code)
    def test_observed_host_pid_required(self):self.assertIn("require(identity is not None,'DockerTopPIDHostStartNotObserved')",self.code)
    def test_all_project_resources(self):
        self.assertIn("['container','network','volume']",self.code)
        self.assertIn("['--all','--no-trunc']if kind=='container'",self.code)
        self.assertIn('ExactOwnedContainerAbsent',self.code)
    def test_default_before_after_fullscope(self):
        self.assertIn("before=audit('before')",self.code);self.assertIn("after=audit('after')",self.code)
        self.assertIn("before['public62']==after['public62']",self.code)
    def test_actual_backend_absence(self):self.assertIn("require(values==[],'ActualOwnedBackendAbsenceAfterSourceExit')",self.code)
    def test_actual_response_and_exception(self):
        self.assertIn('client.raise_request_exception = False',self.fixture)
        self.assertIn("getattr(response, 'exc_info', None)",self.fixture)
        self.assertIn('outer_fixture_catch',self.fixture)
    def test_capture_only_false_preserved(self):self.assertIn("'passed': False, 'recorder_is_capture_only': True",self.fixture)
    def test_attempt_before_send(self):self.assertLess(self.fixture.index("report['actual_request_count'] += 1"),self.fixture.index('response = client.generic('))
    def test_complete_input_preflight(self):
        self.assertIn('CompleteSealedInputInventory',self.code)
        self.assertIn('Actual55DependencyClosure',self.code)
        self.assertIn('ActualNeverStartedDependencyProof',self.code)
        self.assertIn('PinnedOriginalFullArchive',self.code)
    def test_no_other_live_service(self):
        self.assertIn("'services':{'baseline':",self.code)
        self.assertNotIn("'app':{",self.code)
    def test_clean_external_git_environment(self):self.assertIn("'GIT_'",self.code)

if __name__=='__main__':unittest.main()
