"""Local synthetic tooling/pre-output guards; no Docker commands or sockets."""
import copy,json,os,subprocess,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import tooling_guard as g
class Controls(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);d=r/'bin';d.mkdir();docker=d/'docker';docker.write_text('synthetic docker');docker.chmod(0o700);config=r/'config';plugin=config/'cli-plugins/docker-compose';plugin.parent.mkdir(parents=True);plugin.write_text('synthetic compose');plugin.chmod(0o700);script=r/'environment.sh';script.write_text('synthetic sourced exports')
        self.env={'CARGO_HOME':str(r/'cargo'),'RUSTUP_HOME':str(r/'rustup'),'RUSTUP_TOOLCHAIN':'stable','DOCKER_CONFIG':str(config),'DOCKER_HOST':g.HOST,'PATH':str(d)}
        self.meta={'selected_environment':copy.deepcopy(self.env),'expected_DOCKER_HOST':g.HOST,'environment_script':{'path':str(script),'sha256':g.sha(script)},'docker':{'path':str(docker),'sha256':g.sha(docker)},'compose':{'path':str(plugin),'sha256':g.sha(plugin)}};self.calls=[]
    def tearDown(self):self.t.cleanup()
    def runner(self,args,**kw):
        self.calls.append((args,kw));value='Docker Compose version v2.39.1'if args[1]=='compose'else'28.0.1';return subprocess.CompletedProcess(args,0,value+'\n','')
    def run_guard(self,runner=None,socket=True):
        with patch.object(g,'socket_ready',return_value=socket):return g.preflight(self.env,self.meta,runner or self.runner)
    def rejected(self,change):
        change()
        with self.assertRaises(g.ToolingRefusal):self.run_guard()
        self.assertFalse(self.calls)
    def test_exact_readonly_commands_controlled_environment(self):
        self.assertTrue(self.run_guard()['passed']);self.assertEqual(len(self.calls),3)
        self.assertEqual([x[0][1]for x in self.calls],['version','compose','info'])
        for _,kw in self.calls:self.assertIs(kw['env'],self.env);self.assertEqual(kw['timeout'],5)
    def test_absent_selected_environment(self):self.rejected(lambda:self.env.pop('PATH'))
    def test_wrong_PATH(self):self.rejected(lambda:self.env.update(PATH='/absent'))
    def test_unresolvable_PATH_even_if_changed_in_both_dicts(self):
        self.env['PATH']='/absent';self.meta['selected_environment']['PATH']='/absent'
        with self.assertRaisesRegex(g.ToolingRefusal,'BoundDockerPATH'):self.run_guard()
        self.assertFalse(self.calls)
    def test_wrong_host(self):self.rejected(lambda:self.env.update(DOCKER_HOST='unix:///wrong'))
    def test_missing_DOCKER_CONFIG(self):self.rejected(lambda:self.env.pop('DOCKER_CONFIG'))
    def test_missing_rust_fields(self):self.rejected(lambda:self.env.pop('RUSTUP_TOOLCHAIN'))
    def test_wrong_environment_script_hash(self):self.rejected(lambda:self.meta['environment_script'].update(sha256='wrong'))
    def test_missing_binary(self):self.rejected(lambda:Path(self.meta['docker']['path']).unlink())
    def test_changed_binary(self):self.rejected(lambda:Path(self.meta['docker']['path']).write_text('changed'))
    def test_missing_compose(self):self.rejected(lambda:Path(self.meta['compose']['path']).unlink())
    def test_changed_compose(self):self.rejected(lambda:Path(self.meta['compose']['path']).write_text('changed'))
    def test_missing_socket(self):
        with self.assertRaises(g.ToolingRefusal):self.run_guard(socket=False)
        self.assertFalse(self.calls)
    def test_each_query_failure_is_not_success(self):
        for failed in ['version','compose','info']:
            with self.subTest(failed=failed):
                def runner(args,**kw):
                    if args[1]==failed:return subprocess.CompletedProcess(args,1,'','unreachable host/plugin')
                    return self.runner(args,**kw)
                with self.assertRaises(g.ToolingRefusal):self.run_guard(runner)
    def test_timeout_before_any_application(self):
        def runner(args,**kw):raise subprocess.TimeoutExpired(args,5)
        with self.assertRaises(g.ToolingRefusal):self.run_guard(runner)
    def test_empty_metadata_not_health(self):
        with self.assertRaises(g.ToolingRefusal):self.run_guard(lambda args,**kw:subprocess.CompletedProcess(args,0,'',''))
if __name__=='__main__':unittest.main(verbosity=2)
