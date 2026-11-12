"""Local synthetic/loopback controls only; no application, Docker or Source."""
import ast
import copy
import importlib
import json
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from identity import kernel,verify_worker,worker
from header_origin import Ledger,ALIASES
from qualify import phase
import run
from phase import request
NONCE='a'*32
POLICY=json.loads((ROOT/'policy.json').read_text())


def witness(native=False):
    pid=1 if native else 123
    comm='dispatcharr-rs' if native else 'python'
    stat=f'{pid} ({comm}) S 0 '+' '.join(['0']*17)+' 12345 0\n'
    return kernel(b'fixture-kernel\n',stat.encode(),b'Uid:\t65532\t65532\t65532\t65532\n',b'0::/owned\n',pid,'12345',native=native)


def example(native=False):
    w=witness(native)
    ledger=Ledger(NONCE)
    class Sock:
        def fileno(self):return 42
    cid=ledger.opened(Sock())
    n=ledger.request(cid,'GET','/health');ledger.response_status(n,200);ledger.returned(n)
    value={'phase_started':1.0,'phase_completed':20.0,'cases':[],'fixture_nonce':NONCE,'actual_SQL_Bearer_user_verified':True,'fixed_caps_before':{'account':2,'profile':2},'fixed_caps_after':{'account':2,'profile':2},'kernel_witness':w}
    for name in POLICY['Native_cases' if native else 'Source_cases']:
        before=ledger.snapshot()
        if name in ['full200','range206','provider404_to500']:
            path='/movie/fixture/fixture/fixture-normal-probe1.mp4' if name=='provider404_to500' else '/movie/fixture/fixture/fixture.mp4'
            n=ledger.request(cid,'GET',path)
            ledger.response_status(n,404 if name=='provider404_to500'else 206 if name=='range206'else 200)
            ledger.headers(n,'response_headers',{'x-worker-id':['owned-provider-spoof']});ledger.returned(n)
        row={'request_started':2.0+len(value['cases']),'request_completed':2.1+len(value['cases']),'case':name,'status':POLICY['status_expectations'].get(name,405),'headers':[],'origin_before':before,'origin_after':ledger.snapshot(),'physical_close':{'fd_after':-1,'original_inode_absent':True},'absolute_observation':{'timer_joined':True}}
        if name in ('full200','range206'):
            row.update(headers=[['X-Worker-ID',w['expected_worker_header']]],prefix_bytes=8192,prefix_sha256=POLICY['prefix_sha256'])
        value['cases'].append(row)
    return value


class Controls(unittest.TestCase):
    def rejects(self, mutate, native=False):
        value=example(native);mutate(value)
        with self.assertRaises((ValueError,KeyError,TypeError)):
            phase(value,POLICY,'native'if native else'source')
    def test_complete_fixed_cases(self):
        for native in (False,True):self.assertTrue(phase(example(native),POLICY,'native'if native else'source')['worker_header_branch_scope_qualified'])
    def test_missing_branch(self):self.rejects(lambda v:v['cases'].pop())
    def test_empty_cases(self):self.rejects(lambda v:v.update(cases=[]))
    def test_nonfinite_clock(self):self.rejects(lambda v:v.update(phase_completed=float('nan')))
    def test_request_outside_phase(self):self.rejects(lambda v:v['cases'][0].update(request_started=-2))
    def test_request_exceeds_absolute_limit(self):self.rejects(lambda v:v['cases'][0].update(request_completed=12))
    def test_comm_bound_to_raw_proc(self):self.rejects(lambda v:v['kernel_witness'].update(comm='fake'))
    def test_auth_must_be_real(self):self.rejects(lambda v:v.update(actual_SQL_Bearer_user_verified=False))
    def test_fixed_cap(self):self.rejects(lambda v:v['fixed_caps_after'].update(profile=1))
    def test_provider_cannot_choose_identity(self):self.rejects(lambda v:next(row for row in v['cases']if row['case']=='full200').update(headers=[['X-Worker-ID','owned-provider-spoof']]))
    def test_environment_cannot_choose_identity(self):self.rejects(lambda v:next(row for row in v['cases']if row['case']=='full200').update(headers=[['X-Worker-ID','owned-environment-spoof-1']]))
    def test_error_has_no_success_header(self):self.rejects(lambda v:v['cases'][0].update(headers=[['X-Worker-ID',v['kernel_witness']['expected_worker_header']]]))
    def test_duplicate_worker_header(self):self.rejects(lambda v:next(row for row in v['cases']if row['case']=='full200')['headers'].append(next(row for row in v['cases']if row['case']=='full200')['headers'][0]))
    def test_missing_actual_provider_canary(self):self.rejects(lambda v:next(row for row in v['cases']if row['case']=='full200')['origin_after']['requests'][-1].update(response_headers={}))
    def test_missing_origin_requests(self):self.rejects(lambda v:v['cases'][0]['origin_before'].update(requests=[]))
    def test_missing_origin_connections(self):self.rejects(lambda v:v['cases'][0]['origin_before'].update(connections=[]))
    def test_counter_totals_not_inferred(self):self.rejects(lambda v:v['cases'][0]['origin_before'].update(accepted_TCP_total=0))
    def test_error_provider_is_not_route_error(self):self.rejects(lambda v:next(row for row in v['cases']if row['case']=='provider404_to500')['origin_after']['requests'][-1].update(response_status=200))
    def test_unclosed_descriptor(self):self.rejects(lambda v:v['cases'][0]['physical_close'].update(fd_after=42))
    def test_inode_still_present(self):self.rejects(lambda v:v['cases'][0]['physical_close'].update(original_inode_absent=False))
    def test_timer_not_joined(self):self.rejects(lambda v:v['cases'][0]['absolute_observation'].update(timer_joined=False))
    def test_transport_failure(self):self.rejects(lambda v:v['cases'][0].update(transport_error='TimeoutError'))
    def test_native_pid_not_probe(self):self.rejects(lambda v:v['kernel_witness'].update(pid=44),True)
    def test_native_uid(self):self.rejects(lambda v:v['kernel_witness'].update(uid=0),True)
    def test_raw_kernel_required(self):self.rejects(lambda v:v['kernel_witness'].pop('raw_witness'))
    def test_raw_kernel_projection_bound(self):self.rejects(lambda v:v['kernel_witness'].update(expected_worker_header='fixture-kernel-456'))
    def test_starttime_reuse(self):
        w=witness()
        raw=w['raw_witness']
        with self.assertRaises(ValueError):kernel(bytes.fromhex(raw['hostname_hex']),bytes.fromhex(raw['stat_hex']),bytes.fromhex(raw['status_hex']),bytes.fromhex(raw['cgroup_hex']),123,'different')
    def test_invalid_head_is_not_parity_gate(self):
        value=example(True);value['cases'][-1]['status']=401
        result=phase(value,POLICY,'native');self.assertEqual(result['Native_invalid_HEAD'],401);self.assertFalse(result['full_header_or_M4_parity']);self.assertTrue(result['invalid_HEAD_compatibility_open'])
    def test_main_hardblock_before_any_output(self):
        with tempfile.TemporaryDirectory()as d,patch.object(sys,'argv',['run','--repo',d,'--output',d+'/fresh']):
            with self.assertRaisesRegex(RuntimeError,'BLOCKED'):run.main()
            self.assertFalse(Path(d,'fresh').exists())
    def test_hardblock_has_no_environment_override(self):
        import os
        with tempfile.TemporaryDirectory()as d,patch.dict(os.environ,{'ALLOW_RUN':'1','HARD_BLOCKED':'false'}),patch.object(sys,'argv',['run','--repo',d,'--output',d+'/fresh']):
            with self.assertRaisesRegex(RuntimeError,'BLOCKED'):run.main()
    def test_all_python_syntax(self):
        for path in (ROOT/'tools').glob('*.py'):ast.parse(path.read_bytes(),filename=str(path))
    def test_absolute_slow_header_physical_close(self):
        server=socket.socket();server.bind(('127.0.0.1',0));server.listen();done=threading.Event()
        def serve():
            client=None
            try:
                client,_=server.accept();client.recv(4096)
                for byte in b'HTTP/1.1 200 OK\r\n':
                    try:client.sendall(bytes([byte]))
                    except OSError:break
                    time.sleep(.04)
            finally:
                if client:client.close()
                server.close();done.set()
        thread=threading.Thread(target=serve);thread.start()
        began=time.monotonic()
        row=request('http://127.0.0.1:'+str(server.getsockname()[1]),'127.0.0.1','/',{},'GET',time.monotonic()+.15)
        self.assertTrue(row.get('transport_error'));self.assertLess(time.monotonic()-began,1)
        self.assertEqual(row['physical_close']['fd_after'],-1);self.assertTrue(row['physical_close']['original_inode_absent']);thread.join(2);self.assertTrue(done.is_set());self.assertFalse(thread.is_alive())

if __name__=='__main__':unittest.main()
