"""Portable read-only evidence/corruption controls; never imports an application."""
import copy,gzip,hashlib,json,tempfile,unittest
from pathlib import Path
import validate as packet
class PacketControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.actual=packet.load_capture();cls.plan=packet.read(packet.HERE/'executed/executor3/inputs/plan.json')
    def corrupt(self,fn):
        v=copy.deepcopy(self.actual);fn(v);self.assertFalse(packet.effects(packet.HERE,v,self.plan)['passed'])
    def test_full_projection(self):self.assertEqual(packet.validate()['recomputed_effect_checks'],1449)
    def test_all_historical_closures(self):self.assertEqual(set(packet.closures()['executors']),{'1','2','3'})
    def test_exact_gzip_bytes(self):
        row=packet.read(packet.HERE/'closures.json')['gzip_raw'][0];raw=gzip.decompress((packet.HERE/row['path']).read_bytes());self.assertEqual(len(raw),row['uncompressed_bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['uncompressed_sha256'])
    def test_preflight_negative_kept(self):self.assertEqual(packet.read(packet.HERE/'raw/Source1/execution.json')['controller_exit'],1)
    def test_swapped_alias(self):
        def f(v):a=v['seed_aliases'];a['A'],a['B']=a['B'],a['A']
        self.corrupt(f)
    def test_wrong_target(self):
        def f(v):r=v['requests'][0];r['target_id']=v['seed_aliases']['B'];r['path']='/api/core/notifications/'+str(r['target_id'])+'/';r['resolver']['kwargs']['pk']=str(r['target_id'])
        self.corrupt(f)
    def test_seed_field(self):self.corrupt(lambda v:v['seed_rows']['notifications'][0].update(title='corrupt'))
    def test_denied_SQL_changed(self):self.corrupt(lambda v:v['requests'][2]['after']['notifications'][0].update(title='corrupt'))
    def test_raw_hash_changed(self):self.corrupt(lambda v:v['requests'][30].update(body_sha256='0'*64))
    def test_wrong_role(self):self.corrupt(lambda v:v['requests'][0].update(role='admin'))
    def test_wrong_clock(self):self.corrupt(lambda v:v['requests'][30]['after']['notifications'][0].update(updated_at='2001-01-01T00:00:00Z'))
    def test_extra_response_field(self):self.corrupt(lambda v:v['requests'][30]['json'].update(extra='corrupt'))
    def test_exception_truncation(self):self.corrupt(lambda v:v['requests'][-1]['framework_exception'].update(complete=False))
    def test_exception_byte_hash(self):self.corrupt(lambda v:v['requests'][-1]['framework_exception']['traceback_evidence'].update(original_sha256='0'*64))
    def test_no_invented_success_for_final500(self):self.corrupt(lambda v:v['requests'][-1].update(actual_status=200))
    def test_outer_fixture_catch(self):self.corrupt(lambda v:v.update(fixture_outer_exception={'phase':'request'}))
    def test_receiver_unexpected_runtime(self):self.corrupt(lambda v:v['requests'][0]['runtime_after']['runtimes']['live_proxy'].update(present=True))
if __name__=='__main__':unittest.main()
