"""Pure plan corruption controls: no app, database, Docker or external I/O."""
import copy,json,tempfile,unittest
from pathlib import Path
from validate_plan import ROOT,validate,verify_inventory
class Controls(unittest.TestCase):
    def setUp(self):self.p=json.loads((ROOT/'plan.json').read_text())
    def reject(self,fn):
        p=copy.deepcopy(self.p);fn(p)
        with self.assertRaises(ValueError):validate(p)
    def test_pristine(self):self.assertFalse(validate(self.p)['Source_executed'])
    def test_measured_state(self):self.reject(lambda p:p.update(state='MEASURED'))
    def test_executed(self):self.reject(lambda p:p.update(fixture_requests_executed=1))
    def test_authorized(self):self.reject(lambda p:p.update(execution_authorized=True))
    def test_controller(self):self.reject(lambda p:p.update(controller_present=True))
    def test_tcp_claim(self):self.reject(lambda p:p.update(Native_or_TCP_or_credentials_claim=True))
    def test_unpinned_revision(self):self.reject(lambda p:p.update(Source_revision='HEAD'))
    def test_unpinned_image(self):self.reject(lambda p:p.update(Source_image='latest'))
    def test_extra_case(self):self.reject(lambda p:p['cases'].append(copy.deepcopy(p['cases'][0])))
    def test_duplicate_case(self):self.reject(lambda p:p['cases'][1].update(name=p['cases'][0]['name']))
    def test_post_method(self):self.reject(lambda p:p['cases'][0].update(method='POST'))
    def test_format_alias(self):self.reject(lambda p:p['cases'][0].update(path='/api/core/notifications/1.json/'))
    def test_prediction_as_observation(self):self.reject(lambda p:p['cases'][0].update(prediction_unmeasured=False))
    def test_missing_default_table(self):self.reject(lambda p:p['audit_tables'].pop())
    def test_wrong_contract_id(self):self.reject(lambda p:p['candidate_contract_ids'].append('unrelated:PUT'))
    def test_missing_network_role(self):self.reject(lambda p:p['cases'][2].update(actor='admin'))
    def test_inactive_forced_actor(self):self.reject(lambda p:p['seed']['users']['admin'].update(is_active=False))
    def test_allow_network_denied_admin(self):self.reject(lambda p:p['seed']['users']['adminDenied'].update(UI='127.0.0.1/32'))
    def test_default_database_writable(self):self.reject(lambda p:p['database_guard']['deny'].remove('dispatcharr'))
    def test_hide_negative_shape_error(self):self.reject(lambda p:p['cases'][-1].update(predicted_status=200))
    def test_collapse_cache_false(self):self.reject(lambda p:p['seed']['cache'].update({'H/admin':True}))
    def test_unbounded_capture(self):self.reject(lambda p:p['bounds'].update(report_bytes=10**10))
    def test_no_second_review(self):self.reject(lambda p:p.update(preexecution_gates=[]))
    def test_corrupt_blob(self):
        with tempfile.TemporaryDirectory()as d:
            f=Path(d)/'x';f.write_bytes(b'bad')
            with self.assertRaises(ValueError):verify_inventory(Path(d),[{'path':'x','bytes':3,'sha256':'0'*64}])
    def test_inventory_escape(self):
        with self.assertRaises(ValueError):verify_inventory(ROOT,[{'path':'../plan.json','bytes':0,'sha256':'0'*64}])
if __name__=='__main__':unittest.main()
