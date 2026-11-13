"""Meaningful socket-free corruption controls; never imports Original app code."""
import copy
import json
import unittest
from pathlib import Path
from validate_definition import validate

class DefinitionControls(unittest.TestCase):
    def setUp(self):
        self.plan=json.loads((Path(__file__).parent/'plan.json').read_text())
    def corrupt(self, change):
        p=copy.deepcopy(self.plan);change(p)
        with self.assertRaises(ValueError):validate(p)
    def test_exact_definition(self):self.assertEqual(validate(self.plan)['cases'],32)
    def test_wrong_warm_key(self):self.corrupt(lambda p:p['cases'][11]['pre_request_setup']['cache_set'][0].__setitem__('key','coresettings:group:system_settings'))
    def test_warm_TTL(self):self.corrupt(lambda p:p['cases'][11]['pre_request_setup']['cache_set'][0].__setitem__('timeout',None))
    def test_unplanned_setup(self):self.corrupt(lambda p:p['cases'][10].__setitem__('pre_request_setup',p['cases'][11]['pre_request_setup']))
    def test_reorder(self):self.corrupt(lambda p:p['cases'].reverse())
    def test_force_auth(self):self.corrupt(lambda p:p['auth'].__setitem__('force_authenticate',True))
    def test_role(self):self.corrupt(lambda p:p['auth']['roles']['standard'].__setitem__('user_level',10))
    def test_UI_denied(self):self.corrupt(lambda p:p['auth']['roles']['admin_denied'].__setitem__('custom_ui','127.0.0.0/8'))
    def test_alias_swap(self):self.corrupt(lambda p:p['cases'][6].__setitem__('target','array'))
    def test_late_alias_swap(self):self.corrupt(lambda p:p['cases'][23].__setitem__('target','array'))
    def test_bool_role(self):self.corrupt(lambda p:p['auth']['roles']['standard'].__setitem__('user_level',True))
    def test_seed_bool_int(self):self.corrupt(lambda p:p['seed']['settings'][2]['value'].__setitem__('untouched',1))
    def test_missing_target(self):self.corrupt(lambda p:p['target_policy'].__setitem__('missing',1002))
    def test_seed(self):self.corrupt(lambda p:p['seed']['settings'][2]['value'].__setitem__('untouched',False))
    def test_body_hash(self):self.corrupt(lambda p:p['cases'][7].__setitem__('body_sha256','0'*64))
    def test_body_size(self):self.corrupt(lambda p:p['cases'][7].__setitem__('body_bytes',0))
    def test_path_escape(self):self.corrupt(lambda p:p['cases'][0].__setitem__('body','../README.md'))
    def test_prediction_measured(self):self.corrupt(lambda p:p['cases'][0]['prediction'].__setitem__('measurement','PASS'))
    def test_bool_status(self):self.corrupt(lambda p:p['cases'][0]['prediction'].__setitem__('status',True))
    def test_budget_extension(self):self.corrupt(lambda p:p['budgets_seconds'].__setitem__('capture',720))
    def test_original_hash(self):self.corrupt(lambda p:p['Original_inputs_sha256'].__setitem__('core/models.py','0'*64))
    def test_preallocated_dynamic_id(self):self.corrupt(lambda p:p['target_policy'].__setitem__('created',1003))
if __name__=='__main__':unittest.main()
