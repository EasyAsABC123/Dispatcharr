"""Synthetic full32 offline effect corruption controls, never an app capture."""
import copy
import unittest
from effects import validate_capture,compare
from synthetic import make,PLAN,ROOT

class CaptureControls(unittest.TestCase):
    def corrupt(self, change):
        report=make();change(report)
        with self.assertRaises((ValueError,KeyError,TypeError)):
            validate_capture(report,PLAN,ROOT/'definition',allow_synthetic=True)
    def test_synthetic32(self):self.assertEqual(validate_capture(make(),PLAN,ROOT/'definition',allow_synthetic=True)['qualified_requests'],32)
    def test_actual_rejects_synthetic(self):self.assertFalse(compare(make(),PLAN)['passed'])
    def test_missing_warm_proof(self):self.corrupt(lambda r:r.pop('between_request_fixture_setup'))
    def test_wrong_warm_sentinel(self):self.corrupt(lambda r:r['requests'][11]['before']['named_cache'].__setitem__('coresettings:group:owned.settings.new',None))
    def test_masked_old_key_invalidation(self):self.corrupt(lambda r:r['requests'][11]['after']['named_cache'].__setitem__('coresettings:group:owned.settings.new',None))
    def test_broker_effect(self):self.corrupt(lambda r:r['requests'][7]['after']['raw_Redis'][2]['rows'].append({'key_hex':'61','type':'list','dump_hex':'01','list_hex':['02'],'pttl':-1}))
    def test_result_effect(self):self.corrupt(lambda r:r['requests'][7]['after']['raw_Redis'][3]['rows'].append({'key_hex':'61','type':'string','dump_hex':'01','value_hex':'02','pttl':-1}))
    def test_raw_incomplete(self):self.corrupt(lambda r:r['requests'][7]['after']['raw_Redis'][2].__setitem__('complete',False))
    def test_bad_role(self):self.corrupt(lambda r:r['requests'][1].__setitem__('role','admin'))
    def test_wrong_existing_alias(self):self.corrupt(lambda r:r['requests'][23].__setitem__('target_id','1004'))
    def test_wrong_path(self):self.corrupt(lambda r:r['requests'][23].__setitem__('path','/api/core/settings/1004/'))
    def test_bad_backend(self):self.corrupt(lambda r:r['requests'][4]['actual_auth'].__setitem__('backend_class','forced'))
    def test_json_bool_int(self):self.corrupt(lambda r:r['requests'][7]['json']['value'].__setitem__('boolean',1))
    def test_foreign_row(self):self.corrupt(lambda r:r['requests'][7]['after']['settings'][2]['value'].__setitem__('untouched',False))
    def test_OTHER_table(self):self.corrupt(lambda r:r['requests'][7]['after']['public_manifest']['tables']['accounts_user'].__setitem__('count',1))
    def test_sequence(self):self.corrupt(lambda r:r['requests'][7]['after']['public_manifest']['sequences'].__setitem__('core_coresettings_id_seq',[9999,True]))
    def test_other_sequence(self):self.corrupt(lambda r:r['requests'][7]['after']['public_manifest']['sequences'].__setitem__('accounts_user_id_seq',[9999,True]))
    def test_extra_table(self):self.corrupt(lambda r:r['requests'][7]['after']['public_manifest']['tables'].__setitem__('extra',{}))
    def test_schema(self):self.corrupt(lambda r:r['requests'][7]['after']['public_manifest'].__setitem__('columns',[]))
    def test_raw_SQL_binding(self):self.corrupt(lambda r:r['requests'][7]['after']['audit']['tables']['core_coresettings'].__setitem__('raw_rows',[]))
    def test_response_digest(self):self.corrupt(lambda r:r['requests'][7].__setitem__('response_body_sha256','0'*64))
    def test_missing_response(self):self.corrupt(lambda r:r['requests'][7].__setitem__('transport','framework_exception'))
    def test_partial(self):self.corrupt(lambda r:r.__setitem__('actual_request_count',31))
    def test_alias_final(self):self.corrupt(lambda r:r['aliases'].__setitem__('epg',1004))

if __name__=='__main__':unittest.main()
