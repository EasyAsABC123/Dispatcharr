"""Portable corruption controls; no sockets, app, Source or ELF execution."""
import copy
import hashlib
import json
import unittest
import tempfile
from pathlib import Path
import verify as verifier
import importlib.util
_rebuild_spec=importlib.util.spec_from_file_location('notification_forms_native19_reconstruct',Path(__file__).parent/'reconstruct.py')
rebuilder=importlib.util.module_from_spec(_rebuild_spec);_rebuild_spec.loader.exec_module(rebuilder)
from validate_native import evidence
from verify import load_native_projection,validate
from validate_native import validate as semantics,Clocks,SOURCE

class Controls(unittest.TestCase):
    def corrupt(self,change):
        value=copy.deepcopy(load_native_projection());change(value)
        with self.assertRaises((ValueError,KeyError,TypeError)):semantics(value)
    def test_actual19(self):
        result=validate();self.assertTrue(result['passed']);self.assertFalse(result['full_cache_parity_passed'])
    def test_recorder_false_preserved(self):self.corrupt(lambda v:v.__setitem__('passed',True))
    def test_fullcache_claim_rejected(self):self.corrupt(lambda v:v.__setitem__('full_cache_parity_claimed',True))
    def test_partial_capture(self):self.corrupt(lambda v:v.__setitem__('actual_request_count',18))
    def test_status(self):self.corrupt(lambda v:v['requests'][0].__setitem__('status',200))
    def test_body_digest(self):self.corrupt(lambda v:v['requests'][0].__setitem__('response_body_sha256','0'*64))
    def test_header_evidence(self):self.corrupt(lambda v:v['requests'][0]['raw_headers_private']['HTTP_X_API_KEY'].__setitem__('sha256','0'*64))
    def test_raw_header_rejected(self):self.corrupt(lambda v:v['requests'][0]['raw_headers_private'].__setitem__('HTTP_X_API_KEY','00'))
    def test_seed_key_evidence(self):self.corrupt(lambda v:v['seed_users'][0]['api_key'].__setitem__('sha256','0'*64))
    def test_no_raw_key_fallback(self):self.corrupt(lambda v:v['seed_users'][0].__setitem__('api_key','native-forms-api-'+v['database'].split('_')[-1]+'-admin'))
    def test_foreign_SQL(self):self.corrupt(lambda v:v['requests'][0]['after_SQL']['tables']['m3u_m3uaccount'].append({'id':999}))
    def test_sequence(self):self.corrupt(lambda v:v['requests'][0]['after_SQL']['sequences']['core_systemnotification'].__setitem__('last_value',999))
    def test_no_cache_import(self):self.corrupt(lambda v:v['cache_after_seed']['named_decoded'][':1:coresettings:group:system_settings'].__setitem__('decoded',{'time_zone':'UTC'}))
    def test_cache_raw_drop(self):self.corrupt(lambda v:v['requests'][0]['after_raw_Redis'][1]['rows'].clear())
    def test_broker_effect(self):self.corrupt(lambda v:v['requests'][0]['after_raw_Redis'][2]['rows'].append({'key_hex':'61','dump_hex':'01','type':'list','pttl':-1,'list_hex':['00']}))
    def test_auth_history(self):self.corrupt(lambda v:v['requests'][0]['after_cache'].__setitem__('api_key_failed_auth_history',[1.0]))
    def test_credential_event(self):self.corrupt(lambda v:v['requests'][0]['after_events'].append({'id':1}))
    def test_input_body(self):self.corrupt(lambda v:v['requests'][0].__setitem__('body_sha256','0'*64))
    def test_Source_identity(self):self.corrupt(lambda v:v.__setitem__('Original_raw_sha256','0'*64))
    def test_server_wait(self):self.corrupt(lambda v:v.__setitem__('server_graceful_wait0',False))
    def test_legacy_diagnostic(self):self.corrupt(lambda v:v['Native_transport_diagnostics'][5].__setitem__('status',413))
    def test_mapped_clock_rewind(self):
        clocks=Clocks(SOURCE);source=next(iter(clocks.generated)).isoformat();native='2030-01-01T00:00:00+00:00';clocks.compare(source,native)
        with self.assertRaises(ValueError):clocks.compare(source,source)
    def test_strict_projected_SHA_no_raw_fallback(self):
        with tempfile.TemporaryDirectory(prefix='forms-projected-control-') as directory:
            root=Path(directory)
            for name in ('reference.json','projection-policy.json','Native19-projection.json'):
                (root/name).write_bytes((verifier.ROOT/name).read_bytes())
            (root/'Native19-projection.json').write_bytes((root/'Native19-projection.json').read_bytes()+b' ')
            old=verifier.ROOT
            try:
                verifier.ROOT=root
                with self.assertRaises(ValueError):verifier.load_native_projection()
            finally:verifier.ROOT=old
    def test_reconstruction_wrong_map_before_Git(self):
        with tempfile.TemporaryDirectory(prefix='forms-map-control-') as directory:
            root=Path(directory);(root/'compiled813-map.json').write_text('{}')
            old=rebuilder.ROOT
            try:
                rebuilder.ROOT=root
                with self.assertRaises(ValueError):rebuilder.reconstruct(root/'destination',root/'NO_GIT')
            finally:rebuilder.ROOT=old
    def test_evidence_type(self):self.assertEqual(evidence(b'owned'),{'sha256':hashlib.sha256(b'owned').hexdigest(),'bytes':5})

if __name__=='__main__':unittest.main()
