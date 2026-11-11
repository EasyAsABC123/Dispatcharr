"""Contract controls; Original Dispatcharr authors retain application credit."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('compare',Path(__file__).with_name('active_recovery_compare.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

def fixture():
    rows=[]
    for name,active,requests,provider,origin in [
        ('old-body-after-loss-sampling',0,0,0,1),
        ('same-id-reopen-before-close',1,1,1,2),
        ('third-current-entry-open-old-and-younger-held',2,2,1,3),
        ('old-body-beyond100th-yield-with-younger-sibling-held',1,2,1,3),
        ('younger-beyond100th-yield-after-old-client-closed',0,0,0,3),
        ('metadata-retired-reopen-while-younger-body-held',1,1,1,4),
        ('younger-extra-read-after-retired-public-reopen',1,1,1,4)]:
        metadata={'active_streams':str(active),'request_count':str(requests),'bytes_sent':'819200'}if active else{}
        rows.append({'name':name,'metadata':metadata,'provider_count':str(provider),'origin_vod':{'requests':origin}})
    report={'observations':rows,'origin_fixture':{'requests':0},
            'old_body':{'bytes':278528,'sha256':'sample','ended':False,'error':None},
            'old_progress_after_reopen':{'bytes_read':827392},
            'third_close_returned_public_count_to_one':True,'old_close_public_effect_observed':True,
            'younger_after_old_close':{'chunks_read':103,'bytes_read':843776,'ended':False,'error':None},
            'younger_after_retired_reopen':{'bytes':8192,'offset':843776,'sha256':'tail'},
            'body_prefix_matches_origin':True,'reopened_body_starts_at_prefix':True,'old_body_after_reopen_matches_offset':True,
            'events':[{'event_type':name}for name in ['vod_start','vod_start','vod_stop','vod_start','vod_stop']]}
    for name in ['same_id_reopen','additional_current_entry','retired_public_reopen']:
        report[name]={'status':200,'first_bytes':8192,'first_sha256':'prefix'}
    return report

class Comparisons(unittest.TestCase):
    def test_relative_origin_and_public_zero_semantics(self):
        source=fixture();native=copy.deepcopy(source);native['origin_fixture']['requests']=20
        for row in native['observations']:
            row['origin_vod']['requests']+=20
            if row['provider_count']=='0':row['provider_count']=None
        self.assertTrue(module.comparison(source,native)['passed'])
    def test_capped_viewer_requires_observed_auth_and_current_identity(self):
        source=fixture();source['viewer_auth']={'mode':'fixture Bearer JWT','user_id':7,'stream_limit':1};source['authenticated_viewer_observed']=True
        for row in source['observations']:
            if row['metadata']:row['metadata']['user_id']='7'
        native=copy.deepcopy(source)
        self.assertTrue(module.comparison(source,native)['passed'])
        native['authenticated_viewer_observed']=False
        self.assertFalse(module.comparison(source,native)['passed'])
        native=copy.deepcopy(source);native['observations'][1]['metadata']['user_id']='8'
        self.assertFalse(module.comparison(source,native)['passed'])
    def test_both_wrong_is_not_parity(self):
        source=fixture();source['observations'][2]['metadata']['active_streams']='9'
        result=module.comparison(source,copy.deepcopy(source))
        self.assertFalse(result['passed']);self.assertFalse(result['checks']['additional_active_entry'])
    def test_counter_continuity_and_event_order_fail_independently(self):
        source=fixture()
        for change in ['counter','continuity','events','missing-stage']:
            native=copy.deepcopy(source)
            if change=='counter':native['observations'][1]['provider_count']='2'
            if change=='continuity':native['younger_after_old_close']['ended']=True
            if change=='events':native['events'].reverse()
            if change=='missing-stage':native['observations']=native['observations'][1:]
            self.assertFalse(module.comparison(source,native)['passed'],change)


class HeaderLossIdentityTests(unittest.TestCase):
    def reports(self):
        source=fixture()
        source.update(viewer_auth={'user_id':2,'stream_limit':1}, authenticated_viewer_observed=True, authorization_omitted_after_loss=True)
        for row in source['observations']:
            if row['metadata']:row['metadata']['user_id']='unknown'
        return source,copy.deepcopy(source)
    def test_explicit_headerless_recreation_uses_source_unknown(self):
        source,native=self.reports()
        result=module.comparison(source,native)
        self.assertTrue(result['checks']['current_hash_user_identity'])
        self.assertTrue(result['checks']['headerless_after_loss_subject'])
    def test_native_must_actually_omit_authorization(self):
        source,native=self.reports();native['authorization_omitted_after_loss']=False
        self.assertFalse(module.comparison(source,native)['checks']['headerless_after_loss_subject'])
    def test_old_authenticated_metadata_is_not_silently_accepted(self):
        source,native=self.reports()
        for row in native['observations']:
            if row['metadata']:row['metadata']['user_id']='2'
        self.assertFalse(module.comparison(source,native)['checks']['current_hash_user_identity'])
if __name__=='__main__':unittest.main()
