"""Synthetic/local controls only; never invoke Docker, Django or Source app."""
import ast,copy,hashlib,json,sys,unittest
from pathlib import Path
from effects import compare,CASES,BASE,serialized_notice
from fixture_guards import database,dataset,unchanged_default,stop_record,TABLES
from bounded_process import Collected,CollectionFailure

def audit(db='rust_probe_notif_0123456789',revision=0):
    tables={t:{'rows':0,'sha256':hashlib.sha256((t+':'+str(revision if t=='core_systemnotification' else 0)).encode()).hexdigest()}for t in TABLES}
    value={'database':db,'tables':tables,'sequences':{'public.'+t+'_id_seq':{'last_value':1,'is_called':False}for t in TABLES},'capture_bytes':0,'scope':'synthetic control, never measured'}
    value['tables']['core_systemnotification']['rows']=1
    value['sequences']['public.core_systemnotification_id_seq']={'last_value':10,'is_called':True}
    value['sequences']['public.core_notificationdismissal_id_seq']={'last_value':22,'is_called':False}
    rehash(value);return value

def rehash(value):value['dataset_sha256']=hashlib.sha256(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode()).hexdigest()
def effect_audit(previous,table):
    result=copy.deepcopy(previous);result['tables'][table]['rows']+=1;result['tables'][table]['sha256']=hashlib.sha256(json.dumps(result['tables'][table]).encode()).hexdigest();prior=result['sequences']['public.'+table+'_id_seq'];result['sequences']['public.'+table+'_id_seq']={'last_value':prior['last_value']+(1 if prior['is_called']else 0),'is_called':True};rehash(result);return result

def synthetic_capture():
    db='rust_probe_notif_0123456789';before='2026-11-12T10:00:00+00:00';after='2026-11-12T10:00:02+00:00';middle='2026-11-12T10:00:01+00:00'
    notice={'id':10,'notification_key':'test.notif','notification_type':'info','priority':'normal','source':'system','title':'Hello','message':'World','action_data':{},'is_active':True,'admin_only':False,'expires_at':None,'created_at':middle,'updated_at':middle}
    value={'synthetic_only':True,'database':db,'seed':{'notification_id':10,'standard_user_id':30,'admin_user_id':31,'standard_user_level':1,'admin_user_level':10},'capture_valid':True,'actual_request_count':8,'connections_close_called':True,'no_network_setting_mutation':True,'resolver':{'list':{'path':BASE,'actions':{'get':'list','post':'create'}},'dismiss':{'path':BASE+'10/dismiss/','actions':{'post':'dismiss'}}},'wrong_path_control':{'resolved':True,'is_notification_view':False},'seed_rows':{'notifications':[notice],'dismissals':[]},'setup_after_seed_named_audit':audit(),'requests':[]}
    previous=copy.deepcopy(value['seed_rows']);previous_audit=copy.deepcopy(value['setup_after_seed_named_audit'])
    for index,(name,role,method,status)in enumerate(CASES):
        current=copy.deepcopy(previous);current_audit=copy.deepcopy(previous_audit)
        if index==3:
            current['dismissals']=[{'id':22,'user_id':30,'notification_id':10,'dismissed_at':middle,'action_taken':None}];current_audit=effect_audit(current_audit,'core_notificationdismissal')
        if index==7:
            created={**notice,'id':11,'notification_key':'owned.created.0123456789'};current['notifications'].append(created);current_audit=effect_audit(current_audit,'core_systemnotification')
        payload=None if method=='GET'else{}if index in(3,4)else{'notification_key':'evil','title':'Phish','message':'Click','is_active':True}if index==2 else{'notification_key':'owned.created.0123456789','title':'Hello','message':'World','is_active':True}
        body={'detail':'synthetic denial'}if index in(0,2)else{'notifications':[serialized_notice(notice)],'count':1,'unread_count':1}if index in(1,6)else{'success':True,'message':'Notification dismissed','notification_key':'test.notif'}if index in(3,4)else{'notifications':[],'count':0,'unread_count':0}if index==5 else serialized_notice(created)
        encoded=json.dumps(body).encode();cache={'rows':[],'capture_bytes':0,'scope':'synthetic control'}
        row={'name':name,'role':role,'method':method,'path':BASE+'10/dismiss/'if index in(3,4)else BASE,'resolver_action':'dismiss'if index in(3,4)else'list'if method=='GET'else'create','request_payload':payload,'follow_redirects':False,'actual_status':status,'body_hex':encoded.hex(),'body_bytes':len(encoded),'body_sha256':hashlib.sha256(encoded).hexdigest(),'body_capture_complete':True,'json':body,'began_utc':before,'completed_utc':after,'before':previous,'after':current,'before_named_audit':previous_audit,'after_named_audit':current_audit,'before_owned_cache':copy.deepcopy(cache),'after_owned_cache':copy.deepcopy(cache)}
        value['requests'].append(row);previous=copy.deepcopy(current);previous_audit=copy.deepcopy(current_audit)
    return value

class EffectsControls(unittest.TestCase):
    def test_synthetic_happy_comparator_does_not_claim_Source(self):
        result=compare(synthetic_capture());self.assertTrue(result['passed'],result);self.assertFalse(result['actual_Source_result']);self.assertFalse(result['new_coverage_registered'])
    def rejected(self,change):
        value=synthetic_capture();change(value);self.assertFalse(compare(value)['passed'])
    def test_all_eight_status_corruptions(self):
        for index in range(8):
            with self.subTest(index=index):self.rejected(lambda v:v['requests'][index].update(actual_status=500))
    def test_wrong_route(self):self.rejected(lambda v:v['requests'][0].update(path='/api/system/notifications/'))
    def test_wrong_resolver(self):self.rejected(lambda v:v['requests'][1].update(resolver_action='retrieve'))
    def test_follow_redirect(self):self.rejected(lambda v:v['requests'][1].update(follow_redirects=True))
    def test_role_widening(self):self.rejected(lambda v:v['seed'].update(standard_user_level=10))
    def test_missing_call(self):self.rejected(lambda v:v['requests'].pop())
    def test_request_payload_drift(self):self.rejected(lambda v:v['requests'][2]['request_payload'].update(title='Other'))
    def test_wrong_default_database(self):self.rejected(lambda v:v.update(database='dispatcharr'))
    def test_body_hash_corruption(self):self.rejected(lambda v:v['requests'][0].update(body_sha256='0'*64))
    def test_body_truncation(self):self.rejected(lambda v:v['requests'][0].update(body_capture_complete=False))
    def test_body_json_not_bound(self):self.rejected(lambda v:v['requests'][1]['json'].update(count=99))
    def test_denied_SQL_mutation(self):self.rejected(lambda v:v['requests'][2]['after']['notifications'][0].update(title='Phish'))
    def test_foreign_user_dismissal(self):self.rejected(lambda v:v['requests'][3]['after']['dismissals'][0].update(user_id=31))
    def test_duplicate_repeat(self):self.rejected(lambda v:v['requests'][4]['after']['dismissals'].append(dict(v['requests'][4]['after']['dismissals'][0],id=23)))
    def test_repeat_timestamp_changed(self):self.rejected(lambda v:v['requests'][4]['after']['dismissals'][0].update(dismissed_at='2026-11-12T11:00:00Z'))
    def test_repeat_sequence_changed(self):
        def change(v):
            a=v['requests'][4]['after_named_audit'];a['sequences']['public.core_notificationdismissal_id_seq']['last_value']+=1;rehash(a)
        self.rejected(change)
    def test_create_defaults(self):self.rejected(lambda v:v['requests'][7]['after']['notifications'][1].update(source='developer'))
    def test_create_clock(self):self.rejected(lambda v:v['requests'][7]['after']['notifications'][1].update(created_at='2026-11-12T11:00:00Z'))
    def test_unexpected_ninth_table_effect(self):
        def change(v):
            a=v['requests'][0]['after_named_audit'];a['tables']['core_coresettings']['rows']+=1;rehash(a)
        self.rejected(change)
    def test_unexpected_outcome_preserved_not_rewritten(self):
        value=synthetic_capture();value['requests'][7]['actual_status']=409;before=copy.deepcopy(value);result=compare(value);self.assertFalse(result['passed']);self.assertEqual(value,before)
    def test_missing_effect_capture(self):self.rejected(lambda v:v['requests'][3].pop('after'))
    def test_cache_capacity(self):self.rejected(lambda v:v['requests'][0]['after_owned_cache'].update(capture_bytes=1048577))
    def test_connection_close_failed(self):self.rejected(lambda v:v.update(connections_close_called=False))
    def test_wrong_path_is_notification(self):self.rejected(lambda v:v['wrong_path_control'].update(is_notification_view=True))

class OwnershipControls(unittest.TestCase):
    def test_only_exact_owned_database(self):
        self.assertEqual(database('rust_probe_notif_0123456789'),'rust_probe_notif_0123456789')
        for name in ['dispatcharr','postgres','rust_pending_x','rust_probe_notif_01234567890','rust_probe_notif_ABCDEF0123','rust_probe_notif_0123456789;DROP DATABASE dispatcharr']:
            with self.subTest(name=name),self.assertRaises(ValueError):database(name)
    def test_eleven_table_default_audit(self):
        a=audit('dispatcharr');self.assertTrue(unchanged_default(a,copy.deepcopy(a)))
        b=copy.deepcopy(a);b['tables']['core_systemnotification']['rows']=2;rehash(b);self.assertFalse(unchanged_default(a,b))
    def test_audit_missing_notification_sequence(self):
        a=audit();a['sequences'].pop('public.core_notificationdismissal_id_seq');rehash(a)
        with self.assertRaises(ValueError):dataset(a,a['database'])
    def test_audit_digest_tamper(self):
        a=audit();a['tables']['accounts_user']['rows']=1
        with self.assertRaises(ValueError):dataset(a,a['database'])
    def test_pid_reuse_rejected(self):
        value={'pid':45,'role':'notifications-shell','nonce':'abc','starttime':'789'}
        self.assertEqual(stop_record(value,'notifications-shell','abc',lambda pid:'789'),45)
        with self.assertRaises(ValueError):stop_record(value,'notifications-shell','abc',lambda pid:'790')
    def test_Source_module_import_has_no_Django_or_execution(self):
        tree=ast.parse(Path(__file__).with_name('source_fixture.py').read_text())
        for node in tree.body:
            if isinstance(node,(ast.Import,ast.ImportFrom)):self.assertFalse(any(x in ast.unparse(node)for x in ('django','rest_framework','core.models')))
        self.assertIn("if __name__ == '__main__'",ast.unparse(tree))

class SealControls(unittest.TestCase):
    def test_default_check_cannot_invoke_Docker(self):
        import subprocess,os
        result=subprocess.run([sys.executable,str(Path(__file__).with_name('run.py')),'--docker','/this/docker/must/not/exist'],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr);self.assertFalse(json.loads(result.stdout)['Source_executed'])
    def test_wrong_review_hash_precedes_output_or_execution(self):
        import subprocess,os,tempfile
        with tempfile.TemporaryDirectory()as directory:
            output=Path(directory)/'must-not-create'
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('run.py')),'--docker','/this/docker/must/not/exist','--execute-reviewed-seal','0'*64,'--output',str(output)],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),capture_output=True,text=True,timeout=5)
            self.assertNotEqual(result.returncode,0);self.assertIn('ReviewedSealHashBinding',result.stderr);self.assertFalse(output.exists())
    def test_inventory_cannot_omit_files(self):
        import run,tempfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);(root/'controller').mkdir();(root/'inputs').mkdir();(root/'controller/run.py').write_text('placeholder');(root/'README.md').write_text('review');(root/'execution-plan.json').write_text('{}')
            value={'state':'SEALED_UNEXECUTED_REQUIRES_ROOT_REVIEW','Original_revision':run.REVISION,'image':run.IMAGE,'fixed_request_count':8,'files':[]};seal=root/'seal.json';seal.write_text(json.dumps(value))
            with patch.object(run,'HERE',root/'controller'),self.assertRaisesRegex(ValueError,'CompleteSealedInputInventory'):run.check_seal(seal)

class BoundedLocalControls(unittest.TestCase):
    def test_local_subprocess_exit_is_reaped(self):
        import os
        job=Collected([sys.executable,'-c','print("local-control")'],dict(os.environ),1024,lambda:None);self.assertEqual(job.wait(2),b'local-control\n');self.assertTrue(job.physically_settled)
    def test_local_overflow_stops_and_reaps(self):
        import os
        stopped=[];job=Collected([sys.executable,'-c','print("x"*4096)'],dict(os.environ),128,lambda:stopped.append(True))
        with self.assertRaises(CollectionFailure):job.wait(2)
        self.assertTrue(stopped);self.assertIsNotNone(job.process.poll());self.assertTrue(all(not t.is_alive()for t in job.threads));self.assertLessEqual(job.used,128)
if __name__=='__main__':unittest.main()
