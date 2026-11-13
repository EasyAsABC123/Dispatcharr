"""Synthetic corruption controls only; no Source/Native/app/socket execution."""
import base64,copy,hashlib,json,tempfile,unittest
from pathlib import Path
from validate_physical import validate,TABLES,ROOT

def fake(case,out):
    scripts=json.loads((ROOT/'inputs/resp-contracts.json').read_text());contract=scripts['publish'if case==5 else'read']
    raw_frame={'headers':{'task':'apps.backups.tasks.create_backup_task','id':'22222222-2222-4222-8222-222222222222','root_id':'22222222-2222-4222-8222-222222222222'},'properties':{'correlation_id':'22222222-2222-4222-8222-222222222222','delivery_info':{'exchange':'','routing_key':'celery'}},'body':base64.b64encode(json.dumps([[],{}, {'callbacks':None,'errbacks':None,'chain':None,'chord':None}]).encode()).decode()}
    frame=json.dumps(raw_frame).encode();metadata=json.dumps({'status':'SUCCESS','result':{'status':'completed'}}).encode()
    sql={'tables':{t:[]for t in TABLES},'sequences':{t:{'last_value':1,'is_called':True}for t in TABLES}}
    empty=lambda db:{'database':db,'rows':[]}
    backend={'database':3,'rows':[{'key_hex':bytes.fromhex(contract['keys_hex'][0]).hex(),'type':'string','dump_hex':'00','pttl':10000,'value_hex':metadata.hex()}]}if case==6 else empty(3)
    state={'SQL':sql,'credentials':{'users':[]},'all_broker':empty(2),'all_backend':backend,'broker_queue':{'value':{'list_hex':[]}},'backend':{'value':{'value_hex':metadata.hex()}}}
    report={'_synthetic_only':True,'database':'rust_probe_backup_jobs_physical_0123456789','case':case,'request_count':1,'passed':True,'capture_complete':True,'phase':'complete','production_changes':False,'Source_app_runs':0,'Main_runs':0,'worker_polls':0,'weak_only_observer':True,'FD_loss_is_not_handler_abort':True,'before':copy.deepcopy(state),'held_raw_state':copy.deepcopy(state),'after':copy.deepcopy(state),'proxy_connections_absent_before_stop':{'connections':0},'proxy_stop':{'connections':0,'listener_joined':True,'invalid':[]},'closed_pool_size':0,'closed_pool_idle':0}
    for name,held in [('logical_completion',True),('while_held_after_logical_completion',True),('after_physical_reply',False)]:report[name]={'calls':1,'logical_done':True,'weak_observer_stores_no_Arc':True,'Weak_Some':held,'real_application_http_slots_available':3 if held else 4,'real_jobs_slots_available':3 if held else 4}
    args=[b'EVALSHA',contract['sha1'].encode(),str(len(contract['keys_hex'])).encode(),*[bytes.fromhex(k)for k in contract['keys_hex']]]
    if case==5:
        args +=[frame,b'1000'];reply=b':1\r\n';report.update(actual_client_FD_shutdown_and_drop=True,handler_abort_claimed=False,actual_client_FD_receipt={'fd':7,'original_link':'socket:[11]','after_link':None,'original_FD_absent_or_reused':True},before_FD_loss={'logical_done':False},pre_FD_loss_read={'successful_HTTP_body_or_status_observed':False,'result':'WouldBlock'},published_owned_task_id=raw_frame['headers']['id'],published_unconsumed_create_frame_sha256=hashlib.sha256(frame).hexdigest())
        broker={'database':2,'rows':[{'key_hex':b'celery'.hex(),'type':'list','dump_hex':'00','pttl':-1,'list_hex':[frame.hex()]},{'key_hex':b'_kombu.binding.celery'.hex(),'type':'set','dump_hex':'00','pttl':-1,'set_hex':['00']}]}
        for name in('held_raw_state','after'):report[name]['all_broker']=copy.deepcopy(broker);report[name]['broker_queue']['value']['list_hex']=[frame.hex()]
    else:
        reply=b'*2\r\n:1\r\n$'+str(len(metadata)).encode()+b'\r\n'+metadata+b'\r\n';body=b'{"detail":"Native timeout"}';report['HTTP_response_while_reply_held']={'status':500,'Source_HTTP_error_parity_claimed':False,'body_hex':body.hex(),'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()};report['Source16_result_seed_context']=json.loads(metadata)
    digest=hashlib.sha256(reply).hexdigest();rows=[{'kind':'request-read','target':True,'target_arguments_hex':[v.hex()for v in args],'monotonic':1},{'kind':'request-forwarded','target':True,'monotonic':1.1},{'kind':'backend-target-reply-read-held','reply_hex':reply.hex(),'reply_sha256':digest,'monotonic':2},{'kind':'backend-target-reply-released','monotonic':5},{'kind':'reply-forwarded','target':True,'reply_sha256':digest,'monotonic':5.1}]
    (out/'proxy-events.jsonl').write_text(''.join(json.dumps(r)+'\n'for r in rows));(out/'proxy-process.json').write_text(json.dumps({'exit':0,'wait':0,'signal':None,'forced':False,'proc_PID_absent_after_wait':True,'stdout_reader_joined':True,'starttime':'123'}))
    return report

class Predicates(unittest.TestCase):
 def run_control(self,case,change=None):
  with tempfile.TemporaryDirectory(prefix='backup-physical-pure-')as d:
   out=Path(d);report=fake(case,out)
   if change:change(report,out)
   return validate(report,case,out,allow_synthetic=True)
 def corrupt(self,case,change):
  with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):self.run_control(case,change)
 def test_publish(self):self.assertTrue(self.run_control(5)['passed'])
 def test_read(self):self.assertTrue(self.run_control(6)['passed'])
 def test_Weak_released_early(self):self.corrupt(5,lambda r,o:r['while_held_after_logical_completion'].__setitem__('Weak_Some',False))
 def test_Weak_on_pending_is_not_proof(self):self.corrupt(5,lambda r,o:r['logical_completion'].__setitem__('logical_done',False))
 def test_fake_owner_label(self):self.corrupt(5,lambda r,o:r['logical_completion'].__setitem__('weak_observer_stores_no_Arc',False))
 def test_slot_early_release(self):self.corrupt(5,lambda r,o:r['logical_completion'].__setitem__('real_jobs_slots_available',4))
 def test_final_owner_retained(self):self.corrupt(6,lambda r,o:r['after_physical_reply'].__setitem__('Weak_Some',True))
 def test_FDloss_not_abort(self):self.corrupt(5,lambda r,o:r.__setitem__('handler_abort_claimed',True))
 def test_unobserved_FD(self):self.corrupt(5,lambda r,o:r['actual_client_FD_receipt'].__setitem__('original_link',None))
 def test_early_HTTP(self):self.corrupt(5,lambda r,o:r['pre_FD_loss_read'].__setitem__('successful_HTTP_body_or_status_observed',True))
 def test_unknown_SQL_row(self):self.corrupt(6,lambda r,o:r['after']['SQL']['tables']['accounts_user'].append({'id':9}))
 def test_sequence(self):self.corrupt(6,lambda r,o:r['after']['SQL']['sequences']['accounts_user'].__setitem__('last_value',2))
 def test_broker_extra(self):self.corrupt(5,lambda r,o:r['after']['all_broker']['rows'].append({'key_hex':'61'}))
 def test_wrong_native_status(self):self.corrupt(6,lambda r,o:r['HTTP_response_while_reply_held'].__setitem__('status',200))
 def test_Source_error_claim(self):self.corrupt(6,lambda r,o:r['HTTP_response_while_reply_held'].__setitem__('Source_HTTP_error_parity_claimed',True))
 def test_bool_request_count(self):self.corrupt(5,lambda r,o:r.__setitem__('request_count',True))
 def test_bool_close_size(self):self.corrupt(6,lambda r,o:r.__setitem__('closed_pool_size',False))
 def test_final_pool_size(self):self.corrupt(5,lambda r,o:r.__setitem__('closed_pool_size',1))
 def test_forced_proxy(self):
  self.corrupt(5,lambda r,o:(o/'proxy-process.json').write_text(json.dumps({'exit':0,'wait':0,'signal':None,'forced':True,'proc_PID_absent_after_wait':True,'stdout_reader_joined':True,'starttime':'123'})))
 def test_noActualSynthetic(self):
  with tempfile.TemporaryDirectory()as d:
   out=Path(d);report=fake(5,out)
   with self.assertRaises(ValueError):validate(report,5,out)
if __name__=='__main__':unittest.main()
