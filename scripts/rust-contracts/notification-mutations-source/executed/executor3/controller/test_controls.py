"""Synthetic corruption controls only; no Source/Docker/application imports."""
import ast,copy,datetime,hashlib,json,os,sys,unittest
from pathlib import Path
from effects import compare,render
from fixture_guards import TABLES,database,dataset,unchanged_default,stop_record
ROOT=Path(__file__).resolve().parent.parent
PLAN=json.loads((ROOT/'inputs/plan.json').read_text())
def timestamp(i):return '2026-11-12T00:00:'+str(i).zfill(2)+'+00:00'
def audit(state,db):
    tables={}
    for t in TABLES:
        v=state['notifications']if t=='core_systemnotification'else state['dismissals']if t=='core_notificationdismissal'else []
        tables[t]={'rows':len(v),'sha256':hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()}
    sequences={'public.'+t+'_id_seq':{'last_value':20,'is_called':True}for t in TABLES};v={'database':db,'tables':tables,'sequences':sequences}
    v['dataset_sha256']=hashlib.sha256(json.dumps({'tables':tables,'sequences':sequences},sort_keys=True).encode()).hexdigest();return v
def rehash(v):v['dataset_sha256']=hashlib.sha256(json.dumps({'tables':v['tables'],'sequences':v['sequences']},sort_keys=True).encode()).hexdigest()
def synthetic_exception():
    from source_fixture import exception_record
    exc=AttributeError("'list' object has no attribute 'get'");return exception_record((AttributeError,exc,None))

def synthetic():
    prefix='owned.notifmut.0123456789';aliases={k:i+1 for i,k in enumerate(PLAN['seed']['notifications'])};rows=[]
    for k,v in PLAN['seed']['notifications'].items():
        row=copy.deepcopy(v);row.update(id=aliases[k],notification_key=prefix+'.'+k,expires_at='2026-11-11T00:00:00+00:00'if k=='E'else'2026-11-13T00:00:00+00:00',created_at='2026-11-12T00:00:00+00:00',updated_at='2026-11-12T00:00:00+00:00');rows.append(row)
    users=[{'id':n,'username':'notifmut_'+role+'_0123456789','user_level':attrs['user_level'],'is_active':True,'custom_properties':{'allowed_networks':{'UI':attrs['UI']}}}for n,(role,attrs)in enumerate((x for x in PLAN['seed']['users'].items()if x[1]),1)]
    ids={u['username'].split('_')[1]:u['id']for u in users}
    dismiss=[{'id':i+1,'notification_id':aliases[a],'user_id':ids[u],'action_taken':action,'dismissed_at':'2026-11-12T00:00:00+00:00'}for i,(u,a,action)in enumerate(PLAN['seed']['dismissals'])]
    state={'notifications':rows,'dismissals':dismiss};seed={k:copy.deepcopy(rows[v-1])for k,v in aliases.items()};runtime={'runtimes':{'live_proxy':{'present':False,'manager_keys':[]},'hls_proxy':{'present':False,'manager_keys':[]}},'receivers':{},'proxy_signals_loaded':False};cache={'rows':[],'capture_bytes':0}
    out={'scope':'SYNTHETIC CONTROL NEVER MEASURED','database':'rust_probe_notifmut_0123456789','forced_auth_framework_only':True,'credentials_or_TCP_verified':False,'capture_valid':True,'actual_request_count':48,'connections_close_called':True,'network_preflight':{'admin':True,'standard':True,'adminDenied':False},'client_raise_request_exception':{k:False for k in PLAN['seed']['users']},'actual_dependency_hashes':[{}for _ in range(9)],'seed_aliases':aliases,'seed_rows':copy.deepcopy(state),'seed_prefix':prefix,'seed_users':users,'owned_rows_after_isolation':{'notifications':[],'dismissals':[]},'requests':[]}
    out.update(seed_anchor_utc=timestamp(0),seed_started_utc=timestamp(0),seed_completed_utc=timestamp(0),seed_audit=audit(out['seed_rows'],out['database']),seed_binding_passed=True)
    from seed_binding import validate_seed
    out['seed_binding']=validate_seed(out,PLAN)
    changes={'unknown-condition-visible-after-prior-cache-miss':{'title':'U updated'},'put-required-only-retains-optionals':{'notification_key':prefix+'.A','title':'Put title','message':'Put message'},'normalize-json-string-number-bool-offset':{'notification_key':prefix+'.renamed','title':'123','message':'trimmed message','is_active':True,'admin_only':False,'expires_at':'2099-02-03T02:05:06+00:00','notification_type':'warning','priority':'critical'},'replace-json-object-not-merge':{'action_data':{'new':['v',None,False,1]}},'json-array-valid-system-notice':{'action_data':[1,{'x':True}]},'json-scalar-valid-system-notice':{'action_data':'text'},'restore-object-null-expiry-and-source':{'action_data':{},'expires_at':None,'source':'system','admin_only':True,'priority':'normal','notification_type':'info'},'inactive-transition-returns-saved-row':{'is_active':False},'expiry-transition-returns-saved-row':{'expires_at':'2000-01-01T00:00:00+00:00'},'set-truthy-nonobject-developer-action-data':{'source':'developer','action_data':[1]}}
    for case in PLAN['cases']:
        before=copy.deepcopy(state);target=aliases.get(case['target'],100009 if case['target']=='missing'else'not-an-integer');effect=case['effect_class'];i=case['index']
        if effect.startswith('delete-'):
            state['notifications']=[x for x in state['notifications']if x['id']!=target];state['dismissals']=[x for x in state['dismissals']if x['notification_id']!=target]
        elif effect!='none':
            notice=next(x for x in state['notifications']if x['id']==target);notice.update(copy.deepcopy(changes.get(case['name'],{})));notice['updated_at']=timestamp(i)
        if case['predicted_status']==200:
            n=next(x for x in state['notifications']if x['id']==target);body={k:v for k,v in n.items()if k!='updated_at'};body['is_dismissed']=any(d['notification_id']==target and d['user_id']==ids[case['actor']]for d in state['dismissals'])
        else:body={'detail':'synthetic error'}
        raw=b''if case['predicted_status']==204 else json.dumps(body).encode()
        row={'index':i,'name':case['name'],'role':case['actor'],'method':case['method'],'path':'/api/core/notifications/'+str(target)+'/','target_id':target,'request_payload':render(case['body'],prefix,seed),'resolver':{'kwargs':{'pk':str(target)},'url_name':'systemnotification-detail','namespaces':['api','core'],'class':'core.api_views.SystemNotificationViewSet','actions':{m.lower():a for m,a in PLAN['candidate_methods'].items()}},'client_raise_request_exception':False,'capture_complete':True,'before':before,'after':copy.deepcopy(state),'before_named_audit':audit(before,out['database']),'after_named_audit':audit(state,out['database']),'before_users':users,'after_users':users,'runtime_before':runtime,'runtime_after':runtime,'before_cache':cache,'after_cache':cache,'before_condition_cache':{'H':False,'V':True,'U':True,'D':None},'after_condition_cache':{'H':False,'V':True,'U':True,'D':None},'began_utc':timestamp(i),'completed_utc':timestamp(i),'transport_kind':'http_response','actual_status':case['predicted_status'],'body_hex':raw.hex(),'body_sha256':hashlib.sha256(raw).hexdigest(),'body_bytes':len(raw),'json':None if not raw else body,'framework_exception':synthetic_exception()if i==47 else None}
        out['requests'].append(row)
    return out
class EffectControls(unittest.TestCase):
    def setUp(self):self.raw=synthetic()
    def corrupt(self,fn):
        fn(self.raw);result=compare(self.raw,PLAN);self.assertFalse(result['passed'],result)
    def test_pristine_synthetic(self):
        result=compare(self.raw,PLAN);self.assertTrue(result['passed'],result['failures'])
    def test_partial(self):self.corrupt(lambda v:v.update(actual_request_count=47))
    def test_outer_catch(self):self.corrupt(lambda v:v.update(fixture_outer_exception={'phase':'requests'}))
    def test_credentials_claim(self):self.corrupt(lambda v:v.update(credentials_or_TCP_verified=True))
    def test_wrong_database(self):self.corrupt(lambda v:v.update(database='dispatcharr'))
    def test_denied_sql_mutation(self):self.corrupt(lambda v:v['requests'][0]['after']['notifications'][0].update(title='bad'))
    def test_denied_account_mutation(self):self.corrupt(lambda v:v['requests'][0].update(after_users=[]))
    def test_denied_audit_mutation(self):
        def f(v):a=v['requests'][2]['after_named_audit'];a['tables']['core_coresettings']['rows']+=1;rehash(a)
        self.corrupt(f)
    def test_sequence_advance(self):
        def f(v):a=v['requests'][30]['after_named_audit'];a['sequences']['public.core_systemnotification_id_seq']['last_value']+=1;rehash(a)
        self.corrupt(f)
    def test_wrong_route_action(self):self.corrupt(lambda v:v['requests'][4]['resolver']['actions'].update(patch='create'))
    def test_payload_changed(self):self.corrupt(lambda v:v['requests'][30].update(request_payload={'title':'wrong'}))
    def test_swapped_seed_alias(self):
        def f(v):a=v['seed_aliases'];a['A'],a['B']=a['B'],a['A']
        self.corrupt(f)
    def test_self_consistent_wrong_target(self):
        def f(v):r=v['requests'][0];r['target_id']=v['seed_aliases']['B'];r['path']='/api/core/notifications/'+str(r['target_id'])+'/';r['resolver']['kwargs']={'pk':str(r['target_id'])}
        self.corrupt(f)
    def test_wrong_derived_missing(self):
        def f(v):r=v['requests'][9];r['target_id']+=1;r['path']='/api/core/notifications/'+str(r['target_id'])+'/';r['resolver']['kwargs']={'pk':str(r['target_id'])}
        self.corrupt(f)
    def test_changed_seed_field(self):self.corrupt(lambda v:v['seed_rows']['notifications'][0].update(title='wrong seed'))
    def test_changed_seed_user_level(self):self.corrupt(lambda v:v['seed_users'][1].update(user_level=10))
    def test_changed_seed_network(self):self.corrupt(lambda v:v['seed_users'][2]['custom_properties']['allowed_networks'].update(UI='127.0.0.1/32'))
    def test_changed_seed_dismissal_pair(self):self.corrupt(lambda v:v['seed_rows']['dismissals'][0].update(notification_id=v['seed_aliases']['D']))
    def test_changed_seed_dismissal_action(self):self.corrupt(lambda v:v['seed_rows']['dismissals'][0].update(action_taken='wrong'))
    def test_exception_hash_corruption(self):self.corrupt(lambda v:v['requests'][-1]['framework_exception']['message_evidence'].update(original_sha256='0'*64))
    def test_exception_marked_truncated(self):self.corrupt(lambda v:v['requests'][-1]['framework_exception']['traceback_evidence'].update(complete=False,truncated=True))
    def test_unicode_exception_byte_metadata(self):
        from source_fixture import exception_record
        exc=ValueError('é'*3000);r=exception_record((ValueError,exc,None));e=r['message_evidence'];self.assertEqual(e['original_bytes'],6000);self.assertEqual(e['original_sha256'],hashlib.sha256(('é'*3000).encode()).hexdigest());self.assertEqual(e['captured_bytes'],4096);self.assertTrue(e['truncated']);self.assertFalse(e['complete']);self.assertFalse(r['complete'])
    def test_wrong_detail_path(self):self.corrupt(lambda v:v['requests'][0].update(path='/api/system/notifications/1/'))
    def test_JSON_boolean_not_integer(self):self.corrupt(lambda v:v['requests'][34]['after']['notifications'][0]['action_data']['new'].__setitem__(2,0))
    def test_status_changed(self):self.corrupt(lambda v:v['requests'][0].update(actual_status=200))
    def test_raw_body_hash(self):self.corrupt(lambda v:v['requests'][0].update(body_sha256='0'*64))
    def test_parsed_JSON_changed(self):self.corrupt(lambda v:v['requests'][30].update(json={'fake':True}))
    def test_field_retention(self):self.corrupt(lambda v:v['requests'][30]['after']['notifications'][0].update(priority='low'))
    def test_readonly_created_at(self):self.corrupt(lambda v:v['requests'][31]['after']['notifications'][0].update(created_at='2001-01-01T00:00:00Z'))
    def test_wrong_clock(self):self.corrupt(lambda v:v['requests'][30]['after']['notifications'][0].update(updated_at='2001-01-01T00:00:00Z'))
    def test_missing_cascade_child(self):self.corrupt(lambda v:v['requests'][41]['after']['dismissals'].append({'id':99,'notification_id':3,'user_id':1}))
    def test_delete_nonempty_body(self):self.corrupt(lambda v:v['requests'][41].update(body_hex='61',body_bytes=1,body_sha256=hashlib.sha256(b'a').hexdigest()))
    def test_final_missing_exception(self):self.corrupt(lambda v:v['requests'][-1].update(framework_exception=None))
    def test_final_raised_instead_of_HTTP(self):
        def f(v):r=v['requests'][-1];r['transport_kind']='framework_exception';r.pop('actual_status');r.pop('body_hex')
        self.corrupt(f)
    def test_raise_policy_hidden(self):self.corrupt(lambda v:v['requests'][-1].update(client_raise_request_exception=True))
    def test_proxy_started(self):self.corrupt(lambda v:v['requests'][0]['runtime_after']['runtimes']['live_proxy'].update(present=True))
    def test_cache_duplicate_key(self):self.corrupt(lambda v:v['requests'][0]['after_cache'].update(rows=[{'key_hex':'61','dump_hex':'61','pttl':3}]*2))
class GuardControls(unittest.TestCase):
    def test_db_names(self):
        self.assertEqual(database('rust_probe_notifmut_0123456789'),'rust_probe_notifmut_0123456789')
        for n in ['dispatcharr','postgres','rust_probe_notifmut_abc','rust_probe_notif_0123456789']:
            with self.assertRaises(ValueError):database(n)
    def test_default_unchanged(self):
        a=audit({'notifications':[],'dismissals':[]},'dispatcharr');self.assertTrue(unchanged_default(a,copy.deepcopy(a)));b=copy.deepcopy(a);b['tables']['accounts_user']['rows']=1;rehash(b);self.assertFalse(unchanged_default(a,b))
    def test_missing_sequence(self):
        a=audit({'notifications':[],'dismissals':[]},'dispatcharr');a['sequences'].popitem();rehash(a)
        with self.assertRaises(ValueError):dataset(a,'dispatcharr')
    def test_pid_reuse(self):
        row={'role':'shell','nonce':'abc','pid':41,'starttime':'12'};self.assertEqual(stop_record(row,'shell','abc',lambda _:'12'),41)
        with self.assertRaises(ValueError):stop_record(row,'shell','abc',lambda _:'13')
    def test_source_no_app_import_on_host(self):
        tree=ast.parse((ROOT/'controller/source_fixture.py').read_text())
        for node in tree.body:
            if isinstance(node,(ast.Import,ast.ImportFrom)):self.assertFalse(any(x in ast.unparse(node)for x in ['django','rest_framework','core.models']))
    def test_pure_bounded_collection(self):
        from bounded_process import Collected
        job=Collected([sys.executable,'-c','print("pure-control")'],dict(os.environ),128,lambda:None);self.assertEqual(job.wait(2),b'pure-control\n');self.assertTrue(job.physically_settled)
    def test_pure_overflow_reaps(self):
        from bounded_process import Collected,CollectionFailure
        stopped=[];job=Collected([sys.executable,'-c','print("x"*4096)'],dict(os.environ),128,lambda:stopped.append(True))
        with self.assertRaises(CollectionFailure):job.wait(2)
        self.assertTrue(stopped);self.assertIsNotNone(job.process.poll());self.assertTrue(all(not t.is_alive()for t in job.threads))

class SealControls(unittest.TestCase):
    def test_local_seal(self):
        from run import check_seal
        self.assertEqual(check_seal(ROOT/'seal.json')['fixed_request_count'],48)
    def corrupt(self,fn):
        import tempfile
        from run import check_seal
        v=json.loads((ROOT/'seal.json').read_text());fn(v)
        with tempfile.TemporaryDirectory()as d:
            f=Path(d)/'seal.json';f.write_text(json.dumps(v))
            with self.assertRaises(ValueError):check_seal(f)
    def test_seal_wrong_count(self):self.corrupt(lambda v:v.update(fixed_request_count=49))
    def test_seal_wrong_image(self):self.corrupt(lambda v:v.update(image='latest'))
    def test_seal_missing_input(self):self.corrupt(lambda v:v['files'].pop())
    def test_seal_wrong_blob(self):self.corrupt(lambda v:v['files'][0].update(sha256='0'*64))
    def test_seal_extra_path(self):self.corrupt(lambda v:v['files'].append({'path':'../outside','bytes':0,'sha256':'0'*64}))
    def test_default_no_Docker(self):
        import subprocess
        p=subprocess.run([sys.executable,str(ROOT/'controller/run.py'),'--docker','/this-does-not-exist'],capture_output=True,timeout=10)
        self.assertEqual(p.returncode,0,p.stderr);self.assertFalse(json.loads(p.stdout)['Source_executed'])
    def test_wrong_review_hash_no_output(self):
        import tempfile,subprocess
        with tempfile.TemporaryDirectory()as d:
            out=Path(d)/'not-created';p=subprocess.run([sys.executable,str(ROOT/'controller/run.py'),'--execute-reviewed-seal','0'*64,'--output',str(out),'--docker','/this-does-not-exist'],capture_output=True,timeout=10)
            self.assertNotEqual(p.returncode,0);self.assertFalse(out.exists());self.assertIn(b'ReviewedSealHashBinding',p.stderr)

class SignalSerializationControls(unittest.TestCase):
    def test_Django6_third_slot_weakref_roundtrip(self):
        import weakref
        from source_fixture import signal_registration
        called=[]
        def receiver(*args,**kwargs):called.append(True);raise AssertionError('must never invoke receiver')
        class Sender:pass
        # Source1 failed because third tuple slot was ReferenceType; preserve it,
        # plus any later bool slot, without assigning slot-index meanings.
        row=((id(receiver),id(Sender)),weakref.ref(receiver),weakref.ref(Sender),False)
        value=signal_registration(row);encoded=json.dumps(value,sort_keys=True);decoded=json.loads(encoded)
        self.assertEqual(decoded['slot_count'],4);self.assertEqual([v['index']for v in decoded['slots']],[0,1,2,3])
        self.assertEqual(decoded['slots'][2]['type'],'weakref.ReferenceType');self.assertEqual(decoded['slots'][2]['kind'],'weak-reference');self.assertTrue(decoded['slots'][2]['alive'])
        self.assertTrue(decoded['slots'][2]['referent']['qualname'].endswith('Sender'));self.assertEqual(decoded['slots'][3]['value'],False);self.assertNotIn('async',decoded);self.assertFalse(called)
        import tempfile
        from source_fixture import save
        with tempfile.TemporaryDirectory()as d:
            path=Path(d)/'snapshot.json';snapshot={'runtime_before_isolation':{'runtimes':{'live_proxy':{'present':False,'manager_keys':[]}},'receivers':{'pre_delete':[value]},'proxy_signals_loaded':False}}
            save(path,snapshot);self.assertEqual(json.loads(path.read_text()),snapshot)
    def test_three_slots_with_no_async_assumption(self):
        import weakref
        from source_fixture import signal_registration
        class Sender:pass
        value=signal_registration(((1,2),None,weakref.ref(Sender)));self.assertEqual(value['slot_count'],3);self.assertEqual(value['slots'][1]['value'],None);json.dumps(value)
    def test_bound_method_metadata_without_invocation(self):
        import weakref
        from source_fixture import signal_registration
        class Receiver:
            def receive(self):raise AssertionError('must not invoke method')
        obj=Receiver();value=signal_registration(((1,2),obj.receive,weakref.WeakMethod(obj.receive)))
        self.assertTrue(value['slots'][1]['entity']['qualname'].endswith('Receiver.receive'));self.assertTrue(value['slots'][2]['bound_method_function']['qualname'].endswith('Receiver.receive'));json.dumps(value)
    def test_dead_weakref_is_explicit(self):
        import weakref,gc
        from source_fixture import signal_registration
        class Sender:pass
        obj=Sender();ref=weakref.ref(obj);del obj;gc.collect()
        value=signal_registration(((1,2),ref,True));self.assertFalse(value['slots'][1]['alive']);self.assertIsNone(value['slots'][1]['referent']);json.dumps(value)
    def test_no_custom_repr_or_callable_invocation(self):
        from source_fixture import signal_registration
        class Receiver:
            def __repr__(self):raise AssertionError('repr must not run')
            def __call__(self):raise AssertionError('call must not run')
        value=signal_registration((('uid',2),Receiver(),b'raw'));self.assertEqual(value['slots'][1]['kind'],'opaque-object-metadata');self.assertEqual(value['slots'][2]['value'],'726177');json.dumps(value)

if __name__=='__main__':unittest.main()
