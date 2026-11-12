"""Pure synthetic/controller corruption controls only. No Source/Native app or Docker."""
import copy,hashlib,json,os,subprocess,sys,tempfile,unittest
from pathlib import Path
from validate_native import TABLES,NOTICE,DISMISS,mutations,legacy,owned,audit,compare_values,clock
ROOT=Path(__file__).resolve().parent.parent
SOURCE=json.loads((ROOT/'inputs/Source48.json').read_text())
def state(rows,sequences):
    tables={t:[]for t in TABLES};tables[NOTICE]=copy.deepcopy(rows['notifications']);tables[DISMISS]=copy.deepcopy(rows['dismissals']);tables['accounts_user']=copy.deepcopy(SOURCE['seed_users'])
    return {'tables':tables,'sequences':{t:copy.deepcopy(sequences['public.'+t+'_id_seq'])for t in TABLES}}
def encode(row,parsed):
    raw=b''if parsed is None else json.dumps(parsed).encode();row.update(json=parsed,body=parsed,body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),body_bytes=len(raw))
def projected_cache(source_cache,values):
    database='rust_probe_notif_native_0123456789';rows=copy.deepcopy(source_cache['rows']);bykey={r['key_hex']:r for r in rows}
    def entry(key,value,marker=False):
        encoded=key.encode().hex();raw=None if value is None else value.encode()if marker else bytes([0x80,4,0x88 if value else 0x89,0x2e])
        return {'key_hex':encoded,'value':value,'present':raw is not None,'raw_hex':None if raw is None else raw.hex(),'raw_bytes':0 if raw is None else len(raw),'raw_sha256':None if raw is None else hashlib.sha256(raw).hexdigest(),'decoded':True,'pttl':-1 if marker else -2 if raw is None else bykey[encoded]['pttl']}
    key='rust-probe-notification-native-owner';marker=entry(key,database,True);rows.append({'key_hex':key.encode().hex(),'dump_hex':b'synthetic-sentinel-dump'.hex(),'pttl':-1})
    admin=SOURCE['seed_binding']['actors']['admin'];entries={alias:entry(':1:dev_notif_condition_'+str(SOURCE['seed_aliases'][alias])+'_'+str(admin),values[alias])for alias in ['H','V','U','D']}
    captured=sum(len(bytes.fromhex(e['key_hex']))+e['raw_bytes']for e in [marker,*entries.values()]);named={'expected_database':database,'sentinel':marker,'values':copy.deepcopy(values),'entries':entries,'capture_bytes':captured}
    total=sum(len(bytes.fromhex(r['key_hex']))+len(bytes.fromhex(r['dump_hex']))for r in rows)+captured
    return {'rows':rows,'capture_bytes':total,'named':named}

def synthetic_mutations():
    seeded=state(SOURCE['seed_rows'],SOURCE['seed_audit']['sequences']);empty=copy.deepcopy(seeded);empty['tables'][NOTICE]=[];empty['tables'][DISMISS]=[]
    result={'passed':True,'scope':'SYNTHETIC PURE CONTROL NOT MEASURED','database':'rust_probe_notif_native_0123456789','owned_server_aborted_and_pool_closed':True,'Source48_raw_sha256':'68542935c46471505dfbc9e1442e45fa9cbe498ef2c587fe0c257fb2ebd8b1bd','final500_renderer_gap_predeclared':True,'isolated_state':empty,'seeded_state':seeded,'cache_after_seed':projected_cache(SOURCE['cache_after_setup'],{'H':False,'V':True,'U':None,'D':None}),'cases':[],'extra_cases':[]}
    cache={'rows':[],'capture_bytes':0}
    for i,s in enumerate(SOURCE['requests']):
        a=state(s['before'],s['before_named_audit']['sequences']);b=state(s['after'],s['after_named_audit']['sequences']);credentials={'accounts_user':copy.deepcopy(SOURCE['seed_users']),'core_systemevent':[],'core_systemevent_sequence':{'last_value':1,'is_called':False}}
        row={k:copy.deepcopy(s[k])for k in ['index','name','role','method','path','request_payload','request_body_encoding','began_utc','completed_utc']};row.update(status=s['actual_status'],before=a,after=b,before_credentials=copy.deepcopy(credentials),after_credentials=copy.deepcopy(credentials),before_cache=projected_cache(s['before_cache'],s['before_condition_cache']),after_cache=projected_cache(s['after_cache'],s['after_condition_cache']),capture_complete=True)
        encode(row,{'detail':'Notification processing failed.'}if i==47 else s['json']);result['cases'].append(row)
    for i,(name,role,method,status)in enumerate([('native-poison-before-malformed-pk-body','admin','PATCH',500),('native-standard-before-poison','standard','PATCH',403),('native-anonymous-before-poison','anonymous','DELETE',401)]):
        row=copy.deepcopy(result['cases'][-1]);row.update(index=48+i,name=name,role=role,method=method,status=status,path='/api/core/notifications/not-an-integer/',request_payload='{',request_body_encoding='raw-invalid-json');row['before']=copy.deepcopy(row['after']);encode(row,{'detail':'Notification processing failed.'}if status==500 else{'detail':'You do not have permission to perform this action.'}if status==403 else{'detail':'Authentication credentials were not provided.'});result['extra_cases'].append(row)
    clocks={}
    for row in SOURCE['requests']:
        if row['actual_status']==200:
            value=next(n['updated_at']for n in row['after']['notifications']if n['id']==row['target_id']);clocks[clock(value)]={'Source':value,'Native':value}
    result['clock_mapping']=list(clocks.values())
    return result

def synthetic_legacy():
    v=json.loads((ROOT/'inputs/legacy-qualified8plus8.json').read_text());v['scope']='SYNTHETIC QA3-DELTA CONTROL, NOT A NEW NATIVE RESULT';previous=v['extra_cases'][4]['after']
    for i,(name,status)in enumerate([('mutation-put-requires-fields',400),('mutation-empty-patch-touches-updated-at',200),('mutation-delete-cascades-target',204)],5):
        r=v['extra_cases'][i];r.update(name=name,status=status,before=copy.deepcopy(previous));after=copy.deepcopy(previous)
        if i==6:after['tables'][NOTICE][0]['updated_at']=r['began_utc']
        if i==7:after['tables'][NOTICE]=[x for x in after['tables'][NOTICE]if x['id']!=1];after['tables'][DISMISS]=[x for x in after['tables'][DISMISS]if x['notification_id']!=1]
        r['after']=after;previous=after;parsed=None if i==7 else{'detail':'synthetic'};encode(r,parsed)
    return v
class MutationControls(unittest.TestCase):
    def setUp(self):self.v=synthetic_mutations()
    def reject(self,fn):
        fn(self.v)
        with self.assertRaises(ValueError):mutations(self.v,SOURCE)
    def test_pristine(self):self.assertTrue(mutations(self.v,SOURCE)['passed'])
    def test_partial(self):self.reject(lambda v:v['cases'].pop())
    def test_wrong_sourcehash(self):self.reject(lambda v:v.update(Source48_raw_sha256='0'*64))
    def test_wrong_seedfield(self):self.reject(lambda v:v['seeded_state']['tables'][NOTICE][0].update(title='wrong'))
    def test_wrong_seedsequence(self):self.reject(lambda v:v['seeded_state']['sequences'][NOTICE].update(last_value=10))
    def test_initial_cached_bool_corrupt(self):
        def f(v):r=next(x for x in v['cache_after_seed']['rows']if bytes.fromhex(x['key_hex']).decode().endswith('dev_notif_condition_6_1'));r['dump_hex']='00'
        self.reject(f)
    def test_initial_U_not_absent(self):
        def f(v):admin=SOURCE['seed_binding']['actors']['admin'];key=(':1:dev_notif_condition_'+str(SOURCE['seed_aliases']['U'])+'_'+str(admin)).encode().hex();v['cache_after_seed']['rows'].append({'key_hex':key,'dump_hex':'00','pttl':1})
        self.reject(f)
    def test_all_cache_rows_cleared(self):self.reject(lambda v:v['cases'][9]['after_cache'].update(rows=[]))
    def test_U_after_case9_missing_raw(self):
        def f(v):key=(':1:dev_notif_condition_'+str(SOURCE['seed_aliases']['U'])+'_'+str(SOURCE['seed_binding']['actors']['admin'])).encode().hex();v['cases'][9]['after_cache']['rows']=[r for r in v['cases'][9]['after_cache']['rows']if r['key_hex']!=key]
        self.reject(f)
    def test_H_before_request_dropped(self):self.reject(lambda v:v['cases'][0]['before_cache']['named']['values'].update(H=None))
    def test_V_after_delete45_dropped(self):self.reject(lambda v:v['cases'][45]['after_cache']['named']['values'].update(V=None))
    def test_owned_sentinel_changed(self):self.reject(lambda v:v['cases'][9]['after_cache']['named']['sentinel'].update(value='rust_probe_notif_native_abcdef1234'))
    def test_named_GET_hash_bad(self):self.reject(lambda v:v['cases'][9]['after_cache']['named']['entries']['U'].update(raw_sha256='0'*64))
    def test_generated_clock_mapping_missing(self):self.reject(lambda v:v['clock_mapping'].pop())
    def test_wrong_role(self):self.reject(lambda v:v['cases'][0].update(role='admin'))
    def test_wrong_target(self):self.reject(lambda v:v['cases'][0].update(path='/api/core/notifications/2/'))
    def test_wrong_bodyhash(self):self.reject(lambda v:v['cases'][30].update(body_sha256='0'*64))
    def test_wrong_payload(self):self.reject(lambda v:v['cases'][30].update(request_payload={}))
    def test_sequence_advance(self):self.reject(lambda v:v['cases'][0]['after']['sequences'][NOTICE].update(last_value=10))
    def test_credential_event_not_masked(self):self.reject(lambda v:v['cases'][0]['after_credentials']['core_systemevent'].append({'id':1}))
    def test_raw_credentials_not_accounts(self):self.reject(lambda v:v['cases'][0]['before_credentials'].update(accounts_user=[]))
    def test_other_table_changed(self):self.reject(lambda v:v['cases'][0]['after']['tables']['core_coresettings'].append({'id':99}))
    def test_native_clock_outside_request(self):self.reject(lambda v:v['cases'][30]['after']['tables'][NOTICE][0].update(updated_at='2001-01-01T00:00:00Z'))
    def test_cascade_omitted(self):self.reject(lambda v:v['cases'][41]['after']['tables'][DISMISS].append({'id':99}))
    def test_204_body_not_empty(self):
        def f(v):r=v['cases'][41];encode(r,{'detail':'wrong'})
        self.reject(f)
    def test_renderer_gap_hidden(self):self.reject(lambda v:v.update(final500_renderer_gap_predeclared=False))
    def test_native_extra_order(self):self.reject(lambda v:v['extra_cases'][0].update(status=404))
    def test_pending_capture(self):self.reject(lambda v:v['cases'][0].update(capture_complete=False))
    def test_duplicate_cachekey(self):self.reject(lambda v:v['cases'][0]['after_cache'].update(rows=[{'key_hex':'61','dump_hex':'61','pttl':1}]*2))
class ClockMapControls(unittest.TestCase):
    def test_mapped_source_instant_cannot_rewind(self):
        source='2026-11-12T10:00:00Z';native='2026-11-12T10:00:01Z';mapping={clock(source):clock(native)};case={'began_utc':source,'completed_utc':source};row={'began_utc':native,'completed_utc':native}
        with self.assertRaises(ValueError):compare_values(source,source,case,row,mapping,'/updated_at')
    def test_generated_coincident_instant_registered(self):
        source='2026-11-12T10:00:00Z';mapping={};case={'began_utc':source,'completed_utc':source};compare_values(source,source,case,case,mapping,'/updated_at');self.assertEqual(mapping,{clock(source):clock(source)})
        changed='2026-11-12T10:00:01Z';later={'began_utc':changed,'completed_utc':changed}
        with self.assertRaises(ValueError):compare_values(source,changed,case,later,mapping,'/updated_at')
    def test_expiry_fractional_zeros_same_instant(self):
        source='2026-11-13T17:09:47.428830+00:00';native='2026-11-13T17:09:47.42883+00:00';mapping={};compare_values(source,native,{}, {},mapping,'/expires_at');self.assertEqual(mapping,{})
    def test_expiry_different_instant_rejected(self):
        with self.assertRaises(ValueError):compare_values('2026-11-13T17:09:47.428830+00:00','2026-11-13T17:09:47.428831+00:00',{}, {},{},'/expires_at')
    def test_unmapped_seed_equal_is_allowed(self):
        source='2026-11-12T09:00:00Z';case={'began_utc':'2026-11-12T10:00:00Z','completed_utc':'2026-11-12T10:00:01Z'};mapping={};compare_values(source,source,case,case,mapping,'/created_at');self.assertEqual(mapping,{})

class LegacyControls(unittest.TestCase):
    def setUp(self):self.v=synthetic_legacy()
    def reject(self,fn):
        fn(self.v)
        with self.assertRaises(ValueError):legacy(self.v)
    def test_pristine(self):self.assertTrue(legacy(self.v)['first5_extra_definitions_preserved'])
    def test_first5_changed(self):self.reject(lambda v:v['extra_cases'][0].update(status=200))
    def test_old501_not_accepted(self):self.reject(lambda v:v['extra_cases'][5].update(status=501))
    def test_empty_patch_otherfield(self):self.reject(lambda v:v['extra_cases'][6]['after']['tables'][NOTICE][0].update(title='wrong'))
    def test_empty_patch_clock(self):self.reject(lambda v:v['extra_cases'][6]['after']['tables'][NOTICE][0].update(updated_at='2001-01-01T00:00:00Z'))
    def test_delete_cascade_omitted(self):self.reject(lambda v:v['extra_cases'][7]['after']['tables'][DISMISS].append({'id':99}))
class ControllerControls(unittest.TestCase):
    def test_owned_guard(self):
        self.assertEqual(owned('rust_probe_notif_native_0123456789'),'rust_probe_notif_native_0123456789')
        for n in ['dispatcharr','postgres','rust_probe_notif_native_abc','rust_probe_notifmut_0123456789']:
            with self.assertRaises(ValueError):owned(n)
    def test_default_seal(self):
        from run import check_seal
        self.assertIn(check_seal(ROOT/'seal.json')['state'],['BUILD_REVIEW_ONLY_NO_EXECUTABLES','SEALED_NATIVE_FIXTURE_REQUIRES_ROOT_EXECUTION_REVIEW'])
    def test_default_has_no_Docker(self):
        p=subprocess.run([sys.executable,str(ROOT/'controller/run.py'),'--docker','/does-not-exist'],capture_output=True,timeout=10);self.assertEqual(p.returncode,0,p.stderr);self.assertFalse(json.loads(p.stdout)['Source_Native_or_Docker_executed'])
    def test_build_review_forbids_execution(self):
        from run import sha
        with tempfile.TemporaryDirectory()as d:
            state=json.loads((ROOT/'seal.json').read_text())['state'];blocked=state=='BUILD_REVIEW_ONLY_NO_EXECUTABLES';flag=sha(ROOT/'seal.json')if blocked else'0'*64
            output=Path(d)/'not-created';p=subprocess.run([sys.executable,str(ROOT/'controller/run.py'),'--execute-reviewed-seal',flag,'--output',str(output),'--docker','/does-not-exist'],capture_output=True,timeout=10);self.assertNotEqual(p.returncode,0);self.assertIn(b'BuildReviewExecutionForbidden'if blocked else b'ExactReviewedExecutionSeal',p.stderr);self.assertFalse(output.exists())
    def test_default_audit_digest_and_sequence(self):
        v=copy.deepcopy(SOURCE['seed_audit']);v['database']='dispatcharr';self.assertTrue(audit(v));v['sequences'].popitem()
        with self.assertRaises(ValueError):audit(v)
    def test_seal_corruption(self):
        from run import check_seal
        v=json.loads((ROOT/'seal.json').read_text());v['files'][0]['sha256']='0'*64
        with tempfile.TemporaryDirectory()as d:
            p=Path(d)/'seal.json';p.write_text(json.dumps(v))
            with self.assertRaises(ValueError):check_seal(p)
    def test_bounded_child(self):
        from bounded_process import Collected
        job=Collected([sys.executable,'-c','print("pure-control")'],dict(os.environ),128,lambda:None);self.assertEqual(job.wait(2),b'pure-control\n');self.assertTrue(job.physically_settled)
if __name__=='__main__':unittest.main()
