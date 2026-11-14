"""Public projected-reference version of the71 pure controls; original test bytes remain exact blobs."""
"""Finite pure acceptance/refusal controls. No sockets, Docker, Cargo or app."""
import copy,hashlib,json,unittest,tempfile
from pathlib import Path
import validate_remaining as V
import origin as O
ROOT=Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0,str(ROOT))
class Controls(unittest.TestCase):
 def test_SQL_lexical_timestamp_same_instant(self):V.sql_equal({'updated_at':'2025-01-01T00:00:00Z'},{'updated_at':'2025-01-01T00:00:00.000000+00:00'})
 def test_SQL_wrong_instant(self):
  with self.assertRaises(ValueError):V.sql_equal({'updated_at':'2025-01-01T00:00:00Z'},{'updated_at':'2025-01-01T00:00:01Z'})
 def test_SQL_naive(self):
  with self.assertRaises(ValueError):V.sql_equal({'updated_at':'2025-01-01T00:00:00Z'},{'updated_at':'2025-01-01T00:00:00'})
 def test_SQL_bool_not_int(self):
  with self.assertRaises(ValueError):V.sql_equal({'id':1},{'id':True})
 def test_SQL_stable_text_not_clock_normalized(self):
  with self.assertRaises(ValueError):V.sql_equal({'url':'2025-01-01T00:00:00Z'},{'url':'2025-01-01T00:00:00.000000+00:00'})
 def test_SQL_missing_child(self):
  with self.assertRaises(ValueError):V.sql_equal([{'id':951},{'id':952}],[{'id':951}])
 def test_SQL_reordered_child(self):
  with self.assertRaises(ValueError):V.sql_equal([{'id':951},{'id':952}],[{'id':952},{'id':951}])
 def test_SQL_field_dropped(self):
  with self.assertRaises(ValueError):V.sql_equal({'id':951,'slug':'keep'},{'id':951})
 def test_origin_refresh_exact(self):self.assertEqual(O.reply(2,'/refresh.json',1,b'owned'),b'owned')
 def test_origin_frames_exact(self):
  for i,path in enumerate(['/physical/frame-1.json','/physical/frame-2.json','/physical/frame-reuse.json'],1):self.assertIn(b'Owned Physical',O.reply(4,path,i,b''))
 def test_origin_fifth_forbidden(self):
  with self.assertRaises(ValueError):O.reply(4,'/physical/forbidden-fifth.json',3,b'')
 def test_origin_reorder(self):
  with self.assertRaises(ValueError):O.reply(4,'/physical/frame-2.json',1,b'')
 def test_origin_extra(self):
  with self.assertRaises(ValueError):O.reply(4,'/physical/frame-reuse.json',4,b'')
 def test_origin_bool_case(self):
  with self.assertRaises(ValueError):O.reply(True,'/physical/frontend-fd-loss.json',1,b'')
 def test_origin_unknown_path(self):
  with self.assertRaises(ValueError):O.reply(1,'/unsealed',1,b'')

def samples():return[{'elapsed_seconds':0.,'original_weak':[True],'available_original_four_slots':3,'physical_cache_operations':0},{'elapsed_seconds':.3,'original_weak':[True],'available_original_four_slots':3,'physical_cache_operations':0}]
def sample_negative(name,fn):
 def test(self):
  value=samples();fn(value)
  with self.assertRaises(ValueError):V.samples(value)
 setattr(Controls,'test_sample_'+name,test)
for name,fn in [('short',lambda x:x[1].__setitem__('elapsed_seconds',.1)),('nan',lambda x:x[0].__setitem__('elapsed_seconds',float('nan'))),('inf',lambda x:x[1].__setitem__('elapsed_seconds',float('inf'))),('boolclock',lambda x:x[0].__setitem__('elapsed_seconds',False)),('reversed',lambda x:x.reverse()),('retired',lambda x:x[1].__setitem__('original_weak',[False])),('newallowance',lambda x:x[0].__setitem__('available_original_four_slots',4)),('boolslot',lambda x:x[0].__setitem__('available_original_four_slots',True)),('missing',lambda x:x.pop())]:sample_negative(name,fn)
def wire():
 c=json.loads((ROOT/'inputs/pg-contracts.json').read_bytes())['save_ready'];return c,{'backend_pid':123,'sql':c['sql'],'rows':1,'commands':['UPDATE 1'],'errors':[],'tx':'I','parameter_hex':[c['parameter_hex'][0],c['parameter_hex'][1],c['parameter_hex'][2],'0000000000000000','0000000000000001'],'sync_seen':True}
def wire_negative(name,fn):
 def test(self):
  c,w=wire();fn(w)
  with self.assertRaises(ValueError):V.pg_wire(w,c,{'actual_backend_pid':123})
 setattr(Controls,'test_wire_'+name,test)
for name,fn in [('backend',lambda x:x.__setitem__('backend_pid',124)),('rowsbool',lambda x:x.__setitem__('rows',True)),('wrongSQL',lambda x:x.__setitem__('sql','SELECT 1')),('describeReady',lambda x:x.__setitem__('sync_seen',False)),('command',lambda x:x.__setitem__('commands',['UPDATE 0'])),('error',lambda x:x.__setitem__('errors',['57014'])),('transaction',lambda x:x.__setitem__('tx','T')),('parameter',lambda x:x['parameter_hex'].__setitem__(0,'0000000000000322')),('timestampLength',lambda x:x['parameter_hex'].__setitem__(3,'00')),('jsonbVersion',lambda x:x['parameter_hex'].__setitem__(2,'02'+x['parameter_hex'][2][2:]))]:wire_negative(name,fn)
def cache():return[{'database':n,'inventory':{'raw_DUMP_PTTL':{'6b6579':{'dump_hex':'6f776e6564','pttl':1000}}}}for n in range(4)]
def cache_negative(name,fn):
 def test(self):
  a=cache();b=copy.deepcopy(a);fn(b)
  with self.assertRaises((ValueError,KeyError)):V.raw_cache(a,b)
 setattr(Controls,'test_cache_'+name,test)
for name,fn in [('missingDB',lambda x:x.pop()),('DBswap',lambda x:x.reverse()),('dropkey',lambda x:x[0]['inventory']['raw_DUMP_PTTL'].clear()),('value',lambda x:x[1]['inventory']['raw_DUMP_PTTL']['6b6579'].__setitem__('dump_hex','6f74686572')),('ttlrenewal',lambda x:x[1]['inventory']['raw_DUMP_PTTL']['6b6579'].__setitem__('pttl',1001)),('ttlbool',lambda x:x[1]['inventory']['raw_DUMP_PTTL']['6b6579'].__setitem__('pttl',True))]:cache_negative(name,fn)
class Baselines(unittest.TestCase):
 def test_samples(self):self.assertEqual(V.samples(samples()),.3)
 def test_wire(self):c,w=wire();V.pg_wire(w,c,{'actual_backend_pid':123})
 def test_raw_cache(self):a=cache();b=copy.deepcopy(a);b[0]['inventory']['raw_DUMP_PTTL']['6b6579']['pttl']=900;V.raw_cache(a,b)


class FiniteReadyClocks(unittest.TestCase):
 def events(self):return[{'kind':'ready-held','stage':'save_ready','monotonic':1.},{'kind':'ready-forwarded','stage':'save_ready','monotonic':1.4}]
 def test_actual_hold_covers_samples_and_disposition(self):self.assertAlmostEqual(V.pg_hold(self.events(),'save_ready',.35),.4)
 def reject(self,fn):
  values=self.events();fn(values)
  with self.assertRaises(ValueError):V.pg_hold(values,'save_ready',.35)
 def test_zero(self):self.reject(lambda r:r[1].__setitem__('monotonic',1.))
 def test_nan(self):self.reject(lambda r:r[0].__setitem__('monotonic',float('nan')))
 def test_inf(self):self.reject(lambda r:r[1].__setitem__('monotonic',float('inf')))
 def test_bool(self):self.reject(lambda r:r[0].__setitem__('monotonic',True))
 def test_reverse(self):self.reject(lambda r:r[1].__setitem__('monotonic',.9))
 def test_shorter_than_disposition_and_sample_span(self):self.reject(lambda r:r[1].__setitem__('monotonic',1.3))
 def test_over25(self):self.reject(lambda r:r[1].__setitem__('monotonic',27.))
 def test_missing_forward(self):self.reject(lambda r:r.pop())
 def test_wrong_stage(self):self.reject(lambda r:r[1].__setitem__('stage','unknown'))
 def test_jsonb_preserved_input_order_not_sorted(self):
  c,w=wire();doc=json.loads((ROOT/'inputs/refresh-origin-body.json').read_bytes());w['parameter_hex'][2]=(b'\x01'+json.dumps(doc,sort_keys=True,separators=(',',':')).encode()).hex()
  with self.assertRaises(ValueError):V.pg_wire(w,c,{'actual_backend_pid':123})


class RefreshScope(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.source=__import__('source_reference').load_source()
 def context(self):
  source=self.source;before=copy.deepcopy(source['seed']);ids={r['id']for r in before['repos']};after=copy.deepcopy(before);after['repos']=[copy.deepcopy(r)for r in source['requests'][13]['after']['repos']if r['id']in ids];after['configs']=copy.deepcopy(source['requests'][13]['after']['configs']);held={'tables':{'plugins_pluginrepo':copy.deepcopy(after['repos'])}};begin=V.instant(source['requests'][13]['began_utc']);end=V.instant(source['requests'][13]['completed_utc']);return before,after,held,begin,end
 def test_refresh_baseline(self):a,b,h,begin,end=self.context();self.assertTrue(V.refresh_delta(self.source,a,b,h,begin,end))
 def reject(self,fn):
  a,b,h,begin,end=self.context();fn(b,h)
  with self.assertRaises((ValueError,KeyError)):V.refresh_delta(self.source,a,b,h,begin,end)
 def test_foreign_repository(self):self.reject(lambda b,h:next(r for r in b['repos']if r['id']==802).__setitem__('name','unmeasured'))
 def test_extra_repo(self):self.reject(lambda b,h:b['repos'].append(copy.deepcopy(b['repos'][0])))
 def test_foreign_config(self):self.reject(lambda b,h:next(r for r in b['configs']if r['id']==953).__setitem__('slug','unmeasured'))
 def test_dropped_child_absent(self):self.reject(lambda b,h:b['configs'].pop(0))
 def test_child_reordered(self):self.reject(lambda b,h:b['configs'].reverse())
 def test_drop_timestamp_must_stay(self):self.reject(lambda b,h:next(r for r in b['configs']if r['id']==951).__setitem__('updated_at','2000-01-01T00:00:00Z'))
 def test_committed_save_matches_final(self):self.reject(lambda b,h:next(r for r in h['tables']['plugins_pluginrepo']if r['id']==801).__setitem__('name','unmeasured'))
 def test_config_truthiness_measured(self):self.reject(lambda b,h:next(r for r in b['configs']if r['id']==952).__setitem__('deprecated',False))


class ObserverCorrection(unittest.TestCase):
 def fixture(self,out):
  a={'actual_backend_pid':123,'actual_database':'rust_probe_pluginrepo_0123456789','actual_application_name':'unsigned-remaining-observer','actual_client_address':'127.0.0.1','actual_client_port':12345,'socket':{'actual_local_port':12345,'actual_remote_port':5432,'original_socket_link':'socket:[999]','original_fds':[8],'native_PID':44,'PID_namespace':'pid:[1]','network_namespace':'net:[2]'}};b={'actual_backend_pid':123,'explicit_connection_close_awaited_once':True,'actual_original_FDs_after_close':[{'fd':8,'after_link':None,'original_generation_absent':True}]};d={'passed':True,'max':1,'whole_drain_budget_seconds':10,'first_pool_close_awaited_once':True,'second_pool_close_or_query_retry':False,'pool_closed':True,'pool_size':0,'actual_connections_before_close':[a],'actual_explicit_closes':[b]};report={'observer_close_pending':False,'observer_drain':d};(out/'observer-drain-before-close.json').write_text(json.dumps({'capture_complete':True,'first_pool_close_not_called_yet':True,'exact_existing_max_checked_out':1,'actual_connections':[a]}));(out/'observer-drain-progress.json').write_text(json.dumps(d));(out/'remaining-peer-began.json').write_text(json.dumps({'owned_database':a['actual_database']}));return report
 def reject(self,change):
  with tempfile.TemporaryDirectory()as tmp:
   out=Path(tmp);v=self.fixture(out);change(v);(out/'observer-drain-progress.json').write_text(json.dumps(v['observer_drain']))
   with self.assertRaises(ValueError):V.observer_drain(out,v,1)
 def test_baseline(self):
  with tempfile.TemporaryDirectory()as tmp:
   out=Path(tmp);self.assertTrue(V.observer_drain(out,self.fixture(out),1)['first_pool_close_size0'])
 def test_size1_is_not_waived(self):self.reject(lambda v:v['observer_drain'].__setitem__('pool_size',1))
 def test_bool_size(self):self.reject(lambda v:v['observer_drain'].__setitem__('pool_size',False))
 def test_second_close_refused(self):self.reject(lambda v:v['observer_drain'].__setitem__('second_pool_close_or_query_retry',True))
 def test_original_FD_still_present(self):self.reject(lambda v:v['observer_drain']['actual_explicit_closes'][0]['actual_original_FDs_after_close'][0].__setitem__('after_link','socket:[999]'))
 def test_other_backend(self):self.reject(lambda v:v['observer_drain']['actual_explicit_closes'][0].__setitem__('actual_backend_pid',124))
 def test_close_not_awaited(self):self.reject(lambda v:v['observer_drain']['actual_explicit_closes'][0].__setitem__('explicit_connection_close_awaited_once',False))
 def test_pending_close(self):self.reject(lambda v:v.__setitem__('observer_close_pending',True))
if __name__=='__main__':unittest.main()
