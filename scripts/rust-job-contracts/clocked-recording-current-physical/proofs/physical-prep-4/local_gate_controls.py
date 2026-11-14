"""Process/socket-free literal wire gate controls; no fabricated actual evidence."""
import copy,importlib.util,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'tools'))
for name in ['physical_pg_proxy','redis_reply_proxy']:
 spec=importlib.util.spec_from_file_location(name,ROOT/'tools'/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);globals()[name]=m
pg=physical_pg_proxy;resp=redis_reply_proxy
class Controls(unittest.TestCase):
 def setup_wire(self):
  old={'status':'scheduled','season':1,'episode':1,'poster_logo_id':1,'program':{'title':'Owned Parented Artwork'},'owned_fixture_marker':'preserve-normal-clocked'};new=copy.deepcopy(old);new.update(status='recording',started_at='2026-11-13 00:00:00')
  value=lambda x:(b'\1'+json.dumps(x).encode()).hex()
  want={'sql':'literal-body-only-fixture','rows':0,'commands':[],'errors':['P0001'],'tx':'I','recording_id':1,'parameter_kind':'prime-properties-cas','expected_old_properties':old}
  state={'sql':want['sql'],'rows':0,'commands':[],'errors':['P0001'],'execute':True,'sync_seen':True,'parameter_hex':[(1).to_bytes(8,'big').hex(),value(new),value(old)]}
  return want,state,new,value
 def test_actual_jsonb_and_id_shape(self):
  want,state,_,_=self.setup_wire();self.assertTrue(pg.ready_match(want,state,'I'))
 def test_actual_error_code_required(self):
  w,s,_,_=self.setup_wire();s['errors']=[];self.assertFalse(pg.ready_match(w,s,'I'))
 def test_sync_required(self):
  w,s,_,_=self.setup_wire();s['sync_seen']=False;self.assertFalse(pg.ready_match(w,s,'I'))
 def test_execute_required(self):
  w,s,_,_=self.setup_wire();s['execute']=False;self.assertFalse(pg.ready_match(w,s,'I'))
 def test_wrong_recording(self):
  w,s,_,_=self.setup_wire();s['parameter_hex'][0]=(2).to_bytes(8,'big').hex();self.assertFalse(pg.ready_match(w,s,'I'))
 def test_non_binary_jsonb(self):
  w,s,n,_=self.setup_wire();s['parameter_hex'][1]=json.dumps(n).encode().hex();self.assertFalse(pg.ready_match(w,s,'I'))
 def test_bool_is_not_season_int(self):
  w,s,n,v=self.setup_wire();n['season']=True;s['parameter_hex'][1]=v(n);self.assertFalse(pg.ready_match(w,s,'I'))
 def test_bool_is_not_old_cas_int(self):
  w,s,n,v=self.setup_wire();bad=copy.deepcopy(w['expected_old_properties']);bad['episode']=True;s['parameter_hex'][2]=v(bad);self.assertFalse(pg.ready_match(w,s,'I'))
 def test_nested_type_changes_refused(self):self.assertFalse(pg.literal_equal({'a':[True]},{'a':[1]}))
 def test_cache_raw_hit_or_miss_both_actual_reply_shapes(self):
  p=json.loads((ROOT/'tools/redis-policy.json').read_text());cmd=[b'EVAL',p['cache_read']['script'].encode(),b'1',p['cache_group_key'].encode()]
  self.assertTrue(resp.target('cache','t',p,b'c',cmd,[0,None],0));self.assertTrue(resp.target('cache','t',p,b'c',cmd,[1,b'actual-returned-bytes'],0))
 def test_cache_invalid_type_never_supported(self):
  p=json.loads((ROOT/'tools/redis-policy.json').read_text());cmd=[b'EVAL',p['cache_read']['script'].encode(),b'1',p['cache_group_key'].encode()]
  for bad in [[False,None],[-1,None],[0,b'non-null'],[1,None],[1,'decoded-not-raw']]:self.assertFalse(resp.target('cache','t',p,b'c',cmd,bad,0))
 def test_wrong_db_or_key_or_script_not_selected(self):
  p=json.loads((ROOT/'tools/redis-policy.json').read_text());cmd=[b'EVAL',p['cache_read']['script'].encode(),b'1',p['cache_group_key'].encode()]
  self.assertFalse(resp.target('cache','t',p,b'c',cmd,[0,None],9))
  for index,value in [(1,b'return 1'),(3,b'elsewhere')]:bad=cmd.copy();bad[index]=value;self.assertFalse(resp.target('cache','t',p,b'c',bad,[0,None],0))
if __name__=='__main__':unittest.main()
