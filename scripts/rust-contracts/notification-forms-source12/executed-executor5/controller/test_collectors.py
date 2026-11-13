"""Pure collector protocol/capacity controls, no Django/Redis/SQL connection."""
import copy,json,unittest
from public_manifest import manifest
from redis_channels import inventory
from effects import SCHEMA
class Redis:
 def __init__(self):
  self.items={b'a':('string',b'v'),b'b':('list',[b'one',b'two']),b'c':('set',{b'x',b'y'}),b'd':('hash',{b'f':b'v'}),b'e':('zset',[(b'z',1.5)]),b'f':('stream',[(b'1-0',{b'f':b'v'})])}
 def scan(self,**kwargs):return 0,list(self.items)
 def type(self,k):return self.items[k][0].encode()
 def dump(self,k):return b'SYNTHETIC-DUMP-'+k
 def pttl(self,k):return -1
 def get(self,k):return self.items[k][1]
 def llen(self,k):return len(self.items[k][1])
 def lrange(self,k,*a):return self.items[k][1]
 def scard(self,k):return len(self.items[k][1])
 def smembers(self,k):return self.items[k][1]
 def hlen(self,k):return len(self.items[k][1])
 def hgetall(self,k):return self.items[k][1]
 def zcard(self,k):return len(self.items[k][1])
 def zrange(self,k,*a,**kw):return self.items[k][1]
 def xlen(self,k):return len(self.items[k][1])
 def xrange(self,k):return self.items[k][1]
class Cursor:
 def __init__(self,schema):self.schema=schema;self.sql=''
 def __enter__(self):return self
 def __exit__(self,*args):pass
 def execute(self,sql,*args):self.sql=sql
 def fetchall(self):
  if 'FROM pg_tables'in self.sql:return[(x,)for x in self.schema['tables']]
  if 'information_schema.columns'in self.sql:return self.schema['columns']
  if 'FROM pg_indexes'in self.sql:return self.schema['indexes']
  if 'FROM pg_constraint'in self.sql:return self.schema['constraints']
  if 'FROM pg_sequences'in self.sql:return[(x,)for x in self.schema['sequences']]
  raise AssertionError('UnexpectedSQL')
 def fetchone(self):
  if 'sum(octet_length'in self.sql:return 0,0,0
  if 'md5(COALESCE'in self.sql:return 0,'d751713988987e9331980363e24189ce'
  if 'SELECT last_value,is_called'in self.sql:return 1,False
  raise AssertionError('UnexpectedSQL')
class Connection:
 class Ops:
  def quote_name(self,name):return '"'+name+'"'
 ops=Ops()
 def __init__(self,schema):self.schema=schema
 def cursor(self):return Cursor(self.schema)
class Collectors(unittest.TestCase):
 def test_all_raw_redis_types_not_just_cache(self):
  result=inventory(Redis(),2);self.assertEqual([r['type']for r in result['rows']],['string','list','set','hash','zset','stream']);self.assertFalse(result['destructive_commands_or_consumption']);self.assertEqual(result['rows'][4]['zset'][0]['score_hex'],(1.5).hex())
 def test_raw_unknown_type_fail_closed(self):
  r=Redis();r.items[b'a']=('module',None)
  with self.assertRaises(ValueError):inventory(r,2)
 def test_raw_oversize_list_no_silent_truncation(self):
  r=Redis();r.items[b'b']=('list',[b'x']*65)
  with self.assertRaises(ValueError):inventory(r,2)
 def test_raw_oversize_string(self):
  r=Redis();r.items[b'a']=('string',b'x'*65537)
  with self.assertRaises(ValueError):inventory(r,2)
 def test_raw_nonfinite_score(self):
  r=Redis();r.items[b'e']=('zset',[(b'z',float('inf'))])
  with self.assertRaises(ValueError):inventory(r,2)
 def test_full_schema62_59_positive_synthetic(self):
  result=manifest(Connection(SCHEMA),SCHEMA);self.assertEqual(len(result['tables']),62);self.assertEqual(len(result['sequences']),59);self.assertEqual(len(result['columns']),501)
 def test_missing_schema_table(self):
  changed=copy.deepcopy(SCHEMA);changed['tables'].pop()
  with self.assertRaises(ValueError):manifest(Connection(changed),SCHEMA)
 def test_missing_schema_sequence(self):
  changed=copy.deepcopy(SCHEMA);changed['sequences'].pop()
  with self.assertRaises(ValueError):manifest(Connection(changed),SCHEMA)
 def test_changed_schema_index(self):
  changed=copy.deepcopy(SCHEMA);changed['indexes'].pop()
  with self.assertRaises(ValueError):manifest(Connection(changed),SCHEMA)
if __name__=='__main__':unittest.main(verbosity=2)
