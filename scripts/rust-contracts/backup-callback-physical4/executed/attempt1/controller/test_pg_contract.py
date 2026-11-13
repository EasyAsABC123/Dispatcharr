"""Pure wire-contract controls only; no socket, SQL, Redis or app execution."""
import copy,struct,unittest
from owned_pg_proxy import bind_parameters,ready_match,GATE_SAFETY_SECONDS
SQL='SELECT value FROM core_coresettings WHERE key=$1'
def wire(value=b'system_settings',formats=(0,)):
 return b'portal\0statement\0'+struct.pack('!H',len(formats))+b''.join(struct.pack('!H',v)for v in formats)+struct.pack('!H',1)+struct.pack('!i',len(value))+value+struct.pack('!H',0)
class Contract(unittest.TestCase):
 def setUp(self):
  self.want={'sql':SQL,'parameter_hex':[b'system_settings'.hex()],'rows':1,'command':'SELECT 1','errors':[],'tx':'I'}
  self.state={'sql':SQL,'parameter_hex':[b'system_settings'.hex()],'execute':True,'rows':1,'commands':['SELECT 1'],'errors':[],'sync_seen':True}
 def test_exact_bind(self):self.assertEqual(bind_parameters(wire()),(b'portal',b'statement',[b'system_settings'.hex()],[0]))
 def test_truncated_bind(self):
  with self.assertRaises(ValueError):bind_parameters(wire()[:-1])
 def test_parameter_format(self):
  with self.assertRaises(ValueError):bind_parameters(wire(formats=(2,)))
 def test_key_bound(self):
  self.state['parameter_hex']=[b'network_access'.hex()];self.assertFalse(ready_match(self.want,self.state,'I'))
 def test_execute_required(self):
  self.state['execute']=False;self.assertFalse(ready_match(self.want,self.state,'I'))
 def test_sync_required(self):
  self.state['sync_seen']=False;self.assertFalse(ready_match(self.want,self.state,'I'))
 def test_exact_ready(self):self.assertTrue(ready_match(self.want,self.state,'I'))
 def test_ping_does_not_match(self):
  self.state['sql']='';self.assertFalse(ready_match(self.want,self.state,'I'))
 def test_timeout_target(self):
  self.want.update(parameter_hex=[b'network_access'.hex()],rows=0,command=None,errors=['57014'])
  self.state.update(parameter_hex=[b'network_access'.hex()],rows=0,commands=[],errors=['57014'])
  self.assertTrue(ready_match(self.want,self.state,'I'))
  self.state['errors']=['XX000'];self.assertFalse(ready_match(self.want,self.state,'I'))
 def test_gate_safety_predeclared(self):self.assertEqual(GATE_SAFETY_SECONDS,45)
if __name__=='__main__':unittest.main()
