"""Synthetic state corruption tests; no SQL, Docker or application actions."""
import ast
import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from qualify import validate,NOTICE,DISMISS,TABLES
POLICY=json.loads((ROOT/'policy.json').read_text())
T='2026-11-12T15:00:00+00:00'

def body(row,value):
    raw=json.dumps(value,separators=(',',':')).encode();row.update(body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest())

def example():
    state={'tables':{k:[]for k in TABLES},'sequences':{k:{'last_value':1,'is_called':False}for k in TABLES}}
    state['tables'][NOTICE]=[{'id':1,'notification_key':'preexisting'}];state['sequences'][NOTICE]={'last_value':1,'is_called':True}
    v={'actual_SQL_Bearer_roles_verified':True,'owned_notification_key':'owned.Main.abc','standard_user_id':11,'admin_user_id':10,'phase_started':1.,'phase_completed':20.,'cases':[],'actual_auth_requests':{}}
    for role,uid in [('admin',10),('standard',11)]:
        row={'status':200,'physical_close':{'fd_after':-1,'original_inode_absent':True},'absolute_observation':{'timer_joined':True}};body(row,{'id':uid});v['actual_auth_requests'][role]=row
    for i,spec in enumerate(POLICY['cases']):
        row={'name':spec['name'],'status':spec['status'],'started':2.+i,'completed':2.1+i,'began_utc':T,'completed_utc':T,'physical_close':{'fd_after':-1,'original_inode_absent':True},'absolute_observation':{'timer_joined':True},'before':copy.deepcopy(state)}
        output={'detail':'bounded guard'}
        if spec['name']=='admin-create':
            notice={'id':2,'notification_key':v['owned_notification_key'],'notification_type':'info','priority':'normal','source':'system','title':'Hello','message':'World','action_data':{},'is_active':True,'admin_only':False,'expires_at':None,'created_at':T,'updated_at':T}
            state['tables'][NOTICE].append(notice);state['sequences'][NOTICE]={'last_value':2,'is_called':True};output=dict(notice);output.pop('updated_at');output['is_dismissed']=False
        if spec['name']=='standard-dismiss':
            state['tables'][DISMISS].append({'id':1,'notification_id':2,'user_id':11,'dismissed_at':T,'action_taken':None});state['sequences'][DISMISS]={'last_value':1,'is_called':True}
        if spec['name']=='standard-truthy-repeat':state['tables'][DISMISS][0]['action_taken']='accepted'
        if spec['name'].startswith('standard-')and spec['name']!='standard-create-denied':output={'success':True,'message':'Notification dismissed','notification_key':v['owned_notification_key']}
        if spec['name']=='duplicate-create':output={'notification_key':['system notification with this notification key already exists.']}
        body(row,output);row['after']=copy.deepcopy(state);v['cases'].append(row)
    return v

class Controls(unittest.TestCase):
    def reject(self,fn):
        x=example();fn(x)
        with self.assertRaises((ValueError,KeyError,TypeError)):validate(x,POLICY)
    def test_complete(self):self.assertTrue(validate(example(),POLICY)['normal_Bearer_Main_notification_slice_qualified'])
    def test_case_omission(self):self.reject(lambda x:x['cases'].pop())
    def test_auth_derived_flag_not_sufficient(self):self.reject(lambda x:x.update(actual_auth_requests={}))
    def test_wrong_auth_user(self):self.reject(lambda x:body(x['actual_auth_requests']['admin'],{'id':11}))
    def test_create_cannot_stay501(self):self.reject(lambda x:x['cases'][0].update(status=501))
    def test_missing_named_table(self):self.reject(lambda x:x['cases'][0]['before']['tables'].pop('vod_movie'))
    def test_substitute_named_table(self):
        def change(x):a=x['cases'][0]['before']['tables'];a['invented']=a.pop('vod_movie')
        self.reject(change)
    def test_missing_sequence(self):self.reject(lambda x:x['cases'][0]['before']['sequences'].pop(NOTICE))
    def test_repeat_mutation(self):self.reject(lambda x:x['cases'][2]['after']['tables'][DISMISS][0].update(dismissed_at='2030-01-01T00:00:00Z'))
    def test_repeat_consumes_sequence(self):self.reject(lambda x:x['cases'][2]['after']['sequences'][DISMISS].update(last_value=2))
    def test_duplicate_consumes_sequence(self):self.reject(lambda x:x['cases'][4]['after']['sequences'][NOTICE].update(last_value=3))
    def test_duplicate_wrong_error(self):self.reject(lambda x:body(x['cases'][4],{}))
    def test_body_own_SQL_link(self):
        def change(x):r=x['cases'][0];b=json.loads(bytes.fromhex(r['body_hex']));b['id']=999;body(r,b)
        self.reject(change)
    def test_created_clock_outside_request(self):self.reject(lambda x:x['cases'][0]['after']['tables'][NOTICE][-1].update(created_at='2030-01-01T00:00:00Z'))
    def test_created_defaults(self):self.reject(lambda x:x['cases'][0]['after']['tables'][NOTICE][-1].update(priority='critical'))
    def test_wrong_user_dismissal(self):self.reject(lambda x:x['cases'][1]['after']['tables'][DISMISS][0].update(user_id=10))
    def test_truthy_updates_only_action(self):self.reject(lambda x:x['cases'][3]['after']['tables'][DISMISS][0].update(dismissed_at='2030-01-01T00:00:00Z'))
    def test_unrelated_table_write(self):self.reject(lambda x:x['cases'][0]['after']['tables']['vod_movie'].append({'id':1}))
    def test_intervening_mutation(self):self.reject(lambda x:x['cases'][1]['before']['sequences'][NOTICE].update(last_value=8))
    def test_body_hash(self):self.reject(lambda x:x['cases'][0].update(body_sha256='0'*64))
    def test_fd_not_closed(self):self.reject(lambda x:x['cases'][0]['physical_close'].update(fd_after=5))
    def test_timer_not_joined(self):self.reject(lambda x:x['cases'][0]['absolute_observation'].update(timer_joined=False))
    def test_request_timeout(self):self.reject(lambda x:x['cases'][0].update(completed=18.))
    def test_nonfinite_phase(self):self.reject(lambda x:x.update(phase_completed=float('nan')))
    def test_syntax(self):
        for p in(ROOT/'tools').glob('*.py'):ast.parse(p.read_bytes(),filename=str(p))

if __name__=='__main__':unittest.main()
