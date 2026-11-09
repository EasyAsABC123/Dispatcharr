"""ServerGroup golden-harness safety tests; actual schema proof is separate."""
import copy
import json
import unittest
from urllib.parse import urlsplit
import server_group_probes

ACCOUNTS={role:{'access':role} for role in ['admin','standard','viewer']}
ACCOUNTS['_fixtures']={'inactive_provider_id':3}


class FakeGroups:
    def __init__(self,touch_account=False,readonly_bug=False,raises=False):
        self.rows={};self.next_id=1;self.account={'id':3,'server_group':None,'updated_at':'fixed-time'}
        self.touch_account=touch_account;self.readonly_bug=readonly_bug;self.raises=raises
    def fixture(self,payload):
        row=self.rows[payload['group_id']]
        if row['name']!=payload['group_name'] or self.account['server_group'] is not None:raise ValueError('Unsafe fixture')
        self.account['server_group']=row['id'];return {'linked':True,'updated_at_preserved':True}
    def __call__(self,url,body=None,method=None,token=None,headers=None):
        parsed=urlsplit(url);candidate=parsed.port==2;method=method or ('POST' if body is not None else 'GET')
        if candidate and self.raises:raise RuntimeError('sensitive-fixture-body')
        if token!='admin':return 403,{'detail':'Permission denied'}
        if parsed.path=='/api/m3u/accounts/3/':return 200,copy.deepcopy(self.account)
        root='/api/m3u/server-groups/';suffix=parsed.path[len(root):].strip('/')
        object_id=int(suffix) if suffix else None
        if object_id is not None and object_id not in self.rows:return 404,{'detail':'No ServerGroup matches the given query.'}
        if method=='GET':return 200,copy.deepcopy(self.rows[object_id])
        if method=='DELETE':
            self.rows.pop(object_id)
            if self.account['server_group']==object_id:
                self.account['server_group']=None
                if candidate and self.touch_account:self.account['updated_at']='incorrectly-touched'
            return 204,{'non_json':True,'length':0}
        if not isinstance(body,dict):return 400,{'non_field_errors':['Invalid data. Expected a dictionary, but got list.']}
        if 'name' not in body:
            if method in ('POST','PUT'):return 400,{'name':['This field is required.']}
            value=self.rows[object_id]['name']
        else:
            value=body['name']
            if value is None:return 400,{'name':['This field may not be null.']}
            if isinstance(value,bool) or not isinstance(value,(str,int,float)):return 400,{'name':['Not a valid string.']}
            value=str(value).strip()
            if not value:return 400,{'name':['This field may not be blank.']}
            if len(value)>100:return 400,{'name':['Ensure this field has no more than 100 characters.']}
            if '\0' in value:return 400,{'name':['Null characters are not allowed.']}
        if any(row['name']==value and key!=object_id for key,row in self.rows.items()):return 400,{'name':['server group with this name already exists.']}
        if method=='POST':object_id=self.next_id;self.next_id+=1
        self.rows[object_id]={'id':object_id,'name':value}
        if candidate and self.readonly_bug and body.get('id'):self.rows[object_id]['id']=body['id']
        return (201 if method=='POST' else 200),copy.deepcopy(self.rows[object_id])


class ServerGroupTests(unittest.TestCase):
    def execute(self,**options):
        http=FakeGroups(**options);rows=[]
        server_group_probes.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',ACCOUNTS,
                                lambda *row:rows.append(row),http.fixture)
        return rows
    def test_source_lifecycles_validate_permissions_identity_and_set_null(self):
        rows=self.execute();self.assertEqual(len(rows),26);self.assertTrue(all(row[1] for row in rows))
    def test_account_save_side_effect_and_readonly_identity_bugs_fail_before_normalization(self):
        for options in [{'touch_account':True},{'readonly_bug':True}]:self.assertTrue(any(not row[1] for row in self.execute(**options)))
    def test_exception_capture_does_not_emit_body(self):
        rows=self.execute(raises=True);self.assertTrue(any(not row[1] for row in rows));self.assertNotIn('sensitive-fixture-body',json.dumps(rows))


if __name__=='__main__':unittest.main()
