"""Offline layout, representative role and isolation validation."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import plan
import run

from connect_matrix_contract import ALLOW, DENIALS, MUTATIONS, ROLES, ROUTES, cell, phase, route_path
from connect_matrix_probe import run_physical

ROLES_FIXTURE={
    'admin':{'access':'local-admin','api_key':'local-admin-key'},
    'inactive':{'access':'local-inactive'},
    'viewer':{'access':'local-viewer'},
    'standard':{'access':'local-standard'},
}


class StoreStub:
    def __init__(self,missing=False):
        self.missing=missing
        self.count=0
        self.monitor={'integrations':[],'subscriptions':[],'logs':[],
                      'sequences':{name:[0,True] for name in ('integrations','subscriptions','logs')}}

    def insert(self,table,values):
        counter=self.monitor['sequences'][table]
        counter[0]+=1
        entry={'id':counter[0],**values}
        self.monitor[table].append(entry)
        return entry

    def assign(self,name,kind):
        row=self.insert('integrations',{'name':name,'config':{},'enabled':False,'type':'webhook','created_at':'owned-date'})
        ids={'integration':row['id']}
        if kind!='subscription-create':
            sub=self.insert('subscriptions',{'integration':row['id'],'enabled':True,'event':'vod_start','payload_template':None})
            ids['subscription']=sub['id']
            if kind in ('read','subscription-delete'):
                log=self.insert('logs',{'subscription_id':sub['id'],'status':'success','created_at':'owned-date'})
                ids['log']=log['id']
        return ids

    def locate(self,table,identifier):
        [row]=[row for row in self.monitor[table] if row['id']==identifier]
        return row

    def fixture(self,data):
        action=data['action']
        if action=='connect_create_receiver_count':
            return {'requests':0}
        if action=='connect_matrix_prepare':
            self.start=deepcopy(self.monitor)
            self.variant=data['variant']
            prefix=phase(data['side'],data['variant'])
            self.ids={'read':self.assign(prefix+'read','read')}
            for method in sorted(MUTATIONS):
                for role in ROLES:
                    label=cell(method,role)
                    self.ids[label]=self.assign(prefix+label,method)
            return {'before':self.start,'seeded':deepcopy(self.monitor),'selected':self.ids}
        if action=='connect_matrix_snapshot':
            return deepcopy(self.monitor)
        if action=='connect_matrix_retire':
            return self.start
        raise AssertionError(action)

    def request(self,_endpoint,url,method,body,headers):
        sequence=self.count
        self.count+=1
        route=ROUTES[sequence//len(ROLES)]
        role=ROLES[sequence%len(ROLES)]
        ids=self.ids[cell(route.key,role)] if route.key in MUTATIONS else self.ids['read']
        assert method==route.method and url==route_path(route,self.variant,ids)
        if role=='admin_api_key':
            assert headers=={'X-API-Key':ROLES_FIXTURE['admin']['api_key']}
        elif role=='anonymous':
            assert not headers
        else:
            slot={'inactive_admin':'inactive','admin_jwt':'admin'}.get(role,role)
            assert headers=={'Authorization':'Bearer '+ROLES_FIXTURE[slot]['access']}
        if self.missing:
            return {'status':404,'json':{'detail':'missing'},'body_bytes':15,'mime':'application/json'}
        if role not in ALLOW:
            return {'status':DENIALS[role],'json':{'detail':'denied'},'body_bytes':20,'mime':'application/json'}
        if self.variant=='json' and route.key=='nested-subscriptions':
            return {'status':500,'json':None,'body_bytes':11,'mime':'text/html'}
        if route.key=='subscription-create':
            payload=self.insert('subscriptions',body)
        elif route.key=='subscription-delete':
            sub=ids['subscription']
            self.monitor['subscriptions']=[row for row in self.monitor['subscriptions'] if row['id']!=sub]
            self.monitor['logs']=[row for row in self.monitor['logs'] if row['subscription_id']!=sub]
            payload=None
        elif route.key in ('subscription-detail','subscription-put','subscription-patch'):
            row=self.locate('subscriptions',ids['subscription'])
            row.update(body or {})
            payload=row
        elif route.key=='subscriptions-list':
            payload=list(reversed(self.monitor['subscriptions']))
        elif route.key in ('integration-get','integration-patch','nested-subscriptions'):
            row=self.locate('integrations',ids['integration'])
            row.update(body or {})
            children=[sub for sub in self.monitor['subscriptions'] if sub['integration']==ids['integration']]
            payload=children if route.key=='nested-subscriptions' else {**row,'subscriptions':children}
        elif route.key=='log-detail':
            row=self.locate('logs',ids['log'])
            payload={key:item for key,item in row.items() if key!='subscription_id'}
            payload['subscription']=self.locate('subscriptions',row['subscription_id'])
        else:
            raise AssertionError(route.key)
        return {'status':route.status,'json':deepcopy(payload),'body_bytes':20 if payload is not None else 0,
                'mime':'application/json' if payload is not None else ''}


class ConnectMatrix(unittest.TestCase):
    def test_exact_20_currently_unassigned_literal_operations(self):
        actual=plan.prepare()
        frozen=json.loads((plan.HERE/'matrix-plan.json').read_bytes())
        self.assertEqual(actual,frozen)
        self.assertEqual(actual['distinct_selected_declarations'],20)
        self.assertEqual(actual['physical_cell_count'],240)
        self.assertEqual(actual['missing_by_shared_family']['django-admin'],69)
        self.assertTrue(all(len(case['role_cases'])==6 for case in actual['cases']))

    def test_every_source_control_and_known_suffix_failure(self):
        for variant in ('bare','json'):
            store=StoreStub()
            result=run_physical('http://127.0.0.1:1','source',variant,ROLES_FIXTURE,store.fixture,store.request)
            self.assertEqual(result['completed_cell_count'],60)
            self.assertTrue(result['complete_selected_table_transition'])
            self.assertTrue(result['expected_sequence_transition'])
            self.assertTrue(result['preserved_unrelated_SQL_rows'])
            self.assertTrue(result['zero_owned_delivery'])
            self.assertEqual(result['source_or_native_scope_matches'],60 if variant=='bare' else 58)
            if variant=='json':
                self.assertEqual(result['cases']['nested-subscriptions-admin_jwt']['actual_status'],500)
                self.assertFalse(result['cases']['nested-subscriptions-admin_jwt']['selected_scope_passed'])

    def test_missing_native_methods_do_not_abort_independent_cells(self):
        store=StoreStub(missing=True)
        result=run_physical('http://127.0.0.1:2','native','bare',ROLES_FIXTURE,store.fixture,store.request)
        self.assertEqual(result['completed_cell_count'],60)
        self.assertEqual(result['source_or_native_scope_matches'],0)
        self.assertFalse(result['complete_selected_table_transition'])
        self.assertTrue(result['preserved_unrelated_SQL_rows'])
        self.assertTrue(result['zero_owned_delivery'])

    def test_frozen_startup_and_inner_namespace_refusal(self):
        self.assertEqual(run.verify_inputs()['scope'],'representative_connect_matrix')
        self.assertTrue(callable(run.load_coordinator().command))
        with tempfile.TemporaryDirectory() as folder:
            args=run.runner_argv(Path(folder)/'build.json','dispatcharr-fixture-only:placeholder',Path(folder)/'run')
            actual=subprocess.run(args,capture_output=True,text=True,env={**os.environ,
                                  'DISPATCHARR_LIFECYCLE_PROJECT':'wrong-isolated-namespace'},timeout=10)
            self.assertEqual(actual.returncode,2)
            self.assertIn('Expected the owned Connect matrix Compose namespace',actual.stderr)
