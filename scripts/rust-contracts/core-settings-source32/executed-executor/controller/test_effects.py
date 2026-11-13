"""Pure draft collector/seed/identity controls, no app, Docker or socket."""
import ast
import copy
import hashlib
import json
import unittest
from pathlib import Path
from effects import validate_seed
from source_fixture import target

ROOT=Path(__file__).resolve().parents[1]
PLAN=json.loads((ROOT/'definition/plan.json').read_text())

def seed():
    users=[]
    for alias,row in PLAN['auth']['roles'].items():
        if row is None:continue
        users.append({'id':row['id'],'user_level':row['user_level'],'is_active':True,
                      'api_key':'settings32-0123456789-'+alias,
                      'custom_properties':{'allowed_networks':{'UI':row['custom_ui']}} if 'custom_ui' in row else {}})
    return {'settings':copy.deepcopy(PLAN['seed']['settings']),'users':users,
            'audit':{'sequences':{'public.core_coresettings_id_seq':{'last_value':1002,'is_called':True},'public.accounts_user_id_seq':{'last_value':3,'is_called':True}}},
            'named_cache':{'coresettings:group:'+name:{'owned_sentinel':name} for name in ('owned.settings.new','owned.settings.renamed')}}

class Controls(unittest.TestCase):
    def corrupt(self,change):
        value=seed();change(value)
        with self.assertRaises(ValueError):validate_seed(value,PLAN)
    def test_seed(self):self.assertTrue(validate_seed(seed(),PLAN)['seed_exact'])
    def test_seed_field(self):self.corrupt(lambda p:p['settings'][2]['value'].__setitem__('untouched',False))
    def test_seed_bool_int(self):self.corrupt(lambda p:p['settings'][2]['value'].__setitem__('untouched',1))
    def test_seed_row_missing(self):self.corrupt(lambda p:p['settings'].pop())
    def test_seed_extra(self):self.corrupt(lambda p:p['settings'].append({'id':1003}))
    def test_role_swap(self):self.corrupt(lambda p:p['users'][1].__setitem__('user_level',10))
    def test_role_bool(self):self.corrupt(lambda p:p['users'][1].__setitem__('user_level',True))
    def test_network_shape(self):self.corrupt(lambda p:p['users'][2].__setitem__('custom_properties',{'network_access':{'UI':'192.0.2.0/24'}}))
    def test_sequence(self):self.corrupt(lambda p:p['audit']['sequences']['public.core_coresettings_id_seq'].__setitem__('last_value',1003))
    def test_sentinel(self):self.corrupt(lambda p:p['named_cache']['coresettings:group:owned.settings.new'].__setitem__('owned_sentinel','other'))
    def test_actual_alias(self):self.assertEqual(target(PLAN['cases'][6],{'created':4321},PLAN),'/api/core/settings/4321/')
    def test_unavailable_alias_no_fallback(self):
        with self.assertRaises(KeyError):target(PLAN['cases'][6],{},PLAN)
    def test_fixed_missing(self):self.assertEqual(target(PLAN['cases'][12],{},PLAN),'/api/core/settings/1000001002/')
    def test_readonly_imports(self):
        tree=ast.parse((ROOT/'controller/source_fixture.py').read_text())
        imports=[node for node in tree.body if isinstance(node,(ast.Import,ast.ImportFrom))]
        self.assertFalse(any(any(x.name.startswith(('django','rest_framework','redis','core.')) for x in n.names) if isinstance(n,ast.Import) else (n.module or '').startswith(('django','rest_framework','redis','core.')) for n in imports))
    def test_dependency_local_bytes(self):
        for row in json.loads((ROOT/'inputs/actual-dependencies.json').read_text()):
            local=ROOT/row['path']
            self.assertEqual(hashlib.sha256(local.read_bytes()).hexdigest(),row['sha256'])
    def test_no_force_auth(self):
        tree=ast.parse((ROOT/'controller/source_fixture.py').read_text())
        self.assertFalse(any(isinstance(n,ast.Call) and ((isinstance(n.func,ast.Name) and n.func.id=='force_authenticate') or (isinstance(n.func,ast.Attribute) and n.func.attr=='force_authenticate')) for n in ast.walk(tree)))

if __name__=='__main__':unittest.main()
