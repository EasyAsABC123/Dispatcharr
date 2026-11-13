"""Socket-free structural checks for the unexecuted settings definition."""
import ast
import copy
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXPECTED_NAMES = (
    'anonymous_create standard_create UI_denied_admin_create required_key_name '
    'create_default_value duplicate_key standard_retrieve typed_json_patch '
    'PUT_required_key PUT_optional_value_retains readonly_unknown_ignored '
    'rename_key_cache_boundary missing_lookup_before_bad_json missing_delete_ignores_body '
    'standard_delete_refusal delete_created repeat_delete_missing create_array_value '
    'patch_scalar_value null_value_rejected blank_name_rejected rename_duplicate_rejected '
    'create_epg_clamps patch_epg_ordered_clamps patch_epg_array empty_epg_patch delete_epg '
    'check_network_membership check_invalid_CIDR standard_check_refusal '
    'UI_denied_admin_check check_unknown_key'
).split()


EXPECTED_ACTIONS = [
    ('anonymous','POST','collection'),('standard','POST','collection'),('admin_denied','POST','collection'),
    ('admin','POST','collection'),('admin','POST','collection'),('admin','POST','collection'),
    ('standard','GET','created'),('admin','PATCH','created'),('admin','PUT','created'),('admin','PUT','created'),
    ('admin','PATCH','created'),('admin','PATCH','created'),('admin','PATCH','missing'),('admin','DELETE','missing'),
    ('standard','DELETE','created'),('admin','DELETE','created'),('admin','DELETE','created'),
    ('admin','POST','collection'),('admin','PATCH','array'),('admin','PATCH','array'),('admin','PATCH','array'),('admin','PATCH','array'),
    ('admin','POST','collection'),('admin','PATCH','epg'),('admin','PATCH','epg'),('admin','PATCH','epg'),('admin','DELETE','epg'),
    ('admin','POST','check'),('admin','POST','check'),('standard','POST','check'),('admin_denied','POST','check'),('admin','POST','check'),
]

def exact(left, right):
    return json.dumps(left, sort_keys=True, separators=(',',':')) == json.dumps(right, sort_keys=True, separators=(',',':'))

def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(plan=None, root=ROOT):
    plan = copy.deepcopy(plan) if plan is not None else json.loads((root / 'plan.json').read_text())
    require(plan['state'] == 'UNEXECUTED_DEFINITION_ONLY', 'NotObserved')
    require(plan['request_count'] == 32 and len(plan['cases']) == 32, 'Finite32')
    require([row['name'] for row in plan['cases']] == EXPECTED_NAMES, 'ExactOrder')
    require(plan['auth']['kind'] == 'persisted_API_key_only' and plan['auth']['force_authenticate'] is False, 'RealAPIKeyOnly')
    require(plan['auth']['REMOTE_ADDR'] == '127.0.0.1', 'FixedAddress')
    require(exact(plan['auth']['roles']['admin']['user_level'],10) and exact(plan['auth']['roles']['standard']['user_level'],1), 'Roles')
    require(plan['auth']['roles']['admin_denied']['custom_ui'] == '192.0.2.0/24', 'DeniedRole')
    require(plan['budgets_seconds'] == {'bootstrap':180,'capture':360,'outer':1200,'cleanup':90}, 'FixedBudgets')
    require(plan['target_policy'] == {'created':'actual successful case5 returned id, retain binding after delete','array':'actual case18 returned id','epg':'actual case23 returned id','missing':1000001002}, 'ActualIdentityBinding')
    require(exact(plan['seed']['settings'], [
        {'id':1000,'key':'network_access','name':'Owned network','value':{'UI':'127.0.0.0/8'}},
        {'id':1001,'key':'system_settings','name':'Owned system','value':{'time_zone':'UTC'}},
        {'id':1002,'key':'owned.settings.foreign','name':'Foreign sentinel','value':{'untouched':True}},
    ]), 'ExactSeed')
    bindings = set()
    reserved_writes = {'system_settings','network_access','reverse_proxy_auth','proxy_settings','stream_settings','dvr_settings'}
    for i, row in enumerate(plan['cases'], 1):
        require(type(row['id']) is int and row['id'] == i, 'SequentialIDs')
        require((row['role'],row['method'],row['target']) == EXPECTED_ACTIONS[i-1], 'ExactRoleMethodTarget')
        require(row['role'] in plan['auth']['roles'], 'DeclaredRole')
        require(row['prediction']['measurement'] == 'UNMEASURED', 'NoMeasuredPredictions')
        require(type(row['prediction']['status']) is int, 'IntegerStatus')
        require(row['target'] in {'collection','check','missing'} | bindings, 'BoundTargetBeforeUse')
        if i == 12:
            require(exact(row.get('pre_request_setup'), {'cache_set':[{'key':'coresettings:group:owned.settings.new','value':{'owned_sentinel':'before_rename'},'timeout':300}]}), 'ExactRenameWarmSetup')
        else:
            require('pre_request_setup' not in row, 'NoOtherBetweenRequestSetup')
        body_path = Path(row['body'])
        require(not body_path.is_absolute() and '..' not in body_path.parts and body_path.parts[:2] == ('inputs','bodies'), 'BodyPathOwned')
        body = (root / body_path).read_bytes()
        require(len(body) == row['body_bytes'] and hashlib.sha256(body).hexdigest() == row['body_sha256'], 'ExactBodyBytes')
        if i in (13,14):
            require(body == b'{' and row['target'] == 'missing', 'MalformedMissingLookup')
        elif body:
            value = json.loads(body)
            require(type(value) is dict, 'JSONObjectRequests')
            if row['target'] != 'check':
                require(value.get('key') not in reserved_writes, 'NoTaskOrAuthReconfigurationWrites')
        if 'bind_created_alias' in row:
            require(row['method'] == 'POST' and row['target'] == 'collection', 'CreateBindingOnly')
            alias = row['bind_created_alias']
            require(alias not in bindings and alias in {'created','array','epg'}, 'UniqueAlias')
            bindings.add(alias)
    for path, digest in plan['Original_inputs_sha256'].items():
        blob = (root / 'inputs/Original' / path).read_bytes()
        require(hashlib.sha256(blob).hexdigest() == digest, 'PinnedOriginalBytes')
        if path.endswith('.py'):
            ast.parse(blob)
    return {'state':'UNEXECUTED_DEFINITION_VALID','cases':32,'Original_files':len(plan['Original_inputs_sha256']), 'Source_requests':0,'Native_requests':0}


if __name__ == '__main__':
    print(json.dumps(validate(), sort_keys=True))
