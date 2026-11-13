"""Draft read-only effect validator; all oracles are premeasurement predictions."""
import hashlib
import json
from pathlib import Path


def require(ok, name):
    if not ok:
        raise ValueError(name)


def exact(left, right):
    return json.dumps(left,sort_keys=True,separators=(',',':')) == json.dumps(right,sort_keys=True,separators=(',',':'))


def validate_seed(seed, plan):
    require(exact(seed['settings'], plan['seed']['settings']), 'ExactSettingsSeedRowsTypes')
    users=seed['users']
    roles=plan['auth']['roles']
    expected=[(alias,row) for alias,row in roles.items() if row is not None]
    require(len(users)==len(expected), 'ExactThreeSeedUsers')
    for alias,row in expected:
        actual=next((user for user in users if type(user['id']) is int and user['id']==row['id']),None)
        require(actual is not None, 'ActualRoleIdentity')
        require(exact(actual['user_level'],row['user_level']) and actual['is_active'] is True, 'ActualRoleFields')
        custom={'allowed_networks':{'UI':row['custom_ui']}} if 'custom_ui' in row else {}
        require(exact(actual['custom_properties'],custom), 'ActualNetworkRoleShape')
        require(type(actual['api_key']) is str and actual['api_key'].startswith('settings32-') and actual['api_key'].endswith('-'+alias), 'GeneratedRoleAPIKey')
    seq=seed['audit']['sequences']
    require(exact(seq['public.core_coresettings_id_seq'],{'last_value':1002,'is_called':True}), 'SettingsSeedSequence')
    require(exact(seq['public.accounts_user_id_seq'],{'last_value':3,'is_called':True}), 'UserSeedSequence')
    named=seed['named_cache']
    for name in ('owned.settings.new','owned.settings.renamed'):
        require(exact(named['coresettings:group:'+name],{'owned_sentinel':name}), 'ExactOwnedGroupSentinel')
    return {'seed_exact':True,'user_roles':3,'settings':3,'imported_system_cache':False}


def raw_unchanged(before, after):
    require(type(before) is dict and type(after) is dict, 'RawInventoryObjects')
    require(before['database']==after['database'] and before['complete'] is True and after['complete'] is True, 'ActualCompleteRawDB')
    require(before['destructive_commands_or_consumption'] is False and after['destructive_commands_or_consumption'] is False, 'NonConsumingRawInventory')
    left={row['key_hex']:row for row in before['rows']};right={row['key_hex']:row for row in after['rows']}
    require(len(left)==len(before['rows']) and len(right)==len(after['rows']) and set(left)==set(right), 'ExactRawKeySet')
    for key,row in left.items():
        other=right[key]
        require(exact({k:v for k,v in row.items() if k!='pttl'},{k:v for k,v in other.items() if k!='pttl'}), 'ExactRawTypeDUMPValuesLists')
        old,new=row['pttl'],other['pttl']
        require(type(old) is int and type(new) is int and ((old==-1 and new==-1) or (old>=0 and 0<=new<=old)), 'RawFiniteTTLMonotonic')


def validate_capture(report, plan, root, *, allow_synthetic=False):
    require(allow_synthetic or report.get("_synthetic_only") is not True, "ActualCaptureCannotBeSynthetic")
    schema=json.loads((root.parent/"inputs/schema62-authority.json").read_text())
    require(report['capture_valid'] is True and report['actual_request_count']==32 and
            report['completed_capture_count']==32 and len(report['requests'])==32, 'ActualComplete32')
    require(report['force_authenticate_used'] is False and report['Source_TCP_or_Native'] is False, 'FrameworkAPIKeyOnly')
    require(report['APIClient_raise_request_exception'] is False, 'ExplicitExceptionPolicy')
    validate_seed(report['seed'],plan)
    aliases={}
    last=report['seed']
    checks=0
    for spec,row in zip(plan['cases'],report['requests']):
        for field in ('id','name','role','method'):
            require(exact(row[field],spec[field]), 'ExactRequestIdentity')
        require(row['target_alias']==spec['target'], 'ActualTargetAlias')
        target=spec['target']
        key=plan['target_policy']['missing'] if target=='missing' else aliases.get(target)
        expected_path=plan['routes'][target] if target in ('collection','check') else plan['routes']['detail'].format(bound_id=key)
        require(row['path']==expected_path and row['target_id']==(None if target in ('collection','check') else str(key)), 'BoundActualTargetID')
        body=(root/spec['body']).read_bytes()
        require(row['body_sha256']==hashlib.sha256(body).hexdigest() and row['body_bytes']==len(body), 'InputBodyBinding')
        require(row['transport']=='http_response' and row['framework_exception'] is None and row['capture_complete'] is True, 'MeasuredResponseNotAssumed500')
        require(row['status']==spec['prediction']['status'], 'PredictedStatusActual')
        raw=bytes.fromhex(row['response_body_hex'])
        require(len(raw)==row['response_body_bytes'] and hashlib.sha256(raw).hexdigest()==row['response_body_sha256'], 'ResponseBytesSHA')
        require(exact(json.loads(raw) if raw else None,row['json']), 'JSONRawBinding')
        before,after=row['before'],row['after']
        for snap in (before,after):
            manifest=snap['public_manifest']
            require(set(manifest['tables'])==set(schema['tables']) and set(manifest['sequences'])==set(schema['sequences']), 'ExactFullPublicSets')
            for field in ('columns','constraints','indexes'):
                require(exact(manifest[field],schema[field]), 'ExactPublicSchema')
            audit=snap['audit']
            require(exact(audit['tables']['core_coresettings']['raw_rows'],snap['settings']), 'RawSQLSettingsBinding')
        
        if spec['id']==12:
            setups=report.get('between_request_fixture_setup',[])
            require(len(setups)==1 and setups[0]['before_case']==12 and exact(setups[0]['declared'],spec['pre_request_setup']), 'ExactDeclaredWarmRecord')
            require(exact(setups[0]['before']['public_manifest'],last['public_manifest']) and exact(setups[0]['after']['public_manifest'],before['public_manifest']), 'NoFixtureSQLMutation')
            oldkey='coresettings:group:owned.settings.new'
            newkey='coresettings:group:owned.settings.renamed'
            require(exact(before['named_cache'][oldkey],{'owned_sentinel':'before_rename'}), 'BeforeRenameRealOldSentinel')
            require(exact(after['named_cache'][oldkey],before['named_cache'][oldkey]), 'LiteralOldKeySurvivesRename')
            require(exact(before['named_cache'][newkey],{'owned_sentinel':'owned.settings.renamed'}) and after['named_cache'][newkey] is None, 'NewKeyInvalidatedOnRename')
        require(exact(last['settings'],before['settings']), 'CrossRequestSettingsContinuity')
        require(exact(last['public_manifest'],before['public_manifest']), 'CrossRequestFullSQLContinuity')
        auth=row['actual_auth']
        if spec['role']!='anonymous':
            require(type(auth['user_id']) is int and auth['user_id']==plan['auth']['roles'][spec['role']]['id'] and auth['is_authenticated'] is True, 'ActualConfiguredAPIKeyIdentity')
            require(auth['backend_class']=='apps.accounts.authentication.ApiKeyAuthentication', 'ActualAPIKeyBackend')
        else:
            require(auth['is_authenticated'] is False, 'ActualAnonymous')
        for snap in (before,after):
            require(len(snap['raw_Redis'])==4 and all(type(item['database']) is int for item in snap['raw_Redis']) and {item['database'] for item in snap['raw_Redis']}=={0,1,2,3}, 'ExactFourRawRedisDatabases')
        for db in (0,2,3):
            left=next(item for item in before['raw_Redis'] if item['database']==db)
            right=next(item for item in after['raw_Redis'] if item['database']==db)
            raw_unchanged(left,right)
        expected={item['id']:dict(item) for item in before['settings']}
        status=row['status']
        if status in (200,201) and target!='check' and spec['method']!='GET':
            value=json.loads(body)
            if spec['method']=='POST':
                actual=row['json'];newid=actual['id']
                require(type(newid) is int and newid not in expected, 'NaturalNewIdentity')
                changes={'id':newid,'key':value['key'],'name':value['name'],'value':value.get('value',{})}
            else:
                changes=dict(expected[key])
                changes.update({name:value[name] for name in ('key','name','value') if name in value})
            if spec['id']==23:
                changes['value']={'epg_history_max_days':365,'epg_history_min_days':0,'epg_default_prev_days':365,'epg_default_days':0,'keep':'epg'}
            if spec['id']==24:
                changes['value']={'epg_history_max_days':7,'epg_history_min_days':7,'epg_default_prev_days':7,'epg_default_days':365}
            expected[changes['id']]=changes
            require(exact(row['json'],changes), 'LiteralSavedSnapshotResponse')
        if status==204:
            require(not raw and row['json'] is None,'Empty204')
            del expected[key]
        require(exact(after['settings'],[expected[k] for k in sorted(expected)]), 'ExactOnlyOwnedSettingDelta')
        for table,entry in before['public_manifest']['tables'].items():
            if table!='core_coresettings':
                require(exact(entry,after['public_manifest']['tables'][table]), 'AllOther61TablesUnchanged')
        old_seq=before['public_manifest']['sequences']['core_coresettings_id_seq']
        new_seq=after['public_manifest']['sequences']['core_coresettings_id_seq']
        expected_seq=[old_seq[0]+1,True] if status==201 else old_seq
        require(exact(new_seq,expected_seq), 'OnlySuccessfulCreateConsumesSettingsSequence')
        for name,seq in before['public_manifest']['sequences'].items():
            if name!='core_coresettings_id_seq' and name!='public.core_coresettings_id_seq':
                require(exact(seq,after['public_manifest']['sequences'][name]), 'AllOther58SequencesUnchanged')
        if 'bind_created_alias' in spec:
            aliases[spec['bind_created_alias']]=row['json']['id']
        last=after;checks+=1
    require(exact(aliases,report['aliases']), 'FinalAliasBinding')
    return {'qualified_requests':checks,'Source_TCP':False,'Native':False,'oracles_predeclared':True,
            'raw_cache_preserved_not_normalized':True,'framework_only':True}


def compare(report, plan):
    try:
        result=validate_capture(report,plan,Path(__file__).resolve().parents[1]/'definition')
        return {'passed':True,**result,'raw_recorder_passed_preserved':report.get('passed')}
    except (ValueError,KeyError,TypeError,AssertionError) as error:
        return {'passed':False,'error_type':type(error).__name__,'error':str(error),'raw_recorder_passed_preserved':report.get('passed')}
