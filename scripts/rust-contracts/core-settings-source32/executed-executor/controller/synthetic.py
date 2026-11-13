"""Explicitly synthetic corruption-control data, never Source evidence."""
import copy
import hashlib
import json
from pathlib import Path
from test_effects import PLAN, seed

ROOT=Path(__file__).resolve().parents[1]

def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()

def snapshot(settings,seq):
    result=seed();result['settings']=copy.deepcopy(settings)
    schema=json.loads((ROOT/'inputs/schema62-authority.json').read_text())
    tables={name:{'count':0,'sha_like_md5':hashlib.md5(b'[]').hexdigest()} for name in schema['tables']}
    tables['core_coresettings']={'count':len(settings),'sha_like_md5':hashlib.md5(encode(settings)).hexdigest()}
    sequences={name:[1,True] for name in schema['sequences']};sequences['core_coresettings_id_seq']=[seq,True]
    result['public_manifest']={'tables':tables,'sequences':sequences,**{key:schema[key] for key in ('columns','constraints','indexes')}}
    result['audit']['tables']={'core_coresettings':{'raw_rows':copy.deepcopy(settings)}}
    result['raw_Redis']=[{'database':db,'rows':[],'complete':True,'destructive_commands_or_consumption':False} for db in range(4)]
    return result

def make():
    report={'_synthetic_only':True,'capture_valid':True,'actual_request_count':32,'completed_capture_count':32,
            'force_authenticate_used':False,'Source_TCP_or_Native':False,'APIClient_raise_request_exception':False,
            'passed':False,'requests':[],'seed':snapshot(PLAN['seed']['settings'],1002),'aliases':{}}
    current=copy.deepcopy(report['seed']);seq=1002;aliases={}
    for spec in PLAN['cases']:
        if 'pre_request_setup' in spec:
            setup={'before_case':spec['id'],'declared':copy.deepcopy(spec['pre_request_setup']),'before':copy.deepcopy(current)}
            for item in spec['pre_request_setup']['cache_set']:
                current['named_cache'][item['key']]=copy.deepcopy(item['value'])
            setup['after']=copy.deepcopy(current)
            report.setdefault('between_request_fixture_setup',[]).append(setup)
        target=spec['target'];key=PLAN['target_policy']['missing'] if target=='missing' else aliases.get(target)
        path=PLAN['routes'][target] if target in ('collection','check') else PLAN['routes']['detail'].format(bound_id=key)
        body=(ROOT/'definition'/spec['body']).read_bytes();status=spec['prediction']['status']
        value=json.loads(body) if body and body!=b'{' else {}
        settings={item['id']:copy.deepcopy(item) for item in current['settings']}
        reply={'error':'synthetic validation placeholder'}
        if status in (200,201) and target!='check':
            if spec['method']=='POST':
                seq+=1;key=seq;settings[key]={'id':key,'key':value['key'],'name':value['name'],'value':value.get('value',{})}
            elif spec['method']!='GET':
                settings[key].update({field:value[field] for field in ('key','name','value') if field in value})
            if spec['id']==23:settings[key]['value']={'epg_history_max_days':365,'epg_history_min_days':0,'epg_default_prev_days':365,'epg_default_days':0,'keep':'epg'}
            if spec['id']==24:settings[key]['value']={'epg_history_max_days':7,'epg_history_min_days':7,'epg_default_prev_days':7,'epg_default_days':365}
            reply=copy.deepcopy(settings[key])
        if status==204:
            del settings[key];reply=None
        raw=b'' if status==204 else encode(reply)
        role=PLAN['auth']['roles'][spec['role']]
        row={'id':spec['id'],'name':spec['name'],'role':spec['role'],'method':spec['method'],'target_alias':target,
             'target_id':None if target in ('collection','check') else str(key),'path':path,
             'body_sha256':spec['body_sha256'],'body_bytes':len(body),'transport':'http_response',
             'framework_exception':None,'capture_complete':True,'status':status,'json':reply,
             'response_body_hex':raw.hex(),'response_body_bytes':len(raw),'response_body_sha256':hashlib.sha256(raw).hexdigest(),
             'actual_auth':{'user_id':None if role is None else role['id'],'is_authenticated':role is not None,
                            'backend_class':None if role is None else 'apps.accounts.authentication.ApiKeyAuthentication'},
             'before':copy.deepcopy(current),'after':snapshot([settings[k] for k in sorted(settings)],seq)}
        row['after']['named_cache']=copy.deepcopy(current['named_cache'])
        if status in (200,201,204) and spec['method'] in ('POST','PUT','PATCH','DELETE') and target!='check':
            changed_name=value.get('key') if spec['method']=='POST' else (value.get('key') or next(item['key'] for item in current['settings'] if item['id']==key))
            cache_key='coresettings:group:'+changed_name
            if cache_key in row['after']['named_cache']:row['after']['named_cache'][cache_key]=None
        report['requests'].append(row);current=copy.deepcopy(row['after'])
        if 'bind_created_alias' in spec:aliases[spec['bind_created_alias']]=key
    report['aliases']=aliases
    return report
