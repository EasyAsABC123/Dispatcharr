#!/usr/bin/env python3
"""Link reviewed Original captures to stable register identities, without promotion.
Original Dispatcharr authors retain all application/algorithm credit and licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def sha(data):return hashlib.sha256(data).hexdigest()
def canonical(value):return sha(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode())
def read(root,path):return json.loads((root/path).read_bytes())
def require(value,message):
    if not value:raise ValueError(message)
def plugin_match(pattern,path):
    # Only these reviewed path() forms are supported; regex/format aliases stay distinct.
    if any(c in pattern for c in '^$()\\'):return False
    escaped=re.escape('/'+pattern).replace(re.escape('<str:key>'),'[^/]+')
    return re.fullmatch(escaped,urlsplit(path).path) is not None

def bind(register,plugin,plugin_proof,passive,passive_proof,help_data):
    routes=register['runtime_routes'];require(len(routes)==592,'route count')
    require(sum(len(r['operations'])for r in routes)==798,'operation count')
    require(len(register['non_HTTP_candidates'])==250,'non-HTTP count')
    require(len({r['route_id']for r in routes})==592,'duplicate route ID')
    require(plugin_proof['passed']is True and plugin_proof['Original_revision']==REVISION,'plugin proof')
    require(plugin_proof['actual_TCP_cases']==35 and plugin_proof['native_verified']is False,'plugin scope')
    require(plugin['native_verified']is False and plugin['performance_verified']is False,'dataset scope')
    cases=plugin['cases'];require(len(cases)==35,'plugin case count')
    require(passive_proof['passed']is True and passive_proof['original_revision']==REVISION,'passive proof')
    require(passive['passed']is True and passive['unresolved_rows']==322 and not passive['violations'],'passive scope')
    require(passive['handler_invocations']==passive['dispatch_executions']==0,'passive invocation')
    require(len(passive['routes'])==592,'passive count')
    metadata=[]
    for ordinal,(r,p)in enumerate(zip(routes,passive['routes'])):
        require(r['resolver_ordinal']==p['resolver_ordinal']==ordinal,'ordinal')
        require(r['route_id']==p['route_id'],'passive ID')
        require(p['identity']=={'pattern':r['pattern_exact'],'name':r['name'],'view':r['source_handler']},'passive identity')
        require(p['method_declarations_unchanged']==[op['method']for op in r['operations']],'method drift')
        if p['state']=='metadata_observed':
            require(not r['declared_methods_resolved'],'metadata declaration')
            metadata.append({'route_id':r['route_id'],'observed_allowed_methods':p['binding']['allowed_methods'],
                'source_pointer':'/routes/'+str(ordinal),'evidence_layer':'passive_framework_binding','HTTP_acceptance':False})
    require(len(metadata)==166,'metadata count')
    links=[];plugin_routes=[r for r in routes if r['source_handler'].startswith('apps.plugins.')]
    for index,case in enumerate(cases):
        require(case['actual_original_TCP']is True,'not actual TCP')
        require(case['role']in {'admin','standard','anonymous'},'unknown role')
        require(case['method']in {'GET','HEAD','OPTIONS','POST','DELETE'},'unknown method')
        candidates=[r for r in plugin_routes if plugin_match(r['pattern_exact'],case['path'])]
        require(len(candidates)==1,'nonunique plugin path')
        route=candidates[0];require(not route['declared_methods_resolved'],'unexpected declaration')
        require(len(route['operations'])==1 and route['operations'][0]['method']is None,'placeholder changed')
        require(type(case['status'])is int and 100<=case['status']<=599,'status')
        require(type(case['body_bytes'])is int and case['body_bytes']>=0 and re.fullmatch('[0-9a-f]{64}',case['body_sha256'])is not None,'body identity')
        if case['method']=='HEAD':require(case['body_bytes']==0 and case['body_sha256']==sha(b''),'HEAD body')
        links.append({'route_id':route['route_id'],'operation_placeholder_id':route['operations'][0]['contract_id'],
            'declared_method':None,'observed_method':case['method'],'source_dataset':'scripts/rust-plugin-contracts/source-plugin-http-4.json',
            'source_pointer':'/cases/'+str(index),'case_canonical_sha256':canonical(case),'path':case['path'],'role':case['role'],
            'status':case['status'],'headers':case['headers'],'body_bytes':case['body_bytes'],'body_sha256':case['body_sha256'],
            'result':'observed_Original_TCP_only','native_evaluated':False,'fully_verified':False})
    require(len({r['route_id']for r in links})==7,'plugin route count')
    require(help_data['passed']is True and help_data['command_execution_verified']is False,'help scope')
    require(len(help_data['cases'])==3,'help count');commands=[]
    for index,case in enumerate(help_data['cases']):
        name=case['command'];require(name in {'proxy','dropdb','kill_processes'},'unknown command')
        require(case['argv']==[name,'--help']and case['exit_code']==0,'not help success')
        require(sha(case['stdout'].encode())==case['stdout_sha256']and sha(case['stderr'].encode())==case['stderr_sha256'],'help stream hash')
        candidates=[r for r in register['non_HTTP_candidates']if r['source']['kind']=='management_command'and Path(r['source']['path']).name==name+'.py']
        require(len(candidates)==1,'command candidate')
        commands.append({'candidate_id':candidates[0]['candidate_id'],'source_path':candidates[0]['source']['path'],
            'source_dataset':'scripts/rust-contracts/source-management-help-2.json','source_pointer':'/cases/'+str(index),
            'case_canonical_sha256':canonical(case),'result':'observed_Original_help_startup_only','handle_execution_verified':False,'native_evaluated':False,'fully_verified':False})
    require(len({r['candidate_id']for r in commands})==3,'duplicate command')
    return {'format_version':1,'scope':'Executable links to committed reviewed Original evidence only; no new replay, method declarations, role completeness or Native acceptance',
        'credit':'Original Dispatcharr authors retain all application/algorithm credit and licensing; framework authors retain framework credit.',
        'original_revision':REVISION,'register_mutated':False,'M0_complete':False,'native_verified':False,'performance_verified':False,
        'counts':{'runtime_routes':592,'operation_work_items':798,'non_HTTP_candidates':250,'Original_TCP_cases':35,'Original_TCP_routes':7,'passive_metadata_rows':166,'Original_help_cases':3,'fully_verified_operations':0},
        'Original_TCP_links':links,'passive_metadata_links':metadata,'Original_help_links':commands}

def build(root):
    files={'register':'docs/rust-contract-register.json','plugin':'scripts/rust-plugin-contracts/source-plugin-http-4.json',
        'plugin_proof':'scripts/rust-plugin-contracts/verified-plugin-http-4.json','passive':'scripts/rust-contracts/source-passive-4.json',
        'passive_proof':'scripts/rust-contracts/verified-passive-help-1.json','help_data':'scripts/rust-contracts/source-management-help-2.json'}
    values={k:read(root,p)for k,p in files.items()}
    require(sha((root/files['plugin']).read_bytes())==values['plugin_proof']['dataset_sha256'],'plugin dataset hash')
    require(sha((root/files['passive']).read_bytes())==values['passive_proof']['passive']['dataset_sha256'],'passive dataset hash')
    require(sha((root/files['help_data']).read_bytes())==values['passive_proof']['management_help']['dataset_sha256'],'help dataset hash')
    result=bind(**values);result['inputs_sha256']={p:sha((root/p).read_bytes())for p in files.values()};return result

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[2]);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    result=build(args.root.resolve());args.output.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n');print(json.dumps(result['counts']))
if __name__=='__main__':main()
