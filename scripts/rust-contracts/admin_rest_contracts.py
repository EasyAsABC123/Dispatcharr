"""Historical Original backup/log/plugin and exact slashless VOD definitions.
Original Dispatcharr authors retain all application/algorithm credit and licenses.
No source module, token helper or application request is executed by this adapter.
"""
import ast
import copy
import hashlib
import itertools
import json
import re
from pathlib import Path
from framework_auth_contracts import ast_fingerprint
from account_settings_contracts import user_defaults, route_for
from channel_guide_contracts import actor, authentication

KIND = 'pinned_original_admin_rest_tests'
PREFIX = 'scripts/rust-contracts/admin-rest/'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CLASSES = {'apps/backups/tests/test_backups.py': {'BackupAPITestCase': None, 'BackupAdminPermissionTestCase': None},
           'core/tests/test_log_files.py': {'LogFilesEndpointTests': None},
           'apps/plugins/tests/test_plugin_logo_accept.py': {'PluginLogoAcceptHeaderTests': None},
           'apps/vod/tests/test_vod_logo_custom_action_permissions.py': {'VODLogoCustomActionPermissionTests': None},
           'apps/vod/tests/test_vod_logo_proxy.py': {'VODLogoProxyTestCase': ['test_cache_accepts_slash_less_url']},
           'apps/vod/tests/test_vod_image_proxy.py': {'VODImageProxyEndpointTestCase': ['test_movie_image_accepts_slash_less_url']}}


IDENTITIES = {'route-694377e0a528f94c81f6:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-22064715e1618d3171fb:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/create/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-40071bf3f16849a49ed0:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/upload/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-e01e7d71e156c75f2ab4:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/schedule/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-1ba31223e53478b40057:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/schedule/update/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-4fb52127b9819ead6d7e:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/status/<str:task_id>/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-579fad53359dc319d56d:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/<str:filename>/download-token/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-44d437b502b88a3bee58:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/<str:filename>/download/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-2c440797f4dc2f744611:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/<str:filename>/delete/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-b6769611fe6ef4f0cae4:METHOD_UNRESOLVED': {'pattern_exact': 'api/backups/<str:filename>/restore/', 'source_handler': 'apps.backups.api_views.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-cfe08a1b9dd2ea9583c3:METHOD_UNRESOLVED': {'pattern_exact': 'api/core/logs/', 'source_handler': 'core.log_files.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M5'}, 'route-54a5519b46ae18bea548:METHOD_UNRESOLVED': {'pattern_exact': 'api/core/logs/<str:name>/download/', 'source_handler': 'core.log_files.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M5'}, 'route-209e806f3f1d6d576eec:METHOD_UNRESOLVED': {'pattern_exact': 'api/core/logs/<str:name>/', 'source_handler': 'core.log_files.WrappedAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M5'}, 'route-9ff2ea23ea752426d5fb:METHOD_UNRESOLVED': {'pattern_exact': 'api/plugins/plugins/<str:key>/logo/', 'source_handler': 'apps.plugins.api_views.PluginLogoAPIView', 'method': None, 'source_action': None, 'owning_milestone': 'M8'}, 'route-c3b37320eb4a72de2f2c:DELETE': {'pattern_exact': 'api/vod/^vodlogos/bulk-delete/$', 'source_handler': 'apps.vod.api_views.VODLogoViewSet', 'method': 'DELETE', 'source_action': 'bulk_delete', 'owning_milestone': 'M5'}, 'route-19951fabc04f73b18c79:POST': {'pattern_exact': 'api/vod/^vodlogos/cleanup/$', 'source_handler': 'apps.vod.api_views.VODLogoViewSet', 'method': 'POST', 'source_action': 'cleanup', 'owning_milestone': 'M5'}, 'route-b2b53c6ce1dbeea3e3d2:GET': {'pattern_exact': 'api/vod/vodlogos/<int:pk>/cache', 'source_handler': 'apps.vod.api_views.VODLogoViewSet', 'method': 'GET', 'source_action': 'cache', 'owning_milestone': 'M5'}, 'route-b75988847c0358b96a1d:GET': {'pattern_exact': 'api/vod/movies/<int:pk>/image', 'source_handler': 'apps.vod.api_views.MovieViewSet', 'method': 'GET', 'source_action': 'image', 'owning_milestone': 'M5'}}
ORIGINAL_FILES = [{'original_path': 'apps/backups/tests/test_backups.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/backups/tests/test_backups.py', 'sha256': 'bfc1b66ee22eba3a281e771f97fc3418e896568ae0f53e52599b90ce48df2a09'}, {'original_path': 'core/tests/test_log_files.py', 'path': 'scripts/rust-contracts/admin-rest/original/core/tests/test_log_files.py', 'sha256': 'cdf79885b1a2c01f7fd0104808047b5de0037b8aebec5220fbfeb1869438a02f'}, {'original_path': 'apps/plugins/tests/test_plugin_logo_accept.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/plugins/tests/test_plugin_logo_accept.py', 'sha256': '22c1a915193152ff27f2cee4957395866874f52753c4095d3dd70efabe60d36a'}, {'original_path': 'apps/vod/tests/test_vod_logo_custom_action_permissions.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/vod/tests/test_vod_logo_custom_action_permissions.py', 'sha256': 'd3e978002e14e934001c8dd93e5a70d56576b935e1a6530ba50d1d79644e8b1f'}, {'original_path': 'apps/vod/tests/test_vod_logo_proxy.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/vod/tests/test_vod_logo_proxy.py', 'sha256': '2deaf770959b4cd0c9ce3adf7e17090ab92ed2d00deb13f24d5829487baddad3'}, {'original_path': 'apps/vod/tests/test_vod_image_proxy.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/vod/tests/test_vod_image_proxy.py', 'sha256': '011f21d9798eceae3c3a84a8b72c4f09a8bf4105a28cbc7282db41ff362718a9'}, {'original_path': 'apps/backups/api_urls.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/backups/api_urls.py', 'sha256': '6723bb65ae87e747fdd81ca7154bd8b2e4dd5124d7b070e56519113c7f7b4c0b'}, {'original_path': 'core/api_urls.py', 'path': 'scripts/rust-contracts/admin-rest/original/core/api_urls.py', 'sha256': 'aa780f16f02f7bcffe5fdf26aa3f658a0e91190e84377540c75ec73b1dbce81e'}, {'original_path': 'apps/plugins/api_urls.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/plugins/api_urls.py', 'sha256': 'd3072e42ee78bcc546f390e45f1190885f8b8eec365f82a0ad6ab033ab07ef45'}, {'original_path': 'apps/vod/api_urls.py', 'path': 'scripts/rust-contracts/admin-rest/original/apps/vod/api_urls.py', 'sha256': 'db21752903db6113e59bc70c2ba19da9f3af92bd73e41c78ca5d19106f01a9d9'}]

AUTHORITIES = [{'path': 'scripts/rust-contracts/framework-auth/full-suite-reference.json', 'sha256': '494e9945c39e94c2c05301efed9bba3ba89b5da3b60eb55142f2c701729091c6'}, {'path': 'scripts/rust-contracts/admin-rest/observations.json', 'sha256': '35e7c7b01cd5e410af00d740e9565c3a67bbade8a3185692c6df895fae2ed382'}, {'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/models.py', 'sha256': '05cab8c590de411e68ff83b72d9d6256ca43007cbbab5d2a7f96a3a8965a9ded'}]

def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def segment(text, node):
    return ast.get_source_segment(text, node)


def literal(node, environment):
    if isinstance(node, ast.Constant) and (isinstance(node.value, str) or node.value is None):
        return node.value
    key = ast.unparse(node)
    if key in environment:
        return environment[key]
    if isinstance(node, (ast.Tuple, ast.List)):
        return tuple(literal(value, environment) for value in node.elts)
    if isinstance(node, ast.Attribute) and node.attr in ['id', 'pk']:
        return '{PK}'
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return literal(node.left, environment) + literal(node.right, environment)
    if isinstance(node, ast.JoinedStr):
        return ''.join(part.value if isinstance(part, ast.Constant) else literal(part.value, environment) for part in node.values)
    raise ValueError('UnqualifiedFiniteURLExpression: '+key)


def loop_environments(test, call, class_literals):
    parents = {child: parent for parent in ast.walk(test) for child in ast.iter_child_nodes(parent)}
    ancestors = []; node = call
    while node in parents:
        node = parents[node]
        if isinstance(node, ast.For):
            ancestors.append(node)
    environments = [dict(class_literals)]
    branches = [[]]
    for loop in reversed(ancestors):
        following=[]; following_branches=[]
        for environment, branch in zip(environments, branches):
            try:
                options=literal(loop.iter, environment)
            except ValueError:
                options=(None,)
            if not isinstance(options, tuple) or not 1<=len(options)<=64:
                raise ValueError('BoundedFiniteLoopOnly')
            for option in options:
                updated=dict(environment); observed=list(branch)
                if isinstance(loop.target, ast.Name) and option is not None:
                    updated[loop.target.id]=option;observed.append({'line':loop.lineno,'variable':loop.target.id,'literal':option})
                following.append(updated); following_branches.append(observed)
        environments,branches=following,following_branches
    result=[]
    for environment, branch in zip(environments, branches):
        for assignment in sorted((node for node in ast.walk(test) if isinstance(node, ast.Assign) and node.lineno<call.lineno),key=lambda node:node.lineno):
            try:value=literal(assignment.value,environment)
            except ValueError:continue
            for target in assignment.targets:
                if isinstance(target, ast.Name):environment[target.id]=value
        result.append((environment,branch))
    return result


def fixture_actor(root, node, statements, functions, levels, default):
    if node is None or isinstance(node,ast.Constant) and node.value is None:
        return {'kind':'anonymous Original framework client','expression':'None','effective_user_level':None}
    try:
        return actor(node,statements,functions,levels,default)
    except ValueError:
        expression=ast.unparse(node)
        assigned=[value for statement in statements for value in ast.walk(statement) if isinstance(value,ast.Assign)
                  and any(ast.unparse(target)==expression for target in value.targets)]
        if assigned:
            result=fixture_actor(root,assigned[-1].value,statements,functions,levels,default)
            return dict(result,expression=expression)
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr=='create_superuser':
            model=root/'scripts/rust-contracts/accounts-settings/original/apps/accounts/models.py'
            manager=next(value for value in ast.parse(model.read_text()).body if isinstance(value,ast.ClassDef) and value.name=='CustomUserManager')
            constructor=next(value for value in manager.body if isinstance(value,ast.FunctionDef) and value.name=='create_superuser')
            setter=next(value for value in ast.walk(constructor) if isinstance(value,ast.Call) and ast.unparse(value.func)=='extra_fields.setdefault'
                        and value.args and ast.literal_eval(value.args[0])=='user_level')
            assert ast.literal_eval(setter.args[1])==10
            explicit=next((keyword.value for keyword in node.keywords if keyword.arg=='user_level'),None)
            assert explicit is None, 'this pinned create_superuser fixture uses actual manager default'
            return {'kind':'Original create_superuser fixture; exact manager User-level default','expression':expression,'effective_user_level':10,
                    'manager_default_source_ast_sha256':ast_fingerprint(constructor),'source_manager_default':10}
        raise


def request_actor(root, test, call, setup, data_setup, methods, functions, text, levels, default):
    authorization=next((keyword.value for keyword in call.keywords if keyword.arg=='HTTP_AUTHORIZATION'),None)
    statements=[*(data_setup.body if data_setup else []),*(setup.body if setup else []),*test.body]
    if authorization is not None:
        if isinstance(authorization,ast.Name):
            assignment=max((node for node in ast.walk(test) if isinstance(node,ast.Assign) and node.lineno<call.lineno
                            and any(isinstance(target,ast.Name) and target.id==authorization.id for target in node.targets)),key=lambda node:node.lineno)
            authorization=assignment.value
        assert isinstance(authorization,ast.Call) and ast.unparse(authorization.func)=='self.get_auth_header' and len(authorization.args)==1
        helper=methods['get_auth_header']
        assert any(isinstance(node,ast.Call) and ast.unparse(node.func)=='RefreshToken.for_user' and ast.unparse(node.args[0])=='user' for node in ast.walk(helper))
        returned=next(node for node in helper.body if isinstance(node,ast.Return))
        assert ast.unparse(returned.value)=="f'Bearer {str(refresh.access_token)}'"
        result=fixture_actor(root,authorization.args[0],statements,functions,levels,default)
        result['authentication_path']='Original APIClient receives fixture-generated JWT Bearer via exact helper; no token helper execution/public token/new authentication run here'
        result['JWT_helper_ast_sha256']=ast_fingerprint(helper)
        result['explicit_staff_superuser_writes']=[ast.unparse(statement) for block in statements for statement in ast.walk(block) if isinstance(statement,ast.Assign) and any(ast.unparse(target) in [ast.unparse(authorization.args[0])+'.is_staff',ast.unparse(authorization.args[0])+'.is_superuser'] for target in statement.targets)]
        return result
    inherited=authentication(setup,{},10**9,None,'self.client') if setup else None
    user=authentication(test,{},call.lineno,inherited,'self.client')
    result=fixture_actor(root,user,statements,functions,levels,default)
    result['authentication_path']='forced Original test authentication' if user is not None and not isinstance(user,ast.Constant) else 'no Bearer/forced actor in this call; original query-token mocks, if any, remain scoped'
    return result


def derive(root, registry):
    root=Path(root);levels,default=user_defaults(root);fixtures={};cases={};assignments={};excluded=[]
    for reference in ORIGINAL_FILES:
        filename=reference['original_path']
        if filename not in CLASSES:continue
        text=(root/reference['path']).read_text();tree=ast.parse(text)
        module_functions={node.name:node for node in tree.body if isinstance(node,ast.FunctionDef)}
        for cls in (node for node in tree.body if isinstance(node,ast.ClassDef) and node.name in CLASSES[filename]):
            methods={node.name:node for node in cls.body if isinstance(node,ast.FunctionDef)};setup=methods.get('setUp');data_setup=methods.get('setUpTestData');functions=dict(module_functions,**methods)
            class_literals={}
            for assignment in (node for node in cls.body if isinstance(node,ast.Assign)):
                try:value=literal(assignment.value,{})
                except ValueError:continue
                for target in assignment.targets:
                    if isinstance(target,ast.Name):class_literals['self.'+target.id]=value
            fixture_id=filename+':'+cls.name
            fixture={'class_ast_sha256':ast_fingerprint(cls),'setup':segment(text,setup) if setup else None,
                     'setUpTestData':segment(text,data_setup) if data_setup else None,'teardown':segment(text,methods['tearDown']) if 'tearDown'in methods else None,
                     'class_decorators':[segment(text,node)for node in cls.decorator_list],
                     'helpers':{name:segment(text,node)for name,node in methods.items()if not name.startswith('test')},
                     'module_helpers':{name:segment(text,node)for name,node in module_functions.items()}}
            for test in (node for node in cls.body if isinstance(node,ast.FunctionDef) and node.name.startswith('test')):
                selected=CLASSES[filename][cls.name]
                if selected is not None and test.name not in selected:continue
                identifier=filename[:-3].replace('/','.')+'.'+cls.name+'.'+test.name
                calls=[node for node in ast.walk(test)if isinstance(node,ast.Call)and isinstance(node.func,ast.Attribute)
                       and ast.unparse(node.func.value)=='self.client'and node.func.attr in ['get','post','put','patch','delete']]
                if not calls:excluded.append(identifier);continue
                sites=[]
                for call in sorted(calls,key=lambda node:(node.lineno,node.col_offset)):
                    for environment,branch in loop_environments(test,call,class_literals):
                        supplied=literal(call.args[0],environment);route,operation=route_for(registry['runtime_routes'],supplied,call.func.attr.upper())
                        if operation['contract_id']not in IDENTITIES:continue
                        sites.append({'line':call.lineno,'source_expression':segment(text,call),'source_ast_sha256':ast_fingerprint(call),
                            'HTTP_method_observed':call.func.attr.upper(),'supplied_framework_path':supplied,'contract_id':operation['contract_id'],
                            'finite_loop_branch':branch,'actor':request_actor(root,test,call,setup,data_setup,methods,functions,text,levels,default),
                            'transport':'Original test-client request with exact original setup/mocks; no TCP/native or complete credentials claim',
                            'site_scope':'static source/finite-literal branch, not a runtime request count'})
                if not sites:excluded.append(identifier);continue
                assertions=[segment(text,node)for node in ast.walk(test)if isinstance(node,ast.Call)and isinstance(node.func,ast.Attribute)and node.func.attr.startswith('assert')]
                assert assertions
                fixtures[fixture_id]=fixture
                cases[identifier]={'test_id':identifier,'source_path':filename,'class':cls.name,'method':test.name,'fixture_id':fixture_id,
                    'source_ast_sha256':ast_fingerprint(test),'assertion_expressions':assertions,'sites':sites,
                    'decorators':[segment(text,node)for node in test.decorator_list],
                    'patch_contexts':[segment(text,node)for node in ast.walk(test)if isinstance(node,ast.Call)and(ast.unparse(node.func)in['patch','override_settings','mock.patch','patch.object','patch.dict']or ast.unparse(node.func).startswith('mock.patch.'))],
                    'assertion_scope':'whole unmodified workflow; original mocks/temporary files/JWT helper apply, not production services or arbitrary effects'}
                for site in sites:
                    assignments.setdefault(site['contract_id'],[])
                    if identifier not in assignments[site['contract_id']]:assignments[site['contract_id']].append(identifier)
    return {'fixtures':fixtures,'cases':cases,'assignments':assignments,'excluded_methods':excluded,'omitted_User_level_default':default,'User_levels':levels}

ASSERTIONS = ['Execute exact historical Original methods with unchanged setup/data setup, temporary files, teardown, token helpers and mocks',
    'Preserve original whole-workflow response/SQL/filesystem/mock assertions and actual named3362 successful result blocks',
    'Associate only exact request source and finite literal URL branches with immutable route IDs; observed methods never resolve null declarations',
    'Keep fixture-generated JWT/query-token paths, forced/anonymous actors, mocked tasks/services/outbound images and framework-only/noTCP/newSource limits explicit']


def dependencies():
    return [PREFIX+'bindings.json']+[row['path']for row in ORIGINAL_FILES+AUTHORITIES]


def generate(root, registry):
    root=Path(root)
    for row in ORIGINAL_FILES+AUTHORITIES:
        assert sha((root/row['path']).read_bytes())==row['sha256'],'exact admin-REST Original authority bytes'
    summary=json.loads((root/AUTHORITIES[0]['path']).read_text());observation=json.loads((root/AUTHORITIES[1]['path']).read_text())
    assert summary['source_commit']==ORIGINAL and summary['tests_passed']==3362 and summary['exit_code']==0 and summary['scope']=='unchanged-upstream-full-python-test-suite'
    assert observation['full_log_sha256']==summary['artifact_sha256']['tests.log'] and observation['full_log_bytes']==881752 and observation['new_Source_executions']==0
    indexed={op['contract_id']:(row,op)for row in registry['runtime_routes']for op in row['operations']}
    for identifier,identity in IDENTITIES.items():
        route,op=indexed[identifier]
        assert {**{k:route[k]for k in ['pattern_exact','source_handler']},**{k:op[k]for k in ['method','source_action','owning_milestone']}}==identity,'exact admin-REST route/method/action/owner identity'
    derived=derive(root,registry)
    assert set(derived['assignments'])==set(IDENTITIES) and len(derived['cases'])==60
    assert set(observation['cases'])==set(derived['cases']) and sum(len(case['sites'])for case in derived['cases'].values())==87
    for identifier,case in derived['cases'].items():
        recorded=observation['cases'][identifier]
        assert recorded['framework_log_block'].startswith(case['method']+' ('+identifier+')') and recorded['historical_result']=='ok'
        assert re.search(r'(?:\.\.\. |\n)ok\s*\Z',recorded['framework_log_block'])
        assert sha(recorded['framework_log_block'].encode())==recorded['framework_log_block_sha256']
        assert recorded['full_log_byte_end']-recorded['full_log_byte_start']==len(recorded['framework_log_block'].encode())
        case.update(recorded);case['replay_kind']='historical_named_Original_workflow_offline_assertion_replay'
    contracts=[];fixtures={}
    for identifier,identity in IDENTITIES.items():
        selected=[derived['cases'][name]for name in derived['assignments'][identifier]];sites=[site for case in selected for site in case['sites']if site['contract_id']==identifier]
        roles=sorted({'anonymous_original_fixture'if site['actor']['effective_user_level']is None else 'Original_user_level_'+str(site['actor']['effective_user_level'])for site in sites})
        fixture_id='admin-rest:'+identifier
        fixtures[fixture_id]={'id':fixture_id,'baseline_revision':ORIGINAL,'baseline_image':summary['image'],
            'execution':{'entrypoint':'scripts/rust-baseline/run.py','argv':['--output','<fresh-empty-output>',*[case['test_id']for case in selected]],'fresh_live_replay_qualified_this_task':False},
            'ownership':'Existing disposable Original TestCase database, temporary directory and original mocks; no production/service execution',
            'class_fixture_definitions':{key:derived['fixtures'][key]for key in sorted({case['fixture_id']for case in selected})},
            'omitted_User_level_actual_default':derived['omitted_User_level_default'],'Original_User_level_enum':derived['User_levels'],
            'JWT_helper_is_source_not_executed_here':True,'real_task_restore_outbound_service_execution_not_proven':True}
        contracts.append(dict(identity,contract_id=identifier,fixture_id=fixture_id,observed_roles=roles,
            source_role_specs={role:{'authority':'exact Original actor/token-helper/forced-or-anonymous context and mocks','complete_role_matrix':False}for role in roles},
            observed_request_methods=sorted({site['HTTP_method_observed']for site in sites}),cases=selected,assertions=ASSERTIONS,
            result={'state':'historical_Original_admin_REST_workflows_passed_not_new_execution','historical_test_cases':len(selected),'new_Source_cases_this_task':0,'historical_authority':'scripts/rust-baseline/verified-full-baseline.json'},
            fully_verified=False,remaining=['Only original assertions and original JWT/query-token/task/service/outbound/filesystem mocks apply; no whole-endpoint guarantee',
                'Source token helper is not executed by this adapter and actual tokens are not published; no fresh credentials/TCP/Native claim',
                'Finite URL branches and actually exercised slashless variants only; no alias/verb or notification wrong-path inference',
                'Unobserved roles/grammar, production restore/service/cleanup, Native/browser/performance and formal M0/M1 remain open']))
    return {'format_version':1,'binding_kind':KIND,'Original_revision':ORIGINAL,'original_test_files':ORIGINAL_FILES,'authorities':AUTHORITIES,
            'operation_count':18,'unique_historical_test_cases':60,'static_source_branch_sites':87,'new_Source_cases_this_task':0,
            'fixtures':fixtures,'contracts':contracts,'non_HTTP_contracts':[],'excluded_helper_only_methods':derived['excluded_methods'],
            'Native_coverage_increased':False,'fully_verified_added':0}


def validate(root,value,bindings=None):
    root=Path(root);registry=json.loads((root/'docs/rust-contract-register.json').read_text())
    assert value==generate(root,registry),'exact admin-REST Source fixture/actor/URL/mocks/assertion/result definitions'
    return {'passed':True,'operations':18,'unique_historical_workflows':60,'static_source_branch_sites':87,'new_Source_executions':0,'fully_verified_added':0}


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[2]);parser.add_argument('--historical-log',type=Path);args=parser.parse_args()
    value=json.loads((args.root/(PREFIX+'bindings.json')).read_text());result=validate(args.root,value)
    if args.historical_log is not None:
        raw=args.historical_log.read_bytes();observation=json.loads((args.root/(PREFIX+'observations.json')).read_text())
        assert sha(raw)==observation['full_log_sha256'] and len(raw)==observation['full_log_bytes']
        for case in observation['cases'].values():assert raw[case['full_log_byte_start']:case['full_log_byte_end']]==case['framework_log_block'].encode()
        result['deep_historical_raw_blocks']=len(observation['cases'])
    print(json.dumps(result))


if __name__=='__main__':main()
