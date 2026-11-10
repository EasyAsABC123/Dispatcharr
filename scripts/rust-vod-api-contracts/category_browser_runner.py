"""Manifest-fixture unchanged React category workflow, no resource claim.
Original Dispatcharr authors retain all application credit; AGPL retained.
"""
import json,os,secrets,subprocess,http.client,ipaddress
from urllib.parse import urlsplit

def login_limit_witness(base,username,password):
    parsed=urlsplit(base);assert parsed.scheme=='http'and ipaddress.ip_address(parsed.hostname).is_loopback
    records=[]
    for ordinal in [3,4]:
        connection=http.client.HTTPConnection(parsed.hostname,parsed.port,timeout=10)
        try:
            connection.request('POST','/api/accounts/token/',body=json.dumps({'username':username,'password':password}),headers={'Content-Type':'application/json'})
            response=connection.getresponse();raw=response.read(65537);assert len(raw)<=65536;body=json.loads(raw)
            safe_body={'redacted_success_token_fields':sorted(body),'access_present':bool(body.get('access')),'refresh_present':bool(body.get('refresh'))}if response.status==200 else body
            records.append({'ordinal_in_owned_phase':ordinal,'status':response.status,'body':safe_body,'retry_after':response.getheader('Retry-After')})
        finally:connection.close()
    return {'passed':[row['status']for row in records]==[200,429],'records':records,'scope':'Actual original shared3/min bucket: two completed UI logins followed by valid third/fourth token requests; successful JWTs redacted, fourth rejection body retained; no runtime/policy modification'}

def probe(command,compose,environment,evidence,tools,source_base,candidate_base):
    password=secrets.token_urlsafe(32)
    helper=compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_browser_fixture.py']
    prepared=False;observations={};report={'passed':False,'scope':__doc__,'browser_not_mocked':True,'frontend_unchanged':True,'unified_provider_artwork_episode_flows_verified':False}
    try:
        setup=command(helper,payload=json.dumps({'op':'prepare','password':password}),timeout=30)
        fixture=json.loads(setup.stdout.splitlines()[-1]);prepared=fixture['prepared'];report['fixture']=fixture
        report['phase_login_isolation']={}
        for target,base in [('original',source_base),('candidate',candidate_base)]:
            if target=='candidate':
                before_witness=command(helper,payload=json.dumps({'op':'login_history'}),timeout=30)
                report['login_history_before_witness']=json.loads(before_witness.stdout.splitlines()[-1])
                witness=login_limit_witness(source_base,fixture['roles'][0]['username'],password);report['actual_login_budget_witness']=witness
                if not witness['passed']:raise RuntimeError('Original login bucket witness differs from expected third/fourth statuses')
            isolation=command(helper,payload=json.dumps({'op':'reset_login_history'}),timeout=30)
            report['phase_login_isolation'][target]=json.loads(isolation.stdout.splitlines()[-1])
            observations[target]=[]
            for role in fixture['roles']:
                config={**role,'password':password,'url':base,'op':'vod_categories'}
                result=subprocess.run(['node',str(tools/'scripts/rust-vod-api-contracts/category_browser.js')],input=json.dumps(config)+'\n',cwd=tools,env=environment,text=True,capture_output=True,timeout=90)
                lines=result.stdout.splitlines();parsed=[json.loads(line)for line in lines if line.startswith('{')]
                observation=parsed[-1]if parsed else{'passed':False,'failure':'No browser result','returncode':result.returncode}
                observations[target].append(observation)
                (evidence/('browser-'+target+'-'+role['kind']+'.json')).write_text(json.dumps({'capability':parsed[0]if parsed else None,'result':observation,'exit_code':result.returncode},indent=2)+'\n')
                if result.returncode or not observation.get('passed'):raise RuntimeError('Actual category browser workflow failed: '+target+'/'+role['kind'])
        comparisons=[]
        for original,candidate in zip(observations['original'],observations['candidate']):
            equal=all(original[key]==candidate[key]for key in ['kind','selected_category','content','visible_cards'])
            comparisons.append({'kind':original['kind'],'exact_selected_label_and_ordered_content_and_visible_count':equal})
        report.update(passed=all(row['exact_selected_label_and_ordered_content_and_visible_count']for row in comparisons),comparisons=comparisons,observations=observations)
    finally:
        if prepared:
            restored=command(helper,payload=json.dumps({'op':'restore'}),timeout=30)
            report['restoration']=json.loads(restored.stdout.splitlines()[-1]);report['passed']=report['passed']and report['restoration']['restored']
        report['observations']=observations
        (evidence/'packaged-category-browser.json').write_text(json.dumps(report,indent=2)+'\n')
    return report
