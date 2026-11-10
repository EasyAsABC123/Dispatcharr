"""Canonical-fixture actual unchanged React Both/All workflow.
Original Dispatcharr authors retain credit; no resource/performance claim.
"""
import json,secrets,subprocess

def probe(command,compose,environment,evidence,tools,source_base,candidate_base):
    password=secrets.token_urlsafe(32)
    helper=compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_browser_fixture.py']
    report={'passed':False,'scope':__doc__,'unchanged_React':True,'browser_not_mocked':True,'posters_provider_refresh_playback_episode_flows_excluded':True,'login_policy':'Exactly one normal UI login per Original/native phase, no runtime3/min adjustment or cache clearing'}
    prepared=False;observations={}
    try:
        setup=command(helper,payload=json.dumps({'op':'prepare','password':password}),timeout=30)
        fixture=json.loads(setup.stdout.splitlines()[-1]);prepared=fixture['prepared'];report['fixture']=fixture
        for target,base in ([('Original',source_base)]if candidate_base is None else[('Original',source_base),('native',candidate_base)]):
            config={**fixture,'password':password,'url':base,'op':'All'}
            process=subprocess.run(['node',str(tools/'scripts/rust-vod-api-contracts/unified_browser.js')],input=json.dumps(config)+'\n',cwd=tools,env=environment,text=True,capture_output=True,timeout=100)
            values=[json.loads(line)for line in process.stdout.splitlines()if line.startswith('{')]
            observation=values[-1]if values else{'passed':False,'error':'No actual browser result'};observations[target]=observation
            (evidence/('All-browser-'+target+'.json')).write_text(json.dumps({'capability':values[0]if values else None,'result':observation,'exit_code':process.returncode},indent=2)+'\n')
            if process.returncode or not observation.get('passed'):raise RuntimeError('Actual Both/All React workflow failed: '+target)
        keys=['initial_count','All_content','All_card_texts','selected_category','selected_content','selected_card_texts']
        equal=True if candidate_base is None else all(observations['Original'][key]==observations['native'][key]for key in keys)
        report.update(passed=equal,Source_only=candidate_base is None,native_verified=candidate_base is not None,exact_ordered_content_kind_ID_name_and_visible_cards=equal if candidate_base is not None else None)
    finally:
        if prepared:
            restore=command(helper,payload=json.dumps({'op':'restore'}),timeout=30);report['restoration']=json.loads(restore.stdout.splitlines()[-1]);report['passed']=report['passed']and report['restoration']['restored']
        report['observations']=observations
        (evidence/('Source-unified-browser.json'if candidate_base is None else'packaged-unified-browser.json')).write_text(json.dumps(report,indent=2)+'\n')
    return report
