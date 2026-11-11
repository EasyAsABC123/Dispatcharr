#!/usr/bin/env python3
"""Guarded original VOD REST oracle and optional native SQL component runner.
Native SQL comparison does not establish native HTTP or performance parity.
All application credit belongs to original Dispatcharr authors/contributors.
Each observation mode receives a fresh owned disposable Source Compose project.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration')]
from shared_ts import BASELINE,IMAGE,fixture_source
from fixture_ports import write_port_overlay
from run import candidate_overlay,validate_candidate_image


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['factory','http','both'],default='both')
    parser.add_argument('--native-sql',action='store_true',help='Compare ordinary read component on actual Source database; not native HTTP/app acceptance')
    parser.add_argument('--frozen-rust-source',type=Path,help='Reuse an already captured coherent Rust tree for a harness-only correction')
    parser.add_argument('--category-query-source',action='store_true',help='Actual original category ordering SQL and installed search parser helpers')
    parser.add_argument('--category-defaults-wire-source',action='store_true',help='Actual original network default BooleanField exception responses/effects')
    parser.add_argument('--category-initial-source',action='store_true',help='Actual original network category initialization fault/committed-prefix capture')
    parser.add_argument('--category-wire-source',action='store_true',help='Actual original category HEAD/OPTIONS/negotiation/JSON indentation capture')
    parser.add_argument('--category-forms-source',action='store_true',help='Original network extra category form/order/search grammar')
    parser.add_argument('--category-defaults-source',action='store_true',help='Original Factory categoryBoolean default/effect corpus')
    parser.add_argument('--category-schedule-source',action='store_true',help='Capture actual original category SQL/freshFK schedule only')
    parser.add_argument('--native-categories',action='store_true',help='New test-only category SQL component; no app routes')
    parser.add_argument('--unified-html-source',action='store_true',help='Original rawHTML/JSONnegotiation GET/HEAD repeats/cookies/assets/license inventory')
    parser.add_argument('--shared-auth-priority-source',action='store_true',help='Original297HTTP+99Factory network/userprops/permission priority matrix; GETonly')
    parser.add_argument('--integer-supplement-source',action='store_true',help='Predeclared37Originalinteger/SQL-phase observations; no nativeacceptance')
    parser.add_argument('--unified-edge-source',action='store_true',help='Original-only long-int/user-property faultboundary observations; no nativeacceptance')
    parser.add_argument('--unified-schedule-source',action='store_true',help='Actual Original raw-page/projection/fresh-count/error scheduling on owned PostgreSQL')
    parser.add_argument('--native-unified-http',action='store_true',help='Independent unified TCP/auth/queue component against actual Original network; no app attachment')
    parser.add_argument('--native-unified',action='store_true',help='Test-only Source unified SQL/page/DTO comparison; no app/HTTP permission acceptance')
    parser.add_argument('--unified-source',action='store_true',help='Actual Original unified HTTP/Factory SQL/query/rating/errors with exact fixture restoration; no native parity')
    parser.add_argument('--native-http',action='store_true',help='Run independent native TCP HTTP peer against original network corpus')
    parser.add_argument('--http-wire',action='store_true',help='Add original network method/detail/negotiation/routing characterization')
    parser.add_argument('--category-closed-gaps',action='store_true',help='Newcategory revision: requirezeroHTTP exclusions and compare18Source/native default exception responses')
    parser.add_argument('--category-browser',action='store_true',help='Actual unchanged React category selection for movie-only/series-only Standard users; requires packaged-categories')
    parser.add_argument('--unified-browser-source',action='store_true',help='Original-only unchangedReactAll proof with exactcompiledfrontend donor; no candidate')
    parser.add_argument('--frontend-build-manifest',type=Path,help='Successful immutableimage frontend donor for Source-only browser')
    parser.add_argument('--packaged-static',action='store_true',help='Require captured68 real Source nginx/static comparisons; requires packaged-html')
    parser.add_argument('--packaged-html',action='store_true',help='Require actual56 Source/package HTML andcookie comparisons; needs packaged-unified')
    parser.add_argument('--packaged-unified',action='store_true',help='Manifest-bound unifiedJSON182/4HTMLopen plusordinary393; no host peer build')
    parser.add_argument('--unified-browser',action='store_true',help='Actual unchangedReact defaultBoth/All andcategory workflow; requires packaged-unified')
    parser.add_argument('--packaged-categories',action='store_true',help='Manifest-bound category+ordinary app replay, mixed burst and initial fault; no host peer build')
    parser.add_argument('--build-manifest','--candidate-build-manifest',dest='build_manifest',type=Path,help='Successful immutable app manifest for packaged replay; skips native peer compilation')
    parser.add_argument('--candidate-image',help='Must equal the successful manifest tag')
    args=parser.parse_args()
    manifest=json.loads(args.build_manifest.read_text())if args.build_manifest else None
    frontend_manifest=json.loads(args.frontend_build_manifest.read_text())if args.frontend_build_manifest else manifest
    if args.unified_browser_source:
        if args.mode!='http'or manifest is not None or frontend_manifest is None or not frontend_manifest.get('passed'):parser.error('Source-onlyAllbrowserrequires--modehttp andsuccessful--frontend-build-manifest, nocandidate')
    if args.candidate_image and manifest is None:parser.error('--candidate-image requires --build-manifest')
    if args.category_closed_gaps and not args.packaged_categories:parser.error('--category-closed-gaps requires --packaged-categories')
    if args.category_browser and not args.packaged_categories:parser.error('--category-browser requires --packaged-categories')
    if args.unified_browser and not args.packaged_unified:parser.error('--unified-browser requires --packaged-unified')
    if args.packaged_static and not args.packaged_html:parser.error('--packaged-static requires --packaged-html')
    if args.packaged_html and not args.packaged_unified:parser.error('--packaged-html requires --packaged-unified')
    if args.packaged_unified:
        if manifest is None:parser.error('--packaged-unified requires successful manifest')
        args.unified_source=True
    if args.packaged_categories and manifest is None:parser.error('--packaged-categories requires --build-manifest')
    if args.packaged_categories:args.category_forms_source=True;args.category_wire_source=True
    if manifest is not None:
        if not manifest.get('passed')or(args.candidate_image and args.candidate_image!=manifest['tag']):parser.error('Successful matching candidate manifest required')
        if args.native_sql or args.native_http or args.native_categories:parser.error('Packaged replay must not compile a host native peer')
        if args.mode!='http':parser.error('Packaged replay requires --mode http')
        args.http_wire=True
    if args.native_unified_http:
        if args.mode!='http' or manifest is not None or args.native_sql or args.native_http or args.native_categories or args.native_unified:parser.error('--native-unified-http requires standalone --mode http')
        args.unified_source=True
    if args.native_unified:
        if args.mode!='factory' or manifest is not None or args.native_sql or args.native_http or args.native_categories:parser.error('--native-unified requires standalone --mode factory with no other native family')
        args.unified_source=True;args.unified_schedule_source=True
    if args.unified_schedule_source and args.mode!='factory':parser.error('--unified-schedule-source observes Factory/SQL only; requires --mode factory')
    if args.native_categories:args.native_sql=True;args.category_schedule_source=True;args.category_defaults_source=True;args.category_initial_source=True;args.category_defaults_wire_source=True;args.category_query_source=True
    if args.native_http:args.native_sql=True;args.http_wire=True
    if args.http_wire and args.mode!='http':parser.error('--http-wire requires --mode http')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    report={'scope':'Original VOD REST metadata-only capture; separate Factory and network fixtures',
        'passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,
        'native_verified':False,'provider_refresh_verified':False,'performance_verified':False,'modes':[]}
    modes=['factory','http']if args.mode=='both'else[args.mode]
    try:
        for mode in modes:
            evidence=out/mode;evidence.mkdir()
            files=[Path(__file__).resolve(),Path(__file__).with_name('source.py'),*([Path(__file__).with_name(name)for name in ['static_packaged_probe.py','static-nginx-source-1.json','unified_chrome_html_source.py']]if args.packaged_static else[]),*([Path(__file__).with_name(name)for name in ['integer_api_supplement_source.py','supplement-api-spec.json']]if args.integer_supplement_source else[]),*([Path(__file__).with_name('unified_html_source.py')]if args.unified_html_source or args.packaged_html else[]),*([Path(__file__).with_name('packaged_html_probe.py')]if args.packaged_html else[]),*([Path(__file__).with_name('shared_auth_priority_source.py')]if args.shared_auth_priority_source else[]),*([Path(__file__).with_name('unified_edges_source.py')]if args.unified_edge_source else[]),*([Path(__file__).with_name('unified_packaged_probe.py')]if args.packaged_unified else[]),*([Path(__file__).with_name(name)for name in ['unified_browser.js','unified_browser_fixture.py','unified_browser_runner.py']]if args.unified_browser or args.unified_browser_source else[]),*([Path(__file__).with_name('unified_burst_source.py')]if args.native_unified_http else[]),*([Path(__file__).with_name('unified-tie-policy.json')]if args.native_unified or args.native_unified_http or args.packaged_unified else[]),*([Path(__file__).with_name('unified_schedule_source.py')]if args.unified_schedule_source else[]),*([Path(__file__).with_name('unified_source.py')]if args.unified_source else[]),*([Path(__file__).with_name('category_query_semantics_source.py')]if args.category_query_source else[]),*([Path(__file__).with_name('category_defaults_wire_source.py')]if args.category_defaults_wire_source or args.category_closed_gaps else[]),*([Path(__file__).with_name('category_initial_source.py')]if args.category_initial_source or args.packaged_categories else[]),*([Path(__file__).with_name('category_burst_source.py')]if args.native_categories or args.packaged_categories else[]),*([Path(__file__).with_name('category_defaults_source.py')]if args.category_defaults_source else[]),*([Path(__file__).with_name('category_schedule_source.py')]if args.category_schedule_source else[]),*([Path(__file__).with_name('packaged_probe.py'),Path(__file__).with_name('effects_source.py'),ROOT/'docker/compose.rust-candidate.yml',ROOT/'scripts/rust-integration/run.py']if manifest is not None else[]),*([Path(__file__).with_name('burst_source.py')]if args.http_wire else[]),
                *([Path(__file__).with_name('category_packaged_probe.py')]if args.packaged_categories else[]),
                *([Path(__file__).with_name(name)for name in ['category_browser.js','category_browser_fixture.py','category_browser_runner.py']]if args.category_browser else[]),
                ROOT/'docker/compose.rust-playback.yml',ROOT/'scripts/rust-integration/shared_ts.py',
                ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'scripts/rust-baseline/dependencies.py',
                *([ROOT/'scripts/rust-vod-api-contracts/source-vod-rest-1.json',ROOT/'scripts/rust-vod-api-contracts/source-default-queries-1.json',ROOT/'scripts/rust-vod-api-contracts/schedule_source.py',ROOT/'scripts/rust-vod-api-contracts/category_fault_source.py']if args.native_sql else []),
                *[ROOT/'scripts/rust-playback-baseline'/name for name in
                  ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']]]
            tools=evidence/'tools';hashes={}
            for path in files:
                target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True)
                data=path.read_bytes();target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
            if args.native_sql or args.native_unified or args.native_unified_http:
                rust=(args.frozen_rust_source or(ROOT/'rust')).resolve()
                if not(rust/'Cargo.toml').is_file():raise RuntimeError('Rust source root missingCargo.toml')
                for directory,names,filenames in os.walk(rust):
                    names[:]=[name for name in names if name not in ['target','.git','__pycache__']]
                    for filename in filenames:
                        path=Path(directory)/filename;target=tools/'rust'/path.relative_to(rust)
                        target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
                rust_hashes={str(path.relative_to(tools/'rust')):hashlib.sha256(path.read_bytes()).hexdigest()for path in(tools/'rust').rglob('*')if path.is_file()}
                result_manifest=hashlib.sha256(json.dumps(rust_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                (evidence/'frozen-source-ready.json').write_text(json.dumps({'rust_manifest_sha256':result_manifest}))
            project='dispatcharr-vod-api-'+mode+'-'+uuid.uuid4().hex[:10]
            result={'scope':'Original '+mode+' VOD REST source capture','passed':False,
                'project':project,'tool_sha256':hashes,'observation':mode,'commands':[]}
            if args.native_sql or args.native_unified or args.native_unified_http:
                result['native_rust_manifest_sha256']=result_manifest
                result['native_rust_source_input']=str(rust)
            with fixture_source(result,evidence,'source-run.json')as temporary:
                source=Path(temporary)/'source';source.mkdir()
                archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT)
                result['baseline_archive_sha256']=hashlib.sha256(archive).hexdigest()
                subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=archive,check=True)
                (source/'media').mkdir(exist_ok=True)
                environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(evidence),
                    FIXTURE_SECONDS='900',DISPATCHARR_ENV='aio')
                ports=evidence/'ports.yml';result['ports']=write_port_overlay(ports,{'baseline':[5656,5432,6379]if args.native_sql or args.native_unified or args.native_unified_http else[5656],'origin':[8080],**({'candidate':[9192]}if manifest is not None else{})})
                overlay=evidence/'oracle.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[
                    str(tools/'scripts/rust-vod-api-contracts')+':/vod-api-probe:ro']}}}))
                compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
                if manifest is not None:
                    candidate=candidate_overlay(tools/'docker/compose.rust-candidate.yml',evidence/'candidate.yml',manifest['tag']);compose+=['-f',str(candidate),'-f',str(ports)]
                    result['build_manifest_sha256']=hashlib.sha256(args.build_manifest.read_bytes()).hexdigest();shutil.copyfile(args.build_manifest,evidence/'build-manifest.json')
                if args.category_browser or args.unified_browser or args.unified_browser_source:
                    browser_overlay=evidence/'browser-network-guard.yml';browser_overlay.write_text(json.dumps({'services':{service:{'environment':{'DISPATCHARR_ENABLE_IP_LOOKUP':'false'}}for service in(['baseline']if manifest is None else['baseline','candidate'])}}));compose+=['-f',str(browser_overlay)]
                    result['browser_network_guard']='IP lookup disabled by documentedenvironment flag onownedbrowser services; posters/provider/playback/episode flows excluded; Allview onlywhenexplicitrequested'
                def command(argv,timeout=180,payload=None):
                    result['commands'].append(argv)
                    completed=subprocess.run(argv,cwd=ROOT,env=environment,input=payload,text=True,capture_output=True,timeout=timeout)
                    if completed.returncode:
                        (evidence/'failed-command.log').write_text(completed.stdout+'\n'+completed.stderr)
                        completed.check_returncode()
                    return completed
                try:
                    if args.category_browser or args.unified_browser or args.unified_browser_source:
                        compiled=evidence/'compiled-frontend';compiled.mkdir();(source/'frontend/dist').mkdir(parents=True,exist_ok=True)
                        inspected_image=json.loads(command(['docker','image','inspect',frontend_manifest['tag']]).stdout)[0];assert inspected_image['Id']==frontend_manifest['image_id'],'Frontend donor image identity changed'
                        frontend_container=command(['docker','create',frontend_manifest['tag']]).stdout.strip()
                        try:command(['docker','cp',frontend_container+':/opt/dispatcharr-frontend/.',str(compiled)])
                        finally:command(['docker','rm',frontend_container])
                        frontend_hashes={str(path.relative_to(compiled)):hashlib.sha256(path.read_bytes()).hexdigest()for path in compiled.rglob('*')if path.is_file()}
                        assert 'index.html'in frontend_hashes and len(frontend_hashes)>10
                        (evidence/'frontend-sha256.json').write_text(json.dumps(frontend_hashes,indent=2)+'\n')
                        ini=evidence/'original-browser-uwsgi.ini';ini.write_text((tools/'scripts/rust-playback-baseline/uwsgi.ini').read_text()+'\nstatic-map = /static=/app/frontend/dist\nstatic-map = /assets=/app/frontend/dist/assets\ncheck-static = /app/frontend/dist\n')
                        frontend_overlay=evidence/'browser-frontend.yml';frontend_overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(compiled)+':/app/frontend/dist:ro',str(ini)+':/verification/uwsgi.ini:ro']}}}));compose+=['-f',str(frontend_overlay)]
                        result['browser_frontend']={'donor_image_id':inspected_image['Id'],'files':len(frontend_hashes),'sha256':hashlib.sha256(json.dumps(frontend_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'scope':'Exact unchanged canonical React bundle, mounted into original git-source fixture and served by uWSGI static-map; no JS/backend view patches'}
                    command(compose+['up','-d','--no-build','baseline','origin'])
                    # Use a Docker exec health probe: no host public-interface
                    # assumptions or retry of observed API workloads.
                    deadline=time.monotonic()+180
                    while True:
                        ready=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',
                            "from urllib.request import urlopen;assert urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],
                            cwd=ROOT,env=environment,text=True,capture_output=True,timeout=10)
                        if ready.returncode==0:break
                        if time.monotonic()>=deadline:raise RuntimeError('Original fixture readiness deadline')
                        time.sleep(.2)
                    captured=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only',
                        '-e','VOD_API_OBSERVATION='+mode,*(['-e','VOD_CATEGORY_WIRE=1']if args.category_wire_source else[]),*(['-e','VOD_CATEGORY_FORMS=1']if args.category_forms_source else[]),*(['-e','VOD_API_HTTP_WIRE=1']if args.http_wire else[]),'baseline','/dispatcharrpy/bin/python','/vod-api-probe/source.py'],timeout=300)
                    (evidence/'source-worker.log').write_text(captured.stderr)
                    corpus=json.loads(captured.stdout.splitlines()[-1]);data=json.dumps(corpus,indent=2,ensure_ascii=False)+'\n'
                    (evidence/'source-corpus.json').write_text(data)
                    result.update(passed=corpus['capture_complete'],cases=len(corpus['cases']),
                        corpus_sha256=hashlib.sha256(data.encode()).hexdigest(),provider_trap_attempts=len(corpus['provider_trap_attempts']),
                        provider_refresh_rows_unchanged=corpus['provider_refresh_rows_unchanged'])
                    assert result['passed'],'Incomplete Source metadata capture or unexpected provider refresh'
                    if args.unified_source:
                        unified=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_UNIFIED_OBSERVATION='+mode,'baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_source.py'],timeout=300)
                        (evidence/'unified-source-worker.log').write_text(unified.stderr)
                        observation=json.loads(unified.stdout.splitlines()[-1]);assert observation['passed'];(evidence/'unified-source.json').write_text(json.dumps(observation,indent=2,ensure_ascii=False)+'\n')
                        result['unified_Source_observations']=len(observation['records']);result['unified_Source_restored']=observation['exact_owned_rows_restored']
                    if args.unified_schedule_source:
                        schedule=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_schedule_source.py'],timeout=30)
                        observed_schedule=json.loads(schedule.stdout.splitlines()[-1]);assert observed_schedule['passed'];(evidence/'unified-schedule-source.json').write_text(json.dumps(observed_schedule,indent=2,ensure_ascii=False)+'\n');result['unified_schedule_Source_passed']=True
                    if args.native_unified_http:
                        burst_unified=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_burst_source.py'],timeout=30)
                        observed_unified_burst=json.loads(burst_unified.stdout.splitlines()[-1]);assert observed_unified_burst['passed'];(evidence/'unified-burst-source.json').write_text(json.dumps(observed_unified_burst,indent=2)+'\n')
                    if args.http_wire:
                        burst=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/burst_source.py'],timeout=30)
                        observed=json.loads(burst.stdout.splitlines()[-1]);assert observed['passed'];(evidence/'source-burst.json').write_text(json.dumps(observed,indent=2)+'\n')
                    if args.native_categories or args.packaged_categories:
                        burst=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_burst_source.py'],timeout=30)
                        observed_categories=json.loads(burst.stdout.splitlines()[-1]);assert observed_categories['passed'];(evidence/'source-category-burst.json').write_text(json.dumps(observed_categories,indent=2)+'\n')
                        mixed=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_CATEGORY_BURST_MIXED=1','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_burst_source.py'],timeout=30)
                        observed_mixed=json.loads(mixed.stdout.splitlines()[-1]);assert observed_mixed['passed'];(evidence/'source-vod-mixed-burst.json').write_text(json.dumps(observed_mixed,indent=2)+'\n')
                    if manifest is not None:
                        command(compose+['up','-d','--no-build','candidate'])
                        identifier=command(compose+['ps','-q','candidate']).stdout.strip()
                        inspected=json.loads(command(['docker','inspect',identifier]).stdout)[0]
                        result['candidate_binding']=validate_candidate_image(manifest,inspected)
                        candidate_base='http://'+command(compose+['port','candidate','9192']).stdout.strip()
                        import urllib.request
                        deadline=time.monotonic()+90
                        while True:
                            try:
                                with urllib.request.urlopen(candidate_base+'/api/core/version/',timeout=2)as response:assert response.status==200
                                break
                            except (OSError,AssertionError):
                                if time.monotonic()>=deadline:raise RuntimeError('Packaged candidate readiness deadline')
                                time.sleep(.1)
                        sys.path.insert(0,str(tools/'scripts/rust-vod-api-contracts'))
                        from packaged_probe import probe
                        packaged=probe(candidate_base,corpus,observed)
                        (evidence/'packaged-http.json').write_text(json.dumps(packaged,indent=2)+'\n')
                        result['packaged_ordinary_http_passed']=packaged['passed'];assert packaged['passed'],'Packaged ordinary HTTP mismatch'
                        if args.packaged_unified:
                            from unified_packaged_probe import probe as unified_probe
                            html_package=None
                            if args.packaged_html:
                                from packaged_html_probe import probe as html_probe
                                html_package=html_probe(command,compose,evidence,result['candidate_binding']);result['packaged_HTML_passed']=html_package['passed'];assert html_package['passed'],'Packaged HTML56/cookie/mask mismatch'
                                if args.packaged_static:
                                    from static_packaged_probe import probe as static_probe
                                    static_package=static_probe(command,compose,evidence,result['candidate_binding']);result['packaged_static_passed']=static_package['passed'];assert static_package['passed'],'Packaged static68 mismatch'
                                    chrome_package=html_probe(command,compose,evidence,result['candidate_binding'],observer='unified_chrome_html_source.py',expected_count=32,label='Chrome-HTML');result['packaged_Chrome_HTML_passed']=chrome_package['passed'];assert chrome_package['passed'],'Packaged realChromeAccept32 mismatch'
                            unified_package=unified_probe(command,compose,evidence,observation,html_proof=html_package);result['packaged_unified_passed']=unified_package['passed'];assert unified_package['passed'],'PackagedunifiedHTTP/body/header/effects mismatch'
                        if args.packaged_categories:
                            from category_packaged_probe import probe as category_probe
                            categories=category_probe(candidate_base,corpus,observed_categories,observed_mixed,expected_exclusions=0 if args.category_closed_gaps else 4)
                            (evidence/'packaged-category-http.json').write_text(json.dumps(categories,indent=2)+'\n');result['packaged_category_http_passed']=categories['passed'];assert categories['passed'],'Packaged category wire/mixed mismatch'
                            initial_observations={}
                            for target in ['original','candidate']:
                                initial=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_CATEGORY_INITIAL_TARGET='+target,'baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_initial_source.py'],timeout=30)
                                initial_observations[target]=json.loads(initial.stdout.splitlines()[-1])
                            if args.category_closed_gaps:
                                default_observations={}
                                for target in ['original','candidate']:
                                    observed_defaults=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_CATEGORY_DEFAULTS_TARGET='+target,'baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_defaults_wire_source.py'],timeout=45)
                                    default_observations[target]=json.loads(observed_defaults.stdout.splitlines()[-1])
                                default_pass=default_observations['original']['records']==default_observations['candidate']['records']and all(row['normal_fixture_props_and_exact_links_restored']for row in default_observations.values())
                                (evidence/'packaged-category-defaults-wire.json').write_text(json.dumps({'passed':default_pass,'observations':default_observations},indent=2)+'\n');result['packaged_category_default_wire_passed']=default_pass;assert default_pass,'Packagedcategory default500wire/effects mismatch'
                            original_initial=initial_observations['original'];candidate_initial=initial_observations['candidate']
                            initial_pass=all(original_initial[key]==candidate_initial[key]for key in ['response','committed_category_type_prefix','application_headers'])and all(row['original_category_and_relationship_rows_exactly_restored']for row in initial_observations.values())
                            (evidence/'packaged-category-initial.json').write_text(json.dumps({'passed':initial_pass,'observations':initial_observations},indent=2)+'\n');result['packaged_category_initial_passed']=initial_pass;assert initial_pass,'Packaged category initial fault/prefix mismatch'

                        effects=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/effects_source.py'],timeout=30)
                        after=json.loads(effects.stdout.splitlines()[-1]);before={key:corpus['final_effects'][key]for key in after}
                        (evidence/'packaged-effects.json').write_text(json.dumps({'passed':after==before,'before':before,'after':after},indent=2)+'\n');assert after==before,'Packaged reads changed owned metadata/provider refresh rows'
                        if args.category_browser:
                            from category_browser_runner import probe as browser_probe
                            source_base='http://'+command(compose+['port','baseline','5656']).stdout.strip()
                            browser=browser_probe(command,compose,environment,evidence,tools,source_base,candidate_base)
                            result['packaged_category_browser_passed']=browser['passed'];assert browser['passed'],'Actual unchanged React category workflow mismatch'
                            restored_effects=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/effects_source.py'],timeout=30)
                            final_browser_effects=json.loads(restored_effects.stdout.splitlines()[-1]);assert final_browser_effects==before,'Browser fixture did not restore metadata/provider state'
                        if args.unified_browser:
                            from unified_browser_runner import probe as All_browser_probe
                            source_base='http://'+command(compose+['port','baseline','5656']).stdout.strip()
                            All_browser=All_browser_probe(command,compose,environment,evidence,tools,source_base,candidate_base,browsable=args.packaged_static);result['packaged_unified_browser_passed']=All_browser['passed'];assert All_browser['passed'],'Actual unchangedReact Both/All mismatch'
                            restored=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/effects_source.py'],timeout=30)
                            assert json.loads(restored.stdout.splitlines()[-1])==before,'Allbrowser didnotrestoreownedVODmetadata'
                        shutdown_at=time.monotonic();command(compose+['stop','--timeout','15','candidate'],timeout=25)
                        stopped=json.loads(command(['docker','inspect',identifier]).stdout)[0]['State']
                        result['candidate_shutdown']={'elapsed_seconds':time.monotonic()-shutdown_at,'exit_code':stopped['ExitCode'],'running':stopped['Running']}
                        assert not stopped['Running']and stopped['ExitCode']==0,'Packaged candidate did not stop gracefully'
                        result['application_image_verified']=True
                    if args.unified_browser_source:
                        sys.path.insert(0,str(tools/'scripts/rust-vod-api-contracts'))
                        from unified_browser_runner import probe as source_All_browser_probe
                        source_base='http://'+command(compose+['port','baseline','5656']).stdout.strip()
                        browser_source=source_All_browser_probe(command,compose,environment,evidence,tools,source_base,None);result['Source_unchangedReact_All']=browser_source['passed'];assert browser_source['passed'],'ActualSourceAllbrowserflowfailed'
                    if args.category_initial_source:
                        initial=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_initial_source.py'],timeout=30)
                        result['original_category_initial']=json.loads(initial.stdout.splitlines()[-1]);(evidence/'category-initial-source.json').write_text(json.dumps(result['original_category_initial'],indent=2)+'\n');assert result['original_category_initial']['passed']
                    if args.category_query_source:
                        query_observed=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_query_semantics_source.py'],timeout=30)
                        result['original_category_query_semantics']=json.loads(query_observed.stdout.splitlines()[-1]);(evidence/'category-query-source.json').write_text(json.dumps(result['original_category_query_semantics'],indent=2)+'\n');assert result['original_category_query_semantics']['passed']
                    if args.category_defaults_wire_source:
                        wire=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_defaults_wire_source.py'],timeout=45)
                        result['original_category_defaults_wire']=json.loads(wire.stdout.splitlines()[-1]);(evidence/'category-defaults-wire-source.json').write_text(json.dumps(result['original_category_defaults_wire'],indent=2)+'\n');assert result['original_category_defaults_wire']['passed']
                    if args.category_defaults_source:
                        defaults=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_defaults_source.py'],timeout=30)
                        result['original_category_defaults']=json.loads(defaults.stdout.splitlines()[-1]);(evidence/'category-defaults-source.json').write_text(json.dumps(result['original_category_defaults'],indent=2)+'\n')
                        assert result['original_category_defaults']['passed']
                    if args.category_schedule_source:
                        category_schedule=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only',*(['-e','VOD_CATEGORY_NATIVE_PREPARE=1']if args.native_categories else[]),'baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_schedule_source.py'],timeout=30)
                        result['original_category_schedule']=json.loads(category_schedule.stdout.splitlines()[-1]);(evidence/'category-schedule-source.json').write_text(json.dumps(result['original_category_schedule'],indent=2)+'\n')
                        assert result['original_category_schedule']['passed']
                    if args.native_sql and not args.native_categories:
                        scheduled=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/schedule_source.py'],timeout=30)
                        (evidence/'source-schedule.json').write_text(json.dumps(json.loads(scheduled.stdout.splitlines()[-1]),indent=2)+'\n')
                        faulted=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_fault_source.py'],timeout=30)
                        (evidence/'source-category-fault.json').write_text(json.dumps(json.loads(faulted.stdout.splitlines()[-1]),indent=2)+'\n')
                    if args.unified_edge_source:
                        edge=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_UNIFIED_EDGE_MODE='+mode,'baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_edges_source.py'],timeout=90)
                        (evidence/'unified-edges-worker.log').write_text(edge.stderr)
                        edges=json.loads(edge.stdout.splitlines()[-1]);assert edges['passed'];(evidence/'unified-edges-source.json').write_text(json.dumps(edges,indent=2,ensure_ascii=False)+'\n');result['Source_unified_edges_count']=len(edges['records'])
                    if args.integer_supplement_source:
                        try:
                            supplement=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_UNIFIED_EDGE_MODE='+mode,'baseline','/dispatcharrpy/bin/python','/vod-api-probe/integer_api_supplement_source.py'],timeout=180)
                        except subprocess.CalledProcessError as error:
                            (evidence/'integer-api-supplement-failure-stderr.log').write_text(error.stderr or '')
                            (evidence/'integer-api-supplement-failure-stdout.log').write_text(error.stdout or '')
                            raise
                        (evidence/'integer-api-supplement-worker.log').write_text(supplement.stderr);observed_supplement=json.loads(supplement.stdout.splitlines()[-1]);assert observed_supplement['passed_observer']and observed_supplement['owned_rows_restored'];(evidence/'integer-api-supplement-source.json').write_text(json.dumps(observed_supplement,indent=2,ensure_ascii=False)+'\n');result['Source_integer_supplement_records']=len(observed_supplement['records'])
                    if args.shared_auth_priority_source:
                        priority=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/shared_auth_priority_source.py'],timeout=120)
                        (evidence/'shared-auth-priority-worker.log').write_text(priority.stderr);value=json.loads(priority.stdout.splitlines()[-1]);assert value['passed'];(evidence/'shared-auth-priority-source.json').write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n');result['Source_shared_auth_priority_records']=len(value['records'])
                    if args.unified_html_source:
                        html=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_html_source.py'],timeout=150)
                        (evidence/'unified-html-worker.log').write_text(html.stderr);value=json.loads(html.stdout.splitlines()[-1]);assert value['passed'];(evidence/'unified-html-source.json').write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n');result['Source_unified_HTML_records']=len(value['records'])
                    if args.native_sql or args.native_unified or args.native_unified_http:
                        pgport=command(compose+['port','baseline','5432']).stdout.strip().rsplit(':',1)[1]
                        redisport=command(compose+['port','baseline','6379']).stdout.strip().rsplit(':',1)[1]
                        original_http_base='http://'+command(compose+['port','baseline','5656']).stdout.strip()
                        native_environment=dict(environment,VOD_UNIFIED_BURST_REFERENCE=str(evidence/'unified-burst-source.json'),VOD_UNIFIED_TIE_POLICY=str(tools/'scripts/rust-vod-api-contracts/unified-tie-policy.json'),VOD_UNIFIED_ORIGINAL_HTTP_BASE=original_http_base,VOD_UNIFIED_SCHEDULE_REFERENCE=str(evidence/'unified-schedule-source.json'),VOD_UNIFIED_SOURCE=str(evidence/'unified-source.json'),VOD_CATEGORY_DEFAULTS_WIRE_REFERENCE=str(evidence/'category-defaults-wire-source.json'),VOD_CATEGORY_QUERY_REFERENCE=str(evidence/'category-query-source.json'),VOD_MIXED_BURST=str(evidence/'source-vod-mixed-burst.json'),VOD_CATEGORY_INITIAL_REFERENCE=str(evidence/'category-initial-source.json'),VOD_CATEGORY_BURST=str(evidence/'source-category-burst.json'),VOD_CATEGORY_DEFAULTS_REFERENCE=str(evidence/'category-defaults-source.json'),VOD_CATEGORY_SCHEDULE_REFERENCE=str(evidence/'category-schedule-source.json'),VOD_CATEGORY_NATIVE='playback-fixture-only',VOD_TEST_NATIVE_HTTP='1'if args.native_http else'0',VOD_SOURCE_BURST=str(evidence/'source-burst.json'),REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:'+redisport,VOD_TEST_CACHED_AUTH='1',DATABASE_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:'+pgport+'/dispatcharr',
                            VOD_API_NATIVE_FIXTURE='playback-fixture-only',VOD_SOURCE_CORPUS=str(evidence/'source-corpus.json'),VOD_NATIVE_EVIDENCE=str(evidence),VOD_SOURCE_DEFAULT_SQL=str(tools/'scripts/rust-vod-api-contracts/source-default-queries-1.json'),VOD_SOURCE_SCHEDULE=str(evidence/'source-schedule.json'),VOD_SOURCE_CATEGORY_FAULT=str(evidence/'source-category-fault.json'))
                        target=Path(os.environ.get('VOD_CARGO_TARGET_DIR',str(ROOT/'artifacts/rust/vod-rest-cargo-target')))
                        strict=['cargo','clippy','--manifest-path',str(tools/'rust/Cargo.toml'),'--target-dir',str(target),'--locked','--offline','-p','dispatcharr-store','--all-targets','--','-D','warnings']
                        with(evidence/'native-strict-clippy.log').open('w')as log:
                            subprocess.run(strict,cwd=ROOT,env=native_environment,text=True,stdout=log,stderr=subprocess.STDOUT,timeout=240,check=True)
                        argv=['cargo','test','--manifest-path',str(tools/'rust/Cargo.toml'),'--target-dir',str(target),'--locked','--offline','-p','dispatcharr-store','--lib',
                            'vod_api_draft::vod_api_unified_http::tests'if args.native_unified_http else'vod_api_draft::vod_api_unified_read'if args.native_unified else'vod_api_draft::vod_api_categories::tests'if args.native_categories else'vod_api_draft::vod_api_native_tests','--',*(['--skip','http_']if not(args.native_http or args.native_unified_http)else[]),'--ignored','--nocapture','--test-threads=1']
                        result['native_command']=argv
                        with(evidence/'native-sql-tests.log').open('w')as log:
                            native=subprocess.run(argv,cwd=ROOT,env=native_environment,text=True,stdout=log,stderr=subprocess.STDOUT,timeout=240)
                        native.check_returncode()
                        if args.native_unified_http:
                            result['native_unified_http']=json.loads((evidence/'unified-http.json').read_text());assert result['native_unified_http']['passed'],'Unified HTTP mismatch'
                            result['native_unified_http_shared']=json.loads((evidence/'unified-http-shared.json').read_text());assert result['native_unified_http_shared']['passed'],'Unified shared queue/auth/mixed burst mismatch'
                            result['native_unified_edges']=json.loads((evidence/'unified-edges-native.json').read_text());assert result['native_unified_edges']['passed']and result['native_unified_edges']['compared']==77,'ExactJSONedges77 mismatch'
                        elif args.native_unified:
                            result['native_unified_component']=json.loads((evidence/'unified-sql.json').read_text());assert result['native_unified_component']['passed'],'Unified SQL component mismatch'
                            result['native_unified_additional_gates']={}
                            for gate in ['unified-schedule-native.json','unified-large-resources.json','unified-physical-sql.json','unified-ordering-plans.json','unified-zero-logo.json']:
                                value=json.loads((evidence/gate).read_text());result['native_unified_additional_gates'][gate]=value['passed'];assert value['passed'],gate+' mismatch'

                        elif args.native_categories:
                            result['native_category_component']=json.loads((evidence/'category-sql.json').read_text());assert result['native_category_component']['passed']
                        else:
                            result['native_ordinary_component']=json.loads((evidence/'ordinary-sql.json').read_text());assert result['native_ordinary_component']['passed'],'Native ordinary component mismatch'
                        if args.native_http:
                            result['native_http_component']=json.loads((evidence/'ordinary-http.json').read_text());assert result['native_http_component']['passed']
                        result['native_scope']='Original unified SQL projection/query/error cases only; auth/HTTP/resources/planner/React/app remain separate'if args.native_unified else'Only captured original category GET vs new test-only SQL component; no categoryHTTP/app/fault claims'if args.native_categories else'Only original ordinary read cases vs nativeSQL component; outside/unsupported families not counted as passes'

                except Exception as error:
                    result.update(failure=type(error).__name__,failure_message=str(error));result['passed']=False
                finally:
                    try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],timeout=60);result['cleanup']='owned Compose project removed'
                    except Exception:result['cleanup_error']=True;result['passed']=False
            report['modes'].append({'mode':mode,'passed':result['passed'],'cases':result.get('cases'),
                'evidence':str(evidence/'source-run.json'),'cleanup':result.get('cleanup')})
            if not result['passed']:break
        report['passed']=len(report['modes'])==len(modes)and all(row['passed']for row in report['modes'])
    finally:
        (out/'source-run.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'modes':report['modes'],'output':str(out)}))
    return 0 if report['passed']else 1

if __name__=='__main__':raise SystemExit(main())
