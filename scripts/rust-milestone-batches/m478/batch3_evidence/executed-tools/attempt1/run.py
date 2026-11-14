"""One packaged Main transport16 + retained unsigned25 acceptance cohort.

Root supplies the built candidate and global fixture slot. This controller never
builds an image or executes Original application/API code. Already measured
Original responses remain immutable inputs; failures remain literal.
"""
import argparse,hashlib,importlib.util,json,os,re,shutil,tarfile,time,uuid,sys,ast
from pathlib import Path
from compare import validate,validate_import,validate_http_sql
from gpg_trace import validate as validate_gpg
from origin_check import validate as validate_origin
HERE=Path(__file__).resolve().parent
REPO=Path('/home/justin/github/Dispatcharr')
PINNED='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
OLD=REPO/'artifacts/rust/m8-pluginrepo-unsigned-native-normal-prep-2'
TRANSPORT=REPO/'artifacts/rust/m4-m7-m8-wide-batch-1/source/transport16'
ASSETS=REPO/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/asset-tool-output-1'

def read(path):return json.loads(Path(path).read_bytes())
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def need(value,label):
    if not value:raise ValueError(label)
def save(path,value):
    raw=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode();need(len(raw)<=134217728,'report128MiB');temporary=path.with_suffix(path.suffix+'.tmp');temporary.write_bytes(raw);temporary.replace(path)
def load_module(path,name):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);sys.path.insert(0,str(path.parent))
    try:spec.loader.exec_module(module)
    finally:sys.path.pop(0)
    return module

def fixture_imports(tools):
    local={path.stem for path in tools.glob('*.py')};external={'psycopg','redis','asgiref','channels'}
    for path in tools.glob('*.py'):
        for node in ast.walk(ast.parse(path.read_text())):
            names=[alias.name for alias in node.names] if isinstance(node,ast.Import) else [node.module] if isinstance(node,ast.ImportFrom) and node.module else []
            for name in names:
                top=name.split('.')[0]
                need(top in local or top in sys.stdlib_module_names or top in external,'unbound fixture dependency '+top+' in '+path.name)
    return {'local_modules':len(local),'explicit_installed_packages':sorted(external),'Source_application_imports':0}

def materialize(out,candidate):
    tools=out/'tools';inputs=out/'inputs';schema=out/'schema';tools.mkdir();inputs.mkdir(mode=0o700);schema.mkdir()
    for path in HERE.iterdir():
        if path.is_file() and path.suffix in ('.py','.sh'):shutil.copyfile(path,tools/path.name)
    for name in ['origin_supervisor.py','tls_origin.py','dns_server.py','contract.py','loopback_http.py','bounded_process.py','bounded_stop.py']:
        shutil.copyfile(TRANSPORT/'controller'/name,tools/name)
    # The loopback witness describes this Native cohort's phase, not Original.
    path=tools/'loopback_http.py';text=path.read_text().replace('Source_started_utc','phase_started_utc');path.write_text(text)
    shutil.copytree(OLD/'inputs/pg-init',inputs/'pg-init')
    shutil.copyfile(OLD/'inputs/schema62-authority.json',inputs/'schema62-authority.json')
    authority=read(HERE/'source-authority.json');merged=None
    for epoch,row in authority.items():
        need(sha(row['source_path'])==row['source_sha256'] and sha(row['plan_path'])==row['plan_sha256'],'exact retained Source '+epoch)
        source=read(row['source_path']);plan=read(row['plan_path']);need(source['actual_request_count']==source['completed_capture_count']==len(source['requests'])==len(plan['cases'])==row['count'] and source['capture_complete'] is True,'complete retained inputs '+epoch)
        row['source_filename']=epoch+'-source.json';row['plan_filename']=epoch+'-plan.json'
        shutil.copyfile(row['source_path'],inputs/row['source_filename']);shutil.copyfile(row['plan_path'],inputs/row['plan_filename'])
        if epoch!='transport16':
            old_origin=OLD/'inputs'/row['actual_Source_origin_filename'];need(sha(old_origin)==row['actual_Source_origin_sha256'],'exact retained unsigned Origin reference');shutil.copyfile(old_origin,inputs/row['actual_Source_origin_filename'])
            if merged is None:merged=json.loads(json.dumps(plan))
            else:
                for path,fixtures in plan['origin_definition']['fixtures'].items():
                    previous=merged['origin_definition']['fixtures'].get(path);need(previous is None or previous==fixtures,'consistent predeclared Origin route');merged['origin_definition']['fixtures'][path]=fixtures
    original_tls=Path(authority['transport16']['actual_Source_TLS_origin_path']);need(sha(original_tls)==authority['transport16']['actual_Source_TLS_origin_sha256'],'exact retained TLS Origin reference');shutil.copyfile(original_tls,inputs/'transport16-origin.jsonl')
    generation=read(ASSETS/'execution-report.json');binding=read(TRANSPORT/'inputs/generated-binding.json')
    need(generation['passed'] is True and read(ASSETS/'independent-tool-review.json')['passed'] is True and sha(ASSETS/'execution-report.json')==binding['asset_execution_sha256'],'same actual Source-bound disposable crypto assets')
    generated=ASSETS/'generated/captured'
    for name,row in generation['generated_files'].items():
        path=generated/name;need(not path.is_symlink() and path.is_file() and path.stat().st_size==row['bytes'] and sha(path)==row['sha256'],'unchanged actual generated asset: '+name)
    body=read(generated/'body-signed-ascii.json');body['manifest']['numeric_value']=1.0
    merged['origin_definition']['fixtures']['/numeric-float.json']=[{'status':200,'content_type':'application/json','body':json.dumps(body,separators=(',',':'))}]
    save(inputs/'merged-origin.json',merged);save(inputs/'authority.json',authority);save(inputs/'owned-finite-context.json',{'owned_fixture':True,'Original_application_runs':0,'public_contacts':0})
    wanted=['rust/crates/store/schema/original-1241b3-schema.sql','rust/crates/store/schema/original-1241b3-defaults.sql','rust/crates/store/schema/original-1241b3-fresh-timestamps.sql']
    build=read(candidate['build_manifest_file'])
    with tarfile.open(candidate['source_archive_file']) as archive:
        for name in wanted:
            member=archive.getmember(name);need(member.isfile() and member.size<=8388608,'bounded committed schema blob');raw=archive.extractfile(member).read();need(hashlib.sha256(raw).hexdigest()==build['source_sha256'][name],'candidate schema exact build');target=schema/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
    (inputs/'owned-resolv.conf').write_text('nameserver 11.253.250.2\noptions timeout:1 attempts:1\n');(inputs/'owned-resolv.conf').chmod(0o444)
    save(out/'fixture-import-closure.json',fixture_imports(tools))
    return tools,inputs,schema,generated,authority

def run(args):
    candidate=read(args.candidate);gate=load_module(REPO/'scripts/rust-milestone-batches/m478/batch.py','owner_candidate');need(candidate.get('plugin_numeric_mode')=='finite-v1','explicit finite-v1 candidate binding')
    need(gate.candidate_ready(candidate),'actual current source/archive/build/image binding')
    out=args.output.absolute();need(out.is_relative_to(REPO/'artifacts/rust') and not out.exists() and not any(p.is_symlink() for p in out.parents),'fresh owned artifact output')
    out.mkdir(mode=0o700);tools,inputs,schema,assets,authority=materialize(out,candidate)
    bounded=load_module(ASSETS.parent/'assets/bounded_cli.py','owned_cli');project='dispatcharr-plugin-functional-'+uuid.uuid4().hex[:10];name=project+'-main';environment=os.environ.copy();environment.update(DOCKER_HOST='unix:///tmp/dispatcharr-docker.sock')
    began=time.monotonic();serial=0;used=0;cleanup=False;cleanup_began=None;live=None;created=False;databases=[];loopback_job=None;loopback_record=None;known_cids=[]
    report={'candidate':candidate,'project':project,'passed':False,'full_M8_accepted':False,'Source_application_runs':0,'Source_API_calls':0,'planned_reference_calls':41,'planned_total_HTTP_calls':48,'planned_diagnostics':3,'planned_startup_calls':4,'plugin_numeric_mode':candidate['plugin_numeric_mode'],'cohorts':{},'commands':[],'fixture_services_reused':True,'handler_drain_adversarial_qualified':False,'shutdown_scope':'Normal completed requests only; explicit Plugin handler-drain API remains an M8 blocker'}
    save(out/'report.json',report)
    def command(argv,seconds=30,check=True,combine=False,docker=True):
        nonlocal serial,used
        deadline=cleanup_began+90 if cleanup else began+1200
        auxiliary=loopback_job.used if loopback_job is not None else 0
        capacity=(64 if cleanup else 56)*1048576-used-auxiliary
        need(serial<(160 if cleanup else 128) and capacity>=65536 and time.monotonic()<deadline,'batch command/output/lifetime bound');serial+=1
        raw,err,row=bounded.collect((['docker','--host','unix:///tmp/dispatcharr-docker.sock',*argv] if docker else argv),min(seconds,max(.01,deadline-time.monotonic())),min(8388608,capacity));used+=len(raw)+len(err);(out/f'command-{serial:03d}.stdout').write_bytes(raw);(out/f'command-{serial:03d}.stderr').write_bytes(err);report['commands'].append(row);save(out/'report.json',report)
        need(row['invalid'] is None and row['both_actual_EOF_observed'] is True and row['forced_termination'] is False,'actual CLI EOF/physical terminal')
        if check:need(row['actual_wait_exit']==row['actual_poll_exit']==0,'actual CLI zero')
        return raw+err if combine else raw
    compose=['compose','-p',project,'-f',str(out/'compose.json')]
    def dc(*argv,seconds=30):return command(compose+list(argv),seconds)
    def driver(action,label,database,epoch='transport16',seconds=45):
        return dc('exec','-T','-e','PLUGIN_REPO_DRIVER=owned-native-pluginrepo-only','-e','PLUGIN_REPO_ACTION='+action,'-e','PLUGIN_REPO_LABEL='+label,'-e','PLUGIN_REPO_DATABASE='+database,'-e','PLUGIN_UNSIGNED_EPOCH='+epoch,'storage','/dispatcharrpy/bin/python','-B','/native-tools/driver.py',seconds=seconds)
    def psql(sql,db='postgres'):
        return dc('exec','-T','-e','PGOPTIONS=-c statement_timeout=7000 -c lock_timeout=7000','storage','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d',db,'-v','ON_ERROR_STOP=1','-c',sql)
    def wait_file(path,seconds=180):
        end=time.monotonic()+seconds
        while not path.is_file():need(time.monotonic()<end,'owned fixture readiness '+str(path.name));time.sleep(.05)
    from bootstrap_env import storage_environment
    storage_env={**storage_environment(),**{k:'' for k in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy']},'NO_PROXY':'*','no_proxy':'*'}
    mounts=[str(tools)+':/native-tools:ro',str(tools)+':/plugin-tools:ro',str(inputs)+':/native-root/inputs:ro',str(schema)+':/native-source:ro',str(inputs/'pg-init')+':/pg-bootstrap:ro',str(out)+':/evidence',str(inputs/'owned-finite-context.json')+':/owned-finite-context:ro',str(assets/'manifest-public-key.asc')+':/plugin-assets/manifest-public-key.asc:ro']
    storage={'image':PINNED,'platform':'linux/amd64','pull_policy':'never','entrypoint':['bash','/native-tools/start.sh'],'dns':['127.0.0.1'],'sysctls':{'net.ipv6.conf.all.disable_ipv6':'1','net.ipv6.conf.default.disable_ipv6':'1'},'environment':storage_env,'volumes':mounts,'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1536m','pids_limit':256,'networks':{'owned':{'ipv4_address':'11.253.250.3'}}}
    origin={'image':PINNED,'platform':'linux/amd64','pull_policy':'never','read_only':True,'entrypoint':['/dispatcharrpy/bin/python','-B','/plugin-tools/origin_cohort.py'],'dns':['127.0.0.1'],'sysctls':{'net.ipv6.conf.all.disable_ipv6':'1','net.ipv6.conf.default.disable_ipv6':'1'},'environment':{**{k:'' for k in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy']},'NO_PROXY':'*','no_proxy':'*'},'volumes':[str(tools)+':/plugin-tools:ro',str(inputs)+':/native-root/inputs:ro',str(assets)+':/plugin-assets:ro',str(out)+':/evidence'],'tmpfs':['/tmp:mode=1777,size=67108864'],'cpus':.25,'mem_limit':'128m','pids_limit':128,'networks':{'owned':{'ipv4_address':'11.253.250.2'}}}
    native_env={'RUST_BIND':'0.0.0.0:9192','DJANGO_SECRET_KEY':'owned-functional-only','REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6379/1','REQUESTS_CA_BUNDLE':'/plugin-assets/trusted-ca.pem','RUST_PLUGIN_GPG_TRACE':'1','NO_COLOR':'1','TZ':'UTC',**{k:'' for k in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy']}}
    native={'image':candidate['image_id'],'platform':'linux/amd64','pull_policy':'never','read_only':True,'network_mode':'service:storage','environment':native_env,'volumes':[str(inputs/'owned-resolv.conf')+':/etc/resolv.conf:ro',str(assets/'trusted-ca.pem')+':/plugin-assets/trusted-ca.pem:ro'],'tmpfs':['/tmp:mode=1777,size=134217728','/data:mode=1777,size=134217728'],'cpus':1,'mem_limit':'512m','pids_limit':128,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true']}
    save(out/'compose.json',{'services':{'storage':storage,'origin':origin,'candidate':native},'networks':{'owned':{'internal':True,'enable_ipv6':False,'driver_opts':{'com.docker.network.bridge.enable_ip_masquerade':'false'},'ipam':{'config':[{'subnet':'11.253.250.0/29','gateway':'11.253.250.1'}]}}}})
    try:
        need(not command(['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).strip(),'fresh owned project')
        # Reuse the existing audited host/IPAM non-overlap algorithm; no contact
        # with any public origin is used as a reachability or readiness test.
        network=load_module(OLD/'controller/network_guard.py','owned_network')
        routes=json.loads(command(['ip','-j','-4','route','show','table','all'],docker=False));ids=command(['network','ls','--format','{{.ID}}']).decode().splitlines();need(len(ids)<=256,'network inventory bound');existing=json.loads(command(['network','inspect',*ids])) if ids else [];save(out/'host-nonoverlap.json',network.host_nonoverlap(routes,existing))
        created=True;dc('up','-d','--no-build','--pull','never','storage','origin',seconds=120);wait_file(out/'bootstrap-ready');wait_file(out/'origins/ready.json',20)
        storage_cid=dc('ps','--all','-q','storage').decode().strip();origin_cid=dc('ps','--all','-q','origin').decode().strip();known_cids.extend([storage_cid,origin_cid])
        inspection=json.loads(command(['inspect',storage_cid,origin_cid]));by_id={row['Id']:row for row in inspection};network_state=json.loads(command(['network','inspect',project+'_owned']))[0];save(out/'actual-network.json',network.actual_network(network_state,by_id[storage_cid],by_id[origin_cid],project));save(out/'service-inspect.json',inspection)
        for service in ['storage','origin']:
            four=dc('exec','-T',service,'cat','/proc/net/route').decode();six=dc('exec','-T',service,'cat','/proc/net/ipv6_route').decode();save(out/(service+'-kernel-routes.json'),network.kernel_routes(four,six))
        driver('default','default-before','dispatcharr')
        shutil.copyfile(assets/'body-unsigned.json',inputs/'loopback-body.json')
        helper_module=load_module(tools/'bounded_process.py','owned_long_process')
        def stop_owned():
            # Collected invokes this only on overflow/timeout. An actual owned
            # Compose stop acknowledges container children, not merely the CLI.
            stop_module=load_module(tools/'bounded_stop.py','owned_stop')
            report['emergency_stop']={}
            stop_module.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock',*compose,'stop','--timeout','10'],environment,out,report['emergency_stop'],limit=65536,timeout=15,save=save)
        loopback_job=helper_module.Collected(['docker','--host','unix:///tmp/dispatcharr-docker.sock',*compose,'exec','-T','storage','/dispatcharrpy/bin/python','-B','/plugin-tools/loopback_http.py','--body','/native-root/inputs/loopback-body.json','--phase-cue','/evidence/native-phase.json','--output','/evidence/loopback'],environment,1048576,stop_owned,stdout_path=out/'loopback.stdout',stderr_path=out/'loopback.stderr')
        proc=loopback_job.process;generation=Path('/proc',str(proc.pid),'stat').read_text().rsplit(')',1)[1].split()[19]
        loopback_record={'pid':proc.pid,'generation':generation,'PID_namespace':os.readlink('/proc/self/ns/pid')};report['loopback_host_CLI']=loopback_record
        wait_file(out/'loopback/ready.json',15)
        for epoch in ['transport16','retained24','fresh404']:
            database='rust_probe_pluginrepo_'+uuid.uuid4().hex[:10];databases.append(database);cell={'actual_source_count':authority[epoch]['count'],'database':database,'passed':False};report['cohorts'][epoch]=cell;save(out/'report.json',report)
            try:
                psql('CREATE DATABASE '+database);dc('exec','-T','storage','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d',database,'-v','ON_ERROR_STOP=1','-1','-f','/native-source/rust/crates/store/schema/original-1241b3-schema.sql')
                driver('clear-owned-redis',epoch+'-redis-reset',database,epoch);driver('import',epoch+'-import',database,epoch);driver('state',epoch+'-before-Main',database,epoch)
                source=read(inputs/authority[epoch]['source_filename']);cell['seed_import_qualification']=validate_import(source,read(out/(epoch+'-before-Main.json')))
                live=dc('run','--no-deps','-d','--name',name+'-'+epoch,'-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'candidate').decode().strip();need(re.fullmatch('[a-f0-9]{64}',live) is not None,'actual candidate CID');known_cids.append(live)
                actual=json.loads(command(['inspect',live]))[0];save(out/(epoch+'-Main-inspect.json'),actual);need(actual['Image']==candidate['image_id'] and actual['Config']['User']=='65532:65532','actual packaged Main identity/UID')
                need(actual['HostConfig']['NetworkMode']=='container:'+storage_cid,'actual shared owned network namespace')
                need(actual['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs'] and actual['HostConfig']['ReadonlyRootfs'] is True and actual['HostConfig']['PidsLimit']==128 and actual['HostConfig']['CapDrop']==['ALL'],'actual production entrypoint/rootfs/PID/cap bounds')
                origin_file=out/('origins/tls/http/events.jsonl' if epoch=='transport16' else 'origins/unsigned/origin-requests.jsonl');origin_offset=len(origin_file.read_text().splitlines()) if origin_file.is_file() else 0
                need(command(['exec',live,'cat','/etc/resolv.conf'])==(inputs/'owned-resolv.conf').read_bytes(),'actual owned DNS resolver bytes')
                if epoch=='transport16':
                    runtime=json.loads(command(['exec',live,'cat','/usr/share/doc/dispatcharr/crypto-runtime.json']));need(runtime['source_image']==PINNED,'pinned Original crypto closure')
                    expected={'/opt/dispatcharr-crypto/bin/gpg':runtime['binary']['sha256'],'/opt/dispatcharr-crypto/trust/cacert.pem':runtime['trust']['sha256']}
                    for library,row in runtime['libraries'].items():
                        need(re.fullmatch('[A-Za-z0-9_.+-]+',library) is not None and row['licenses'],'every ELF dependency has owning-package credit')
                        expected['/opt/dispatcharr-crypto/lib/'+library]=row['sha256']
                    for row in [runtime['binary'],*runtime['libraries'].values()]:
                        need(row['licenses'],'actual binary/library license binding')
                        for notice in row['licenses']:
                            path=notice['retained_path'];need(path.startswith('/usr/share/doc/dispatcharr/') and '..' not in Path(path).parts,'owned package notice path');expected[path]=notice['sha256']
                    for label,row in runtime['licenses'].items():expected['/usr/share/doc/dispatcharr/crypto-copyright/'+label+'-LICENSE']=row['sha256']
                    loaders=[name for name in runtime['libraries'] if name.startswith('ld-linux-')];need(len(loaders)==1,'exact packaged GPG ELF interpreter')
                    wrapper='#!/bin/sh\nexec /opt/dispatcharr-crypto/lib/'+loaders[0]+' --library-path /opt/dispatcharr-crypto/lib /opt/dispatcharr-crypto/bin/gpg \"$@\"\n'
                    expected['/usr/local/bin/gpg']=hashlib.sha256(wrapper.encode()).hexdigest()
                    need(len(expected)<=128,'bounded crypto closure inventory')
                    observed={line.split(None,1)[1]:line.split(None,1)[0] for line in command(['exec',live,'sha256sum',*expected]).decode().splitlines()};need(observed==expected,'actual packaged binary/libraries/trust/notices byteexact')
                    save(out/'packaged-crypto-probe.json',{'passed':True,'runtime_manifest':runtime,'actual_file_sha256':observed})
                if epoch=='transport16':save(out/'native-phase.json',{'phase_started_utc':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),'Original_API_calls':0})
                try:driver('collect',epoch+'-capture',database,epoch,seconds=480)
                except Exception as error:cell['collection_error']={'type':type(error).__name__,'message':str(error)}
                logs=command(['logs','--timestamps',live],combine=True);(out/(epoch+'-Main.logs')).write_bytes(logs)
                capture=out/(epoch+'-capture.json')
                if capture.is_file():
                    observed=read(capture);cell['actual_calls']=observed['actual_request_count'];cell['completed_calls']=observed['completed_capture_count']
                    try:cell['HTTP_SQL']=validate_http_sql(source,observed)
                    except Exception as error:cell['HTTP_SQL_error']={'type':type(error).__name__,'message':str(error)}
                    try:cell['functional']=validate(source,observed)
                    except Exception as error:cell['functional_error']={'type':type(error).__name__,'message':str(error)}
                    try:
                        trace=validate_gpg(source,observed,logs,numeric_mode=candidate['plugin_numeric_mode']);cell['GPG']=trace
                        pids=[str(row['pid']) for row in trace['children']]
                        script='readlink /proc/self/ns/pid; for pid in "$@"; do if [ -r "/proc/$pid/stat" ]; then cat "/proc/$pid/stat"; else printf "ABSENT %s\\n" "$pid"; fi; done; printf "HOMES\\n"; find /tmp -maxdepth 1 -type d -name "dispatcharr-gpg-*" -print'
                        raw=command(['exec',live,'/bin/sh','-c',script,'owned-GPG-observer',*pids]);(out/(epoch+'-GPG-absence.stdout')).write_bytes(raw)
                        lines=raw.decode().splitlines();namespace=lines.pop(0);need(all(row['PID_namespace']==namespace for row in trace['children']),'same actual candidate child namespace');index=lines.index('HOMES');need(lines[index+1:]==[],'independent owned home absence')
                        for child,line in zip(trace['children'],lines[:index]):
                            if line=='ABSENT '+str(child['pid']):continue
                            need(line.split(' ',1)[0]==str(child['pid']) and line.rsplit(')',1)[1].split()[19]!=child['generation'],'actual same-generation child absence')
                        need(index==len(trace['children']),'every child independently checked')
                    except Exception as error:cell['GPG_error']={'type':type(error).__name__,'message':str(error)}
                    if observed.get('numeric_diagnostic'):cell['numeric_diagnostic']={'status':observed['numeric_diagnostic']['status'],'full_domain_accepted':False,'capability_status':'failed' if observed['numeric_diagnostic']['status']==501 else 'blocked-unmeasured'}
                    cell['passed']=cell.get('functional',{}).get('passed') is True and cell.get('GPG',{}).get('passed') is True and 'GPG_error' not in cell
                command(['stop','--time','10',live],20);wait=command(['wait',live],15).decode().strip();cell['actual_Main_exit']=int(wait);need(wait=='0','actual Main graceful shutdown0')
                if origin_file.is_file():
                    origin_lines=origin_file.read_text().splitlines()[origin_offset:];save(out/(epoch+'-Origin-observations.json'),[json.loads(line) for line in origin_lines])
                    try:cell['Origin']=validate_origin(out,epoch,authority)
                    except Exception as error:cell['Origin_error']={'type':type(error).__name__,'message':str(error)}
                    cell['passed']=cell['passed'] and cell.get('Origin',{}).get('passed') is True
                else:cell['passed']=False;cell['Origin_error']={'message':'actual Origin observations missing'}
                backend=json.loads(psql("SELECT COALESCE(json_agg(t),'[]') FROM (SELECT pid,state FROM pg_stat_activity WHERE datname='"+database+"')t"));save(out/(epoch+'-backend-after-Main.json'),backend);need(backend==[],'no backend after actual Main wait');command(['rm','--volumes',live]);live=None;save(out/'report.json',report)
            except Exception as error:
                cell['context_error']={'type':type(error).__name__,'message':str(error)};cell['passed']=False
                if live:
                    # Continue another independent seed only after the failed
                    # context's actual process and SQL backends are settled.
                    command(['stop','--time','5',live],10);exit_code=command(['wait',live],10).decode().strip();cell['failed_context_Main_exit']=exit_code
                    command(['rm','--volumes',live]);live=None
                backend=json.loads(psql("SELECT COALESCE(json_agg(t),'[]') FROM (SELECT pid,state FROM pg_stat_activity WHERE datname='"+database+"')t"));need(backend==[],'failed context backend physical settlement before next seed')
                save(out/'report.json',report)
        from startup_cohort import collect as collect_startup
        report['startup_finite6']=collect_startup(out,name,candidate,command,dc,psql,driver,known_cids)
        from startup_review import qualify as qualify_startup
        report['startup_finite6_review']=qualify_startup(out)
        save(out/'report.json',report)
        ready=read(out/'loopback/ready.json')
        signal_script="import os,json;from pathlib import Path;r=json.loads("+repr(json.dumps(ready))+");assert os.readlink('/proc/self/ns/pid')==r['PID_namespace'];assert Path('/proc',str(r['pid']),'stat').read_text().rsplit(')',1)[1].split()[19]==r['starttime'];os.kill(r['pid'],15)"
        dc('exec','-T','storage','/dispatcharrpy/bin/python','-c',signal_script)
        loopback_job.wait(10);loopback_record.update(actual_wait_exit=loopback_job.process.poll(),physical_settled=loopback_job.physically_settled,readers_settled=not any(t.is_alive() for t in loopback_job.threads),invalid=loopback_job.invalid)
        need(loopback_record['actual_wait_exit']==0 and loopback_record['physical_settled'] and loopback_record['readers_settled'] and loopback_record['invalid'] is None,'actual loopback helper CLI wait/EOF0')
        terminal=read(out/'loopback/terminal.json');need(terminal['complete'] is True and terminal['contacts']==1,'actual measured rebind loopback contact only');report['loopback']=terminal
        dc('stop','--timeout','10','origin',seconds=20);origin_terminal=read(out/'origins/terminal.json');need(origin_terminal['passed'] is True,'actual Origin child waits0');report['origin_terminal']=origin_terminal
        for name in ['http','dns']:
            terminal=read(out/('origins/tls/'+name+'/terminal.json'));need(terminal['complete'] is True and terminal['errors']==[],'actual bounded '+name+' shutdown');report[name+'_terminal']=terminal
        driver('default','default-after','dispatcharr');need((out/'default-before.json').read_bytes()==(out/'default-after.json').read_bytes(),'literal default62/59 unchanged')
        report['reference_functional_passed']=all(c['passed'] for c in report['cohorts'].values());report['numeric_domain_complete']=False
        report['legacy_framework_cache_errors']={k:v.get('functional_error')for k,v in report['cohorts'].items()if v.get('functional_error')}
        report['normal_HTTP_SQL_GPG_scope_passed']=len(report['cohorts'])==3 and all(c.get('HTTP_SQL',{}).get('passed')is True and c.get('GPG',{}).get('passed')is True and c.get('Origin',{}).get('passed')is True and c.get('actual_Main_exit')==0 and 'context_error'not in c for c in report['cohorts'].values())
        report['passed']=report['normal_HTTP_SQL_GPG_scope_passed']and report['startup_finite6_review']['passed']
        report['prior_actual_framework_controller_exit2_preserved']=True
        report['full_M8_accepted']=False
    except Exception as error:report['error']={'type':type(error).__name__,'message':str(error)}
    finally:
        cleanup=True;cleanup_began=time.monotonic();errors=[]
        if live:
            try:command(['stop','--time','5',live],10);command(['rm','--volumes',live])
            except Exception as error:errors.append({'stage':'Main removal','message':str(error)})
        if created:
            try:dc('down','--volumes','--remove-orphans','--timeout','10',seconds=30)
            except Exception as error:errors.append({'stage':'fixture down','message':str(error)})
        for kind in ['container','network','volume']:
            try:
                argv=[kind,'ls']+(['--all','--no-trunc'] if kind=='container' else [])+['--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}' if kind=='volume' else '{{.ID}}'];raw=command(argv);need(not raw.strip(),'exact owned '+kind+' absence')
            except Exception as error:errors.append({'stage':kind+' inventory','message':str(error)})
        if loopback_job is not None and not loopback_job.physically_settled:
            try:loopback_job.wait(10)
            except Exception as error:errors.append({'stage':'loopback actual wait','message':str(error)})
        for cid in known_cids:
            try:need(not command(['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']).strip(),'exact actual CID absence')
            except Exception as error:errors.append({'stage':'CID inventory','message':str(error)})
        for row in report['commands']+([loopback_record] if loopback_record else []):
            try:
                need(row['PID_namespace']==os.readlink('/proc/self/ns/pid'),'actual same host PID namespace')
                pid=row.get('pid');generation=row.get('observed_starttime',row.get('generation'))
                need(type(pid) is int and type(generation) is str and generation.isdecimal() and int(generation)>0,'actual positive host generation')
                try:current=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
                except FileNotFoundError:current=None
                need(current!=generation,'actual host generation absent')
            except Exception as error:errors.append({'stage':'host PID cleanup','message':str(error)})
        report['cleanup_errors']=errors;report['cleanup_verified']=not errors;report['actual_commands']=serial;report['actual_cli_output_bytes']=used+(loopback_job.used if loopback_job is not None else 0);report['cleanup_seconds']=time.monotonic()-cleanup_began;report['elapsed_seconds']=time.monotonic()-began;save(out/'report.json',report)
    report['actual_controller_exit']=0 if report['passed']and report['cleanup_verified']and 'error'not in report else 2
    save(out/'report.json',report)
    return report['actual_controller_exit']

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute-batch',action='store_true');parser.add_argument('--emit-binding',action='store_true');parser.add_argument('--candidate',type=Path);parser.add_argument('--output',type=Path);args=parser.parse_args()
    if args.emit_binding:
        need(args.candidate is not None and args.output is not None,'concrete future candidate/output paths required')
        inputs={str(p):sha(p) for p in HERE.iterdir() if p.is_file() and p.suffix in ('.py','.sh','.json')}
        external=[OLD/'controller/network_guard.py',OLD/'inputs/schema62-authority.json',ASSETS.parent/'assets/bounded_cli.py',ASSETS/'execution-report.json',TRANSPORT/'inputs/generated-binding.json']
        external.extend((OLD/'inputs/pg-init').glob('*'))
        external.extend(TRANSPORT/'controller'/name for name in ['origin_supervisor.py','tls_origin.py','dns_server.py','contract.py','loopback_http.py','bounded_process.py','bounded_stop.py'])
        for path in external:
            if path.is_file():inputs[str(path)]=sha(path)
        for row in read(HERE/'source-authority.json').values():
            inputs[row['source_path']]=row['source_sha256'];inputs[row['plan_path']]=row['plan_sha256']
        binding={'id':'native/plugin-startup-transport-GPG48','owner':'m478','covers_existing_cells':['native/plugin-TLS-DNS-GPG16','native/plugin-unsigned25'],'entrypoint':'production Main retained41 plus startup4 and numeric3 with explicit finite-v1 binding','depends_on':[],'resources':['ipam:11.253.250.0/29'],'heavy':False,'argv':['python','-B',str(HERE/'run.py'),'--execute-batch','--candidate',str(args.candidate.absolute()),'--output',str(args.output.absolute())],'evaluator_argv':['python','-B',str(HERE/'qualify_scope.py'),'--output',str(args.output.absolute())],'cleanup_argv':['python','-B',str(HERE/'cleanup.py'),'--output',str(args.output.absolute())],'seconds':1200,'cleanup_seconds':90,'output_bytes':67108864,'cwd':str(REPO),'input_sha256':inputs,'environment':{'DOCKER_HOST':'unix:///tmp/dispatcharr-docker.sock'},'global_fixture_slots':1,'one_dispatch_only':True,'Source_API_calls':0,'image_builds':0,'full_M8_acceptance':False,'explicit_blockers':['Floating-point signing remains unsupported','Adversarial Plugin actor drain remains unqualified','Original lifecycle passive network gates remain failed; observation authority reused only']}
        print(json.dumps(binding,indent=2));return 0
    if not args.execute_batch:
        print(json.dumps({'status':'not_run','reference_cases':41,'numeric_diagnostics':1,'Source_API_calls':0,'image_builds':0,'ordinary_global_slots':1,'exclusive_resource':'ipam:11.253.250.0/29','performance_claims':False}));return 0
    need(args.candidate is not None and args.output is not None,'Root actual candidate and output required');return run(args)
if __name__=='__main__':raise SystemExit(main())
