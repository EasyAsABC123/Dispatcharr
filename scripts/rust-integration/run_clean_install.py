#!/usr/bin/env python3
"""Pure-native fresh Compose and unchanged React first-admin/login/restart gate.

Official PostgreSQL/Redis and the immutable Rust candidate are the only services.
No Django/original app or source mounts populate the database. All original
application credit remains with Dispatcharr authors and contributors.
"""
import argparse,hashlib,json,os,select,shutil,signal,subprocess,time,urllib.request,uuid
from pathlib import Path
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
from run import validate_candidate_image
ROOT=Path(__file__).resolve().parents[2]
def browser_tools():
    node = os.environ.get('RUST_BROWSER_NODE') or shutil.which('node')
    chromium = os.environ.get('RUST_BROWSER_CHROMIUM') or shutil.which('chromium') or shutil.which('chromium-browser')
    if not node or not chromium:
        raise RuntimeError('Browser checks require Node.js22+ and Chromium on PATH, or RUST_BROWSER_NODE/RUST_BROWSER_CHROMIUM')
    node = str(Path(node).resolve(strict=True))
    chromium = str(Path(chromium).resolve(strict=True))
    version = subprocess.check_output([node, '--version'], text=True, timeout=10).strip()
    if int(version.removeprefix('v').split('.')[0]) < 22:
        raise RuntimeError('Browser checks require Node.js22+ for the built-in WebSocket client')
    return node, chromium, version
class Browser:
    def __init__(self,script,log):
        node, chromium, version = browser_tools()
        self.log=log.open('w');self.process=subprocess.Popen([node,str(script)],env=dict(os.environ,RUST_BROWSER_CHROMIUM=chromium),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,start_new_session=True);self.browser_pid=None
        self.ready=self.read(20);self.browser_pid=self.ready.pop('browser_pid',None)
        self.ready.update(node_version=version, browser_launcher_sha256=hashlib.sha256(Path(chromium).read_bytes()).hexdigest())
        if self.ready.get('ready')is not True:raise RuntimeError('Browser startup failed')
    def read(self,seconds=90):
        if not select.select([self.process.stdout],[],[],seconds)[0]:raise TimeoutError('Browser response deadline')
        line=self.process.stdout.readline()
        if not line:raise RuntimeError('Browser peer exited')
        return json.loads(line)
    def send(self,value):
        self.process.stdin.write(json.dumps(value)+'\n');self.process.stdin.flush();return self.read()
    def close(self):
        try:
            if self.process.poll()is None:self.send({'op':'close'})
            self.process.wait(timeout=10)
        except (OSError,ValueError,RuntimeError,TimeoutError,subprocess.SubprocessError):
            try:os.killpg(self.process.pid,signal.SIGTERM)
            except ProcessLookupError:pass
        finally:
            if self.browser_pid:
                try:os.killpg(self.browser_pid,signal.SIGKILL)
                except ProcessLookupError:pass
            self.log.close()
def http(url):
    with urllib.request.urlopen(url,timeout=3)as response:return response.status,json.load(response)
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--candidate-build-manifest',required=True,type=Path);parser.add_argument('--logo-workflow',action='store_true');args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(raw)
    if manifest.get('passed')is not True:parser.error('Successful immutable build required')
    tools=output/'tool-source';hashes=snapshot_sources(ROOT,tools)
    relative=Path('docker/compose.rust-clean-install.yml');content=(ROOT/relative).read_bytes();(tools/relative).write_bytes(content);hashes[str(relative)]=hashlib.sha256(content).hexdigest()
    project='dispatcharr-native-install-'+uuid.uuid4().hex[:10];environment=dict(os.environ,RUST_CANDIDATE_IMAGE=manifest['tag']);ports=output/'ports.yml';selected=write_port_overlay(ports,{'candidate':[9192]})
    base=['docker','compose','-p',project,'-f',str(tools/relative),'-f',str(ports)]
    off=output/'initialization-off.yml';off.write_text('services:\n  candidate:\n    environment: {RUST_INITIALIZE_EMPTY_DATABASE: "0"}\n')
    report={'passed':False,'scope':'pure-native fresh empty officialPG17/Redis Compose; actual unchanged React first-admin/login/init/restart','project':project,'checks':[],'tool_source_sha256':hashes,'build_manifest_sha256':hashlib.sha256(raw).hexdigest(),'fixture_ports':selected,'no_django_service':True,'performance_verified':False};browser=None;status=1;stage='setup'
    def command(arguments,timeout=120,body=None):return subprocess.check_output(arguments,env=environment,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
    def record(name,passed,details=None):report['checks'].append({'name':name,'passed':bool(passed),'details':details})
    def wait(check,seconds=60):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            try:
                if check():return
            except (OSError,ValueError,subprocess.SubprocessError):pass
            time.sleep(.2)
        raise TimeoutError('Fixture readiness deadline')
    def sql(query):return command(base+['exec','-T','database','psql','-U','dispatch','-d','dispatcharr','-At','-c',query]).strip()
    try:
        stage='infrastructure';command(base+['up','-d','database','cache']);wait(lambda:command(base+['exec','-T','database','pg_isready','-h','127.0.0.1','-U','dispatch','-d','dispatcharr']).strip().endswith('accepting connections'))
        record('fresh-empty-database',sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'")=='0')
        stage='default-off';command(base+['-f',str(off),'up','-d','--no-build','candidate']);container=command(base+['ps','-a','-q','candidate']).strip()
        wait(lambda:json.loads(command(['docker','inspect',container]))[0]['State']['Status']=='exited',30)
        state=json.loads(command(['docker','inspect',container]))[0]['State'];record('initialization-default-off-no-ddl',state['ExitCode']!=0 and sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'")=='0',{'exit_code':state['ExitCode']})
        stage='explicit-initialize';command(base+['up','-d','--no-build','--force-recreate','candidate']);url='http://'+command(base+['port','candidate','9192']).strip();wait(lambda:http(url+'/health/ready')[0]==200)
        container=command(base+['ps','-q','candidate']).strip();inspection=json.loads(command(['docker','inspect',container]))[0];report['candidate_image_binding']=validate_candidate_image(manifest,inspection)
        report['services']=sorted(command(base+['config','--services']).splitlines());record('native-only-service-set',report['services']==['cache','candidate','data-init','database'])
        record('rust-initialized-original-schema',sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'")=='62' and sql('SELECT count(*) FROM django_migrations')=='184' and sql('SELECT count(*) FROM accounts_user')=='0',{'tables':62,'migration_records':184,'users_before_browser':0})
        no_python=command(base+['exec','-T','candidate','sh','-c','if command -v python >/dev/null 2>&1 || command -v python3 >/dev/null 2>&1; then echo present; else echo absent; fi'],timeout=10).strip()=='absent'
        record('candidate-has-no-python-runtime',no_python)
        # Avoid real external geolocation: only Redis is seeded, never SQL. The
        # original source worker/cache behavior has its own verified gate.
        subprocess.run(base+['exec','-T','cache','redis-cli','-a','playback-fixture-only','-x','SET',':1:dispatcharr:ip_lookup_lock'],env=environment,input=bytes([0x80,4,0x88,ord('.')]),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        command(base+['exec','-T','cache','redis-cli','-a','playback-fixture-only','EXPIRE',':1:dispatcharr:ip_lookup_lock','600'])
        initial=http(url+'/api/accounts/initialize-superuser/');record('first-admin-form-status',initial[0]==200 and initial[1].get('superuser_exists')is False and initial[1].get('setup_allowed')is True)
        stage='browser-first-admin';browser=Browser(tools/'scripts/rust-integration/clean_browser.js',output/'browser.log');report['browser']=browser.ready
        credentials={'username':'rust-probe-clean-admin','password':'native-clean-fixture-'+uuid.uuid4().hex,'email':'Native@EXAMPLE.INVALID','secret':'playback-fixture-only','url':url}
        result=browser.send({'op':'setup',**credentials});record('actual-react-first-admin-login-init',result.get('authenticated')is True and result.get('required_api_paths_passed')==result.get('required_api_paths') and not result.get('failed_api_responses') and result.get('exceptions')==0,result)
        if result.get('failed'):raise RuntimeError('Browser setup failed')
        picture=output/'first-admin-channels.png';saved=browser.send({'op':'screenshot','path':str(picture)});report['screenshot_sha256']=hashlib.sha256(picture.read_bytes()).hexdigest()if saved.get('saved')else None
        query="SELECT json_build_object('id',id,'username',username,'email',email,'level',user_level,'staff',is_staff,'superuser',is_superuser,'active',is_active,'algorithm',split_part(password,'$',1),'iterations',split_part(password,'$',2),'joined_set',date_joined IS NOT NULL,'login_set',last_login IS NOT NULL) FROM accounts_user"
        row=json.loads(sql(query));record('browser-created-real-admin-row',row['id']==1 and row['username']==credentials['username']and row['email']=='Native@example.invalid'and row['level']==10 and row['staff']and row['superuser']and row['active']and row['algorithm']=='pbkdf2_sha256'and row['iterations']=='1200000'and row['joined_set']and row['login_set'],{'row_sha256':hashlib.sha256(json.dumps(row,sort_keys=True).encode()).hexdigest()})
        logo_config=None;logo_fingerprint=None
        if args.logo_workflow:
            stage='browser-logo-upload-edit';file=output/'rust-probe-browser-logo.svg';file.write_text('<svg xmlns="http://www.w3.org/2000/svg" width="2" height="3"><rect width="2" height="3" fill="green"/></svg>')
            logo_config={'url':url,'file':str(file),'logo_name':'rust-probe-browser-logo','renamed_logo':'rust-probe-browser-logo-edited'}
            result=browser.send({'op':'logo_create',**logo_config});record('actual-react-logo-upload-edit-image',result.get('uploaded')and result.get('edited')and result.get('upload201')and not result.get('failed_api_responses')and result.get('exceptions')==0,result)
            if result.get('failed'):raise RuntimeError('Browser logo creation failed')
            logo_fingerprint=sql("SELECT md5(string_agg(id::text||':'||name||':'||url,','ORDER BY id))FROM dispatcharr_channels_logo")
            witness=command(base+['exec','-T','candidate','sha256sum','/data/logos/rust-probe-browser-logo.svg']).split()[0];record('browser-uploaded-file-real-native-volume',witness==hashlib.sha256(file.read_bytes()).hexdigest()and sql("SELECT count(*)FROM dispatcharr_channels_logo WHERE name='rust-probe-browser-logo-edited'")=='1')
        fingerprint_query="SELECT md5(string_agg(app||':'||name||':'||applied::text,',' ORDER BY id)) FROM django_migrations";before=sql(fingerprint_query)
        stage='restart';command(base+['restart','candidate']);wait(lambda:http(url+'/health/ready')[0]==200)
        result=browser.send({'op':'logo_reload',**logo_config}if logo_config else{'op':'reload',**credentials})
        record('restart-react-token-persistence-init',result.get('persisted')is True and result.get('stored_access_present')is True and not result.get('failed_api_responses')and result.get('exceptions')==0 if logo_config else result.get('authenticated')is True and not result.get('failed_api_responses')and result.get('exceptions')==0,result)
        if logo_config:
            record('restart-preserves-logo-file-and-row',logo_fingerprint==sql("SELECT md5(string_agg(id::text||':'||name||':'||url,','ORDER BY id))FROM dispatcharr_channels_logo")and command(base+['exec','-T','candidate','sha256sum','/data/logos/rust-probe-browser-logo.svg']).split()[0]==hashlib.sha256(file.read_bytes()).hexdigest())
            result=browser.send({'op':'logo_delete',**logo_config});record('actual-react-logo-delete-file',result.get('deleted')and result.get('delete204')and not result.get('failed_api_responses')and result.get('exceptions')==0,result)
            removed=command(base+['exec','-T','candidate','sh','-c','if test -e /data/logos/rust-probe-browser-logo.svg; then echo present; else echo absent; fi']).strip()=='absent';record('browser-delete-removes-file-and-db-row',removed and sql('SELECT count(*)FROM dispatcharr_channels_logo')=='0')
        record('restart-preserves-existing-schema-and-admin',before==sql(fingerprint_query)and sql('SELECT count(*) FROM accounts_user')=='1'and json.loads(sql(query))==row)
        record('setup-closed-after-browser-create',http(url+'/api/accounts/initialize-superuser/')[1]=={'superuser_exists':True})
        status=0 if all(check['passed']for check in report['checks'])else 1;report['passed']=status==0
    except (OSError,ValueError,RuntimeError,TimeoutError,subprocess.SubprocessError)as error:
        report.update(error_type=type(error).__name__,error_stage=stage);status=1
        if isinstance(error,subprocess.CalledProcessError):
            # Private ignored diagnostics only; summaries retain category/hash.
            detail=error.stderr or error.stdout or ''
            if isinstance(detail,bytes):detail=detail.decode('utf-8',errors='replace')
            (output/'failed-command.log').write_text(detail[:32768])
        try:
            (output/'failed-services.log').write_text(command(base+['logs','--no-color','--tail','40'],timeout=10)[-65536:])
        except (OSError,subprocess.SubprocessError):pass
    finally:
        if browser:browser.close()
        try:command(base+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
        except subprocess.SubprocessError:report['cleanup_error']=True;report['passed']=False;status=1
        (output/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
