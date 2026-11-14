"""Actual Main Core51 orchestration on the already bootstrapped shared fixture."""
import hashlib,json,os,re,signal,subprocess,time,uuid
from pathlib import Path

def need(x,label):
    if not x:raise ValueError(label)
def save(p,x):p.write_text(json.dumps(x,indent=2,allow_nan=False)+'\n')
def collect(out,name,candidate,command,dc,psql,known_cids,compose,environment,report,persist):
    root=out/'core51';root.mkdir();definition=json.loads((out/'core51-config/definition/plan.json').read_bytes());nonce=uuid.uuid4().hex[:10];result={'passed':False,'actual_Main_API_calls':0,'Source_API_calls':0,'cohorts':[],'peer_private_control_endpoints':0};report['Core51_host_CLIs']=[];persist();dc('exec','-T','-e','PYTHONPATH=/core51-tools:/native-tools','storage','/dispatcharrpy/bin/python','-B','/core51-tools/default_audit_raw.py','before')
    for cohort in definition['cohorts']:
        group=cohort['group'];db='rust_special_'+nonce+'_'+group;live=None;process=None;streams=[];row={'group':group,'database':db,'passed':False};result['cohorts'].append(row);save(root/'result.json',result)
        def stop_observer():
            ready=root/(group+'-setup-ready.json')
            if ready.exists():
                who=json.loads(ready.read_bytes());script="import os,json,signal;from pathlib import Path;r=json.loads("+repr(json.dumps(who))+");assert os.readlink('/proc/self/ns/pid')==r['PID_namespace'];p=Path('/proc',str(r['PID']),'stat');assert not p.exists() or p.read_text().rsplit(')',1)[1].split()[19]==r['start_generation'];os.kill(r['PID'],signal.SIGINT) if p.exists() else None"
                dc('exec','-T','storage','/dispatcharrpy/bin/python','-c',script)
        try:
            psql('CREATE DATABASE '+db+' TEMPLATE dispatcharr')
            rdb=cohort['Redis_database'];log='/shared-core-logs/'+group
            argv=['docker','--host','unix:///tmp/dispatcharr-docker.sock',*compose,'exec','-T','-w','/app']
            guest={'SPECIAL_SOURCE_AUTHORITY':'owned-new-special-only','SPECIAL_STAGE':'exercise','SPECIAL_GROUP':group,'SPECIAL_NONCE':nonce,'POSTGRES_DB':db,'REDIS_DB':str(rdb),'REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6379/'+str(rdb),'DJANGO_SECRET_KEY':'owned-functional-context-only','PYTHONPATH':'/core51-tools:/app','DISPATCHARR_LOG_DIR':log,'DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY':'1','DISPATCHARR_ENABLE_IP_LOOKUP':'false'}
            for k,v in guest.items():argv+=['-e',k+'='+v]
            argv+=['storage','/dispatcharrpy/bin/python','manage.py','shell','-c',"import runpy;runpy.run_path('/core51-tools/core51_main.py',run_name='__main__')"]
            paths=[root/(group+'-observer.stdout'),root/(group+'-observer.stderr')];streams=[p.open('xb')for p in paths];process=subprocess.Popen(argv,env=environment,stdin=subprocess.DEVNULL,stdout=streams[0],stderr=streams[1]);start=Path('/proc',str(process.pid),'stat').read_text().rsplit(')',1)[1].split()[19];identity={'pid':process.pid,'observed_starttime':start,'PID_namespace':os.readlink('/proc/self/ns/pid'),'kind':'file-backed Original model/setup and read-only observer CLI; not a Source API request'};report['Core51_host_CLIs'].append(identity);persist();end=time.monotonic()+180
            ready=root/(group+'-setup-ready.json')
            while not ready.exists():
                need(process.poll()is None and time.monotonic()<end,'CoreModelSetupReady180s');need(all(p.stat().st_size<=1048576 for p in paths),'ObserverStdFiles1MiB');time.sleep(.03)
            live=dc('run','--no-deps','-d','--name',name+'-core-'+group,'-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+db,'-e','REDIS_URL=redis://:playback-fixture-only@127.0.0.1:6379/'+str(rdb),'-e','RUST_BIND=0.0.0.0:9194','-e','DISPATCHARR_LOG_DIR='+log,'candidate').decode().strip();need(re.fullmatch('[a-f0-9]{64}',live)is not None,'ActualCoreMainCID');known_cids.append(live)
            actual=json.loads(command(['inspect',live]))[0];save(root/(group+'-Main-inspect.json'),actual);need(actual['Image']==candidate['image_id']and actual['Config']['User']=='65532:65532'and actual['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs'],'ActualCoreProductionMainImageUIDEntry')
            save(root/(group+'-start-Main.json'),{'group':group,'image_id':candidate['image_id'],'port':9194,'actual_Main_CID':live})
            while process.poll()is None:
                need(time.monotonic()<end,'CoreContext180s');need(all(p.stat().st_size<=1048576 for p in paths),'ObserverStdFiles1MiB');time.sleep(.03)
            need(process.wait()==0,'ActualCoreObserverWait0');capture=json.loads((root/(group+'-Main-private.json')).read_bytes());row.update(actual_API_calls=capture['actual_request_count'],completed_API_captures=capture['completed_capture_count'],capture_valid=capture['capture_valid']);result['actual_Main_API_calls']+=capture['actual_request_count'];need(capture['capture_valid']and capture['actual_request_count']==capture['completed_capture_count']==len(cohort['case_ids']),'CompleteCoreFixedSubset')
            row['passed']=True
        except Exception as error:row['error']={'type':type(error).__name__,'message':str(error)}
        finally:
            if process is not None:
                if process.poll()is None:
                    try:stop_observer();process.wait(timeout=10)
                    except Exception as error:
                        row['observer_stop_error']={'type':type(error).__name__,'message':str(error)}
                        process.terminate()
                        try:process.wait(timeout=2)
                        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
                for f in streams:f.flush();f.close()
                identity.update(actual_wait_exit=process.wait(),actual_poll_exit=process.poll(),physical_files_closed=all(f.closed for f in streams),stdout_bytes=paths[0].stat().st_size,stderr_bytes=paths[1].stat().st_size);persist()
            if live:
                try:command(['stop','--time','10',live],20);wait=command(['wait',live],15).decode().strip();row['actual_Main_exit']=int(wait);need(wait=='0','ActualCoreNormalMain0');command(['rm','--volumes',live]);live=None
                except Exception as error:row['Main_stop_error']={'type':type(error).__name__,'message':str(error)}
            try:
                backend=json.loads(psql("SELECT COALESCE(json_agg(t),'[]') FROM (SELECT pid,state FROM pg_stat_activity WHERE datname='"+db+"')t"));row['backends_after_Main']=backend;need(backend==[],'CoreNoBackendsAfterMain')
            except Exception as error:row['backend_error']={'type':type(error).__name__,'message':str(error)}
            row['passed']=row['passed']and row.get('actual_Main_exit')==0 and row.get('backends_after_Main')==[] and not any(k in row for k in ['error','observer_stop_error','Main_stop_error','backend_error']);save(root/'result.json',result)
    dc('exec','-T','-e','PYTHONPATH=/core51-tools:/native-tools','storage','/dispatcharrpy/bin/python','-B','/core51-tools/default_audit_raw.py','after')
    before=json.loads((root/'default-before.json').read_bytes());after=json.loads((root/'default-after.json').read_bytes());need(before==after,'CoreDefault6259AndDB7LiteralUnchanged');result['default6259_and_RedisDB7_unchanged']=True
    result['passed']=result['actual_Main_API_calls']==51 and len(result['cohorts'])==6 and all(r['passed']for r in result['cohorts']);save(root/'result.json',result);return result
