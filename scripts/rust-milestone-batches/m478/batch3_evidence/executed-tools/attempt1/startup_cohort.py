"""Add cache4+finite2 to the same full Native41 fixture, with an owned DB reset."""
import json,re,uuid
from pathlib import Path
from gpg_trace import validate,need

def collect(out,name,candidate,command,dc,psql,driver,known_cids):
    database='rust_probe_pluginrepo_'+uuid.uuid4().hex[:10];live=None;result={'passed':False,'database':database,'planned_cache_calls':4,'planned_finite_calls':2,'Source_API_calls':0}
    def save():
        (out/'startup-finite-result.json').write_text(json.dumps(result,indent=2)+'\n')
    def guest(script,*args,seconds=45):
        return dc('exec','-T','-e','POSTGRES_DB='+database,'-e','PYTHONPATH=/native-tools','storage','/dispatcharrpy/bin/python','-B','/native-tools/'+script,*args,seconds=seconds)
    save()
    try:
        # Prior Native41 contexts completed and all their SQL backends settled.
        # A new clone isolates the setter/version side effects from Source41.
        psql('CREATE DATABASE '+database+' TEMPLATE dispatcharr')
        driver('clear-owned-redis','startup-finite-redis-reset',database)
        guest('seed.py')
        guest('finite_assets.py','--execute-owned-container','--output','/evidence/finite-assets',seconds=120)
        guest('native_http.py','before')
        live=dc('run','--no-deps','-d','--name',name+'-startup-finite','-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'-e','RUST_BIND=0.0.0.0:9194','candidate').decode().strip()
        need(re.fullmatch('[a-f0-9]{64}',live)is not None,'ActualStartupMainCID');known_cids.append(live);result['actual_Main_CID']=live
        actual=json.loads(command(['inspect',live]))[0];(out/'startup-finite-Main-inspect.json').write_text(json.dumps(actual,indent=2)+'\n')
        need(actual['Image']==candidate['image_id']and actual['Config']['User']=='65532:65532'and actual['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs'],'ActualStartupProductionMain')
        guest('native_http.py','collect',seconds=120)
        captured=json.loads((out/'native-startup.json').read_bytes());finite=[q for q in captured['requests']if q['label'].startswith('finite-signed-')]
        logs=command(['logs',live],combine=True);(out/'startup-finite-Main.logs').write_bytes(logs)
        receipts=[json.loads(f.read_bytes())for f in sorted((out/'finite-assets/receipts').glob('command-*.json'))];verified=[r for r in receipts if '--verify'in r['argv']];need(len(verified)==2 and [r['wait']for r in verified]==[0,1],'ActualToolSignatureVerdicts0and1')
        reference={'requests':[{'index':i}for i in range(2)],'GPG_observer_terminal':{'value':{'direct_GPG_processes':[]}}}
        for i,tool in enumerate(verified):
            reference['GPG_observer_terminal']['value']['direct_GPG_processes'] += [{'case':i,'argv':['gpg','--import'],'actual_wait_result':[None,0]},{'case':i,'argv':['gpg','--verify'],'actual_wait_result':[None,tool['wait']<<8]}]
        proof=validate(reference,{'capture_complete':captured['capture_complete'],'actual_request_count':2,'completed_capture_count':2,'requests':finite},logs,numeric_mode=candidate['plugin_numeric_mode'])
        proof['reference_scope']='Required import success; actual owned tool verify0/1 over Python Original canonical bytes, not Source API replay';(out/'finite-GPG-review.json').write_text(json.dumps(proof,indent=2)+'\n');result['GPG']=proof
        script='readlink /proc/self/ns/pid; for pid in "$@"; do if [ -r "/proc/$pid/stat" ]; then cat "/proc/$pid/stat"; else printf "ABSENT %s\\n" "$pid"; fi; done; printf "HOMES\\n"; find /tmp -maxdepth 1 -type d -name "dispatcharr-gpg-*" -print'
        raw=command(['exec',live,'/bin/sh','-c',script,'owned-finite-GPG-observer',*[str(c['pid'])for c in proof['children']]]);(out/'finite-GPG-absence.stdout').write_bytes(raw)
        lines=raw.decode().splitlines();namespace=lines.pop(0);at=lines.index('HOMES');need(at==len(proof['children'])and lines[at+1:]==[],'ActualFiniteHomesAbsent')
        for child,line in zip(proof['children'],lines[:at]):
            need(child['PID_namespace']==namespace,'ActualFiniteChildSameNamespace')
            need(line=='ABSENT '+str(child['pid'])or(line.split(' ',1)[0]==str(child['pid'])and line.rsplit(')',1)[1].split()[19]!=child['generation']),'ActualFiniteChildGenerationAbsent')
        result['live_namespace_child_home_absence']=True
        need(captured['capture_complete']and captured['actual_HTTP_calls']==6 and captured['default_unchanged'],'ActualCache4Finite2Capture')
        result['actual_HTTP_calls']=captured['actual_HTTP_calls'];result['capture_complete']=True
    except Exception as error:result['error']={'type':type(error).__name__,'message':str(error)}
    finally:
        if live:
            try:
                command(['stop','--time','10',live],20);status=command(['wait',live],15).decode().strip();result['actual_Main_exit']=int(status);need(status=='0','ActualStartupMainNormalExit0');command(['rm','--volumes',live]);live=None
            except Exception as error:result['stop_error']={'type':type(error).__name__,'message':str(error)}
        try:
            backends=json.loads(psql("SELECT COALESCE(json_agg(t),'[]') FROM (SELECT pid,state FROM pg_stat_activity WHERE datname='"+database+"')t"));result['backends_after_Main']=backends;need(backends==[],'StartupActualNoBackendsAfterMain')
        except Exception as error:result['backend_error']={'type':type(error).__name__,'message':str(error)}
        result['passed']=result.get('capture_complete')is True and result.get('actual_Main_exit')==0 and result.get('backends_after_Main')==[] and result.get('live_namespace_child_home_absence')is True and not any(k in result for k in ['error','stop_error','backend_error']);save()
    return result
