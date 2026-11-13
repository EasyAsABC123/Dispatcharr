"""Read-only post-wait lifecycle queries for the single SDK execution."""
import json,os,subprocess
from pathlib import Path
D=Path(__file__).resolve().parent
actual=json.loads((D/'actual-process-exit.json').read_text());assert actual['wait_completed'] and actual['returncode']==0
p=json.loads((D/'plan.json').read_text());i=json.loads((D/'independent-container-inspect.json').read_text());cid=i['container_id'];project=p['project']
queries=[]
for args in [['docker','container','ls','--all','-q','--filter','label=com.docker.compose.project='+project],['docker','network','ls','-q','--filter','label=com.docker.compose.project='+project],['docker','volume','ls','-q','--filter','label=com.docker.compose.project='+project],['docker','container','ls','--all','-q','--filter','id='+cid],['docker','inspect',cid]]:
 r=subprocess.run(args,capture_output=True,text=True,timeout=30);queries.append({'argv':args,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr})
for q in queries[:4]:assert q['exit']==0 and q['stdout']=='',q
gone=queries[4];assert gone['exit']==1 and json.loads(gone['stdout'])==[] and 'no such object:'in gone['stderr'].lower()
present=Path('/proc/'+str(actual['host_pid'])).exists();assert not present
r={'all_project_resources_absent':True,'exact_CID_absent':True,'SDK_host_process_waited_exit0':True,'SDK_host_PID_proc_present':present,'queries':queries}
(D/'independent-cleanup-query-receipt.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps({'passed':True,'queries':len(queries),'CID':cid,'project':project,'SDK_host_PID_absent':not present},indent=2))
