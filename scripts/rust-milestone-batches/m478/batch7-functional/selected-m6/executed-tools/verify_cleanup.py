"""Independent read-only verification after the existing host's owned teardown."""
import argparse,hashlib,importlib.util,json,os,re
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr')
def need(v,k):
 if not v:raise ValueError(k)
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();out=a.output;report=json.loads((out/'execution-private.json').read_text());project=report['project'];need(re.fullmatch('dispatcharr-m6-main-[a-f0-9]{10}',project),'ExactOwnedProject')
 need(len(report['cleanup_steps'])==10 and all(s['passed']for s in report['cleanup_steps']),'ActualHostCleanupNotReplacedByReadOnlyProbe');roles=json.loads((out/'owned-role.json').read_text());spec=importlib.util.spec_from_file_location('bounded_cleanup',ROOT/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py');bounded=importlib.util.module_from_spec(spec);spec.loader.exec_module(bounded);receipts=[]
 for kind,fmt in [('container','{{.ID}}'),('network','{{.ID}}'),('volume','{{.Name}}')]:
  argv=['docker','--host','unix:///tmp/dispatcharr-docker.sock',kind,'ls',*(['--all','--no-trunc']if kind=='container'else[]),'--filter','label=com.docker.compose.project='+project,'--format',fmt];raw,err,row=bounded.collect(argv,15,65536);receipts.append(row);need(row['capture_complete']and row['actual_wait_exit']==0 and not raw.strip(),'ExactProjectAbsent:'+kind)
 for role,identity in roles.items():
  raw,err,row=bounded.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock','container','ls','--all','--no-trunc','--filter','id='+identity['CID'],'--format','{{.ID}}'],15,65536);receipts.append(row);need(row['capture_complete']and row['actual_wait_exit']==0 and not raw.strip(),'ExactRoleCIDAbsent:'+role)
 namespace=os.readlink('/proc/self/ns/pid');known=report['commands']+[report['startup_host_CLI']]
 for row in known:
  need(row['observer_PID_namespace']==namespace,'SameHostPIDNamespace');pid=row['PID'];generation=row['starttime']
  try:now=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
  except FileNotFoundError:now=None
  need(now!=generation,'OriginalHostGenerationAbsent')
 result={'passed':True,'project':project,'independent_inventory_checks':len(receipts),'actual_CIDs_absent':len(roles),'known_original_host_CLIs_absent':len(known),'startup_observer_included':True,'Original_API_task_replay':False,'apps_started':0,'receipts':receipts};(out/'selected-independent-cleanup.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items()if k!='receipts'}));return 0
if __name__=='__main__':raise SystemExit(main())
