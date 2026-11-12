"""Bounded same-UID /proc helper; explicit activation required, no import sampling."""
import argparse,json,os,sys
from pathlib import Path
from metrics import bounded,stat,collect_pid
EXECUTION_BLOCKED=True
def identity(pid,proc=Path('/proc')):
 fields=dict(line.split(':',1)for line in bounded(Path(proc)/str(pid)/'status').splitlines()if ':'in line)
 return[int(fields[k].split()[1])for k in ['Uid','Gid']]
def inventory(proc=Path('/proc')):
 if os.geteuid()!=0:raise ValueError('RootInventoryOnly')
 pids=sorted(int(p.name)for p in Path(proc).iterdir()if p.name.isdigit())
 if len(pids)>4096:raise ValueError('PIDInventoryCapacity')
 rows=[];errors=[];parents={}
 for pid in pids:
  try:
   data=stat(bounded(Path(proc)/str(pid)/'stat',16384));parents[pid]=data['ppid'];rows.append({'pid':pid,'identity':identity(pid,proc),'start_ticks':data['start_ticks']})
  except (FileNotFoundError,PermissionError)as error:errors.append({'pid':pid,'kind':type(error).__name__})
 excluded={os.getpid()}
 for _ in range(64):
  children={p for p,parent in parents.items()if parent in excluded}
  if children<=excluded:break
  excluded|=children
 else:raise ValueError('ObserverDescendantDepth')
 rows=[r for r in rows if r['pid']not in excluded];identities={tuple(r['identity'])for r in rows}
 if len(identities)>16:raise ValueError('UIDInventoryCapacity')
 return {'processes':rows,'errors':errors,'coverage_complete':not errors,'excluded_observer_descendants':sorted(excluded)}
def rows(payload):
 if not isinstance(payload,dict)or set(payload)!={'identity','processes'}:raise ValueError('CollectorPayloadShape')
 ident=payload['identity'];items=payload['processes']
 if not isinstance(ident,list)or len(ident)!=2 or any(type(i)is not int or i<0 for i in ident):raise ValueError('CollectorUIDShape')
 if ident!=[os.geteuid(),os.getegid()]:raise ValueError('CollectorIdentityMismatch')
 if not isinstance(items,list)or len(items)>4096 or len(set(items))!=len(items):raise ValueError('CollectorPIDList')
 result=[collect_pid(pid,ident)for pid in items]
 if len(json.dumps(result,allow_nan=False).encode())>1048576:raise ValueError('RoleRowsCapacity')
 return result
def main():
 if EXECUTION_BLOCKED:raise RuntimeError('SupplementalProcExecutionBlockedPendingRootReview')
 parser=argparse.ArgumentParser();parser.add_argument('--mode',choices=['inventory','rows'],required=True);args=parser.parse_args()
 if args.mode=='inventory':value=inventory()
 else:
  raw=sys.stdin.buffer.read(65537)
  if len(raw)>65536:raise ValueError('CollectorInputCapacity')
  value=rows(json.loads(raw))
 encoded=json.dumps(value,allow_nan=False,separators=(',',':')).encode()
 if len(encoded)>1048576:raise ValueError('HelperReplyCapacity')
 sys.stdout.buffer.write(encoded+b'\n')
if __name__=='__main__':main()
