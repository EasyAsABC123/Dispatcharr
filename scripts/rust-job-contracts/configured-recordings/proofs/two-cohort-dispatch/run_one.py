"""One exact approved plan dispatch; no automatic retry."""
from pathlib import Path
import sys,json,subprocess,os,datetime
ROOT=Path(__file__).resolve().parents[3];OUT=Path(__file__).resolve().parent
label=sys.argv[1];folders={'known9':'configured-recordings-known-insert-prep-1','physical8':'configured-recordings-physical-prep-3'};prep=ROOT/'artifacts/rust'/folders[label];plan=json.loads((prep/'once-plan.json').read_text());record={'label':label,'argv':plan['argv'],'cwd':plan['cwd'],'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'spawned':False}
with(OUT/(label+'-dispatch.stdout')).open('xb')as out,(OUT/(label+'-dispatch.stderr')).open('xb')as err:
 p=subprocess.Popen(plan['argv'],cwd=plan['cwd'],env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')},stdout=out,stderr=err);record.update(spawned=True,pid=p.pid,starttime=Path('/proc',str(p.pid),'stat').read_text().rsplit(')',1)[1].split()[19]);(OUT/(label+'-dispatch-started.json')).write_text(json.dumps(record,indent=2)+'\n');code=p.wait();record.update(actual_wait_exit=code,actual_poll_exit=p.poll(),settled=True,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(OUT/(label+'-dispatch-exit.json')).write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record));raise SystemExit(code)
