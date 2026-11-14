"""UNEXECUTED bounded owned-container role preflight, before task fixture setup.
Does not stop processes or replace application behavior. Host stops owned web.
"""
import json,os
from pathlib import Path
from roles import unexpected,validate
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise ValueError('OwnedFixtureRequired')
rows=[];bad=[]
for path in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  with path.open('rb')as stream:raw=stream.read(8193)
 except (FileNotFoundError,PermissionError,ProcessLookupError):continue
 if len(rows)>=256 or len(raw)>8192:raise ValueError('OwnedRoleInventoryCapacity')
 if not raw:continue
 args=[v.decode('utf-8',errors='strict')for v in raw.rstrip(b'\0').split(b'\0')];pid=int(path.parent.name)
 row={'pid':pid,'argv':args};rows.append(row)
 if unexpected(args):bad.append(row)
report={'passed':not bad,'scope':'Point-in-time bounded proc roles only; not arbitrary-process settlement or SQL silence','unexpected_web_worker_beat':bad,'rows':rows,'own_pid':os.getpid()}
if report['passed']:validate(report)
Path('/evidence/configured-roles-before.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'rows':len(rows)}))
raise SystemExit(0 if report['passed']else 1)
