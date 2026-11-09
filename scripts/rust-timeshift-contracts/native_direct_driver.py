"""Drive a native host server using only the disposable Compose ORM controls."""
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import urlsplit,urlunsplit
from direct_probe import capture,compare
from xc_probe import capture as capture_xc

kind=os.environ.get('TIMESHIFT_NATIVE_KIND','direct')
if kind=='management':
    from management_probe import capture as capture_management,compare as compare_management
compose=json.loads(os.environ['TIMESHIFT_DIRECT_COMPOSE'])
seed=json.loads(os.environ['TIMESHIFT_DIRECT_SEED'])
base=os.environ['TIMESHIFT_DIRECT_BASE']
output=Path(os.environ['TIMESHIFT_DIRECT_OUTPUT'])
if os.environ.get('DIRECT_DISPOSABLE_FIXTURE')!='playback-fixture-only':
    raise RuntimeError('Restricted to disposable direct-auth fixture')


def control(action,label,session_id=None):
    result=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/'+({'xc':'xc_control.py','management':'management_control.py'}.get(kind,'direct_control.py')),action,label]+([session_id] if session_id else []),capture_output=True,text=True,timeout=30)
    result.check_returncode()
    return json.loads(result.stdout.splitlines()[-1])


report=capture_management(base,seed,control,'candidate',active=os.environ.get('TIMESHIFT_MANAGEMENT_ACTIVE')=='1') if kind=='management' else capture_xc(base,control,'candidate') if kind=='xc' else capture(base,seed,control,'candidate')
physical=urlsplit(os.environ['TIMESHIFT_DIRECT_PHYSICAL_ORIGIN'])
logical=urlsplit(os.environ['TIMESHIFT_DIRECT_LOGICAL_ORIGIN'])
for event in report['events']:
    location=event['headers'].get('location')
    if location:
        parsed=urlsplit(location)
        if parsed.scheme==physical.scheme and parsed.netloc==physical.netloc:
            event['headers']['location']=urlunsplit((logical.scheme,logical.netloc,parsed.path,parsed.query,parsed.fragment))
report['host_fixture_origin_mapping']='Published host origin replaces Docker-only server URL; matching redirect origin restored to logical Compose origin for comparison'
oracle=json.loads(Path(os.environ['TIMESHIFT_DIRECT_ORACLE']).read_text())
comparison=compare_management(oracle,report) if kind=='management' else compare(oracle,report)
matched=comparison['captured_management_matches'] if kind=='management' else comparison['captured_direct_auth_matches']
report['source_comparison']=comparison
output.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'native_direct_capture_complete':report['capture_complete'],'native_direct_wire_matches':matched,'differences':comparison['differences'],'cases':len(report['events'])}))
if not matched:raise SystemExit(1)
