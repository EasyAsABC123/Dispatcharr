"""Native host M3U HTTP proof with captured original ORM controls."""
import json
import os
from pathlib import Path
import subprocess
from m3u_probe import capture,capture_large,compare
if os.environ.get('M3U_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Restricted to disposable M3U fixture')
compose=json.loads(os.environ['M3U_HTTP_COMPOSE'])

def control(action,label):
    result=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/control.py',action,label],capture_output=True,text=True,timeout=30)
    result.check_returncode();return json.loads(result.stdout.splitlines()[-1])

report=(capture_large if os.environ.get('M3U_LARGE_OUTPUT')=='1' else capture)(os.environ['M3U_HTTP_BASE'],control,'candidate')
source=json.loads(Path(os.environ['M3U_SOURCE_ORACLE']).read_text())
comparison=compare(source,report)
report['source_comparison']=comparison
Path(os.environ['M3U_HTTP_OUTPUT']).write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'cases':len(report['events']),'source_comparison':comparison}))
if not comparison['captured_m3u_http_matches']:raise SystemExit(1)
