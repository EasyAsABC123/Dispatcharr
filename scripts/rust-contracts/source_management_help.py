"""Original Dispatcharr command help only; authors retain full credit."""
import hashlib,json,os,subprocess,sys
from pathlib import Path
assert os.environ.get('OPERATOR_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_operator_')
cases=[]
for name in ('proxy','dropdb','kill_processes'):
 result=subprocess.run([sys.executable,'/app/manage.py',name,'--help'],capture_output=True,timeout=20,input=b'')
 row={'command':name,'argv':[name,'--help'],'exit_code':result.returncode,'stdout':result.stdout.decode(),'stderr':result.stderr.decode(),'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(),'stderr_sha256':hashlib.sha256(result.stderr).hexdigest(),'scope':'actual subprocess help/startup only; command handle not invoked'}
 cases.append(row)
 assert result.returncode==0,(name,result.returncode)
 assert b'usage:' in result.stdout and b'--help' in result.stdout
Path('/evidence/operator-cli-source.json').write_text(json.dumps({'passed':True,'cases':cases,'credit':'Original Dispatcharr authors retain application and algorithm credit.','command_execution_verified':False},indent=2)+'\n')
