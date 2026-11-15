"""Physical packaged PG17 probe; no database or application acceptance implied."""
from pathlib import Path
import hashlib,json,os,subprocess
O=Path(__file__).resolve().parent
b=json.loads((O/'build.json').read_text());assert b['passed'] and b['exit_code']==0
commands=[];prefix='dispatcharr-batch13-pg-';env=os.environ.copy()
def invoke(args,timeout=30):
 p=subprocess.run(['docker',*args],env=env,capture_output=True,text=True,timeout=timeout)
 row={'argv':p.args,'actual_exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr};commands.append(row)
 (O/'Root-postgres-commands.json').write_text(json.dumps(commands,indent=2)+'\n')
 assert p.returncode==0,row
 return p.stdout
def run(label,args):
 name=prefix+label;cidfile=O/(name+'.cid');assert not cidfile.exists()
 assert not invoke(['container','ls','--all','--filter','name=^/'+name+'$','--format','{{.ID}}']).strip()
 try:
  return invoke(['run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','32','--memory','128m','--name',name,'--cidfile',str(cidfile),'--entrypoint',args[0],b['image_id'],*args[1:]])
 finally:
  if cidfile.exists():
   cid=cidfile.read_text().strip();assert len(cid)==64 and all(c in '0123456789abcdef'for c in cid)
   if invoke(['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']).strip():invoke(['container','rm','--force',cid])
   assert not invoke(['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']).strip()
raw=run('manifest',['cat','/usr/share/doc/dispatcharr/postgres-runtime.json']);m=json.loads(raw)
assert m['source_image']=='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
assert set(m['binaries'])=={'pg_dump','pg_restore','psql'}
expected={}
def retain(path,member):
 assert path.startswith('/') and '\n'not in path and '\0'not in path
 expected[path]=member['sha256']
 for notice in member.get('licenses',[]):expected[notice['retained_path']]=notice['sha256']
for name,member in m['binaries'].items():
 assert member['licenses'] and '(PostgreSQL) 17.'in member['version'];retain('/opt/dispatcharr-postgres/bin/'+name,member)
for name,member in m['libraries'].items():
 assert member['licenses'];retain('/opt/dispatcharr-postgres/lib/'+name,member)
for path,member in m['auxiliary'].items():assert member['licenses'];retain(path,member)
for member in m['wrappers'].values():retain(member['path'],member)
hashes=run('hashes',['sha256sum',*sorted(expected)])
assert {line.split(maxsplit=1)[1]:line.split(maxsplit=1)[0]for line in hashes.splitlines()}==expected
versions={name:run(name,[name,'--version']).strip()for name in sorted(m['binaries'])}
assert all(versions[name]==m['binaries'][name]['version']for name in versions)
uid=run('uid',['id','-u']).strip();assert uid=='65532'
assert not invoke(['container','ls','--all','--no-trunc','--filter','name=^/'+prefix,'--format','{{.ID}}']).strip()
(O/'postgres-runtime.json').write_text(raw)
report={'passed':True,'image_id':b['image_id'],'git_revision':b['git_revision'],'actual_UID':int(uid),'actual_versions':versions,'exact_hashed_PG_library_auxiliary_wrapper_license_files':len(expected),'all_binary_library_auxiliary_owning_licenses_present':True,'exact_probe_cleanup':True,'database_dump_restore_workflow_acceptance':False,'performance_claims':False}
(O/'Root-postgres-runtime-probe.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
