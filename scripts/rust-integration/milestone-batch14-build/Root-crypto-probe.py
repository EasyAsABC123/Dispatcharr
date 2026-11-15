from pathlib import Path
import subprocess,os,json
O=Path(__file__).resolve().parent;b=json.loads((O/'build.json').read_bytes());rows=[];env=dict(os.environ,DOCKER_HOST='unix:///tmp/dispatcharr-docker.sock')
def run(label,args):
 argv=['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','32','--memory','64m','--name','dispatcharr-batch14-crypto-'+label,'--entrypoint',args[0],b['image_id'],*args[1:]]
 p=subprocess.run(argv,env=env,capture_output=True,text=True,timeout=30);row={'argv':argv,'actual_exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr};rows.append(row);(O/'Root-crypto-commands.json').write_text(json.dumps(rows,indent=2)+'\n');assert p.returncode==0,row['stderr'];return p.stdout
raw=run('manifest',['cat','/usr/share/doc/dispatcharr/crypto-runtime.json']);m=json.loads(raw)
assert m['source_image']=='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
expected={'/opt/dispatcharr-crypto/bin/gpg':m['binary']['sha256'],'/opt/dispatcharr-crypto/trust/cacert.pem':m['trust']['sha256']}
for name,member in m['libraries'].items():expected['/opt/dispatcharr-crypto/lib/'+name]=member['sha256']
for member in [m['binary'],*m['libraries'].values()]:
 assert member['licenses']
 for license in member['licenses']:expected[license['retained_path']]=license['sha256']
for name,license in m['licenses'].items():expected['/usr/share/doc/dispatcharr/crypto-copyright/'+name+'-LICENSE']=license['sha256']
hashes=run('hashes',['sha256sum',*sorted(expected)]);assert {line.split(maxsplit=1)[1]:line.split(maxsplit=1)[0]for line in hashes.splitlines()}==expected
version=run('version',['gpg','--version']).splitlines()[0];assert version==m['version']
argv=['docker','container','ls','--all','--no-trunc','--filter','name=^/dispatcharr-batch14-crypto-','--format','{{.ID}}'];p=subprocess.run(argv,env=env,capture_output=True,text=True,timeout=15);assert p.returncode==0 and not p.stdout.strip();rows.append({'argv':argv,'actual_exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
(O/'Root-crypto-runtime-probe.json').write_text(json.dumps({'passed':True,'image_id':b['image_id'],'actual_gpg_version':version,'exact_hashed_crypto_and_license_files':len(expected),'all_ELF_package_licenses_retained':True,'trust_sha256':m['trust']['sha256'],'commands':rows,'signature_or_TLS_workflow_acceptance':False},indent=2)+'\n');(O/'crypto-runtime.json').write_text(raw);print(json.dumps({'passed':True,'files':len(expected),'version':version}))
