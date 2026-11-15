from pathlib import Path
import hashlib,io,json,os,subprocess,tarfile
R=Path('/home/justin/github/Dispatcharr');O=Path(__file__).resolve().parent
build=json.loads((O/'build.json').read_bytes());assert build['passed'] and build['exit_code']==0
assert len(build['git_revision'])==40
h=lambda b:hashlib.sha256(b).hexdigest()
raw=subprocess.check_output(['git','archive',build['git_revision'],'--','LICENSE','ATTRIBUTION.md','rust','frontend'],cwd=R)
with tarfile.open(fileobj=io.BytesIO(raw))as t:expected={m.name:t.extractfile(m).read()for m in t.getmembers()if m.isfile()}
with tarfile.open(O/'source.tar')as t:
 actual={m.name:t.extractfile(m).read()for m in t.getmembers()if m.isfile()}
assert actual==expected
assert {k:h(v)for k,v in actual.items()}==build['source_sha256']
mapping=json.loads((R/'scripts/rust-integration/milestone-batch13/source-inputs.json').read_bytes())
assert {k:v for k,v in mapping.items()if k.startswith('rust/')}=={k:v for k,v in build['source_sha256'].items()if k.startswith('rust/')}
env=dict(os.environ,DOCKER_HOST='unix:///tmp/dispatcharr-docker.sock');commands=[]
def call(argv,seconds=30):
 p=subprocess.run(['docker',*argv],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=seconds)
 row={'argv':p.args,'actual_exit':p.returncode,'stdout':p.stdout.decode(),'stderr':p.stderr.decode()};commands.append(row)
 (O/'Root-probe-commands.json').write_text(json.dumps(commands,indent=2)+'\n')
 assert p.returncode==0,row['stderr'];return p.stdout
name='dispatcharr-batch13-physical-probe';cidfile=O/'Root-probe.cid';assert not cidfile.exists()
assert not call(['container','ls','--all','--filter','name=^/'+name+'$','--format','{{.ID}}']).strip()
script='id -u && pwd && sha256sum /usr/share/doc/dispatcharr/LICENSE /usr/share/doc/dispatcharr/ATTRIBUTION.md /usr/share/doc/dispatcharr/Celery-LICENSE /usr/share/doc/dispatcharr/Kombu-LICENSE && sha256sum /usr/share/doc/dispatcharr/sqlx-core/LICENSE-MIT /usr/share/doc/dispatcharr/sqlx-core/LICENSE-APACHE /usr/share/doc/dispatcharr/sqlx-postgres/LICENSE-MIT /usr/share/doc/dispatcharr/sqlx-postgres/LICENSE-APACHE && find /opt/dispatcharr-frontend -type f -exec sha256sum {} + && sha256sum /usr/local/bin/dispatcharr-rs && head -c 4 /usr/local/bin/dispatcharr-rs | od -An -tx1'
failure=None
try:
 out=call(['run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','32','--memory','128m','--name',name,'--cidfile',str(cidfile),'--entrypoint','sh',build['image_id'],'-c',script]).decode()
 lines=out.splitlines();assert lines[:2]==['65532','/app']and lines[-1].strip()=='7f 45 4c 46'
 installed={line.split(maxsplit=1)[1]:line.split(maxsplit=1)[0]for line in lines[2:-2]}
 old=json.loads((R/'scripts/rust-integration/image57-build/Root-credit-frontend-probe.json').read_bytes())
 oldfiles={line.split(maxsplit=1)[1]:line.split(maxsplit=1)[0]for line in old['stdout'].splitlines()[1:]}
 assert len(oldfiles)==114 and all(installed.get(k)==v for k,v in oldfiles.items())
 extras={f'/usr/share/doc/dispatcharr/{package}-LICENSE':h(expected[f'rust/crates/store/datasets/{package}-LICENSE'])for package in['Celery','Kombu']}
 extras.update({f'/usr/share/doc/dispatcharr/{package}/{license}':h(expected[f'rust/vendor/{package}-0.8.6/{license}'])for package in ['sqlx-core','sqlx-postgres']for license in ['LICENSE-MIT','LICENSE-APACHE']})
 assert len(installed)==120 and all(installed.get(k)==v for k,v in extras.items())and set(installed)==set(oldfiles)|set(extras)
 binary=lines[-2].split()[0];assert len(binary)==64
 image=json.loads(call(['image','inspect',build['image_id']]))[0]
 assert image['Id']==build['image_id']and image['Config']['User']=='65532:65532'and image['Config']['WorkingDir']=='/app'and image['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs']
 assert image['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==build['source_tree_sha256']
except BaseException as error:
 failure={'type':type(error).__name__,'message':str(error)}
finally:
 if cidfile.exists():
  cid=cidfile.read_text().strip();assert len(cid)==64
  # A failed probe may still leave an owned container: remove only that CID.
  if call(['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']).strip():call(['container','rm','--force',cid])
  assert not call(['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']).strip()
 assert not call(['container','ls','--all','--filter','name=^/'+name+'$','--format','{{.ID}}']).strip()
 (O/'Root-probe-failure.json').write_text(json.dumps(failure,indent=2)+'\n')
assert failure is None,failure
report={'passed':True,'actual_build_wait_exit':0,'git_revision':build['git_revision'],'image_id':build['image_id'],'source_tree_sha256':build['source_tree_sha256'],'all_archive_files_byteexact_Git':True,'archive_files':len(actual),'Rust_inputs':sum(k.startswith('rust/')for k in actual),'React_inputs':sum(k.startswith('frontend/')for k in actual),'credit_inputs':2,'runtime_Rust_map_exact_validated_candidate':True,'unchanged_original114_React_credit_hashes':True,'installed_assets_and_notices':120,'additional_dependency_notices':extras,'probe_actual_UID':65532,'unchanged_installed_React_and_original_credits':True,'installed_asset_credit_files':114,'actual_UID':65532,'actual_working_directory':'/app','production_binary_sha256':binary,'actual_ELF_magic':True,'actual_production_entrypoint':True,'exact_probe_cleanup':True,'new_Main_or_performance_acceptance':False}
(O/'Root-build-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
