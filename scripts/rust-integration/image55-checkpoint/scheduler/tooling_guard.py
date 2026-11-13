"""Bound host tooling preflight. Only fixed read-only Docker metadata queries.
No Docker config/auth contents read, no application resources created.
"""
import hashlib,json,os,re,shutil,stat,subprocess,time
from pathlib import Path
SAFE={'CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN','DOCKER_CONFIG','DOCKER_HOST','PATH'}
HOST='unix:///tmp/dispatcharr-docker.sock'
class ToolingRefusal(ValueError):pass
def need(value,label):
    if not value:raise ToolingRefusal(label)
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def assert_inputs(env,meta):
    selected=meta.get('selected_environment',{})
    need(set(selected)==SAFE and all(type(selected[k])is str and selected[k]for k in SAFE),'ExactSixSafeExports')
    need(all(env.get(k)==selected[k]for k in SAFE),'BoundSourcedEnvironmentRequired')
    need(env.get('DOCKER_HOST')==meta.get('expected_DOCKER_HOST')==HOST,'ExactOwnedDockerHostRequired')
    for key in ['environment_script','docker','compose']:
        row=meta[key];p=Path(row['path'])
        need(p.is_file()and sha(p)==row['sha256'],'ExactToolingFile:'+key)
    docker=Path(meta['docker']['path']);compose=Path(meta['compose']['path'])
    need(os.access(docker,os.X_OK)and os.access(compose,os.X_OK),'ExecutableDockerAndComposeRequired')
    resolved=shutil.which('docker',path=env['PATH'])
    need(resolved is not None and Path(resolved).resolve()==docker.resolve(),'BoundDockerPATHRequired')
    need(compose==Path(env['DOCKER_CONFIG'])/'cli-plugins/docker-compose','BoundComposeDiscoveryRequired')
    return True
def socket_ready():
    try:return stat.S_ISSOCK(Path(HOST[len('unix://'):]).stat().st_mode)
    except OSError:return False
def preflight(env,meta,runner=None):
    assert_inputs(env,meta);need(socket_ready(),'OwnedDockerSocketRequired')
    runner=runner or subprocess.run;docker=meta['docker']['path'];results=[]
    commands=[('version',[docker,'version','--format','{{.Client.Version}}']),('compose',[docker,'compose','version']),('info',[docker,'info','--format','{{.ServerVersion}}'])]
    for name,argv in commands:
        try:r=runner(argv,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=5,check=False)
        except (OSError,subprocess.TimeoutExpired)as error:raise ToolingRefusal('DockerPreflight:'+name+':'+type(error).__name__)from None
        need(type(r.returncode)is int and r.returncode==0,'DockerPreflight:'+name+':Exit')
        need(type(r.stdout)is str and type(r.stderr)is str and len(r.stdout)<=4096 and len(r.stderr)<=4096,'DockerPreflight:'+name+':MetadataShape')
        value=r.stdout.strip();need(bool(value),'DockerPreflight:'+name+':Empty')
        if name=='compose':need(value.startswith('Docker Compose version ')and len(value)<=128,'DockerComposeVersion')
        else:need(re.fullmatch('[0-9A-Za-z][0-9A-Za-z.+_-]{0,127}',value)is not None,'DockerVersionShape')
        results.append({'query':name,'returncode':r.returncode,'stdout':value,'stderr_sha256':hashlib.sha256(r.stderr.encode()).hexdigest()})
    return {'passed':True,'checked_epoch':time.time(),'queries':results,'six_safe_exports_exact':True,'Docker_Compose_binary_hashes_exact':True,'owned_unix_socket':True,'Docker_resources_created':False}
