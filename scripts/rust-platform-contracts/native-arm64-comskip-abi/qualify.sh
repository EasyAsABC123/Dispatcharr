#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-comskip-collector-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-comskip-abi.XXXXXXXX")
phase=host
cid=
collector_cid=
image=
image_retired=false
name="dispatcharr-arm64-comskip-donor-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
owner="org.dispatcharr.validation.run=$name"
finish() {
    result=$?
    trap - EXIT
    settled=false
    retired=0
    for native_cid in "$cid" "$collector_cid"; do
        if [[ -n $native_cid ]] && docker container rm -fv "$native_cid" > "$work/removed.log" 2>&1 && \
           ! docker container inspect "$native_cid" > "$work/retired-inspect.log" 2>&1 && \
           grep -Eq 'No such (object|container)' "$work/retired-inspect.log"; then
            retired=$((retired+1))
        fi
    done
    acquired=0
    [[ -z $cid ]] || acquired=$((acquired+1))
    [[ -z $collector_cid ]] || acquired=$((acquired+1))
    if [[ $retired == "$acquired" ]] && \
       docker container ls -a --no-trunc --filter "label=$owner" --format '{{.ID}}' > "$work/remainder" && \
       [[ ! -s $work/remainder ]]; then
        settled=true
    fi
    if [[ -n $image ]]; then
        if docker image rm "$image" > "$work/image-remove.log" 2>&1 && \
           ! docker image inspect "$image" > "$work/retired-image.log" 2>&1 && \
           grep -Eq 'No such (image|object)' "$work/retired-image.log"; then
            image_retired=true
        fi
    fi
    if [[ $result == 0 && ( $settled != true || $image_retired != true || $retired != 2 ) ]]; then
        result=1
        phase=cleanup
    fi
    printf '%s\n' "$result" > "$output/actual-exit"
    printf '%s\n' "$phase" > "$output/terminal-phase"
    printf '{"exact_container_and_inventory_absence":%s,"exact_ephemeral_image_absence":%s,"exact_containers_acquired":%d,"exact_containers_retired":%d}\n' "$settled" "$image_retired" "$acquired" "$retired" > "$output/cleanup.json"
    rm -rf -- "$work"
    exit "$result"
}
trap finish EXIT

parent=5885226a9085c335392b92e0398c8430afd673b0
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-comskip-collector-5885226a-1 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-comskip-abi.yml rust/runtime/{collect_comskip_arm64.py,comskip-arm64-abi-inputs.json} scripts/rust-platform-contracts/native-arm64-comskip-abi/{Dockerfile,inputs.json,qualify.sh,stage.py})
[[ $(git -C "$root" diff-tree --no-commit-id --name-only -r HEAD) == "$paths" ]]
[[ $(uname -s) == Linux && $(uname -m) == aarch64 && ${RUNNER_ARCH:?} == ARM64 ]]
[[ $(docker info --format '{{.OSType}}/{{.Architecture}}') =~ ^linux/(arm64|aarch64)$ ]]
printf '%s\n' "$GITHUB_SHA" > "$output/validation-commit"
printf '%s\n' "$parent" > "$output/validation-parent"

phase=exact-donor-pull
docker pull --platform linux/arm64 "$donor" > "$work/pull.log" 2>&1
phase=donor-identity
docker image inspect "$donor" > "$work/image.json"
python3 -B - "$work/image.json" "$donor" "$manifest" "$config" > "$output/oci-identity.json" <<'PY'
from hashlib import sha256
import json
from pathlib import Path
import sys
inspect_path, donor, manifest, config = sys.argv[1:]
[image] = json.loads(Path(inspect_path).read_bytes())
assert (image['Os'], image['Architecture']) == ('linux','arm64')
descriptor = image.get('Descriptor')
if descriptor:
    assert descriptor['digest'] == manifest and image['Id'] in {manifest,config}
else:
    assert image['Id'] == config
assert donor in image['RepoDigests']
assert image['Config']['Labels']['org.opencontainers.image.revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
assert not image['Config'].get('Volumes')
layers = image['RootFS']['Layers']
stamp = sha256(json.dumps(layers,separators=(',',':')).encode()).hexdigest()
assert len(layers) == 17 and stamp == '6fb7a5b77449c48fdf92aeb9eb8ec812b1902af77a8a8d57b22da02e2f934b39'
print(json.dumps({'donor':donor,'manifest':manifest,'config':config,'OCI_layers':len(layers),'OCI_diffid_SHA256':stamp,
                  'native_platform':'linux/arm64','descriptor_reported_by_engine':bool(descriptor)},indent=2,sort_keys=True))
PY

phase=acquire-exact-inputs
mkdir -- "$work/context"
python3 -B "$area/stage.py" acquire --root "$root" --context "$work/context" > "$output/acquired-inputs.json"
phase=native-no-egress-build
build_status=0
DOCKER_BUILDKIT=1 docker build --no-cache --platform linux/arm64 --network none --pull=false --target smoke \
    --iidfile "$work/replacement.iid" "$work/context" || build_status=$?
printf '%s\n' "$build_status" > "$output/build-exit"
[[ $build_status == 0 ]]
built=$(cat "$work/replacement.iid")
[[ $built =~ ^sha256:[a-f0-9]{64}$ && $built != "$config" && $built != "$manifest" ]]
image=$built
phase=fresh-candidate-image
expected_layers=$(python3 -B -c 'import json,sys; print(json.load(open(sys.argv[1]))["OCI_layers"])' "$output/oci-identity.json")
docker image inspect "$image" > "$work/smoke-image.json"
python3 -B - "$work/smoke-image.json" "$work/image.json" "$image" "$expected_layers" > "$output/smoke-image.json" <<'PY'
import json,sys
from pathlib import Path
candidate,base = [json.loads(Path(path).read_bytes())[0] for path in sys.argv[1:3]]
expected_id,count=sys.argv[3],int(sys.argv[4])
assert candidate['Id']==expected_id and (candidate['Os'],candidate['Architecture'])==('linux','arm64')
assert candidate['RootFS']['Layers'][:count]==base['RootFS']['Layers'] and len(candidate['RootFS']['Layers'])==count+3
assert not candidate['Config'].get('Volumes') and candidate['Config']['User']=='65532:65532'
print(json.dumps({'id':expected_id,'platform':'linux/arm64','original_OCI_layer_prefix_count':count,
                  'candidate_runtime_layers':3},indent=2,sort_keys=True))
PY

phase=smoke-create
cid=$(docker container create --name "$name" --label "$owner" \
    --platform linux/arm64 --pull never --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user 65532:65532 --pids-limit 64 \
    --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777,size=134217728 \
    --entrypoint /dispatcharrpy/bin/python "$image" -I -B /opt/dispatcharr-comskip-abi/stage.py smoke)
[[ $cid =~ ^[0-9a-f]{64}$ ]]
docker container inspect "$cid" > "$work/smoke-container.json"
python3 -B - "$work/smoke-container.json" "$cid" "$image" <<'PY'
import json,sys
from pathlib import Path
item,=json.loads(Path(sys.argv[1]).read_bytes())
assert item['Id']==sys.argv[2] and item['Image']==sys.argv[3] and item['Config']['Image']==sys.argv[3]
assert item['State']['Status']=='created' and item['Config']['User']=='65532:65532'
assert item['Path']=='/dispatcharrpy/bin/python' and item['Args']==['-I','-B','/opt/dispatcharr-comskip-abi/stage.py','smoke']
host=item['HostConfig']
assert host['NetworkMode']=='none' and host['ReadonlyRootfs'] is True and host['PidsLimit']==64
assert set(host['CapDrop'])=={'ALL'} and set(host['Tmpfs'])=={'/tmp'}
assert set(host['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and host['SecurityOpt']
assert all(row['Type']=='tmpfs' and row['Destination']=='/tmp' for row in item['Mounts'])
PY
phase=native-generated-media
smoke=0
timeout 180 docker container start --attach "$cid" > "$work/smoke.stdout" 2> "$work/smoke.stderr" || smoke=$?
printf '%s\n' "$smoke" > "$output/smoke-exit"
if [[ $smoke != 0 ]]; then
    sed -n 's/^\(AssertionError\|FileNotFoundError\|subprocess\.CalledProcessError\|subprocess\.TimeoutExpired\).*$/\1/p' "$work/smoke.stderr" > "$output/smoke-failure-class"
    exit 1
fi
[[ ! -s $work/smoke.stderr ]]
docker container inspect --format '{{json .State}}' "$cid" > "$work/smoke-state.json"
python3 -B - "$work/smoke-state.json" "$work/smoke.stdout" > "$output/replacement.json" <<'PY'
from pathlib import Path
import json,sys
state_path,proof_path=map(Path,sys.argv[1:])
state=json.loads(state_path.read_bytes())
assert state['Status']=='exited' and state['ExitCode']==0 and state['OOMKilled'] is False
raw=proof_path.read_bytes()
assert 0<len(raw)<100000
result=json.loads(raw)
assert result['UID']==65532 and result['release_donor_package_status_preserved'] is True
assert result['direct_donor_licenses_verified']==6 and result['retained_candidate_source_and_recipe_notices'] is True
assert result['media']['original_unchanged'] is result['media']['empty_EDL'] is True
assert (result['media']['help_exit'],result['media']['detector_exit'])==(2,1)
assert result['distribution_qualified'] is result['full_ARM64_image_verified'] is False
print(json.dumps(result,indent=2,sort_keys=True))
PY
phase=rebuilt-binary-digest
expected=9cd2bd32020bb84f27c9a0b187a408610d861137b1eb59a081ce24e7bf2f7dbb
python3 -B - "$output/replacement.json" "$expected" > "$output/rebuilt-binary.json" <<'PY'
import json,re,sys
from pathlib import Path
report=json.loads(Path(sys.argv[1]).read_bytes())
expected=sys.argv[2]
actual=report['compiled']['candidate_sha256']
assert re.fullmatch('[a-f0-9]{64}',actual)
match=actual==expected
print(json.dumps({'rebuilt_binary_sha256':actual,'expected_accepted_binary_sha256':expected,
                  'exact_match':match,'separate_native_smoke_completed':True},indent=2,sort_keys=True))
if not match:
    raise SystemExit(1)
PY

phase=collector-source-material
collector=$root/rust/runtime/collect_comskip_arm64.py
lock=$root/rust/runtime/comskip-arm64-abi-inputs.json
source=$work/Comskip-source.tar.gz
curl --fail --show-error --silent --proto '=https' --max-time 40 --max-filesize 1048576 \
    https://codeload.github.com/erikkaashoek/Comskip/tar.gz/a140b6ac8bc8f596729e9052819affc779c3b377 -o "$source"
printf '%s  %s\n' 5563d65c0cc5f76883e26b9e460348b4826cc496d10f8d53b25e7648e47f87c2 "$source" | sha256sum --strict --quiet -c
cat > "$work/collector-guest.py" <<'GUEST'
from hashlib import file_digest
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory

runtime=Path('/opt/dispatcharr-comskip-abi')
collector=Path('/fixture/collect_comskip_arm64.py')
manifest,config,expected=sys.argv[1:]
assert os.getuid()==65532 and os.uname().machine=='aarch64'
with (runtime/'comskip').open('rb') as stream:
    actual=file_digest(stream,'sha256').hexdigest()
assert actual==expected
materials={'comskip':runtime/'comskip','Comskip-source.tar.gz':Path('/fixture/Comskip-source.tar.gz'),
           'Comskip-LICENSE':runtime/'notices/Comskip-LICENSE','Original-DispatcharrBase':runtime/'notices/Original-DispatcharrBase',
           'comskip.ini':runtime/'comskip.ini','Dispatcharr-ATTRIBUTION.md':runtime/'notices/Dispatcharr-ATTRIBUTION.md'}
with TemporaryDirectory(prefix='dispatcharr-arm64-collector-intake-',dir='/tmp') as directory:
    target=Path(directory)
    for name,path in materials.items():
        shutil.copyfile(path,target/name)
    (target/'comskip').chmod(0o755)
    process=subprocess.run([sys.executable,'-I','-B',collector,'--input-root',target,
                            '--donor-oci-manifest',manifest,'--donor-oci-config',config],
                           text=True,capture_output=True,timeout=120)
    if process.returncode:
        print(json.dumps({'collector_cli_exit':process.returncode,'independent_binary_sha256':actual},sort_keys=True))
        raise SystemExit(1)
    assert not process.stderr and len(process.stdout)<180000
    report=json.loads(process.stdout)
assert report['scope']=='exact native ARM64 replacement collector intake' and report['distribution_qualified'] is False
lock=report['provenance']
assert (lock['original_runtime']['manifest'],lock['original_runtime']['config'])==(manifest,config)
assert lock['input_files_sha256']['comskip']==actual and len(report['copy_input_files'])==6
assert len(report['resolved_libraries'])==len(report['copy_library_files_by_SONAME'])==138
assert len(report['copy_notice_files_by_package'])==len(report['verified_notice_sha256'])==126
with collector.with_name('comskip-arm64-abi-inputs.json').open('rb') as stream:
    lock_digest=file_digest(stream,'sha256').hexdigest()
print(json.dumps({'collector_cli_exit':0,'independent_binary_sha256':actual,'exact_material_inputs':6,
                  'native_loader_libraries':138,'installed_package_notices':126,
                  'verified_native_lock_sha256':lock_digest,
                  'OCI_manifest':manifest,'OCI_config':config,'native_UID':65532,'distribution_qualified':False},
                 indent=2,sort_keys=True))
GUEST
chmod 0444 "$source" "$work/collector-guest.py"
phase=collector-create
collector_cid=$(docker container create --name "$name-intake" --label "$owner" \
    --platform linux/arm64 --pull never --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user 65532:65532 --pids-limit 48 \
    --tmpfs /tmp:rw,nosuid,nodev,exec,mode=1777,size=134217728 \
    --mount "type=bind,src=$collector,dst=/fixture/collect_comskip_arm64.py,readonly" \
    --mount "type=bind,src=$lock,dst=/fixture/comskip-arm64-abi-inputs.json,readonly" \
    --mount "type=bind,src=$source,dst=/fixture/Comskip-source.tar.gz,readonly" \
    --mount "type=bind,src=$work/collector-guest.py,dst=/fixture/collector-guest.py,readonly" \
    --entrypoint /dispatcharrpy/bin/python "$image" -I -B /fixture/collector-guest.py "$manifest" "$config" "$expected")
[[ $collector_cid =~ ^[0-9a-f]{64}$ && $collector_cid != "$cid" ]]
docker container inspect "$collector_cid" > "$work/collector-container.json"
python3 -B - "$work/collector-container.json" "$collector_cid" "$image" "$manifest" "$config" "$expected" <<'PY'
import json,sys
from pathlib import Path
path,cid,image,manifest,config,expected=sys.argv[1:]
item,=json.loads(Path(path).read_bytes())
assert item['Id']==cid and item['Image']==image and item['Config']['Image']==image
assert item['State']['Status']=='created' and item['Config']['User']=='65532:65532'
assert item['Path']=='/dispatcharrpy/bin/python' and item['Args']==['-I','-B','/fixture/collector-guest.py',manifest,config,expected]
host=item['HostConfig']
assert host['NetworkMode']=='none' and host['ReadonlyRootfs'] is True and host['PidsLimit']==48
assert set(host['CapDrop'])=={'ALL'} and set(host['Tmpfs'])=={'/tmp'}
assert 'exec' in host['Tmpfs']['/tmp'].split(',') and 'noexec' not in host['Tmpfs']['/tmp'].split(',')
assert set(host['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and host['SecurityOpt']
found={row['Destination'] for row in item['Mounts'] if row['Type']=='bind' and row['RW'] is False}
assert found=={'/fixture/collector-guest.py','/fixture/collect_comskip_arm64.py','/fixture/comskip-arm64-abi-inputs.json','/fixture/Comskip-source.tar.gz'}
assert all(row['Destination'] in found or row['Type']=='tmpfs' and row['Destination']=='/tmp' for row in item['Mounts'])
PY
phase=collector-native-intake
intake=0
timeout 160 docker container start --attach "$collector_cid" > "$work/collector.stdout" 2> "$work/collector.stderr" || intake=$?
printf '%s\n' "$intake" > "$output/collector-exit"
if [[ $intake != 0 ]]; then
    python3 -B - "$work/collector.stdout" "$expected" > "$output/collector-failure.json" <<'PY'
import json,sys
from pathlib import Path
path,expected=sys.argv[1:]
try:
    raw=Path(path).read_bytes()
    result=json.loads(raw) if 0<len(raw)<=2048 else {}
except ValueError:
    result={}
if result.get('collector_cli_exit') in (1,2) and result.get('independent_binary_sha256')==expected:
    print(json.dumps({'bounded_collector_cli_exit':result['collector_cli_exit'],'independent_binary_match':True}))
else:
    print(json.dumps({'bounded_collector_cli_exit':None,'independent_binary_match_unverified':True}))
PY
    exit 1
fi
[[ ! -s $work/collector.stderr ]]
docker container inspect --format '{{json .State}}' "$collector_cid" > "$work/collector-state.json"
python3 -B - "$work/collector-state.json" "$work/collector.stdout" "$expected" > "$output/collector-intake.json" <<'PY'
import json,sys
from pathlib import Path
state,output=[json.loads(Path(path).read_bytes()) for path in sys.argv[1:3]]
assert state['Status']=='exited' and state['ExitCode']==0 and state['OOMKilled'] is False
assert output['collector_cli_exit']==0 and output['independent_binary_sha256']==sys.argv[3]
assert (output['native_UID'],output['exact_material_inputs'],output['native_loader_libraries'],output['installed_package_notices'])==(65532,6,138,126)
assert output['verified_native_lock_sha256']=='cbfe929a644b4a1134b34af05c00dfe6b03e5b6ea4ceef83ef9f1a26f6aa75fd'
assert output['distribution_qualified'] is False
print(json.dumps(output,sort_keys=True,indent=2))
PY
phase=complete
