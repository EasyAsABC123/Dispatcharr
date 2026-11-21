#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-comskip-abi-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-comskip-abi.XXXXXXXX")
phase=host
cid=
image=
image_retired=false
name="dispatcharr-arm64-comskip-donor-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
owner="org.dispatcharr.validation.run=$name"
finish() {
    result=$?
    trap - EXIT
    settled=false
    if [[ -n $cid ]]; then
        if docker container rm -fv "$cid" > "$work/removed.log" 2>&1 && \
           ! docker container inspect "$cid" > "$work/retired-inspect.log" 2>&1 && \
           grep -Eq 'No such (object|container)' "$work/retired-inspect.log" && \
           docker container ls -a --no-trunc --filter "label=$owner" --format '{{.ID}}' > "$work/remainder" && \
           [[ ! -s $work/remainder ]]; then
            settled=true
        fi
    fi
    if [[ -n $image ]]; then
        if docker image rm "$image" > "$work/image-remove.log" 2>&1 && \
           ! docker image inspect "$image" > "$work/retired-image.log" 2>&1 && \
           grep -Eq 'No such (image|object)' "$work/retired-image.log"; then
            image_retired=true
        fi
    fi
    if [[ $result == 0 && ( $settled != true || $image_retired != true ) ]]; then
        result=1
        phase=cleanup
    fi
    printf '%s\n' "$result" > "$output/actual-exit"
    printf '%s\n' "$phase" > "$output/terminal-phase"
    printf '{"exact_container_and_inventory_absence":%s,"exact_ephemeral_image_absence":%s}\n' "$settled" "$image_retired" > "$output/cleanup.json"
    rm -rf -- "$work"
    exit "$result"
}
trap finish EXIT

parent=032965d1de75a7afba6918ef7a6c9e75beaabf47
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-comskip-target-abi-032965d1-1 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-comskip-abi.yml scripts/rust-platform-contracts/native-arm64-comskip-abi/{Dockerfile,inputs.json,qualify.sh,stage.py})
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
DOCKER_BUILDKIT=1 docker build --platform linux/arm64 --network none --pull=false --target smoke \
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
phase=complete
