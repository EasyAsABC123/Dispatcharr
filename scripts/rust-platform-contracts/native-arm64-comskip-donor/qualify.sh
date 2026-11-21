#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-comskip-donor-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-comskip-donor.XXXXXXXX")
phase=host
cid=
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
    if [[ $result == 0 && $settled != true ]]; then
        result=1
        phase=cleanup
    fi
    printf '%s\n' "$result" > "$output/actual-exit"
    printf '%s\n' "$phase" > "$output/terminal-phase"
    printf '{"exact_container_and_inventory_absence":%s}\n' "$settled" > "$output/cleanup.json"
    rm -rf -- "$work"
    exit "$result"
}
trap finish EXIT

parent=ee4c04da14e41da18dbf584baebfa1999763f7cd
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-comskip-donor-ee4c04da-1 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-comskip-donor.yml scripts/rust-platform-contracts/native-arm64-comskip-donor/{probe.py,qualify.sh})
[[ $(git -C "$root" diff-tree --no-commit-id --name-only -r HEAD) == "$paths" ]]
[[ $(uname -s) == Linux && $(uname -m) == aarch64 && ${RUNNER_ARCH:?} == ARM64 ]]
[[ $(docker info --format '{{.OSType}}/{{.Architecture}}') =~ ^linux/(arm64|aarch64)$ ]]
printf '%s\n' "$GITHUB_SHA" > "$output/validation-commit"
printf '%s\n' "$parent" > "$output/validation-parent"

phase=exact-donor-pull
docker pull --platform linux/arm64 "$donor" > "$work/pull.log" 2>&1
phase=donor-identity
docker image inspect --platform linux/arm64 "$donor" > "$work/image.json"
python3 -B - "$work/image.json" "$donor" "$manifest" "$config" > "$output/oci-identity.json" <<'PY'
from hashlib import sha256
import json
from pathlib import Path
import sys
inspect_path, donor, manifest, config = sys.argv[1:]
[image] = json.loads(Path(inspect_path).read_bytes())
assert (image['Os'], image['Architecture']) == ('linux','arm64')
assert image['Id'] in {manifest,config} and image['Descriptor']['digest'] == manifest
assert donor in image['RepoDigests']
assert image['Config']['Labels']['org.opencontainers.image.revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
assert not image['Config'].get('Volumes')
layers = image['RootFS']['Layers']
stamp = sha256(json.dumps(layers,separators=(',',':')).encode()).hexdigest()
assert len(layers) == 17 and stamp == '6fb7a5b77449c48fdf92aeb9eb8ec812b1902af77a8a8d57b22da02e2f934b39'
print(json.dumps({'donor':donor,'manifest':manifest,'config':config,'OCI_layers':len(layers),'OCI_diffid_SHA256':stamp,
                  'native_platform':'linux/arm64'},indent=2,sort_keys=True))
PY

phase=probe-create
cid=$(docker container create --name "$name" --label "$owner" \
    --platform linux/arm64 --pull never --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user 65532:65532 --pids-limit 32 \
    --mount "type=bind,src=$area/probe.py,dst=/probe.py,readonly" \
    --entrypoint /dispatcharrpy/bin/python "$donor" -I -B /probe.py \
    --verified-oci-manifest "$manifest" --verified-oci-config "$config")
[[ $cid =~ ^[0-9a-f]{64}$ ]]
docker container inspect "$cid" > "$work/container.json"
python3 -B - "$work/container.json" "$cid" "$manifest" "$config" "$donor" <<'PY'
import json
from pathlib import Path
import sys
inspect_path, cid, manifest, config, donor = sys.argv[1:]
[item] = json.loads(Path(inspect_path).read_bytes())
assert item['Id'] == cid and item['State']['Status'] == 'created'
assert item['Image'] in {manifest,config}
assert (item.get('ImageManifestDescriptor') or {'digest':manifest})['digest'] == manifest
assert item['Config']['Image'] == donor and item['Config']['User'] == '65532:65532'
assert item['Path'] == '/dispatcharrpy/bin/python'
assert item['Args'] == ['-I','-B','/probe.py','--verified-oci-manifest',manifest,'--verified-oci-config',config]
assert item['HostConfig']['NetworkMode'] == 'none' and item['HostConfig']['ReadonlyRootfs'] is True
assert set(item['HostConfig']['CapDrop']) == {'ALL'}
assert set(item['HostConfig']['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and item['HostConfig']['SecurityOpt']
assert item['HostConfig']['PidsLimit'] == 32
assert [(row['Type'],row['Destination'],row['RW']) for row in item['Mounts']] == [('bind','/probe.py',False)]
PY

phase=probe-run
child=0
timeout 120 docker container start --attach "$cid" > "$work/probe.stdout" 2> "$work/probe.stderr" || child=$?
printf '%s\n' "$child" > "$output/probe-exit"
[[ $child == 0 && ! -s $work/probe.stderr ]]
docker container inspect --format '{{json .State}}' "$cid" > "$work/state.json"
python3 -B - "$work/state.json" "$work/probe.stdout" "$manifest" "$config" > "$output/donor-packages.json" <<'PY'
import json
from pathlib import Path
import sys
state_path, projection_path, manifest, config = sys.argv[1:]
state = json.loads(Path(state_path).read_bytes())
assert state['Status'] == 'exited' and state['ExitCode']==0 and state['OOMKilled'] is False
raw = Path(projection_path).read_bytes()
assert len(raw) <= 12000
receipt = json.loads(raw)
assert receipt['oci_identity_from_qualified_launcher'] == {'manifest':manifest,'config':config}
assert receipt['native_platform'] == 'linux/arm64' and receipt['historical_detector_executed'] is False
assert len(receipt['direct_SONAMEs']) == 6 and all(row['architecture']=='arm64' for row in receipt['direct_SONAMEs'].values())
print(json.dumps(receipt,indent=2,sort_keys=True))
PY
phase=complete
