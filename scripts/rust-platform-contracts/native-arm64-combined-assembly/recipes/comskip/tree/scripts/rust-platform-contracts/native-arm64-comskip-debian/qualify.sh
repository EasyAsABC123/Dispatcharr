#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-comskip-debian-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-comskip-debian.XXXXXXXX")
phase=host
cid=
image=
image_retired=false
name="dispatcharr-arm64-comskip-debian-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
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
    if [[ -n $image ]] && docker image rm "$image" > "$work/image-remove.log" 2>&1 && \
       ! docker image inspect "$image" > "$work/retired-image.log" 2>&1 && \
       grep -Eq 'No such (image|object)' "$work/retired-image.log"; then
        image_retired=true
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

parent=3344bef38b82f92dee2db3cc6f87a366d41a788b
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-comskip-debian-3344bef3-1 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$({
    printf '%s\n' .github/workflows/native-arm64-comskip-debian.yml
    printf '%s\n' scripts/rust-platform-contracts/native-arm64-comskip-debian/{Dockerfile,assemble_comskip.py,qualify.sh,smoke.sh,verify.py}
    printf '%s\n' scripts/rust-platform-contracts/native-arm64-comskip-debian/abi/{Dockerfile,collect_comskip_arm64.py,comskip-arm64-abi-inputs.json,inputs.json,stage.py}
    printf '%s\n' scripts/rust-platform-contracts/native-arm64-comskip-debian/ffmpeg/{assemble.py,collect_media_arm64.py,inputs.json}
} | LC_ALL=C sort)
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

phase=exact-debian-pull
debian_manifest=sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a
debian_config=sha256:5b6c2c6cbc90b6085e3f9b3086110b1eccedb643374137bfdcd1d2f5c78f6cd1
debian=docker.io/library/debian:bookworm-slim@$debian_manifest
docker pull --platform linux/arm64 "$debian" > "$work/debian-pull.log" 2>&1
docker image inspect "$debian" > "$work/debian-image.json"
python3 -B - "$work/debian-image.json" "$debian_manifest" "$debian_config" > "$output/debian-identity.json" <<'PY'
import json,sys
from pathlib import Path
path,manifest,config=sys.argv[1:]
[item]=json.loads(Path(path).read_bytes())
assert (item['Os'],item['Architecture'])==('linux','arm64')
descriptor=item.get('Descriptor')
if descriptor:
    assert descriptor['digest']==manifest and item['Id'] in (config,manifest)
else:
    assert item['Id']==config
assert set(item['RepoDigests']) & {f'{repository}@{manifest}' for repository in ('debian','library/debian','docker.io/library/debian')}
assert item['RootFS']['Layers']==['sha256:c96c411090da09187c55ab267826b3903b542b5c03f1cafa912d60e060e7850c']
assert not item['Config'].get('Volumes')
print(json.dumps({'config':config,'manifest':manifest,'platform':'linux/arm64','exact_debian_layers':1},indent=2,sort_keys=True))
PY
phase=standalone-offline-assembly
source_scope=(ATTRIBUTION.md LICENSE rust/runtime/comskip-Original-DispatcharrBase rust/runtime/comskip-source.ini)
tree_sha=$(git -C "$root" ls-tree -rz HEAD -- "${source_scope[@]}" | sha256sum | cut -f1 -d' ')
[[ $tree_sha == 06066c1702bba726568fba2e94ed07510fc6ed1f0d17025b2fa461f5a3c35243 ]]
printf '%s\n' "$tree_sha" > "$output/collector-source-tree-sha256"
mkdir "$work/source" "$work/context"
git -C "$root" archive HEAD -- "${source_scope[@]}" | tar -xp -C "$work/source"
phase=exact-ABI-dependency-acquisition
python3 -B "$area/abi/stage.py" acquire --root "$work/source" --context "$work/context" > "$output/acquired-inputs.json"
archive="$work/context/Comskip-source.tar.gz"
curl --fail --show-error --silent --proto '=https' --max-time 40 --max-filesize 1048576 \
    https://codeload.github.com/erikkaashoek/Comskip/tar.gz/a140b6ac8bc8f596729e9052819affc779c3b377 -o "$archive"
printf '%s  %s\n' 5563d65c0cc5f76883e26b9e460348b4826cc496d10f8d53b25e7648e47f87c2 "$archive" | sha256sum --strict --quiet -c
cp -p "$area/Dockerfile" "$area/assemble_comskip.py" "$area/abi/collect_comskip_arm64.py" \
    "$area/abi/comskip-arm64-abi-inputs.json" "$work/source/ATTRIBUTION.md" "$work/context/"
cp -a "$area/ffmpeg" "$work/context/"
phase=private-ABI-build-and-standalone-assembly
built=0
DOCKER_BUILDKIT=1 docker build --no-cache --platform linux/arm64 --network none --pull=false --target verify \
    --iidfile "$work/unit.iid" "$work/context" || built=$?
printf '%s\n' "$built" > "$output/build-exit"
[[ $built == 0 ]]
compiled=$(cat "$work/unit.iid")
[[ $compiled =~ ^sha256:[a-f0-9]{64}$ && $compiled != "$debian_config" && $compiled != "$config" ]]
image=$compiled
phase=verify-unit-image
docker image inspect "$image" > "$work/unit-image.json"
python3 -B - "$work/unit-image.json" "$image" > "$output/unit-image.json" <<'PY'
import json,sys
from pathlib import Path
[item]=json.loads(Path(sys.argv[1]).read_bytes())
assert item['Id']==sys.argv[2] and (item['Os'],item['Architecture'])==('linux','arm64')
root=item['RootFS']['Layers']
assert len(root)==3 and root[0]=='sha256:c96c411090da09187c55ab267826b3903b542b5c03f1cafa912d60e060e7850c'
assert item['Config']['User']=='65532:65532' and not item['Config'].get('Volumes')
print(json.dumps({'id':item['Id'],'platform':'linux/arm64','exact_debian_layers':1,'collector_only_layers':2},indent=2,sort_keys=True))
PY
phase=standalone-comskip-create
cid=$(docker container create --name "$name" --label "$owner" \
    --platform linux/arm64 --pull never --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user 65532:65532 --pids-limit 96 \
    --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777,size=134217728 \
    --mount "type=bind,src=$area/smoke.sh,dst=/probe.sh,readonly" \
    --entrypoint /bin/sh "$image" /probe.sh)
[[ $cid =~ ^[a-f0-9]{64}$ ]]
docker container inspect "$cid" > "$work/container.json"
python3 -B - "$work/container.json" "$cid" "$image" <<'PY'
import json,sys
from pathlib import Path
[item]=json.loads(Path(sys.argv[1]).read_bytes())
assert item['Id']==sys.argv[2] and item['Image']==sys.argv[3] and item['Config']['Image']==sys.argv[3]
assert item['State']['Status']=='created' and item['Config']['User']=='65532:65532'
assert item['Path']=='/bin/sh' and item['Args']==['/probe.sh']
host=item['HostConfig']
assert host['NetworkMode']=='none' and host['ReadonlyRootfs'] is True and host['PidsLimit']==96
assert set(host['CapDrop'])=={'ALL'} and set(host['Tmpfs'])=={'/tmp'}
assert {'nosuid','nodev','noexec'} <= set(host['Tmpfs']['/tmp'].split(','))
assert set(host['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and host['SecurityOpt']
assert [(row['Destination'],row['RW']) for row in item['Mounts'] if row['Type']=='bind']==[('/probe.sh',False)]
assert all(row['Destination']=='/probe.sh' or row['Type']=='tmpfs' and row['Destination']=='/tmp' for row in item['Mounts'])
PY
phase=standalone-comskip-run
status=0
timeout 120 docker container start --attach "$cid" > "$work/runtime.stdout" 2> "$work/runtime.stderr" || status=$?
printf '%s\n' "$status" > "$output/comskip-probe-exit"
if [[ $status != 0 ]]; then
    grep -Ex 'private-loader-and-help=complete|owned-audio-video-decoded=complete|owned-detector-exit=[0-9]{1,3}|empty-edl-and-input-unchanged=complete' \
        "$work/runtime.stderr" > "$output/probe-checkpoint" || true
    exit 1
fi
docker container inspect --format '{{json .State}}' "$cid" > "$work/runtime-state.json"
phase=standalone-comskip-evidence
python3 -B "$area/verify.py" "$work/runtime-state.json" "$work/runtime.stdout" "$work/runtime.stderr" \
    "$area/abi/comskip-arm64-abi-inputs.json" > "$output/runtime.json"
phase=complete
