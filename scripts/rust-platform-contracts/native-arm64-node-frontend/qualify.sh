#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-node-frontend-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-node-frontend.XXXXXXXX")
phase=host
cid=
image=
image_retired=false
name="dispatcharr-arm64-node-frontend-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
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

parent=dd9c2450357c7ab4c88db024d5b0a460408c7a35
manifest=sha256:0bab91d6a954a4a75de4a887edfda7386ed8711aa8b1eb5fb309ee0f0a2c544f
config=sha256:087a47239071fc43868067cccb9d68f1a2b30aeb0042a4bc4b687481820e0516
node_base="node:24@$manifest"
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-node-frontend-dd9c2450-1 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-node-frontend.yml \
    scripts/rust-platform-contracts/native-arm64-node-frontend/{Dockerfile,qualify.sh,smoke.cjs})
[[ $(git -C "$root" diff-tree --no-commit-id --name-only -r HEAD) == "$paths" ]]
[[ $(uname -s) == Linux && $(uname -m) == aarch64 && ${RUNNER_ARCH:?} == ARM64 ]]
[[ $(docker info --format '{{.OSType}}/{{.Architecture}}') =~ ^linux/(arm64|aarch64)$ ]]
printf '%s\n' "$GITHUB_SHA" > "$output/validation-commit"
printf '%s\n' "$parent" > "$output/validation-parent"

phase=frontend-source-custody
scope=(LICENSE ATTRIBUTION.md frontend)
tree=$(git -C "$root" ls-tree -rz HEAD -- "${scope[@]}" | sha256sum | cut -f1 -d' ')
[[ $tree == 40ee75ccaff25edea784dd7fc9355ffab832b9ce93a4498629ec9f62c3ea6d86 ]]
printf '%s\n' "$tree" > "$output/frontend-source-tree-sha256"
mkdir "$work/context"
git -C "$root" archive HEAD -- "${scope[@]}" | tar -x -C "$work/context"
cp "$area/Dockerfile" "$work/context/"
python3 -B - "$work/context/frontend/package-lock.json" > "$output/npm-lock.json" <<'PY'
from base64 import b64decode
from hashlib import sha256
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit
source=Path(sys.argv[1]).read_bytes()
assert sha256(source).hexdigest()=='85c702648c56a102931b3577785eb61b54caabdc2f990fc170cbd1666414d4f9'
lock=json.loads(source)
assert lock['lockfileVersion']==3
rows=lock['packages']
assert len(rows)==445
github=rows['node_modules/webworkify-webpack']
assert github['version']=='2.1.5' and github['resolved']=='git+ssh://git@github.com/xqq/webworkify-webpack.git#24d1e719b4a6cac37a518b2bb10fe124527ef4ef'
assert not github.get('integrity')
registry=0
for key,row in rows.items():
    if key in ('','node_modules/webworkify-webpack'):
        continue
    url=urlsplit(row['resolved'])
    assert url.scheme=='https' and url.netloc=='registry.npmjs.org'
    algorithm,digest=row['integrity'].split('-',1)
    assert algorithm=='sha512' and len(b64decode(digest,validate=True))==64
    registry+=1
assert registry==443
print(json.dumps({'lock_sha256':sha256(source).hexdigest(),'registry_sha512_entries':registry,
                  'pinned_public_git_commit':github['resolved'].rsplit('#',1)[1],
                  'native_tool_lock_entries':[key for key in ('node_modules/@esbuild/linux-arm64',
                    'node_modules/@rollup/rollup-linux-arm64-gnu','node_modules/@swc/core-linux-arm64-gnu')
                    if key in rows]},indent=2,sort_keys=True))
PY

phase=exact-node-base
docker pull --platform linux/arm64 "$node_base" > "$work/pull.log" 2>&1
docker image inspect "$node_base" > "$work/image.json"
python3 -B - "$work/image.json" "$manifest" "$config" > "$output/node-identity.json" <<'PY'
from hashlib import sha256
import json
from pathlib import Path
import sys
file,manifest,config=sys.argv[1:]
[item]=json.loads(Path(file).read_bytes())
assert (item['Os'],item['Architecture'])==('linux','arm64')
descriptor=item.get('Descriptor')
if descriptor:
    assert descriptor['digest']==manifest and item['Id'] in (manifest,config)
else:
    assert item['Id']==config
assert set(item['RepoDigests']) & {f'{repo}@{manifest}' for repo in ('node','library/node','docker.io/library/node')}
assert 'NODE_VERSION=24.22.2' in item['Config']['Env'] and not item['Config'].get('Volumes')
layers=item['RootFS']['Layers']
assert len(layers)==8 and sha256(json.dumps(layers,separators=(',',':')).encode()).hexdigest()=='53078d645b593a5e03a57c4143660ec2554e0c2788c9e27d69bbc488f959a33b'
print(json.dumps({'manifest':manifest,'config':config,'native_platform':'linux/arm64',
                  'version':'24.22.2','OCI_layers':len(layers)},indent=2,sort_keys=True))
PY
phase=pinned-npm-and-offline-frontend-build
built=0
DOCKER_BUILDKIT=1 docker build --no-cache --platform linux/arm64 --pull=false --target frontend \
    --iidfile "$work/frontend.iid" "$work/context" || built=$?
printf '%s\n' "$built" > "$output/build-exit"
[[ $built == 0 ]]
compiled=$(cat "$work/frontend.iid")
[[ $compiled =~ ^sha256:[a-f0-9]{64}$ && $compiled != "$config" ]]
image=$compiled
phase=exact-frontend-image
docker image inspect "$image" > "$work/frontend-image.json"
python3 -B - "$work/frontend-image.json" "$work/image.json" "$image" > "$output/frontend-image.json" <<'PY'
import json,sys
from pathlib import Path
[actual]=json.loads(Path(sys.argv[1]).read_bytes())
[base]=json.loads(Path(sys.argv[2]).read_bytes())
assert actual['Id']==sys.argv[3] and (actual['Os'],actual['Architecture'])==('linux','arm64')
assert actual['RootFS']['Layers'][:8]==base['RootFS']['Layers']
assert actual['Config']['User']=='65532:65532' and not actual['Config'].get('Volumes')
print(json.dumps({'id':actual['Id'],'platform':'linux/arm64','exact_Node_layers':8},sort_keys=True))
PY
phase=frontend-asset-inspection-create
cid=$(docker container create --name "$name" --label "$owner" \
    --platform linux/arm64 --pull never --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user 65532:65532 --pids-limit 32 \
    --mount "type=bind,src=$area/smoke.cjs,dst=/probe.cjs,readonly" \
    --entrypoint /usr/local/bin/node "$image" /probe.cjs)
[[ $cid =~ ^[a-f0-9]{64}$ ]]
docker container inspect "$cid" > "$work/container.json"
python3 -B - "$work/container.json" "$cid" "$image" <<'PY'
import json,sys
from pathlib import Path
[item]=json.loads(Path(sys.argv[1]).read_bytes())
assert item['Id']==sys.argv[2] and item['Image']==sys.argv[3] and item['Config']['Image']==sys.argv[3]
assert item['State']['Status']=='created' and item['Config']['User']=='65532:65532'
assert item['Path']=='/usr/local/bin/node' and item['Args']==['/probe.cjs']
host=item['HostConfig']
assert host['NetworkMode']=='none' and host['ReadonlyRootfs'] is True and host['PidsLimit']==32
assert set(host['CapDrop'])=={'ALL'} and not host.get('Tmpfs')
assert set(host['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and host['SecurityOpt']
assert [(row['Type'],row['Destination'],row['RW']) for row in item['Mounts']]==[('bind','/probe.cjs',False)]
PY
phase=frontend-asset-inspection
status=0
timeout 120 docker container start --attach "$cid" > "$work/runtime.stdout" 2> "$work/runtime.stderr" || status=$?
printf '%s\n' "$status" > "$output/frontend-probe-exit"
if [[ $status != 0 ]]; then
    sed -n 's!^.*(/probe.cjs:\([0-9]\+\):[0-9]\+).*$!\1!p' "$work/runtime.stderr" | head -n 1 > "$output/probe-failure-line"
    exit 1
fi
[[ ! -s $work/runtime.stderr ]]
docker container inspect --format '{{json .State}}' "$cid" > "$work/runtime-state.json"
python3 -B - "$work/runtime-state.json" "$work/runtime.stdout" > "$output/frontend-assets.json" <<'PY'
import json,sys
from pathlib import Path
state=json.loads(Path(sys.argv[1]).read_bytes())
assert state['Status']=='exited' and state['ExitCode']==0 and state['OOMKilled'] is False
raw=Path(sys.argv[2]).read_bytes()
assert 0<len(raw)<4000
result=json.loads(raw)
assert (result['uid'],result['arch'],result['platform'],result['node'])==(65532,'arm64','linux','v24.22.2')
assert set(result['native_tool_sha256'])=={'esbuild','rollup','swc'}
assert result['frontend_started'] is result['integrated_package_verified'] is result['distribution_qualified'] is False
print(json.dumps(result,indent=2,sort_keys=True))
PY
phase=complete
