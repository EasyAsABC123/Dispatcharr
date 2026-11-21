#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-vlc-donor-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-vlc-donor.XXXXXXXX")
phase=host
cid=
name="dispatcharr-arm64-vlc-donor-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
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

parent=a7ccdd1f28a2af744a48c1958624535779e68a91
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-vlc-donor-a7ccdd1f-4 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-vlc-donor.yml scripts/rust-platform-contracts/native-arm64-vlc-donor/{probe.py,qualify.sh})
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
timeout 420 docker container start --attach "$cid" > "$work/probe.stdout" 2> "$work/probe.stderr" || child=$?
printf '%s\n' "$child" > "$output/probe-exit"
if [[ $child != 0 || -s $work/probe.stderr ]]; then
    python3 -B - "$work/probe.stdout" "$work/probe.stderr" "$child" > "$output/probe-failure.json" <<'PYDIAG'
import json
from pathlib import Path
import re
import sys

stdout, stderr, code = sys.argv[1:]
sizes = {name: Path(name).stat().st_size for name in (stdout, stderr)}
row = {"actual_probe_exit": int(code), "bounded_structured_diagnostic": False,
       "stdout_bytes": sizes[stdout], "stderr_bytes": sizes[stderr]}
if 0 < sizes[stdout] <= 8192:
    try:
        data = json.loads(Path(stdout).read_bytes())
    except (json.JSONDecodeError, UnicodeDecodeError):
        data = None
    if isinstance(data, dict) and set(data) == {"probe_failed", "exception_class", "frames"}:
        frames = data["frames"]
        name = data["exception_class"]
        if (data["probe_failed"] is True and isinstance(name, str)
                and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]{0,63}", name)
                and isinstance(frames, list) and 1 <= len(frames) <= 8
                and all(isinstance(frame, dict) and set(frame) == {"function", "line"}
                        and isinstance(frame["function"], str)
                        and re.fullmatch(r"<(?:module|lambda|genexpr|listcomp|dictcomp|setcomp)>|[a-z_][a-z_0-9]{0,63}", frame["function"])
                        and type(frame["line"]) is int and 1 <= frame["line"] <= 1024 for frame in frames)):
            row.update(bounded_structured_diagnostic=True, exception_class=name, frames=frames)
print(json.dumps(row, sort_keys=True))
PYDIAG
    exit 1
fi
docker container inspect --format '{{json .State}}' "$cid" > "$work/state.json"
python3 -B - "$work/state.json" "$work/probe.stdout" "$manifest" "$config" > "$output/donor-vlc-inventory.json" <<'PY'
import json
from pathlib import Path
import sys
state_path, projection_path, manifest, config = sys.argv[1:]
state = json.loads(Path(state_path).read_bytes())
assert state['Status'] == 'exited' and state['ExitCode']==0 and state['OOMKilled'] is False
raw = Path(projection_path).read_bytes()
assert len(raw) <= 1 << 20
receipt = json.loads(raw)
assert receipt['oci_identity_from_qualified_launcher'] == {'manifest':manifest,'config':config}
assert receipt['native_platform'] == 'linux/arm64'
assert receipt['vlc_playback_executed'] is receipt['candidate_packaging_qualified'] is False
assert receipt['vlc']['ELF_machine'] == 183 and receipt['vlc']['interpreter'] == '/lib/ld-linux-aarch64.so.1'
assert receipt['plugin_directory'] == '/usr/lib/aarch64-linux-gnu/vlc/plugins'
assert 1 <= len(receipt['plugins']) <= 1024 and all(path.startswith(receipt['plugin_directory'] + '/') for path in receipt['plugins'])
assert any(row.get('ELF_machine') == 183 for row in receipt['plugins'].values())
gaps = receipt['dpkg_ownership_gaps']
allowed = {'diversion-from', 'diversion-to', 'unrecognized-dpkg-owner-row'}
assert gaps.keys() <= set(receipt['missing_notice_origins'])
assert all(reasons and reasons == sorted(set(reasons)) and set(reasons) <= allowed for reasons in gaps.values())
assert receipt['license_inventory_complete'] is (not bool(receipt['missing_notice_origins']))
assert receipt['loader_inventory_complete'] is (not bool(receipt['loader_inventory_gaps']))
print(json.dumps(receipt,indent=2,sort_keys=True))
PY
phase=complete
