#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-ffmpeg-debian-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-ffmpeg-debian.XXXXXXXX")
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

parent=132764102ccdc20a811460cde8e5abfd08d9a0a9
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-ffmpeg-debian-13276410-2 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-ffmpeg-debian.yml rust/runtime/collect_media_arm64.py scripts/rust-platform-contracts/native-arm64-ffmpeg-debian/{Dockerfile,assemble.py,inputs.json,qualify.sh})
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
mkdir "$work/context"
cp "$area/Dockerfile" "$area/inputs.json" "$area/assemble.py" "$root/ATTRIBUTION.md" "$root/rust/runtime/collect_media_arm64.py" "$work/context/"
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
assert len(root)==2 and root[0]=='sha256:c96c411090da09187c55ab267826b3903b542b5c03f1cafa912d60e060e7850c'
assert item['Config']['User']=='65532:65532' and not item['Config'].get('Volumes')
print(json.dumps({'id':item['Id'],'platform':'linux/arm64','exact_debian_layers':1,'collector_only_layers':1},indent=2,sort_keys=True))
PY
cat > "$work/runtime.sh" <<'GUEST'
set -eu
export LC_ALL=C TZ=UTC PATH=/usr/local/bin:/usr/bin:/bin
unset LD_LIBRARY_PATH LD_PRELOAD
prefix=/opt/dispatcharr-media
docs=/usr/share/doc/dispatcharr-standalone-media
[ "$(id -u)" = 65532 ]
[ "$(uname -m)" = aarch64 ]
[ "$(dpkg --print-architecture)" = arm64 ]
[ ! -d /dispatcharrpy ]
[ ! -e /verified ]
for tool in ffmpeg ffprobe; do
    "$prefix/lib/ld-linux-aarch64.so.1" --inhibit-cache --library-path "$prefix/lib" --list "$prefix/bin/$tool" > "/tmp/$tool.list"
    "/usr/local/bin/$tool" -version > "/tmp/$tool.version"
done
printf 'closure-and-wrappers-loaded\n' >&2
cd /tmp
/usr/local/bin/ffmpeg -hide_banner -v error -f lavfi -i 'testsrc2=size=320x240:rate=25' \
    -f lavfi -i 'sine=frequency=450:sample_rate=44100' -t 8 -threads 1 -c:v mpeg2video -c:a mp2 -f mpegts owned.ts
sha256sum owned.ts > before.sha256
/usr/local/bin/ffprobe -v error -show_entries stream=codec_name:format=duration -of json owned.ts > media.json
/usr/local/bin/ffmpeg -hide_banner -v error -i owned.ts -map 0:v:0 -map 0:a:0 -f null -
sha256sum --strict --quiet -c before.sha256
printf 'BEGIN inventory\n'
cat "$docs/unit-inputs.json"
printf 'END inventory\nBEGIN files\n'
find "$prefix" "$docs" -type f ! -name unit-inputs.json -print | sort | while IFS= read -r name; do sha256sum "$name"; done
sha256sum /usr/local/bin/ffmpeg /usr/local/bin/ffprobe
printf 'END files\nBEGIN ffmpeg-list\n'
cat ffmpeg.list
printf 'END ffmpeg-list\nBEGIN ffprobe-list\n'
cat ffprobe.list
printf 'END ffprobe-list\nBEGIN versions\n'
head -n 1 ffmpeg.version
head -n 1 ffprobe.version
printf 'END versions\nBEGIN media\n'
cat media.json
printf 'END media\nBEGIN digest\n'
cat before.sha256
printf 'END digest\n'
GUEST
chmod 0444 "$work/runtime.sh"
phase=wrapper-create
cid=$(docker container create --name "$name" --label "$owner" \
    --platform linux/arm64 --pull never --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user 65532:65532 --pids-limit 96 \
    --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777,size=134217728 \
    --mount "type=bind,src=$work/runtime.sh,dst=/fixture.sh,readonly" \
    --entrypoint /bin/sh "$image" /fixture.sh)
[[ $cid =~ ^[a-f0-9]{64}$ ]]
docker container inspect "$cid" > "$work/container.json"
python3 -B - "$work/container.json" "$cid" "$image" <<'PY'
import json,sys
from pathlib import Path
[item]=json.loads(Path(sys.argv[1]).read_bytes())
assert item['Id']==sys.argv[2] and item['Image']==sys.argv[3] and item['Config']['Image']==sys.argv[3]
assert item['State']['Status']=='created' and item['Config']['User']=='65532:65532'
assert item['Path']=='/bin/sh' and item['Args']==['/fixture.sh']
host=item['HostConfig']
assert host['NetworkMode']=='none' and host['ReadonlyRootfs'] is True and host['PidsLimit']==96
assert set(host['CapDrop'])=={'ALL'} and set(host['Tmpfs'])=={'/tmp'}
assert 'noexec' in host['Tmpfs']['/tmp'].split(',')
assert set(host['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and host['SecurityOpt']
assert [(row['Destination'],row['RW']) for row in item['Mounts'] if row['Type']=='bind']==[('/fixture.sh',False)]
assert all(row['Destination']=='/fixture.sh' or row['Type']=='tmpfs' and row['Destination']=='/tmp' for row in item['Mounts'])
PY
phase=wrapped-generated-media
status=0
timeout 150 docker container start --attach "$cid" > "$work/runtime.stdout" 2> "$work/runtime.stderr" || status=$?
printf '%s\n' "$status" > "$output/wrapper-exit"
if [[ $status != 0 ]]; then
    grep -Fx 'closure-and-wrappers-loaded' "$work/runtime.stderr" > "$output/smoke-checkpoint" || true
    sed -n 's/^.*error while loading shared libraries: \([a-zA-Z0-9_.+-]*\):.*$/\1/p' "$work/runtime.stderr" > "$output/unresolved-library"
    exit 1
fi
docker container inspect --format '{{json .State}}' "$cid" > "$work/runtime-state.json"
phase=wrapper-evidence
python3 -B - "$work/runtime-state.json" "$work/runtime.stdout" "$work/runtime.stderr" "$area/inputs.json" > "$output/wrapped-media.json" <<'PY'
import hashlib,json,re,sys
from pathlib import Path
state,stdout,stderr,lock=map(Path,sys.argv[1:])
state=json.loads(state.read_bytes())
assert state['Status']=='exited' and state['ExitCode']==0 and state['OOMKilled'] is False
assert stderr.read_bytes()==b'closure-and-wrappers-loaded\n'
inputs=lock.read_bytes(); digest='95064285b5a2bac0b7f1ae235b845caccc6b6bf452c52fcb449511977b6b0d0a'
assert hashlib.sha256(inputs).hexdigest()==digest
known=json.loads(inputs)
received=stdout.read_text()
fields=dict(re.findall(r'^BEGIN ([a-z-]+)\n(.*?)^END \1\n',received,re.MULTILINE|re.DOTALL))
assert set(fields)=={'inventory','files','ffmpeg-list','ffprobe-list','versions','media','digest'}
manifest=json.loads(fields['inventory']); assert manifest['inventory_sha256']==digest and manifest['distribution_qualified'] is False
prefix='/opt/dispatcharr-media'; docs='/usr/share/doc/dispatcharr-standalone-media'; loader='ld-linux-aarch64.so.1'
expected={f'{prefix}/lib/{key}':row['sha256'] for key,row in known['library_inputs_by_SONAME'].items()}
expected.update({f'{docs}/media-copyright/{key}/copyright':row['sha256'] for key,row in known['available_installed_notice_inputs'].items()})
expected[f'{docs}/FFmpeg-reported-license.txt']=known['FFmpeg_reported_license_sha256']
expected[f'{docs}/ATTRIBUTION.md']='1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7'
for name,row in known['binaries'].items():
    expected[f'{prefix}/bin/{name}']=row['sha256']
    wrapper=f'#!/bin/sh\nexec {prefix}/lib/{loader} --inhibit-cache --library-path {prefix}/lib {prefix}/bin/{name} "$@"\n'
    expected[f'/usr/local/bin/{name}']=hashlib.sha256(wrapper.encode()).hexdigest()
actual={}
for line in fields['files'].splitlines():
    value,path=line.split('  ',1)
    assert path not in actual
    actual[path]=value
assert actual==manifest['collected_files']==expected and len(actual)==112
assert manifest['tool_package_notice_gaps']==known['inputs_without_installed_package_notice']==['ffmpeg','ffprobe']
closure_sizes={}
for name in ('ffmpeg','ffprobe'):
    seen=set()
    for line in fields[name+'-list'].splitlines():
        parts=line.split()
        if not parts or parts[0]=='linux-vdso.so.1':
            continue
        if len(parts)==2:
            assert parts[0] in (f'{prefix}/lib/{loader}',f'/lib/{loader}',loader)
            soname=loader
        else:
            # Direct ld.so --list may name the requested interpreter by its ELF path.
            soname=loader if parts[0]==f'/lib/{loader}' else parts[0]
            assert len(parts)==4 and parts[1]=='=>' and parts[2]==f'{prefix}/lib/{soname}'
        assert soname not in seen
        seen.add(soname)
    assert seen==set(known['binaries'][name]['libraries'])
    closure_sizes[name]=len(seen)
assert fields['versions'].splitlines()==[known['binaries'][key]['version'] for key in ('ffmpeg','ffprobe')]
media=json.loads(fields['media'])
assert sorted(row['codec_name'] for row in media['streams'])==['mp2','mpeg2video']
assert 7.9<=float(media['format']['duration'])<=8.2
media_sha, filename=fields['digest'].strip().split('  ',1)
assert filename=='owned.ts' and media_sha=='562fa93972763130c3079330b5e028a1e248ff98d59202129164d20a8c67d0b1'
print(json.dumps({'scope':'exact ARM64 Debian standalone FFmpeg and ffprobe wrapper smoke',
                  'native_inventory_sha256':digest,'collected_files':112,'private_library_counts':closure_sizes,
                  'exact_original_tool_bytes':True,'reported_FFmpeg_license_retained':True,'installed_package_notices_retained':51,
                  'tool_package_notice_gaps':['ffmpeg','ffprobe'],'nonroot_uid':65532,'wrapped_exit':0,
                  'generated_media':{'duration_seconds':float(media['format']['duration']),'codec_names':['mp2','mpeg2video'],
                                     'sha256':media_sha,'video_and_audio_decode_completed':True,'input_unchanged':True},
                  'whole_ARM64_package_verified':False,'distribution_qualified':False,
                  'credit':'Original Dispatcharr, FFmpeg, Ubuntu, Debian and dependency authors retain full credit.'},indent=2,sort_keys=True))
PY
phase=complete
