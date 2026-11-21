#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C
area=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$area" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-postgres-roundtrip-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-postgres-roundtrip.XXXXXXXX")
phase=host
cid=
server=
network=
image=
image_retired=false
name="dispatcharr-arm64-postgres-roundtrip-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
owner="org.dispatcharr.validation.run=$name"
finish() {
    result=$?
    trap - EXIT
    settled=false
    removed=true
    for item in "$cid" "$server"; do
        if [[ -n $item ]]; then
            if ! docker container rm -fv "$item" > "$work/removed.log" 2>&1 ||
               docker container inspect "$item" > "$work/retired-inspect.log" 2>&1 ||
               ! grep -Eq 'No such (object|container)' "$work/retired-inspect.log"; then
                removed=false
            fi
        fi
    done
    if [[ -n $network && $removed == true ]] &&
       docker network rm "$network" > "$work/removed-network.log" 2>&1 &&
       ! docker network inspect "$network" > "$work/retired-network.log" 2>&1 &&
       grep -Eq 'not found|No such' "$work/retired-network.log" &&
       docker container ls -a --no-trunc --filter "label=$owner" --format '{{.ID}}' > "$work/remainder" &&
       [[ ! -s $work/remainder ]]; then
        settled=true
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
    printf '{"exact_two_containers_and_network_absent":%s,"exact_ephemeral_image_absence":%s}\n' "$settled" "$image_retired" > "$output/cleanup.json"
    rm -rf -- "$work"
    exit "$result"
}
trap finish EXIT

parent=0ad0b34fcaaf8f669138fb580236bca8117ad3ce
manifest=sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace
config=sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43
donor=ghcr.io/dispatcharr/dispatcharr@$manifest
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-postgres-roundtrip-0ad0b34f-3 ]]
[[ $(git -C "$root" rev-parse HEAD) == "${GITHUB_SHA:?}" && $(git -C "$root" rev-parse HEAD^) == "$parent" ]]
paths=$(printf '%s\n' .github/workflows/native-arm64-postgres-roundtrip.yml \
    scripts/rust-platform-contracts/native-arm64-postgres-roundtrip/{client.sh,qualify.sh,server.sh})
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
wrapper="$root/scripts/rust-platform-contracts/native-arm64-postgres-debian"
mkdir "$work/context"
printf '%s  %s\n' 1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7 "$root/ATTRIBUTION.md" \
  | sha256sum --check --status
cp "$wrapper/Dockerfile" "$wrapper/inputs.json" "$wrapper/assemble.py" "$root/ATTRIBUTION.md" "$wrapper/inventory.py" "$work/context/"
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
phase=fresh-private-fixture-network
cp "$area"/{server.sh,client.sh} "$work/"
python3 -B -c 'from secrets import token_hex; print(token_hex(32))' > "$work/owned-password"
chmod 0444 "$work"/{client.sh,server.sh,owned-password}
network=$(docker network create --internal --label "$owner" "$name")
[[ $network =~ ^[a-f0-9]{64}$ ]]
docker network inspect "$network" > "$work/network.json"
python3 -B - "$work/network.json" "$network" "$name" > "$work/owned-network-cidr" <<'PY'
import ipaddress,json,sys
from pathlib import Path
[record]=json.loads(Path(sys.argv[1]).read_bytes())
assert record['Id']==sys.argv[2] and record['Name']==sys.argv[3]
assert record['Internal'] is True and record['Driver']=='bridge' and not record['Containers']
assert record['Labels']['org.dispatcharr.validation.run']==sys.argv[3]
[subnet]=[ipaddress.ip_network(row['Subnet'], strict=True) for row in record['IPAM']['Config']]
assert subnet.version==4 and any(subnet.subnet_of(ipaddress.ip_network(private))
                                 for private in ('10.0.0.0/8','172.16.0.0/12','192.168.0.0/16'))
print(subnet)
PY
owned_cidr=$(cat "$work/owned-network-cidr")
phase=exact-native-donor-server
server=$(docker container create --name "$name-server" --label "$owner" --platform linux/arm64 --pull never \
    --network "$name" --network-alias owned-pg --env "OWNED_PG_CIDR=$owned_cidr" --read-only --cap-drop ALL --security-opt no-new-privileges \
    --user postgres --pids-limit 64 --memory 384m --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777,size=167772160 \
    --mount "type=bind,src=$work/server.sh,dst=/fixture.sh,readonly" \
    --mount "type=bind,src=$work/owned-password,dst=/run/owned-pg-password,readonly" --entrypoint /bin/sh "$donor" /fixture.sh)
[[ $server =~ ^[a-f0-9]{64}$ ]]
phase=exact-clean-debian-client-create
cid=$(docker container create --name "$name-client" --label "$owner" --platform linux/arm64 --pull never \
    --network "$name" --read-only --cap-drop ALL --security-opt no-new-privileges --user 65532:65532 \
    --pids-limit 32 --memory 256m --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777,size=16777216 \
    --mount "type=bind,src=$work/client.sh,dst=/fixture.sh,readonly" \
    --mount "type=bind,src=$work/owned-password,dst=/run/owned-pg-password,readonly" --entrypoint /bin/sh "$image" /fixture.sh)
[[ $cid =~ ^[a-f0-9]{64}$ && $cid != "$server" ]]
docker container inspect "$server" "$cid" > "$work/roles.json"
python3 -B - "$work/roles.json" "$name" "$server" "$cid" "$donor" "$manifest" "$config" "$image" "$owned_cidr" <<'PY'
import json,sys
from pathlib import Path
path, name, server, client, donor, manifest, config, image, owned_cidr = sys.argv[1:]
a,b=json.loads(Path(path).read_bytes())
assert a['Image'] in {manifest,config} and (a.get('ImageManifestDescriptor') or {'digest':manifest})['digest']==manifest
assert b['Image']==image
assert [value for value in a['Config']['Env'] if value.startswith('OWNED_PG_CIDR=')]==['OWNED_PG_CIDR='+owned_cidr]
assert not any(value.startswith('OWNED_PG_CIDR=') for value in b['Config'].get('Env') or ())
for row,cid,configured,user in ((a,server,donor,'postgres'),(b,client,image,'65532:65532')):
    assert row['Id']==cid and row['State']['Status']=='created'
    assert row['Config']['User']==user and row['Config']['Image']==configured
    assert row['Config']['Labels']['org.dispatcharr.validation.run']==name
    assert row['Path']=='/bin/sh' and row['Args']==['/fixture.sh']
    host=row['HostConfig']
    assert host['NetworkMode']==name and not host['PortBindings'] and not host['PublishAllPorts']
    assert host['ReadonlyRootfs'] is True and set(host['CapDrop'])=={'ALL'} and set(host['Tmpfs'])=={'/tmp'}
    assert 'noexec' in host['Tmpfs']['/tmp'].split(',')
    assert set(host['SecurityOpt']) <= {'no-new-privileges','no-new-privileges:true'} and host['SecurityOpt']
    mounts={item['Destination'] for item in row['Mounts'] if item['Type']=='bind' and item['RW'] is False}
    assert mounts=={'/fixture.sh','/run/owned-pg-password'}
    assert all(item['Type']=='bind' and not item['RW'] or item['Type']=='tmpfs' and item['Destination']=='/tmp'
               for item in row['Mounts'])
PY
phase=server-start-readiness
timeout 20 docker container start "$server" > "$work/server-start.log"
ready=false
for _ in {1..45}; do
    if timeout 5 docker exec --user postgres "$server" /usr/lib/postgresql/17/bin/pg_isready -q -h /tmp -p 5432; then
        ready=true
        break
    fi
    sleep 1
done
[[ $ready == true ]]
phase=effective-fixture-scram-rule
rule_count=$(timeout 10 docker exec --user postgres "$server" /usr/lib/postgresql/17/bin/psql \
    -XAtq -v ON_ERROR_STOP=1 -h /tmp -U fixture_owner -d postgres -c \
    "SELECT count(*) FROM pg_hba_file_rules WHERE type='host' AND database=ARRAY['postgres','fixture_source','fixture_restored'] AND user_name=ARRAY['fixture_owner'] AND auth_method='scram-sha-256' AND error IS NULL")
[[ $rule_count == 1 ]]
printf '{"exact_fixture_scram_rule_loaded":true}\n' > "$output/fixture-auth-policy.json"
phase=ordinary-psql-dump-restore
status=0
timeout 90 docker container start --attach "$cid" > "$work/client.stdout" 2> "$work/client.stderr" || status=$?
printf '%s\n' "$status" > "$output/client-exit"
if [[ $status != 0 || -s $work/client.stderr ]]; then
    python3 -B - "$work/client.stdout" "$work/client.stderr" "$status" > "$output/client-diagnostic.json" <<'PY'
from hashlib import sha256
import json,re,sys
from pathlib import Path
stdout, stderr = (Path(name).read_bytes() for name in sys.argv[1:3])
exit_status = int(sys.argv[3])
assert len(stdout) <= 65536 and len(stderr) <= 65536 and 0 <= exit_status <= 255
allowed = {"identity","wrapper-integrity","server-version","create-databases","seed-table","source-rows","dump",
           "archive-size","archive-catalog","archive-expected-table","restore","restored-rows","constraints","terminal-projection"}
marks = re.findall(rb"(?m)^owned-postgres-roundtrip-phase:([a-z-]+)$", stderr)
phase = marks[0].decode("ascii") if len(marks) == 1 and marks[0].decode("ascii") in allowed else "not-observed"
patterns = {
    "postgres_connection": rb"could not (?:connect|translate host name)|connection to server at|connection refused",
    "fixture_authentication": rb"password authentication failed|no password supplied|no pg_hba.conf entry",
    "database_version": rb"server version mismatch|unsupported version",
    "argument_usage": rb"invalid (?:option|compression)|unrecognized option|option requires an argument",
    "artifact_or_file": rb"checksum did not match|Permission denied|could not open (?:input|output) file",
    "shared_library": rb"error while loading shared libraries",
    "postgres_command_error": rb"(?:psql|pg_dump|pg_restore): (?:error|fatal):|ERROR:",
}
classes = [name for name, pattern in patterns.items() if re.search(pattern, stderr, re.I)]
print(json.dumps({"literal_client_exit":exit_status,"client_subphase":phase,"diagnostic_classes":classes,
                  "stdout_bytes":len(stdout),"stderr_bytes":len(stderr),"stdout_sha256":sha256(stdout).hexdigest(),
                  "stderr_sha256":sha256(stderr).hexdigest(),"raw_client_streams_retained":False},indent=2,sort_keys=True))
PY
    exit 1
fi
docker container inspect --format '{{json .State}}' "$cid" > "$work/client-state.json"
phase=ordinary-server-stop
timeout 30 docker container stop --time 18 "$server" > "$work/server-stop.log"
docker container inspect --format '{{json .State}}' "$server" > "$work/server-state.json"
phase=validating-roundtrip
python3 -B - "$work/client.stdout" "$work/client-state.json" "$work/server-state.json" > "$output/postgres-roundtrip.json" <<'PY'
import json,re,sys
from pathlib import Path
client, state, server = map(Path,sys.argv[1:])
for path in (state,server):
    selected=json.loads(path.read_bytes())
    assert selected['Status']=='exited' and selected['ExitCode']==0 and selected['OOMKilled'] is False
assert 0<client.stat().st_size<=4096
record=json.loads(client.read_bytes())
assert set(record)=={'db_major_minor','archive_bytes','archive_sha256','exact_three_rows','exact_two_constraint_types',
                     'single_transaction_restore','retained_wrappers_sha256'}
assert record['db_major_minor']=='17.11' and 512<record['archive_bytes']<262144 and re.fullmatch('[a-f0-9]{64}',record['archive_sha256'])
assert all(record[key] is True for key in ('exact_three_rows','exact_two_constraint_types','single_transaction_restore'))
assert record['retained_wrappers_sha256']=='04456807c73b1c5898cda0bcdbe9d51447c97f8c67ea8bd1b6320964aff0b44c'
print(json.dumps(record|{'scope':'ordinary generated PostgreSQL roundtrip through native Debian clients on an internal fixture network',
                         'server_exit':0,'client_exit':0,'credential_values_serialized':False,
                         'full_arm64_package_verified':False,'distribution_qualified':False},indent=2,sort_keys=True))
PY
phase=complete
