#!/usr/bin/env bash
set -euo pipefail
packet=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(git rev-parse --show-toplevel)
test "${GITHUB_REF:?}" = refs/heads/validation/m10-arm64-rust-build-29a02d10-1
test "${RUNNER_ARCH:?}" = ARM64
test "$(uname -m)" = aarch64
[[ $(docker info --format '{{.OSType}}/{{.Architecture}}') =~ ^linux/(arm64|aarch64)$ ]]
[[ "${GITHUB_RUN_ID:?}" =~ ^[0-9]+$ && "${GITHUB_RUN_ATTEMPT:?}" =~ ^[0-9]+$ ]]
rust_index=rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f
rust_arm=rust:1.90.0-bookworm@sha256:4c632e493dfa97f0fe014c3910d1690c149bba85ed8678d47d3563ec6f258ead
rust_config=sha256:f6d52726c6c740d6ee8ea9ff348db1cb4bfbe325d1e86f3295388666500481ec
rust_layers=472b7a79d00e1038155e085f49b84167bde1d632468cba7edd0c8026d5742534
toolchain=/usr/local/rustup/toolchains/1.90.0-aarch64-unknown-linux-gnu/bin
suffix="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
seed_name="dispatcharr-arm64-rust-seed-$suffix"
smoke_name="dispatcharr-arm64-rust-smoke-$suffix"
tag="dispatcharr-arm64-rust-build:$suffix"
evidence="${RUNNER_TEMP:?}/native-arm64-rust-build-evidence"
mkdir -m 0700 "$evidence"
work=$(mktemp -d -- "${RUNNER_TEMP}/dispatcharr-arm64-rust.XXXXXXXX")
phase=source
cleanup() {
    local status=$?
    local clean=true
    trap - EXIT
    if docker container inspect "$seed_name" >/dev/null 2>&1; then
        docker container rm -f "$seed_name" >/dev/null || clean=false
    fi
    if docker container inspect "$smoke_name" >/dev/null 2>&1; then
        docker container rm -f "$smoke_name" >/dev/null || clean=false
    fi
    if docker image inspect "$tag" >/dev/null 2>&1; then
        docker image rm "$tag" >/dev/null || clean=false
    fi
    if docker container inspect "$seed_name" >/dev/null 2>&1 ||
        docker container inspect "$smoke_name" >/dev/null 2>&1 ||
        docker image inspect "$tag" >/dev/null 2>&1; then
        clean=false
    fi
    docker info --format '{{.OSType}}' >/dev/null || clean=false
    rm -rf -- "$work" || clean=false
    test ! -e "$work" || clean=false
    test "$clean" = true || status=1
    printf '{"controller_exit":%d,"last_phase":"%s","owned_containers_tag_and_workspace_retired":%s}\n' \
        "$status" "$phase" "$clean" >"$evidence/terminal.json"
    exit "$status"
}
trap cleanup EXIT
python3 "$packet/prepare.py" source --workspace "$root" --checkout "$work/context" \
    --head "${GITHUB_SHA:?}" >"$evidence/source.json"

phase=exact-official-rust-base
docker pull --platform linux/arm64 "$rust_arm"
docker pull --platform linux/arm64 "$rust_index"
for ref in "$rust_arm" "$rust_index"; do
    test "$(docker image inspect --format '{{.Os}}/{{.Architecture}} {{.Id}}' "$ref")" = "linux/arm64 $rust_config"
    observed_layers=$(docker image inspect --format '{{range .RootFS.Layers}}{{println .}}{{end}}' "$ref" |
        sed '/^$/d' | sha256sum | cut -f1 -d' ')
    test "$observed_layers" = "$rust_layers"
done
printf 'index=%s\narm64_manifest=%s\nconfig=%s\nrootfs_diff_id_sequence_sha256=%s\n' \
    "$rust_index" "$rust_arm" "$rust_config" "$rust_layers" >"$evidence/base.txt"
phase=locked-registry-acquisition
mkdir -m 0700 "$work/seed"
docker run --rm --name "$seed_name" --pull never --platform linux/arm64 --read-only \
    --user "$(id -u):$(id -g)" --cap-drop ALL --security-opt no-new-privileges \
    --mount "type=bind,src=$work/context,dst=/workspace,readonly" \
    --mount "type=bind,src=$work/seed,dst=/seed" --tmpfs /tmp:rw,nosuid,nodev \
    --workdir /workspace --env HOME=/seed --env CARGO_HOME=/seed/cargo-home \
    --env "RUSTC=$toolchain/rustc" --entrypoint "$toolchain/cargo" \
    "$rust_arm" vendor --manifest-path /workspace/rust/Cargo.toml --locked --versioned-dirs \
    /seed/vendor >"$work/vendor-config.txt"
phase=locked-archive-admission
python3 "$packet/prepare.py" admit --workspace "$work" --checkout "$work/context" \
    >"$evidence/cargo-admission.json"

phase=signed-debian-helper-inputs
python3 "$packet/helper/prepare.py" --source "$work/context" --output "$work/helper" \
    >"$evidence/helper-inputs.json"
python3 "$packet/prepare.py" compile-context --workspace "$work" --checkout "$work/context" \
    >"$evidence/build-context.json"
phase=offline-dockerfile-build
DOCKER_BUILDKIT=1 docker build --platform linux/arm64 --pull=false --network none --target build \
    --file "$work/context/rust/Dockerfile.app" --tag "$tag" "$work/context"
id=$(docker image inspect --format '{{.Id}}' "$tag")
[[ "$id" =~ ^sha256:[a-f0-9]{64}$ ]]
test "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$id")" = linux/arm64
printf '%s\n' "$id" >"$evidence/compiled-builder-id.txt"

phase=readonly-native-executable-inspection
docker run --rm -i --name "$smoke_name" --pull never --platform linux/arm64 --read-only --network none \
    --user 65532:65532 --cap-drop ALL --security-opt no-new-privileges --entrypoint /bin/bash \
    "$id" -s -- "$toolchain" >"$evidence/native-executables.txt" <<'GUEST'
set -euo pipefail
export LC_ALL=C
test "$(id -u)" = 65532
test "$(uname -m)" = aarch64
compiler=$("$1/rustc" -vV)
grep -Fx 'release: 1.90.0' <<<"$compiler"
grep -Fx 'host: aarch64-unknown-linux-gnu' <<<"$compiler"
cargo=$("$1/cargo" -V)
[[ "$cargo" == 'cargo 1.90.0 '* ]]
printf '%s\n%s\n' "$compiler" "$cargo"
for name in dispatcharr-rs dispatcharr-operator; do
    file="/out/$name"
    test -f "$file" -a -x "$file"
    elf=$(readelf -W -h -l "$file")
    grep -Eq 'Class:[[:space:]]+ELF64' <<<"$elf"
    grep -Eq 'Data:.*little endian' <<<"$elf"
    grep -Eq 'Machine:[[:space:]]+AArch64' <<<"$elf"
    grep -Fq 'Requesting program interpreter: /lib/ld-linux-aarch64.so.1' <<<"$elf"
    sha256sum "$file"
    printf 'elf=%s ELF64 little endian AArch64 /lib/ld-linux-aarch64.so.1\n' "$name"
done
helper_digest=$(cat /tmp/taskless-helper.digest)
[[ "$helper_digest" =~ ^[a-f0-9]{64}$ ]]
grep -aqF "$helper_digest" /out/dispatcharr-rs
printf 'embedded_guarded_helper_sha256=%s\n' "$helper_digest"
help_sha=$(/out/dispatcharr-operator --help | sha256sum | cut -d' ' -f1)
test "$help_sha" = dbde241bbef84e0df18c41ddbd297b1b601920e50bdd3617d1b6b95f49540595
printf 'operator_help_sha256=%s\n' "$help_sha"
GUEST
phase=qualified
