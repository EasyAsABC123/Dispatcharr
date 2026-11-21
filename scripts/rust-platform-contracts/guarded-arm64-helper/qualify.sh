#!/usr/bin/env bash
set -euo pipefail
umask 077

repo=EasyAsABC123/Dispatcharr
validation_ref=refs/heads/validation/m10-arm64-helper-2581d75b-1
parent=aad849f80100069518bd62d771bf6aafab02e611
builder=rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f
arm_manifest=sha256:4c632e493dfa97f0fe014c3910d1690c149bba85ed8678d47d3563ec6f258ead
builder_id=sha256:f6d52726c6c740d6ee8ea9ff348db1cb4bfbe325d1e86f3295388666500481ec
tools=scripts/rust-platform-contracts/guarded-arm64-helper

test "$GITHUB_ACTIONS:$GITHUB_EVENT_NAME:$GITHUB_REPOSITORY:$GITHUB_REF" = "true:push:$repo:$validation_ref"
test "$RUNNER_OS:$RUNNER_ARCH:$(uname -m):$(dpkg --print-architecture)" = Linux:ARM64:aarch64:arm64
test "$(git rev-parse HEAD)" = "$GITHUB_SHA"
test "$(git rev-list --parents -n 1 HEAD)" = "$GITHUB_SHA $parent"
expected_paths=(
    .github/workflows/native-arm64-guarded-helper.yml
    "$tools/helper-text.patch"
    "$tools/inputs.json"
    "$tools/prepare.py"
    "$tools/qualify.sh"
    "$tools/verify_runtime.py"
)
test "$(git diff --name-status "$parent" "$GITHUB_SHA")" = "$(printf 'A\t%s\n' "${expected_paths[@]}")"

work=$(mktemp -d "$RUNNER_TEMP/dispatcharr-arm64-helper.XXXXXXXX")
container="dispatcharr-arm64-helper-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
docker_cmd=(docker --host unix:///var/run/docker.sock)
owned_container=0
cleanup() {
    if test "$owned_container" = 1 && "${docker_cmd[@]}" container inspect "$container" >/dev/null 2>&1; then
        "${docker_cmd[@]}" container rm -f "$container" >/dev/null
    fi
    rm -rf -- "$work"
}
trap cleanup EXIT
case "$("${docker_cmd[@]}" info --format '{{.OSType}}/{{.Architecture}}')" in
    linux/aarch64|linux/arm64) ;;
    *) exit 1 ;;
esac

python3 -B "$tools/prepare.py" --source "$PWD" --output "$work/context" > "$work/context.json"
"${docker_cmd[@]}" buildx imagetools inspect --raw "$builder" > "$work/oci-index.json"
python3 -B - "$work/oci-index.json" "$arm_manifest" <<'PY'
import json
from pathlib import Path
import sys
index = json.loads(Path(sys.argv[1]).read_bytes())
arm = [row for row in index["manifests"] if row["platform"].get("os") == "linux"
       and row["platform"].get("architecture") == "arm64"]
if len(arm) != 1 or arm[0]["digest"] != sys.argv[2]:
    raise SystemExit("pinned native ARM64 OCI manifest does not match")
PY
"${docker_cmd[@]}" pull --platform linux/arm64 "$builder" > "$work/base-pull.log"
test "$("${docker_cmd[@]}" image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}}' "$builder")" = "$builder_id linux/arm64"

DOCKER_BUILDKIT=1 "${docker_cmd[@]}" build --platform linux/arm64 --network none --pull=false \
    --target taskless-restore-helper --iidfile "$work/image.iid" "$work/context"
image=$(cat "$work/image.iid")
[[ "$image" =~ ^sha256:[0-9a-f]{64}$ ]]
test "$("${docker_cmd[@]}" image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}}' "$image")" = "$image linux/arm64"
if "${docker_cmd[@]}" container inspect "$container" >/dev/null 2>&1; then
    exit 1
fi
owned_container=1
"${docker_cmd[@]}" run --name "$container" --read-only --network none --user 65532:65532 \
    --cap-drop ALL --security-opt no-new-privileges --tmpfs /opt:rw,exec,mode=1777,size=64m \
    --entrypoint /usr/bin/python3 -i "$image" -B - < "$tools/verify_runtime.py" > "$work/runtime.json"
actual=$("${docker_cmd[@]}" container inspect \
    --format '{{.Image}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.HostConfig.NetworkMode}} {{.HostConfig.ReadonlyRootfs}} {{.Config.User}}' "$container")
test "$actual" = "$image 0 false none true 65532:65532"
"${docker_cmd[@]}" container rm "$container" >/dev/null
if "${docker_cmd[@]}" container inspect "$container" >/dev/null 2>&1; then
    exit 1
fi
owned_container=0
python3 -B - "$work" "$image" "$builder_id" "$arm_manifest" <<'PY'
import json
import os
from pathlib import Path
import sys
work = Path(sys.argv[1])
context, runtime = [json.loads((work / f"{name}.json").read_bytes()) for name in ("context", "runtime")]
if runtime["exact_loader_closure_verified"] is not True:
    raise SystemExit("missing actual native loader check")
report = {"scope": "native ARM64 guarded PostgreSQL helper only", "GitHub_commit": os.environ["GITHUB_SHA"],
          "GitHub_run": int(os.environ["GITHUB_RUN_ID"]), "GitHub_run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
          "GitHub_runner_architecture": os.environ["RUNNER_ARCH"],
          "runner_uname_machine": "aarch64", "OCI_builder_manifest": sys.argv[4], "OCI_builder_image": sys.argv[3],
          "locally_built_image": sys.argv[2], "container_runtime_exit": 0, "exact_container_retired": True,
          "network_none_read_only_runtime": True, "staged_context": context, "actual_runtime": runtime,
          "full_ARM_distribution_accepted": False, "application_image_published": False,
          "credit": "Original Dispatcharr, PostgreSQL, Rust, and Debian contributors retain full credit."}
print(json.dumps(report, sort_keys=True))
with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as summary:
    summary.write("## Native ARM64 guarded PostgreSQL helper\n\n")
    summary.write("Actual non-root helper runtime and complete private/system dependency closure passed.\n\n")
    summary.write("The helper image was built locally on the runner. The scope covers the helper only.\n")
PY
