"""Verify the exact release source, locked registry bytes and build context."""

import argparse
from hashlib import file_digest, sha256
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tomllib

HERE = Path(__file__).resolve().parent
BASE = "d426d322b27a197aaaaa057ef73dfb53c516c452"
PARENT = "29a02d10e0c4de683fd0021b8a665637cec55202"
TREE = "3abf6730458363ceaaacc4981da92ca26bf04b68c828d5288f9e010dad9f11f0"
LOCK = "9bc9661784b91bca0a88fbd9c3313168dfbb4e0b5c0e7115c34d198bc461225c"
CACHE_PATCH = "54e839c022c3e4cec5c40fcde25d978e832d12c89cc577a847eb6c94aff3f767"
SCOPE = (
    "ATTRIBUTION.md", "LICENSE", "rust/Dockerfile.app", "rust/Cargo.toml",
    "rust/Cargo.lock", "rust/rust-toolchain.toml", "rust/crates", "rust/vendor",
    "rust/runtime/taskless-guard", "rust/runtime/drf-static-source-1.json",
)
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
HEX = re.compile(r"[0-9a-f]{64}")
DOCKERFILE = Path("rust/Dockerfile.app")
TASKLESS = Path("rust/runtime/taskless-guard")
WORKFLOW = ".github/workflows/native-arm64-rust-build.yml"
CONTROLS = "scripts/rust-platform-contracts/native-arm64-rust-build/"
CONTROL_PATHS = {WORKFLOW, *(CONTROLS + name for name in (
    "cache.patch", "prepare.py", "qualify.sh", "helper/prepare.py",
    "helper/inputs.json", "helper/helper-text.patch",
))}


def need(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    need(path.is_file() and not path.is_symlink(), "expected regular input")
    with path.open("rb") as content:
        return file_digest(content, "sha256").hexdigest()


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], timeout=30)


def source(root, checkout, expected_head):
    head = git(root, "rev-parse", "HEAD").decode().strip()
    parent_line = git(root, "rev-list", "--parents", "-n", "1", head).decode().split()
    need(head == expected_head and parent_line == [head, PARENT], "exact validation child")
    changed = git(root, "diff", "--name-only", "-z", PARENT, head).decode().strip("\0").split("\0")
    need(set(changed) == CONTROL_PATHS, "only reviewed workflow/control input changes")
    tree = git(root, "ls-tree", "-rz", head, "--", *SCOPE)
    need(sha256(tree).hexdigest() == TREE and tree.count(b"\0") == 1470,
         "exact d426d322 compiler inputs including license and attribution")
    checkout.mkdir(mode=0o700)
    with subprocess.Popen(["git", "-C", str(root), "archive", head, "--", *SCOPE],
                          stdout=subprocess.PIPE) as archive:
        subprocess.run(["tar", "-x", "-C", str(checkout)], stdin=archive.stdout, check=True)
        need(archive.wait() == 0, "exact scoped source archive")
    registry_rows(checkout)
    return {"application_base": BASE, "parent": PARENT, "validation_revision": head,
            "scoped_git_entries": 1470, "scoped_git_tree_sha256": TREE,
            "original_authors_retain_credit": True, "cargo_lock_sha256": LOCK}


def registry_rows(checkout):
    lock = checkout / "rust/Cargo.lock"
    need(digest(lock) == LOCK, "exact committed Cargo lock")
    packages = tomllib.loads(lock.read_text())["package"]
    remote = [p for p in packages if "source" in p]
    need(len(remote) == 328 and all(p["source"] == REGISTRY and HEX.fullmatch(p["checksum"])
                                    for p in remote), "exact crates.io-only locked registry")
    rows = {f"{p['name']}-{p['version']}": p["checksum"] for p in remote}
    need(len(rows) == 328, "unique locked source identifiers")
    return rows


def admit(checkout, seed):
    rows = registry_rows(checkout)
    cache = seed / "cargo-home/registry/cache"
    archives = list(cache.glob("*/*.crate"))
    need(len(archives) == len(rows) and {p.stem for p in archives} == set(rows),
         "exactly one raw archive for every dependency")
    for archive in archives:
        need(digest(archive) == rows[archive.stem], "Cargo.lock archive digest mismatch")
    vendor = seed / "vendor"
    need({p.name for p in vendor.iterdir()} == set(rows), "complete lock-directory closure")
    file_count = 0
    for key, checksum in sorted(rows.items()):
        directory = vendor / key
        need(directory.is_dir() and not directory.is_symlink(), "regular dependency directory")
        manifest = directory / ".cargo-checksum.json"
        digest(manifest)
        facts = json.loads(manifest.read_bytes())
        need(set(facts) == {"files", "package"} and facts["package"] == checksum,
             "vendor declaration matches Cargo.lock")
        actual = set()
        for path in directory.rglob("*"):
            need(not path.is_symlink(), "no dependency symlinks")
            if path.is_file() and path != manifest:
                actual.add(path.relative_to(directory).as_posix())
            elif path != manifest:
                need(path.is_dir(), "regular Cargo input")
        need(actual == set(facts["files"]), "vendored files match Cargo inventory")
        for key_name, sha in facts["files"].items():
            rel = PurePosixPath(key_name)
            need(not rel.is_absolute() and ".." not in rel.parts and HEX.fullmatch(sha)
                 and digest(directory / rel) == sha, "vendored file checksum mismatch")
        file_count += len(actual)
    inventory = "".join(f"{rows[key]}  {key}.crate\n" for key in sorted(rows))
    return {"registry_archives": len(rows), "vendored_source_files": file_count,
            "locked_archive_inventory_sha256": sha256(inventory.encode()).hexdigest(),
            "cargo_lock_sha256": LOCK}


def compile_context(checkout, helper, seed):
    cache_patch = HERE / "cache.patch"
    need(digest(cache_patch) == CACHE_PATCH, "exact architecture-only cache patch")
    shutil.copyfile(helper / DOCKERFILE, checkout / DOCKERFILE)
    shutil.copyfile(helper / TASKLESS / "build_helper.py", checkout / TASKLESS / "build_helper.py")
    shutil.copytree(helper / TASKLESS / "debian-arm64", checkout / TASKLESS / "debian-arm64")
    subprocess.run(["git", "-C", str(checkout), "apply", "--whitespace=error", str(cache_patch)],
                   check=True, timeout=20)
    text = (checkout / DOCKERFILE).read_text()
    head, separator, tail = text.partition("\nFROM build AS xmltv-parser-tests\n")
    copy_point = "COPY rust/runtime/drf-static-source-1.json ./rust/runtime/drf-static-source-1.json\n"
    build = "cargo build --manifest-path rust/Cargo.toml --locked --release --jobs 2 -p dispatcharr-rs"
    need(separator and head.count(copy_point) == head.count(build) == 1
         and "AS build\nARG TARGETARCH\n" in head and "AS xmltv-parser-tests" not in head
         and head.count("bookworm-${TARGETARCH}") == 2, "exact offline patch seam")
    head = head.replace(copy_point, copy_point + "COPY _deps/vendor/ /opt/dispatcharr-cargo-vendor/\n"
                        "COPY _deps/config.toml /workspace/.cargo/config.toml\n")
    dockerfile = checkout / DOCKERFILE
    dockerfile.write_text(head.replace(build, build.replace("--locked", "--offline --locked"))
                          + separator + tail)
    deps = checkout / "_deps"
    deps.mkdir(mode=0o700)
    (seed / "vendor").rename(deps / "vendor")
    (deps / "config.toml").write_text(
        '[source.crates-io]\nreplace-with = "dispatcharr-locked-vendor"\n'
        '[source.dispatcharr-locked-vendor]\ndirectory = "/opt/dispatcharr-cargo-vendor"\n'
        '[net]\noffline = true\n'
    )
    return {"offline_dockerfile_sha256": digest(dockerfile), "architecture_cache_patch_sha256": CACHE_PATCH,
            "target": "build", "registry_config_sha256": digest(deps / "config.toml")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("source", "admit", "compile-context"))
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--head")
    args = parser.parse_args()
    if args.phase == "source":
        result = source(args.workspace, args.checkout, args.head)
    elif args.phase == "admit":
        result = admit(args.checkout, args.workspace / "seed")
    else:
        result = compile_context(args.checkout, args.workspace / "helper", args.workspace / "seed")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
