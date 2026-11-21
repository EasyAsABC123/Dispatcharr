"""Assemble only the reviewed, signed-input guarded helper build context."""

import argparse
from hashlib import file_digest, sha256
import json
from pathlib import Path
import shutil
import subprocess
from urllib.parse import quote


HERE = Path(__file__).resolve().parent
PINS_SHA = "d890dfe69de686e78310cec71fa03893e8a4ffb619e8f095ecf4ccfcab599ac6"
MIRROR = "https://deb.debian.org/debian/"
HELPER_STAGE = "taskless-restore-helper"
RUST_BUILDER = "rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f"
TASKLESS = Path("rust/runtime/taskless-guard")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    require(path.is_file() and not path.is_symlink(), "regular exact input required")
    with path.open("rb") as data:
        return file_digest(data, "sha256").hexdigest()


def read_inputs():
    file = HERE / "inputs.json"
    require(digest(file) == PINS_SHA, "retained reviewed input manifest")
    facts = json.loads(file.read_bytes())
    require(facts["application_base"] == "2581d75bc4c0ed7f522b08288f335a6db58d141a"
            and facts["original_authors_retain_credit"] is True, "exact application and upstream authors")
    require(set(facts["debian_archive_packages"]) == {"bison", "flex", "liblz4-dev"},
            "exact missing ARM packages")
    return facts


def get_package(source, candidate, target):
    filename = Path(candidate["file"]).name
    require(candidate["architecture"] == "arm64" and filename.endswith("_arm64.deb")
            and candidate["file"].startswith("pool/main/")
            and 0 < candidate["bytes"] < 2 * 1024 * 1024, "exact bounded signed Debian package")
    if source is not None:
        file = source / filename
        require(digest(file) == candidate["sha256"] and file.stat().st_size == candidate["bytes"],
                "exact retained package bytes")
        shutil.copyfile(file, target)
    else:
        subprocess.run(
            ["curl", "--fail", "--silent", "--show-error", "--location", "--proto", "=https",
             "--proto-redir", "=https", "--retry", "2", "--connect-timeout", "15", "--max-time", "90",
             "--max-filesize", str(candidate["bytes"]), "--output", str(target),
             MIRROR + quote(candidate["file"], safe="/")],
            check=True, timeout=320,
        )
        require(digest(target) == candidate["sha256"] and target.stat().st_size == candidate["bytes"],
                "download matches retained signed Debian bytes")


def prepare(source, output, packages=None):
    facts = read_inputs()
    require(not output.exists() and output.parent.is_dir(), "fresh scoped build context")
    for name, checksum in facts["base_input_sha256"].items():
        require(digest(source / name) == checksum, f"exact preimage {name}")
    patch = HERE / "helper-text.patch"
    require(digest(patch) == facts["helper_text_patch_sha256"], "exact reviewed helper patch")
    output.mkdir(mode=0o700)
    for name in facts["base_input_sha256"]:
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
    subprocess.run(["git", "-C", str(output), "apply", "--check", "--whitespace=error", str(patch)],
                   check=True, timeout=20)
    subprocess.run(["git", "-C", str(output), "apply", "--whitespace=error", str(patch)],
                   check=True, timeout=20)
    arm64 = output / TASKLESS / "debian-arm64"
    arm64.mkdir()
    sums = []
    for row in sorted(facts["debian_archive_packages"].values(), key=lambda row: row["file"]):
        filename = Path(row["file"]).name
        get_package(packages, row, arm64 / filename)
        sums.append((filename, row["sha256"]))
    checksum = "".join(f"{value}  {name}\n" for name, value in sorted(sums))
    (arm64 / "SHA256SUMS").write_text(checksum)
    for name, expected in facts["patched_input_sha256"].items():
        require(digest(output / name) == expected, f"exact reviewed postimage {name}")
    dockerfile = (output / "rust/Dockerfile.app").read_bytes()
    lines = dockerfile.splitlines(keepends=True)
    start = lines.index(f"FROM {RUST_BUILDER} AS {HELPER_STAGE}\n".encode())
    stop = next(i for i in range(start + 1, len(lines)) if lines[i].startswith(b"FROM "))
    stage = b"".join(lines[start:stop])
    (output / "Dockerfile").write_bytes(stage)
    return {"source_base": facts["application_base"], "reviewed_input_sha256": PINS_SHA,
            "exact_dockerfile_stage_sha256": sha256(stage).hexdigest(), "signed_archive_packages": 3,
            "stage": HELPER_STAGE}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--local-packages", type=Path)
    args = parser.parse_args()
    result = prepare(args.source.resolve(), args.output.resolve(), args.local_packages)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
