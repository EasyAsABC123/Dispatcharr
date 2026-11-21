"""Construct the previously inventoried private PostgreSQL runtime."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

sys.path.insert(0, "/verified")
import inventory


INPUT_SHA = "d9b2b5118eb9183b8ff046b3cb931c92ba56e39cca233d0715fc25dd5ce8b300"
ROOT = Path("/closure")
PREFIX = Path("/opt/dispatcharr-postgres")
DOCS = Path("/usr/share/doc/dispatcharr-standalone-postgres")
PROVIDERS = Path("/usr/lib") / inventory.TRIPLET / "ossl-modules"
EXPECTED_LIBS = 26
EXPECTED_NOTICE_PACKAGES = 20


def need(ok, label):
    if not ok:
        raise ValueError(label)


class Collector(inventory.Inventory):
    def __init__(self):
        super().__init__()
        self.paths = {}

    def library(self, path):
        self.paths.setdefault(path.name, path)
        super().library(path)


def copy(source, target, files, *, expected=None):
    full = ROOT / target.relative_to("/")
    need(source.is_file() and not full.exists(), "exact fresh copied file")
    real = source.resolve(strict=True)
    need(real.is_relative_to("/usr") or real.is_relative_to("/lib") or real == Path("/verified/ATTRIBUTION.md"),
         "owned installed file or project attribution")
    stamp = inventory.sha(source)
    need(expected is None or stamp == expected, "source file differs from inventoried input")
    full.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, full)
    full.chmod(0o755 if target.parent == PREFIX / "bin" or target.name == Path(inventory.INTERPRETER).name else 0o644)
    need(inventory.sha(full) == stamp, "exact closure copy")
    files[target.as_posix()] = stamp


def main():
    need(os.getuid() == 65532 and ROOT.is_dir() and not any(ROOT.iterdir()), "fresh standalone non-root build output")
    raw = Path("/verified/inputs.json").read_bytes()
    need(hashlib.sha256(raw).hexdigest() == INPUT_SHA, "exact hosted native donor inventory")
    pinned = json.loads(raw)
    collector = Collector()
    current = collector.collect()
    inventory.validate(current)
    need(current == pinned and len(current["libraries"]) == EXPECTED_LIBS and
         len(current["packages"]) == EXPECTED_NOTICE_PACKAGES, "closed observed installed input set")
    files = {}
    for name, metadata in current["binaries"].items():
        copy(inventory.BINARY / name, PREFIX / "bin" / name, files, expected=metadata["sha256"])
        wrapper = ROOT / "usr/local/bin" / name
        wrapper.parent.mkdir(parents=True, exist_ok=True)
        wrapper.write_text('#!/bin/sh\nexport OPENSSL_MODULES=/opt/dispatcharr-postgres/ossl-modules\n'
                           f'exec {PREFIX}/lib/{Path(inventory.INTERPRETER).name} --inhibit-cache '
                           f'--library-path {PREFIX}/lib {PREFIX}/bin/{name} "$@"\n')
        wrapper.chmod(0o755)
        files["/usr/local/bin/" + name] = inventory.sha(wrapper)
    for name, metadata in current["libraries"].items():
        copy(collector.paths[name], PREFIX / "lib" / name, files, expected=metadata["sha256"])
    for name, metadata in current["openssl_providers"].items():
        copy(PROVIDERS / name, PREFIX / "ossl-modules" / name, files, expected=metadata["sha256"])
    for owner, package in current["packages"].items():
        name = owner.split(":", 1)[0]
        target = DOCS / "copyright" / name / "copyright"
        if target.as_posix() not in files:
            copy(Path("/usr/share/doc") / name / "copyright", target, files, expected=package["copyright_sha256"])
        else:
            need(files[target.as_posix()] == package["copyright_sha256"], "duplicate owning notices must agree")
    share = sorted(path for path in inventory.SHARE.rglob("*") if path.is_file())
    need(len(share) == current["public_support_files"], "all observed public PostgreSQL support files")
    for path in share:
        copy(path, path, files)
    common = Path("/usr/share/common-licenses")
    for path in sorted(path for path in common.iterdir() if path.is_file()):
        copy(path, DOCS / "common-licenses" / path.name, files)
    copy(Path("/verified/ATTRIBUTION.md"), DOCS / "ATTRIBUTION.md", files)
    note_dir = ROOT / DOCS.relative_to("/")
    output = "\n".join(f"{digest}  {name}" for name, digest in sorted(files.items())) + "\n"
    (note_dir / "files.sha256").write_text(output)
    (note_dir / "manifest.json").write_text(json.dumps({"input_inventory_sha256": INPUT_SHA,
        "copied_manifest_sha256": hashlib.sha256(output.encode()).hexdigest(), "copied_files": len(files),
        "packages": len(current["packages"]), "public_support_files": len(share),
        "client_versions": {name: item["version"] for name, item in current["binaries"].items()}},
        indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
