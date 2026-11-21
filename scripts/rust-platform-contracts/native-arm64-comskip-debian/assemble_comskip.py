"""Materialize only the admitted native Comskip replacement and independent notices."""

from hashlib import file_digest, sha256
import json
from pathlib import Path
import re
import shutil

from collect_comskip_arm64 import collect, LOCK_SHA256

PREFIX = "/opt/dispatcharr-comskip"
DOCS = "/usr/share/doc/dispatcharr-standalone-comskip"
LOADER = "ld-linux-aarch64.so.1"
DEST = Path("/comskip-closure")
SOURCE = Path("/materials")
OCI_MANIFEST = "sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace"
OCI_CONFIG = "sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43"


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def materialize(report, dest=DEST):
    entries = {}
    admitted = report["provenance"]
    assert report["distribution_qualified"] is False
    assert len(report["copy_input_files"]) == 6
    assert len(report["copy_library_files_by_SONAME"]) == 138
    assert len(report["copy_notice_files_by_package"]) == 126

    def add(source, output, expected, mode):
        relative = Path(output).relative_to("/")
        assert not any(part == ".." for part in relative.parts)
        assert source.is_file() and digest(source) == expected
        target = dest / relative
        if output in entries:
            assert entries[output] == expected and digest(target) == expected
            assert target.stat().st_mode & 0o777 == mode
            return
        assert not target.exists()
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        shutil.copyfile(source, target)
        target.chmod(mode)
        assert digest(target) == expected
        entries[output] = expected

    for name, source in report["copy_input_files"].items():
        if name == "comskip":
            output, mode = PREFIX + "/bin/comskip", 0o755
        elif name == "comskip.ini":
            output, mode = "/app/docker/comskip.ini", 0o644
        else:
            assert re.fullmatch(r"[A-Za-z0-9.-]+", name)
            output, mode = DOCS + "/" + name, 0o644
        add(Path(source), output, admitted["input_files_sha256"][name], mode)
    for soname, source in report["copy_library_files_by_SONAME"].items():
        assert re.fullmatch(r"[A-Za-z0-9_.+-]+", soname)
        add(Path(source), PREFIX + "/lib/" + soname, report["resolved_libraries"][soname]["sha256"], 0o755)
    for package, source in report["copy_notice_files_by_package"].items():
        assert re.fullmatch(r"[a-z0-9][a-z0-9.+-]*(?::(?:arm64|all))?", package)
        output = DOCS + "/copyright/" + package.partition(":")[0] + "/copyright"
        add(Path(source), output, report["verified_notice_sha256"][package], 0o644)
    assert PREFIX + "/lib/" + LOADER in entries
    wrapper = ('#!/bin/sh\nexec ' + PREFIX + '/lib/' + LOADER
               + ' --inhibit-rpath "" --inhibit-cache --library-path ' + PREFIX
               + '/lib ' + PREFIX + '/bin/comskip "$@"\n').encode()
    runtime_name = "/usr/local/bin/comskip"
    target = dest / runtime_name.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    assert not target.exists()
    target.write_bytes(wrapper)
    target.chmod(0o755)
    entries[runtime_name] = sha256(wrapper).hexdigest()
    facts = {"scope": "native ARM64 Comskip replacement assembly",
             "ABI_lock_sha256": LOCK_SHA256, "collector_input_files": 6,
             "private_libraries": 138, "installed_package_notices": 126,
             "resolved_library_sha256": {name: row["sha256"] for name,row in report["resolved_libraries"].items()},
             "path_sha256": entries, "input_OCI_manifest": OCI_MANIFEST, "input_OCI_config": OCI_CONFIG,
             "distribution_qualified": False, "credit": report["credit"]}
    output = dest / DOCS.lstrip("/") / "unit-inputs.json"
    assert not output.exists()
    output.write_text(json.dumps(facts, sort_keys=True, indent=2) + "\n")
    output.chmod(0o644)


if __name__ == "__main__":
    materialize(collect(SOURCE, OCI_MANIFEST, OCI_CONFIG))
