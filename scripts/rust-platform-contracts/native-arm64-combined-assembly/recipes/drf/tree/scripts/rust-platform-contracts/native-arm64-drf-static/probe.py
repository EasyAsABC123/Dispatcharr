"""Execute the exact pinned DRF collector and compare its ARM donor preimage."""

from hashlib import file_digest
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path("/validation")
OUT = Path("/media-runtime/opt/dispatcharr-frontend/static/rest_framework")
DOCS = Path("/media-runtime/usr/share/doc/dispatcharr")
MANIFEST = ROOT / "drf-static-source-1.json"
FIELDS = ("path", "bytes", "sha256", "mtime_ns")


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def file_table(root):
    records = {}
    for path in sorted(root.rglob("*")):
        assert not path.is_symlink() and (path.is_dir() or path.is_file())
        if path.is_dir():
            continue
        info = path.stat()
        name = path.relative_to(root).as_posix()
        assert re.fullmatch("[A-Za-z0-9._/-]{1,200}", name) and info.st_size <= 32 * 1024 * 1024
        records[name] = {"path": name, "bytes": info.st_size, "sha256": digest(path), "mtime_ns": info.st_mtime_ns}
        assert len(records) <= 128
    return records


def main():
    assert (os.uname().machine, os.geteuid()) == ("aarch64", 65532)
    expected = json.loads(MANIFEST.read_bytes())
    run = subprocess.run([sys.executable, "-I", "-B", str(ROOT / "collect_static.py")],
                         capture_output=True, timeout=40, check=False)
    assert len(run.stderr) < 16000 and not run.stdout
    frames = re.findall(rb'File "/validation/collect_static.py", line ([0-9]+), in main', run.stderr)
    if run.returncode:
        assert run.returncode == 1
        line = int(frames[-1]) if frames else None
        assert line is None or 48 <= line <= 72
    else:
        assert not run.stderr
        line = None
    spec = importlib.util.find_spec("rest_framework")
    assert spec and spec.origin
    source = Path(spec.origin).parent / "static/rest_framework"
    actual = file_table(source)
    expected_rows = {row["path"]: {key: row[key] for key in FIELDS} for row in expected["files"]}
    common = actual.keys() & expected_rows.keys()
    changes = {key: [field for field in FIELDS if actual[key][field] != expected_rows[key][field]]
               for key in sorted(common) if actual[key] != expected_rows[key]}
    version = importlib.metadata.version("djangorestframework")
    licenses = {}
    if run.returncode == 0:
        assert version == expected["DRF_version"] and actual == expected_rows and file_table(OUT) == actual
        assert digest(DOCS / MANIFEST.name) == digest(MANIFEST)
        exported = json.loads((DOCS / "drf-static-export.json").read_bytes())
        assert {row["path"]: row for row in exported["files"]} == actual
        dist = importlib.metadata.distribution("djangorestframework")
        for name in expected["licenses"]:
            path = Path(dist.locate_file(name))
            retained = DOCS / ("DRF-" + path.name)
            assert retained.is_file() and digest(retained) == digest(path)
            licenses[name] = digest(path)
    mtimes = [{"path": name, "expected_mtime_ns": expected_rows[name]["mtime_ns"],
               "actual_mtime_ns": actual[name]["mtime_ns"]} for name, fields in changes.items() if "mtime_ns" in fields]
    print(json.dumps({
        "scope": "unmodified DRF collector against exact Original ARM64 donor",
        "uid": 65532, "native_platform": "linux/arm64",
        "collector_exit": run.returncode, "collector_main_failure_line": line,
        "expected_DRF_version": expected["DRF_version"], "actual_DRF_version": version,
        "expected_asset_count": len(expected_rows), "actual_asset_count": len(actual),
        "missing_paths": sorted(expected_rows.keys() - actual.keys()),
        "extra_paths": sorted(actual.keys() - expected_rows.keys()), "differing_fields": changes,
        "mtime_differences": mtimes, "retained_LICENSE_sha256": licenses,
        "collector_source_sha256": digest(ROOT / "collect_static.py"), "manifest_sha256": digest(MANIFEST),
        "copied_assets_verified": run.returncode == 0, "application_started": False,
        "integrated_package_verified": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
