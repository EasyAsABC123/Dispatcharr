"""Verify stand-alone plugin dependency files without starting a plugin or Torch."""

from hashlib import file_digest
import importlib
import json
import os
from pathlib import Path
import struct
import sys

CONTROL = Path("/validation")
INPUT_SHA = "3dd2636717701c1397308bb98d49b1c3e764de21f0bad56231509704f5431ba5"
PREFIX = Path("/opt/dispatcharr-python")
DOCS = Path("/usr/share/doc/dispatcharr")
MANIFEST = DOCS / "python-host/manifest.json"
SITE = PREFIX / "lib/python3.13/dist-packages"
DONOR_SITE = Path("/dispatcharrpy/lib/python3.13/site-packages")
MODULES = ("django", "psycopg", "celery", "kombu", "redis", "django_redis", "channels",
           "channels_redis", "rest_framework", "regex")


def stamp(path):
    with path.open("rb") as content:
        return file_digest(content, "sha256").hexdigest()


def assert_arm(path):
    with path.open("rb") as stream:
        header = stream.read(64)
    assert len(header) == 64 and header[:7] == b"\x7fELF\x02\x01\x01"
    assert struct.unpack_from("<H", header, 18)[0] == 183


def main():
    global PHASE
    PHASE = "runtime-identity"
    assert os.geteuid() == 65532 and os.uname().machine == "aarch64"
    assert sys.version_info[:3] == (3, 13, 18) and Path(sys.prefix) == PREFIX
    assert not (Path("/dispatcharrpy")).exists() and not (Path("/app")).exists()
    assert all(Path(path).is_relative_to(PREFIX) for path in sys.path if path)
    assert stamp(CONTROL / "inputs.json") == INPUT_SHA
    known = json.loads((CONTROL / "inputs.json").read_bytes())
    manifest = json.loads(MANIFEST.read_bytes())
    assert manifest["original_http_admin_management_entrypoints_excluded"] is True
    assert manifest["source_backend_started"] is manifest["application_started"] is False
    assert manifest["source_application_assets_copied"] is True
    assert manifest["external_key_homes_or_runtime_data_copied"] is False
    elf_inputs = manifest["ELF"]
    files = manifest["files"]
    assert 50 <= len(elf_inputs) <= 2000 and 1000 <= len(files) <= 150000
    PHASE = "notice-inputs"
    records = {key.lower(): value for key, value in manifest["distributions"].items()}
    assert set(records) == set(known["distributions"]) and len(records) == 127
    torch_notices = {}
    for name, record in sorted(records.items()):
        authority = known["distributions"][name]
        assert record["version"] == authority["version"]
        copies = record["notices"]
        assert copies
        installed = [item for item in copies if item["authority"] == "installed-wheel"]
        explicit = [item for item in copies if item["authority"] == "exact-version-upstream"]
        assert len(installed) == authority["installed_notice_count"]
        assert sorted(item["sha256"] for item in explicit) == sorted(authority["tracked_exact_version_notice_sha256"])
        for item in copies:
            path = Path(item["retained"])
            assert path.is_relative_to(DOCS / "python-host/distributions") and path.is_file()
            assert stamp(path) == item["sha256"]
            if name == "torch" and item["authority"] == "installed-wheel":
                source = Path(item["source"]).relative_to(DONOR_SITE).as_posix()
                assert source not in torch_notices
                torch_notices[source] = item["sha256"]
    assert torch_notices == known["torch_notices"] and len(torch_notices) == 90
    PHASE = "file-and-shim-closure"
    [exclusion] = manifest["excluded_dependency_assets"]
    shim = known["torch_shim"]
    assert tuple(exclusion[key] for key in ("path", "version", "sha256", "bytes")) == tuple(
        shim[key] for key in ("path", "version", "sha256", "bytes"))
    assert not (SITE / shim["path"]).exists()
    assert not (PREFIX / "application/apps/api/urls.py").exists()
    assert not (PREFIX / "application/dispatcharr/wsgi.py").exists()
    files_by_path = {}
    for record in files:
        path = Path(record["path"])
        assert path.is_relative_to(PREFIX) and path.is_file() and not path.is_symlink()
        assert str(path) not in files_by_path and path.stat().st_size == record["bytes"]
        assert stamp(path) == record["sha256"]
        files_by_path[str(path)] = record
    assert sum(x["bytes"] for x in files) > 100 * 1024 * 1024
    PHASE = "arm-elf-closure"
    loader = PREFIX / "elf/ld-linux-aarch64.so.1"
    assert_arm(loader)
    torch = SITE / "torch"
    key_libs = [torch / "lib" / name for name in ("libtorch.so", "libtorch_cpu.so", "libc10.so")]
    digest_by_name = {}
    for library in key_libs:
        assert str(library) in files_by_path
        assert_arm(library)
        digest_by_name[library.name] = files_by_path[str(library)]["sha256"]
    for elf in elf_inputs:
        retain = Path(elf["retained"])
        assert str(retain) in files_by_path and elf["sha256"] == files_by_path[str(retain)]["sha256"]
        assert elf["licenses"]
        assert_arm(retain)
        for link in elf["dependencies"]:
            library = Path(link["retained"])
            assert library.is_relative_to(PREFIX / "elf") and str(library) in files_by_path
            assert link["licenses"] and link["sha256"] == files_by_path[str(library)]["sha256"]
            assert_arm(library)
    PHASE = "dependency-origins"
    origins = {}
    for name in MODULES:
        library = importlib.import_module(name)
        origin = Path(library.__file__).resolve()
        assert origin.is_relative_to(SITE)
        origins[name] = origin.relative_to(PREFIX).as_posix()
    assert origins == manifest["actual_relocated_dependency_module_audit"]
    assert "torch" not in sys.modules and "dispatcharr.settings" not in sys.modules
    assert all(Path(path).is_relative_to(PREFIX) for path in sys.path if path)
    assert stamp(DOCS / "LICENSE") == "8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef"
    assert stamp(DOCS / "ATTRIBUTION.md") == "1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7"
    print(json.dumps({"scope": "standalone relocated ARM64 Python plugin dependency-host package",
                      "uid": 65532, "platform": "linux/arm64", "python": "3.13.18",
                      "retained_file_count": len(files), "retained_elf_count": len(elf_inputs),
                      "distribution_count": len(records), "torch_notice_count": 90,
                      "torch_shim_excluded": True, "retained_torch_library_sha256": digest_by_name,
                      "manifest_sha256": stamp(MANIFEST), "dependency_module_origins": origins,
                      "native_loader_sha256": stamp(loader), "source_application_started": False,
                      "plugin_started": False, "torch_imported": False, "distribution_qualified": False}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, ImportError, OSError, ValueError, KeyError):
        print(f"python-host-failure={PHASE}", file=sys.stderr)
        sys.exit(1)
