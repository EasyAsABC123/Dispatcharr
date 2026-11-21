"""Inventory native Python host notices and reconcile the Torch exclusion candidate."""

from base64 import urlsafe_b64encode
from hashlib import file_digest
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import re
import struct
import sys

CONTROL = Path("/validation")
SITE = Path("/dispatcharrpy/lib/python3.13/site-packages").resolve()
WORDS = ("LICENSE", "LICENCE", "COPYING", "COPYRIGHT", "NOTICE")
SHIM = "torch/bin/test_shim"
SHIM_SHA = "7a6ed5a36b82b29aa78469736dca7b9ba36f8534bf40c7c30291d4c5339f86ba"


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def inside(path):
    path = Path(path).resolve()
    return path if path.is_relative_to(SITE) else None


def legal(path):
    return path.is_file() and any(word in path.name.upper() for word in WORDS)


def notices(dist, members):
    info = inside(dist._path)
    assert info is not None
    paths = set()
    for path in info.rglob("*"):
        if legal(path):
            resolved = inside(path)
            assert resolved is not None
            paths.add(resolved)
    for member in members:
        path = Path(dist.locate_file(member))
        if legal(path):
            resolved = inside(path)
            assert resolved is not None
            paths.add(resolved)
    assert len(paths) <= 200
    files = {}
    for path in sorted(paths):
        relative = path.relative_to(SITE).as_posix()
        assert len(relative) < 500 and all(byte.isprintable() for byte in relative)
        assert 0 <= path.stat().st_size <= 4 * 1024 * 1024
        files[relative] = digest(path)
    return files


def known_upstream():
    source = CONTROL / "python_host_licenses"
    manifest = json.loads((source / "manifest.json").read_bytes())
    assert manifest["schema"] == 1
    result = {}
    for row in manifest["notices"]:
        key = (row["distribution"].lower(), row["version"])
        path = (source / row["file"]).resolve()
        assert path.is_relative_to(source) and path.is_file()
        assert digest(path) == row["sha256"]
        result.setdefault(key, []).append(row["sha256"])
    return result


def shim_record(dist, members):
    found = [member for member in members if str(member) == SHIM]
    assert len(found) == 1
    row = found[0]
    exact = SITE / SHIM
    assert dist.version == "2.14.1+cpu" and Path(dist.locate_file(row)).resolve() == exact
    assert exact.is_file() and not exact.is_symlink() and exact.stat().st_mode & 0o111
    assert exact.stat().st_size == 688016 and digest(exact) == SHIM_SHA
    with exact.open("rb") as stream:
        elf_header = stream.read(64)
    assert elf_header[:7] == b"\x7fELF\x02\x01\x01" and struct.unpack_from("<H", elf_header, 18)[0] == 183
    record_digest = urlsafe_b64encode(bytes.fromhex(SHIM_SHA)).decode().rstrip("=")
    hash_bound = row.hash is not None and row.hash.mode == "sha256" and row.hash.value == record_digest
    if row.hash is not None:
        assert hash_bound
    if row.size is not None:
        assert row.size == exact.stat().st_size
    old = CONTROL / "python_host_exclusions"
    oc = json.loads((old / "manifest.json").read_bytes())
    [amd] = oc["excluded"]
    primary = old / amd["primary_source_file"]
    primary_text = primary.read_text()
    assert digest(primary) == amd["primary_source_sha256"]
    assert "if(INSTALL_TEST)" in primary_text and "target_link_libraries(test_shim PRIVATE gtest_main torch)" in primary_text
    assert amd["path"] == SHIM and amd["version"] == dist.version
    assert amd["bytes"] == 630280 and amd["sha256"] != SHIM_SHA
    return {"path": SHIM, "version": dist.version, "bytes": exact.stat().st_size, "sha256": SHIM_SHA,
            "ELF64_AArch64": True, "wheel_record_member": True, "wheel_record_sha256_matches": hash_bound,
            "wheel_record_size_present": row.size is not None, "upstream_test_config_sha256": amd["primary_source_sha256"],
            "exact_AMD64_exclusion_sha256": amd["sha256"], "byte_match_with_AMD64": False,
            "isolated_ARM64_exclusion_candidate": True}


def main():
    assert (os.uname().machine, os.geteuid()) == ("aarch64", 65532)
    assert sys.version_info[:3] == (3, 13, 18) and sys.prefix == "/dispatcharrpy"
    upstream = known_upstream()
    rows = {}
    torch_notices = {}
    torch_shim = None
    for dist in metadata.distributions(path=[str(SITE)]):
        name = dist.metadata["Name"]
        version = dist.version
        assert isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_.+-]+", name)
        assert isinstance(version, str) and re.fullmatch(r"[A-Za-z0-9_.!+-]+", version)
        key = name.lower()
        assert key not in rows and len(rows) < 300
        members = list(dist.files or ())
        assert len(members) <= 120000
        observed = notices(dist, members)
        fallback = upstream.get((key, version), [])
        rows[key] = {"version": version, "installed_notice_count": len(observed),
                     "tracked_exact_version_notice_sha256": fallback,
                     "notice_inputs_present": bool(observed or fallback), "record_entries": len(members)}
        if key == "torch":
            torch_notices = observed
            torch_shim = shim_record(dist, members)
    assert torch_shim and len(torch_notices) == 90
    missing = sorted(name for name,row in rows.items() if not row["notice_inputs_present"])
    print(json.dumps({"scope": "native donor plugin dependency input and Torch notice reconciliation",
                      "uid": 65532, "native_platform": "linux/arm64", "python_version": "3.13.18",
                      "distributions": rows, "notices_missing": missing, "torch_notices": torch_notices,
                      "torch_shim": torch_shim, "third_party_modules_imported": False,
                      "plugin_python_host_built": False, "plugin_started": False, "distribution_qualified": False},
                     sort_keys=True))


if __name__ == "__main__":
    main()
