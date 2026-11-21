"""Verify the owned regex-only closure after relocation into Debian Bookworm."""

from hashlib import file_digest
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

PREFIX = Path("/opt/dispatcharr-python")
DOCS = Path("/usr/share/doc/dispatcharr")
LOADER = PREFIX / "elf/ld-linux-aarch64.so.1"
EXE = PREFIX / "bin/python3.13"
REGEX = PREFIX / "lib/python3.13/dist-packages/regex/_regex.cpython-313-aarch64-linux-gnu.so"
MANIFEST = DOCS / "regex-host/manifest.json"
EXPECTED = {
    EXE: "b36bf122396f0485ebe506b475ce785bd880fc89807f1ced3076b0e6f55bee71",
    REGEX: "04bba78ca6a164e2a269359c34e63e1f6f055a6ec1ac82fa6104de25a8253310",
}
LIB = PREFIX / "elf"
BRIDGE = "/usr/local/bin/dispatcharr-regex-python"


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def native_elf(path):
    with path.open("rb") as stream:
        header = stream.read(64)
    assert header[:7] == b"\x7fELF\x02\x01\x01" and struct.unpack_from("<H", header, 18)[0] == 183


def pack(value):
    raw = json.dumps(value, separators=(",", ":")).encode()
    return struct.pack("!I", len(raw)) + raw


def unpack(raw):
    values = []
    while raw:
        assert len(raw) >= 4
        size = struct.unpack_from("!I", raw)[0]
        assert 0 < size < 16384 and len(raw) >= size + 4
        values.append(json.loads(raw[4:size + 4]))
        raw = raw[size + 4:]
    return values


def private_links(path):
    child = subprocess.run([str(LOADER), "--inhibit-cache", "--library-path", str(LIB), "--list", str(path)],
                           capture_output=True, text=True, check=True, timeout=10)
    assert not child.stderr and "not found" not in child.stdout
    known = set()
    for line in child.stdout.splitlines():
        parts = line.split()
        if not parts or parts[0] == "linux-vdso.so.1":
            continue
        if len(parts) == 2:
            assert parts[0] in (str(LOADER), f"/lib/{LOADER.name}", LOADER.name)
            known.add(LOADER)
            continue
        assert len(parts) >= 4 and parts[1] == "=>"
        target = Path(parts[2]).resolve()
        assert target.is_relative_to(LIB) and target.is_file()
        known.add(target)
    assert len(known) >= 2
    return len(known)


def main():
    global PHASE
    PHASE = "runtime-identity"
    assert os.geteuid() == 65532 and os.uname().machine == "aarch64"
    assert tuple(sys.version_info[:3]) == (3, 13, 18) and Path(sys.prefix) == PREFIX
    assert Path(sys.executable).resolve() == EXE
    assert all(Path(path).is_relative_to(PREFIX) for path in sys.path if path)
    for absent in ("django", "torch"):
        assert importlib.util.find_spec(absent) is None
    assert not (PREFIX / "application").exists()
    assert (DOCS / "LICENSE").is_file() and (DOCS / "ATTRIBUTION.md").is_file()
    for path in (LOADER, EXE, REGEX):
        native_elf(path)
    assert all(digest(path) == expected for path, expected in EXPECTED.items())
    PHASE = "manifest-and-notices"
    manifest = json.loads(MANIFEST.read_bytes())
    assert manifest["mode"] == "regex-only" and not manifest["application_started"]
    assert manifest["source_application_assets_copied"] is False
    assert manifest["external_key_homes_or_runtime_data_copied"] is False
    for name, version in (("regex", "2026.9.29"), ("packaging", "26.3")):
        assert importlib.metadata.version(name) == version
    for record in manifest["files"]:
        path = Path(record["path"])
        assert path.is_relative_to(PREFIX) and path.stat().st_size == record["bytes"]
        assert digest(path) == record["sha256"]
    notices = {}
    for path in sorted((DOCS / "regex-host").rglob("*")):
        if path.is_file() and path != MANIFEST:
            notices[str(path.relative_to(DOCS))] = digest(path)
    assert len(notices) >= 4
    PHASE = "module-origins"
    import packaging.version
    import regex._regex
    import unicodedata
    assert Path(regex._regex.__file__).resolve() == REGEX
    assert Path(packaging.version.__file__).resolve().is_relative_to(PREFIX)
    assert unicodedata.unidata_version == manifest["source_unicode_version"]
    PHASE = "ordinary-frames"
    messages = [
        {"kind": "call", "id": 1, "method": "regex", "args": {
            "kind": "named_groups", "names": ["News 1080", "Weather"],
            "config": {"pattern": "^(?<label>News) (?<width>[0-9]+)$"}}},
        {"kind": "call", "id": 2, "method": "regex", "args": {
            "kind": "version_compare", "current": "9.0", "latest": "1!0.1"}},
        {"kind": "call", "id": 3, "method": "shutdown", "args": {}},
    ]
    child = subprocess.run([BRIDGE, "--mode", "regex"], input=b"".join(map(pack, messages)),
                           capture_output=True, timeout=24, check=True)
    assert not child.stderr and len(child.stdout) < 16384
    ready, capture, version, shutdown = unpack(child.stdout)
    assert ready["kind"] == "ready" and ready["dependency_only"] and not ready["application_server_started"]
    assert ready["regex_version"] == "2026.9.29" and ready["version"].startswith("3.13.18 ")
    expected = [
        {"compiled": True, "caught": None, "rows": [
            {"matched": True, "groups": [["label", "News"], ["width", "1080"]], "caught": None},
            {"matched": False, "groups": None, "caught": None}]},
        {"kind": "parsed", "newer": True, "ordering": 1}, None,
    ]
    for idx, (row, value) in enumerate(zip((capture, version, shutdown), expected, strict=True), start=1):
        assert row == {"id": idx, "kind": "result", "error": None, "result": value}
    PHASE = "private-loader"
    print(json.dumps({
        "scope": "standalone Debian ARM64 dependency-only regex bridge",
        "python": "3.13.18", "regex": "2026.9.29", "packaging": "26.3", "uid": 65532,
        "verified_closure_files": len(manifest["files"]), "verified_notice_count": len(notices),
        "manifest_sha256": digest(MANIFEST), "loader_sha256": digest(LOADER),
        "python_sha256": digest(EXE), "regex_extension_sha256": digest(REGEX),
        "private_python_links": private_links(EXE), "private_regex_links": private_links(REGEX),
        "unicode_version": unicodedata.unidata_version,
        "ordinary_framed_named_groups": True, "ordinary_framed_version_comparison": True,
        "bridge_child_exit": child.returncode, "source_application_started": False,
        "full_plugin_host_verified": False, "distribution_qualified": False,
    }, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, ValueError, subprocess.SubprocessError, ImportError):
        print(f"probe-failure={PHASE}", file=sys.stderr)
        sys.exit(1)
