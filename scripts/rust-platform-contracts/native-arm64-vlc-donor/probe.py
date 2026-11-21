"""Read public VLC and owning-package bytes inside the pinned Original ARM64 donor."""
from argparse import ArgumentParser
from collections import defaultdict
from hashlib import file_digest
import json
import os
from pathlib import Path
import platform
import re
import shlex
import struct
import subprocess


OCI_MANIFEST = "sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace"
OCI_CONFIG = "sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43"
PLUGINS = Path("/usr/lib/aarch64-linux-gnu/vlc/plugins")
DOCS = Path("/usr/share/doc")
INTERPRETER = "/lib/ld-linux-aarch64.so.1"
CREDIT = "Original Dispatcharr, VideoLAN, Debian/Ubuntu and dependency authors and contributors retain full credit."
CUSTOM = {
    "libva": ("intel-media-licenses/libva/COPYING", r"libva(?:-drm|-x11)?\.so(?:\.[0-9]+)*"),
    "libvpl": ("intel-media-licenses/libvpl/LICENSE", r"libvpl\.so(?:\.[0-9]+)*"),
    "vpl-gpu-rt": ("intel-media-licenses/vpl-gpu-rt/LICENSE", r"libmfx-gen\.so(?:\.[0-9]+)*"),
}


def digest(path):
    assert 0 < path.stat().st_size <= 256 * 1024 * 1024
    with path.open("rb") as content:
        return file_digest(content, "sha256").hexdigest()


def elf(path):
    with path.open("rb") as stream:
        head = stream.read(64)
        if head[:4] != b"\x7fELF":
            return None
        assert len(head) == 64 and head[4:7] == b"\x02\x01\x01"
        machine = struct.unpack_from("<H", head, 18)[0]
        assert machine == 183, "ARM64 ELF roster"
        offset = struct.unpack_from("<Q", head, 32)[0]
        entry_size, count = struct.unpack_from("<HH", head, 54)
        assert entry_size == 56 and 0 <= count <= 128
        assert offset + entry_size * count <= path.stat().st_size
        interpreter = None
        for index in range(count):
            stream.seek(offset + entry_size * index)
            kind, _, begin, _, _, length, _, _ = struct.unpack("<IIQQQQQQ", stream.read(entry_size))
            if kind == 3:
                assert interpreter is None and 2 <= length <= 256 and begin + length <= path.stat().st_size
                stream.seek(begin)
                name = stream.read(length)
                assert name.endswith(b"\x00") and b"\x00" not in name[:-1]
                interpreter = name[:-1].decode("ascii")
        return {"ELF_machine": machine, "interpreter": interpreter}


def wrapper(path):
    with path.open() as stream:
        text = stream.read(4097)
    assert len(text) <= 4096
    [line] = [row.strip() for row in text.splitlines() if row.strip() and not row.lstrip().startswith("#")]
    words = shlex.split(line)
    assert words[:2] == ["exec", "/usr/bin/vlc"] and words[-1] == "$@"
    arguments = words[2:-1]
    assert all(not any(char in arg for char in '$`;|&<>\n\r') for arg in arguments)
    return arguments


def dpkg_lookups(paths):
    wanted, aliases, packages = set(), defaultdict(set), defaultdict(set)
    for path in paths:
        for synonym in {str(path), str(path.resolve())}:
            wanted.add(synonym)
            aliases[synonym].add(path)
            paired = synonym[4:] if synonym.startswith("/usr/lib/") else "/usr" + synonym if synonym.startswith("/lib/") else None
            if paired:
                wanted.add(paired)
                aliases[paired].add(path)
    ordered = sorted(wanted)
    for index in range(0, len(ordered), 100):
        query = ordered[index:index+100]
        result = subprocess.run(["dpkg-query", "--search", *query], capture_output=True, text=True, timeout=10)
        assert result.returncode in {0, 1} and len(result.stdout) <= 96 * 1024
        for line in result.stdout.splitlines():
            owners, mark, filename = line.rpartition(": ")
            if mark and filename in aliases:
                for owner in owners.split(", "):
                    assert re.fullmatch(r"[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?", owner)
                    for path in aliases[filename]:
                        packages[path].add(owner)
    return packages


def notice_inventory(paths):
    dpkg = dpkg_lookups(paths)
    origin, notices, missing = {}, {}, []
    for path in sorted(paths):
        if dpkg[path]:
            owners = sorted(dpkg[path])
            origin[str(path)] = owners
            for owner in owners:
                package = owner.split(":", 1)[0]
                notice = DOCS / package / "copyright"
                if owner not in notices and notice.is_file() and notice.resolve().is_relative_to(DOCS):
                    notices[owner] = {"path": str(notice), "sha256": digest(notice)}
        else:
            matches = [name for name, (_, pattern) in CUSTOM.items()
                       if path.resolve().is_relative_to("/usr/local/lib") and re.fullmatch(pattern, path.resolve().name)]
            if matches:
                [component] = matches
                owner = "Original-custom-" + component
                origin[str(path)] = [owner]
                notice = Path("/usr/local/share/doc/dispatcharr-ffmpeg") / CUSTOM[component][0]
                if notice.is_file() and notice.resolve().is_relative_to("/usr/local/share/doc/dispatcharr-ffmpeg"):
                    notices[owner] = {"path": str(notice), "sha256": digest(notice)}
            else:
                origin[str(path)] = []
        if not origin[str(path)] or any(owner not in notices for owner in origin[str(path)]):
            missing.append(str(path))
    return origin, notices, missing


def dependencies(path):
    result = subprocess.run(["ldd", str(path)], capture_output=True, text=True, timeout=5)
    assert len(result.stdout) + len(result.stderr) <= 64 * 1024
    resolved = set()
    absent = set()
    for row in result.stdout.splitlines():
        if "=> not found" in row:
            name = row.split("=>", 1)[0].strip()
            assert re.fullmatch(r"[A-Za-z0-9+_.-]+", name)
            absent.add(name)
        for token in row.split():
            if token.startswith("/"):
                item = Path(token)
                assert any(item.is_relative_to(root) for root in ("/lib", "/usr/lib", "/usr/local/lib"))
                assert item.is_file() and elf(item) is not None
                resolved.add(item)
    issue = {"exit": result.returncode, "unresolved_SONAMEs": sorted(absent), "resolved_count": len(resolved)}
    return resolved, issue if result.returncode or absent or not resolved else None


def inventory():
    assert platform.system() == "Linux" and platform.machine() == "aarch64"
    assert os.getuid() == os.getgid() == 65532
    assert subprocess.check_output(["dpkg", "--print-architecture"], timeout=5).strip() == b"arm64"
    command, cvlc = Path("/usr/bin/vlc"), Path("/usr/bin/cvlc")
    required = elf(command)
    assert required == {"ELF_machine": 183, "interpreter": INTERPRETER}
    assert PLUGINS.is_dir() and PLUGINS.resolve() == PLUGINS
    plugin_files = [path for path in sorted(PLUGINS.rglob("*")) if path.is_file()]
    assert 1 <= len(plugin_files) <= 1024
    assert all(path.resolve().is_relative_to(PLUGINS) for path in plugin_files)
    metadata = {}
    for file in plugin_files:
        metadata[str(file)] = {"sha256": digest(file)}
        header = elf(file)
        if header:
            metadata[str(file)].update(header)
        if file.name == "plugins.dat":
            metadata[str(file)]["generated_plugin_cache"] = True
    roots = [command, *(Path(path) for path, item in metadata.items() if "ELF_machine" in item)]
    assert len(roots) > 1 and len({str(path) for path in roots}) == len(roots)
    links, libraries, unresolved = {}, set(), {}
    for path in roots:
        libs, gap = dependencies(path)
        links[str(path)] = libs
        libraries.update(libs)
        if gap:
            unresolved[str(path)] = gap
    nss = {path for path in Path("/lib/aarch64-linux-gnu").glob("libnss_*.so*") if path.is_file()}
    assert all(elf(path) is not None for path in nss)
    libraries.update(nss)
    library_paths = sorted(libraries)
    library_index = {path: index for index, path in enumerate(library_paths)}
    public_share = Path("/usr/share/vlc")
    shared_data = [path for path in sorted(public_share.rglob("*")) if path.is_file()]
    assert len(shared_data) <= 256 and all(path.resolve().is_relative_to(public_share) for path in shared_data)
    # Generated caches have no intake requirement because the collector omits them.
    paths = {cvlc, command, *[path for path in plugin_files if path.name != "plugins.dat"], *libraries, *shared_data}
    assert len(paths) <= 1536
    owners, notices, missing = notice_inventory(paths)
    for path, row in metadata.items():
        row["notice_origins"] = owners.get(path, [])
    return {
        "native_platform": "linux/arm64", "plugin_directory": str(PLUGINS),
        "vlc": {**required, "sha256": digest(command), "notice_origins": owners[str(command)]},
        "cvlc": {"sha256": digest(cvlc), "literal_prefix_arguments": wrapper(cvlc), "notice_origins": owners[str(cvlc)]},
        "plugins": metadata,
        "public_vlc_data": {str(path): {"sha256": digest(path), "notice_origins": owners[str(path)]}
                            for path in shared_data},
        "resolved_libraries": {str(path): {"sha256": digest(path), "notice_origins": owners[str(path)],
                                                   "default_nss_companion": path in nss} for path in library_paths},
        "loader_library_index_zero_based": list(map(str, library_paths)),
        "loader_dependencies_by_ELF": {path: sorted(library_index[item] for item in linked) for path, linked in links.items()},
        "loader_inventory_gaps": unresolved,
        "notice_files": notices, "missing_notice_origins": missing,
        "loader_inventory_complete": not unresolved, "license_inventory_complete": not missing,
        "vlc_playback_executed": False, "candidate_packaging_qualified": False, "credit": CREDIT,
    }


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--verified-oci-manifest", required=True)
    parser.add_argument("--verified-oci-config", required=True)
    args = parser.parse_args()
    assert (args.verified_oci_manifest, args.verified_oci_config) == (OCI_MANIFEST, OCI_CONFIG)
    result = inventory()
    result["oci_identity_from_qualified_launcher"] = {"manifest": OCI_MANIFEST, "config": OCI_CONFIG}
    body = json.dumps(result, sort_keys=True, separators=(",", ":"))
    assert len(body.encode()) <= 1 << 20
    print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
