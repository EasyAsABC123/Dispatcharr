"""Verify the exact helper and dependency closure inside its own native ARM image."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess


OUTPUT = Path("/taskless-out")
PREFIX = Path("/opt/dispatcharr-pg-guarded-restore")
NOTICE = OUTPUT / "usr/share/doc/dispatcharr/pg-guarded-restore"
INTERPRETER = "/lib/ld-linux-aarch64.so.1"
BINARY = "dispatcharr-pg-guarded-restore"
BUILDER = "rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f"
TARBALL = "dd27f2b3c59e73ed14aa3324901242bf69a032a6347805f274e6260322d42979"
PATCH = "c84656a13ea27cb838d78297dddec3aa633627192803b641ea7eb026c4ccbd0d"
DIGEST = r"[0-9a-f]{64}"
ENV = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def output(*args):
    return subprocess.check_output(args, text=True, env=ENV, timeout=15)


def digest(path):
    with path.open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def runtime_environment():
    require(os.getuid() == os.geteuid() == 65532 and os.getgid() == os.getegid() == 65532,
            "same image runs as the intended non-root account")
    require(os.uname().machine == "aarch64" and output("dpkg", "--print-architecture").strip() == "arm64",
            "actual ARM64 container process")
    status = Path("/proc/self/status").read_text()
    require(re.search(r"(?m)^CapEff:\s*0+$", status) and re.search(r"(?m)^NoNewPrivs:\s*1$", status),
            "runtime has no effective capabilities and no new privileges")


def elf64(header):
    return (re.findall(r"(?m)^\s*Class:\s*([^\n]+)", header) == ["ELF64"]
            and re.findall(r"(?m)^\s*Machine:\s*([^\n]+)", header) == ["AArch64"])


def verify_dynamic_tags(raw, private_lib):
    identity = [line for line in raw.splitlines() if re.search(r"\((RPATH|RUNPATH)\)", line)]
    return len(identity) == 1 and re.search(
        r"\(RPATH\)\s+[^\[]+\[" + re.escape(str(private_lib)) + r"\]", identity[0]
    ) is not None


def loader_paths(raw):
    resolved = {}
    for line in raw.splitlines():
        require("not found" not in line, "all dependencies load")
        parts = line.split()
        if parts and parts[0] == "linux-vdso.so.1":
            continue
        if len(parts) == 4 and parts[1] == "=>":
            name, location = parts[0], Path(parts[2])
        elif len(parts) == 2 and parts[0].startswith("/"):
            location = Path(parts[0])
            name = location.name
        else:
            raise ValueError("unrecognized ARM64 loader line")
        require(name not in resolved and location.is_absolute(), "one exact loader path per SONAME")
        resolved[name] = location
    return resolved


def inspect():
    runtime_environment()
    source = OUTPUT / PREFIX.relative_to("/")
    require(source.is_dir() and not source.is_symlink() and not PREFIX.exists(), "fresh private execution path")
    shutil.copytree(source, PREFIX)
    tool = PREFIX / "bin" / BINARY
    private_lib = PREFIX / "lib"
    record = json.loads((NOTICE / "provenance.json").read_bytes())
    helper_hash = record["helper_sha256"]
    require(record["builder"] == BUILDER and record["target"] == "debian-bookworm-linux-arm64"
            and record["tarball_sha256"] == TARBALL and record["patch_sha256"] == PATCH
            and record["helper_path"] == str(tool) and re.fullmatch(DIGEST, helper_hash),
            "actual ARM builder provenance")
    require(digest(tool) == digest(source / "bin" / BINARY) == helper_hash
            and (OUTPUT / "taskless-helper.digest").read_text() == helper_hash + "\n",
            "actual staged and relocated helper digest")
    require(elf64(output("readelf", "-hW", str(tool)))
            and re.findall(r"\[Requesting program interpreter: ([^\]]+)\]",
                           output("readelf", "-lW", str(tool))) == [INTERPRETER]
            and verify_dynamic_tags(output("readelf", "-dW", str(tool)), private_lib)
            and record["dynamic_search_path"] == [["RPATH", str(private_lib)]],
            "native ELF, exact interpreter and transitive private DT_RPATH")
    version = output(str(tool), "--version").strip()
    require(version == record["tool_version"] == "pg_restore (PostgreSQL) 17.11",
            "actual non-root PG version")
    private, system = record["libraries"], record["glibc_system_dependencies"]
    require("libpq.so.5" in private and "libc.so.6" in system and Path(INTERPRETER).name in system
            and not private.keys() & system.keys(), "typed private and system dependency inventory")
    require({path.name for path in private_lib.iterdir()} == set(private), "all packaged libraries enumerated")
    resolved = loader_paths(output("ldd", str(tool)))
    require(set(resolved) == set(private) | set(system), "loader is exactly the complete provenance closure")
    for name, location in resolved.items():
        if name in private:
            require(location == private_lib / name and digest(source / "lib" / name) == private[name],
                    "private library loads from the final guarded prefix")
            expected = private[name]
        else:
            require(location.is_relative_to("/lib") or location.is_relative_to("/usr/lib"),
                    "system dependency loads from the base image")
            expected = system[name]
        require(digest(location) == expected and elf64(output("readelf", "-hW", str(location))),
                "loader dependency bytes and architecture match")
    owners, licenses = record["library_owners"], record["licenses"]
    require(set(owners) == set(private) - {"libpq.so.5"}
            and set(licenses) == {owner["package"].split(":", 1)[0] for owner in owners.values()},
            "all Debian private library owners have provenance notices")
    for package, expected in licenses.items():
        require(re.fullmatch(r"[a-z0-9+.-]+", package)
                and digest(NOTICE / (package + "-COPYRIGHT")) == expected, "exact retained Debian notice")
    require(digest(NOTICE / "POSTGRESQL-COPYRIGHT") == record["postgresql_copyright_sha256"],
            "exact PostgreSQL notice")
    return {"container_arch": "aarch64", "image_debian_architecture": "arm64", "uid": os.geteuid(),
            "gid": os.getegid(), "helper_sha256": helper_hash, "helper_version": version,
            "ELF_machine": "AArch64", "interpreter": INTERPRETER, "transitive_private_RPATH": str(private_lib),
            "actual_private_libraries": len(private), "actual_system_dependencies": len(system),
            "debian_and_postgresql_notices_verified": len(licenses) + 1,
            "exact_loader_closure_verified": True, "empty_LD_LIBRARY_PATH": True}


if __name__ == "__main__":
    print(json.dumps(inspect(), sort_keys=True))
