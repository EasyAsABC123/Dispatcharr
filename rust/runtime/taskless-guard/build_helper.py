#!/usr/bin/env python3
"""Build the exact guarded helper in the pinned Debian Bookworm AMD64 builder."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile

TARBALL_SHA256 = "dd27f2b3c59e73ed14aa3324901242bf69a032a6347805f274e6260322d42979"
PATCH_SHA256 = "c84656a13ea27cb838d78297dddec3aa633627192803b641ea7eb026c4ccbd0d"
PREFIX = Path("/opt/dispatcharr-pg-guarded-restore")
NOTICE = Path("/usr/share/doc/dispatcharr/pg-guarded-restore")
BUILDER = "rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f"
SYSTEM = frozenset(("libc.so.6", "libm.so.6", "libpthread.so.0", "libdl.so.2",
                    "librt.so.1", "libresolv.so.2", "ld-linux-x86-64.so.2"))


def sha(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def run(*args, cwd=None, env=None, timeout=900):
    return subprocess.check_output(args, cwd=cwd, env=env, text=True, timeout=timeout)


def exact_inputs(tarball, checksum, patch):
    assert checksum.read_text().split() == [TARBALL_SHA256, "postgresql-17.11.tar.bz2"]
    assert sha(tarball) == TARBALL_SHA256 and sha(patch) == PATCH_SHA256
    with tarfile.open(tarball, "r:bz2") as archive:
        return archive.extractfile("postgresql-17.11/COPYRIGHT").read()


def copy_artifact(source, destination, mode):
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    shutil.copyfile(source, destination)
    destination.chmod(mode)


def owner_notice(library):
    """Bind installed Debian package metadata to the actual resolved file."""
    resolved = str(library.resolve(strict=True))
    paths = {str(library), resolved}
    for path in tuple(paths):
        if path.startswith("/usr/lib/"):
            paths.add(path[4:])
        elif path.startswith("/lib/"):
            paths.add("/usr" + path)
    for candidate in sorted(paths):
        if not Path(candidate).exists() or not library.samefile(candidate):
            continue
        result = subprocess.run(
            ["dpkg-query", "-S", candidate], check=False, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            continue
        owners = []
        for line in result.stdout.splitlines():
            owner, separator, path = line.partition(": ")
            if separator and path == candidate and re.fullmatch(r"[a-z0-9+.-]+(?::amd64)?", owner):
                owners.append(owner)
        if len(owners) != 1:
            raise ValueError(f"expected one Debian package owner for {library.name}")
        owner = owners[0]
        installed = run("dpkg-query", "-W", "-f=${db:Status-Status}\t${Architecture}\t${Version}", owner,
                        timeout=10).split("\t")
        assert len(installed) == 3 and installed[0] == "installed" and installed[1] == "amd64"
        assert re.fullmatch(r"[a-zA-Z0-9+.:~_-]+", installed[2])
        notice = Path("/usr/share/doc") / owner.partition(":")[0] / "copyright"
        assert notice.is_file()
        return owner, installed[2], candidate, notice
    raise ValueError(f"no installed Debian package and copyright for {library.name}")


def verify_linker_path(elf):
    dynamic = run("readelf", "-dW", str(elf))
    tags = []
    for line in dynamic.splitlines():
        if re.search(r"\((?:RPATH|RUNPATH)\)", line):
            match = re.search(r"\((RPATH|RUNPATH)\)\s+[^[]+\[([^]]*)\]", line)
            if not match:
                raise ValueError("unrecognized dynamic linker search-path tag")
            tags.append(list(match.groups()))
    print(json.dumps({"elf": elf.name, "dynamic_search_path": tags}, sort_keys=True), flush=True)
    if tags != [["RPATH", str(PREFIX / "lib")]]:
        raise ValueError("guarded helper requires exactly one transitive private DT_RPATH")
    return tags


def unpack_verified_release(tarball, destination):
    """Extract the pinned release's bounded directory and regular-file members."""
    from pathlib import PurePosixPath

    seen = set()
    total = 0
    with tarfile.open(tarball, "r:bz2") as archive:
        for member in archive:
            relative = PurePosixPath(member.name)
            if (
                not relative.parts or relative.parts[0] != "postgresql-17.11"
                or relative.is_absolute() or relative.as_posix() != member.name
                or ".." in relative.parts or member.name in seen
                or not (member.isdir() or member.isreg()) or len(seen) >= 10000
            ):
                raise ValueError("unexpected member in the pinned PostgreSQL source")
            total += member.size
            if total > 256 * 1024**2:
                raise ValueError("pinned PostgreSQL extraction exceeds expected size")
            seen.add(member.name)
            path = destination.joinpath(*relative.parts)
            if member.isdir():
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as body, path.open("xb") as output:
                    shutil.copyfileobj(body, output)
                path.chmod(0o755 if member.mode & 0o111 else 0o644)


def build(args, copyright_bytes):
    assert platform.machine() in {"x86_64", "amd64"}
    assert 'VERSION_CODENAME=bookworm' in Path("/etc/os-release").read_text().splitlines()
    args.output.mkdir(mode=0o755)
    out_prefix = args.output / PREFIX.relative_to("/")
    out_notice = args.output / NOTICE.relative_to("/")
    out_notice.mkdir(parents=True, mode=0o755)
    with tempfile.TemporaryDirectory(prefix="dispatcharr-pg-helper-build-") as scratch:
        work = Path(scratch)
        unpack_verified_release(args.tarball, work)
        src = work / "postgresql-17.11"
        run("patch", "--batch", "--forward", "--fuzz=0", "-p1", "-i", args.patch, cwd=src)
        obj = work / "build"
        obj.mkdir()
        env = {key: value for key, value in os.environ.items() if key in {"PATH", "LANG", "LC_ALL"}}
        env["LDFLAGS"] = f"-Wl,--disable-new-dtags -Wl,-rpath,{PREFIX}/lib"
        run(
            str(src / "configure"), f"--prefix={PREFIX}", f"--libdir={PREFIX}/lib",
            "--disable-rpath", "--without-icu", "--without-readline", "--with-openssl", "--with-lz4", "--with-zstd",
            cwd=obj, env=env,
        )
        settings = [f"-j{args.jobs}"]
        with (args.output / "pg-helper-build.log").open("w") as log:
            subprocess.run(["make", *settings], cwd=obj, env=env, check=True,
                           timeout=1200, stdout=log, stderr=subprocess.STDOUT)
            subprocess.run(["make", *settings, "install", f"DESTDIR={work / 'install'}"],
                           cwd=obj, env=env, check=True, timeout=300,
                           stdout=log, stderr=subprocess.STDOUT)
        staged = work / "install" / PREFIX.relative_to("/")
        elf = staged / "bin/pg_restore"
        built_version = run(str(elf), "--version",
                            env={**env, "LD_LIBRARY_PATH": str(staged / "lib")}).strip()
        assert built_version == "pg_restore (PostgreSQL) 17.11"
        final = out_prefix / "bin/dispatcharr-pg-guarded-restore"
        copy_artifact(elf, final, 0o555)
        linker_paths = verify_linker_path(final)
        copied = {}
        glibc = {}
        licenses = {}
        library_owners = {}
        linked = run("ldd", str(elf), env={**env, "LD_LIBRARY_PATH": str(staged / "lib")})
        assert "not found" not in linked
        for line in linked.splitlines():
            fields = line.split()
            if len(fields) >= 4 and fields[1] == "=>":
                soname, source = fields[0], Path(fields[2])
            elif fields and fields[0].startswith("/"):
                source = Path(fields[0])
                soname = source.name
            elif fields and fields[0].startswith("linux-vdso"):
                continue
            else:
                raise ValueError("unrecognized helper link record")
            if soname in SYSTEM:
                glibc[soname] = sha(source)
                continue
            assert re.fullmatch(r"lib[A-Za-z0-9+._-]+\.so(?:\.[0-9]+)*", soname)
            copied[soname] = sha(source)
            copy_artifact(source, out_prefix / "lib" / soname, 0o444)
            if soname.startswith("libpq.so."):
                assert source.resolve().is_relative_to(staged / "lib")
                continue
            owner, version, package_path, notice = owner_notice(source)
            package = owner.partition(":")[0]
            target = out_notice / (package + "-COPYRIGHT")
            copy_artifact(notice, target, 0o444)
            licenses[package] = sha(target)
            library_owners[soname] = {"package": owner, "version": version, "path": package_path}
        assert "libpq.so.5" in copied
        assert "libc.so.6" in glibc
        (out_notice / "POSTGRESQL-COPYRIGHT").write_bytes(copyright_bytes)
        (out_notice / "POSTGRESQL-COPYRIGHT").chmod(0o444)
        report = {
            "builder": BUILDER, "target": "debian-bookworm-linux-amd64", "tool_version": built_version,
            "tarball_sha256": TARBALL_SHA256, "patch_sha256": PATCH_SHA256,
            "helper_path": str(PREFIX / "bin" / final.name), "helper_sha256": sha(final),
            "libraries": copied, "glibc_system_dependencies": glibc, "licenses": licenses,
            "dynamic_search_path": linker_paths, "library_owners": library_owners,
            "postgresql_copyright_sha256": sha(out_notice / "POSTGRESQL-COPYRIGHT"),
        }
        (out_notice / "provenance.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        (args.output / "taskless-helper.digest").write_text(report["helper_sha256"] + "\n")
        print(json.dumps(report, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    for name in ("tarball", "checksum", "patch"):
        parser.add_argument("--" + name, required=True, type=lambda value: Path(value).resolve())
    parser.add_argument("--output", type=lambda value: Path(value).resolve())
    parser.add_argument("--jobs", type=int, choices=range(1, 9), default=6)
    parser.add_argument("--check-inputs", action="store_true")
    args = parser.parse_args()
    notice = exact_inputs(args.tarball, args.checksum, args.patch)
    if args.check_inputs:
        print(json.dumps({"release_and_patch_exact": True, "upstream_license_present": bool(notice)}))
    else:
        assert args.output and not args.output.exists()
        build(args, notice)


if __name__ == "__main__":
    main()
