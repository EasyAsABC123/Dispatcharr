"""Read bounded Python dependency and GPG input metadata from the pinned donor."""

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import sys
import sysconfig

SITE = Path("/dispatcharrpy/lib/python3.13/site-packages")
EXPECTED_CERTIFI = "2026.10.23"
NAMES = (
    "regex", "packaging", "Django", "psycopg", "celery", "kombu", "redis",
    "django-redis", "channels", "channels-redis", "djangorestframework",
    "certifi", "torch", "tokenizers", "tqdm",
)
LEGAL = ("LICENSE", "LICENCE", "COPYING", "COPYRIGHT", "NOTICE")


def file_sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def file_record(path):
    if not path.is_file():
        return {"present": False}
    size = path.stat().st_size
    assert 0 < size < 128 * 1024 * 1024
    return {"present": True, "bytes": size, "sha256": file_sha(path), "symlink": path.is_symlink()}


def run(*argv, allowed=(0,)):
    result = subprocess.run(argv, check=False, capture_output=True, text=True, timeout=12,
                            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
    assert result.returncode in allowed and len(result.stdout) < 65536 and len(result.stderr) < 2000
    return result.stdout


def package_notices(path):
    for query in dict.fromkeys([str(path.resolve()), str(path)]):
        rows = run("/usr/bin/dpkg-query", "--search", query, allowed=(0, 1)).splitlines()
        owners = set()
        for row in rows:
            owner, sep, name = row.rpartition(": ")
            if sep and name == query:
                owners.update(owner.split(", "))
        if owners:
            break
    result = {}
    for owner in sorted(owners):
        assert re.fullmatch(r"[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?", owner)
        package = owner.partition(":")[0]
        version = run("/usr/bin/dpkg-query", "-W", "-f=${Version}", owner).strip()
        assert re.fullmatch(r"[A-Za-z0-9+.:~_-]{1,100}", version)
        result[owner] = {"version": version, "notice": file_record(Path("/usr/share/doc") / package / "copyright")}
    return result


def elf(path):
    with path.open("rb") as stream:
        header = stream.read(64)
    assert len(header) == 64 and header[:7] == b"\x7fELF\x02\x01\x01"
    assert struct.unpack_from("<H", header, 18)[0] == 183
    output = run("/usr/bin/ldd", str(path))
    missing = set()
    linked = {}
    for line in output.splitlines():
        line = line.strip()
        if line.endswith("=> not found"):
            missing.add(line.partition("=>")[0].strip())
            continue
        for token in line.split():
            if token.startswith("/") and Path(token).is_file():
                link = Path(token)
                assert re.fullmatch(r"[A-Za-z0-9_.+/-]{1,240}", token)
                linked[token] = {"sha256": file_sha(link), "owners": package_notices(link)}
    return {"sha256": file_sha(path), "elf64_little_endian_AArch64": True,
            "libraries": linked, "unresolved": sorted(missing), "owners": package_notices(path)}


def distribution(name):
    try:
        dist = importlib.metadata.distribution(name)
    except importlib.metadata.PackageNotFoundError:
        return {"present": False}
    root = Path(dist._path)
    assert root.resolve().is_relative_to(SITE.resolve())
    assert re.fullmatch(r"[a-zA-Z0-9.!+_-]{1,110}", dist.version)
    notice_paths = {file for file in root.rglob("*") if legal(file)}
    regex_extensions = []
    for member in dist.files or ():
        path = Path(dist.locate_file(member))
        if not path.resolve().is_relative_to(SITE.resolve()) or not path.is_file():
            continue
        if legal(path):
            notice_paths.add(path)
        if name == "regex" and path.suffix == ".so":
            regex_extensions.append(path)
    assert len(notice_paths) <= 64 and len(regex_extensions) <= 4
    result = {"present": True, "version": dist.version,
              "notices": {path.relative_to(SITE).as_posix(): file_sha(path) for path in sorted(notice_paths)}}
    if regex_extensions:
        result["extensions"] = {path.relative_to(SITE).as_posix(): elf(path) for path in sorted(regex_extensions)}
    return result


def legal(path):
    return path.is_file() and any(word in path.name.upper() for word in LEGAL)


def main():
    global PHASE
    PHASE = "python-identity"
    assert os.geteuid() == 65532 and os.uname().machine == "aarch64"
    assert sys.version_info[:2] == (3, 13) and sys.prefix == "/dispatcharrpy" and sys.base_prefix == "/usr"
    assert Path(sysconfig.get_path("purelib")) == SITE
    PHASE = "python-distributions"
    names = {name: distribution(name) for name in NAMES}
    shim = file_record(SITE / "torch/bin/test_shim")
    certifi = names["certifi"]
    PHASE = "python-executable"
    interpreter = elf(Path(sys.executable).resolve())
    PHASE = "gpg-executable"
    gpg = elf(Path("/usr/bin/gpg"))
    PHASE = "collector-projection"
    result = {
        "scope": "read-only donor input inventory",
        "native_platform": "linux/arm64", "uid": 65532,
        "python": {"version": list(sys.version_info[:3]), "soabi": sysconfig.get_config_var("SOABI"),
                   "interpreter": interpreter, "distributions": names},
        "crypto": {"gpg": gpg, "gpg_regular_file": not Path("/usr/bin/gpg").is_symlink(),
                   "trust": file_record(SITE / "certifi/cacert.pem"),
                   "expected_certifi_version": EXPECTED_CERTIFI,
                   "certifi_version_matches": certifi.get("version") == EXPECTED_CERTIFI,
                   "explicit_certifi_notice": file_record(
                       SITE / f"certifi-{EXPECTED_CERTIFI}.dist-info/licenses/LICENSE")},
        "torch_test_shim": {"donor_input": shim,
                            "matches_current_exclusion_bytes": shim.get("sha256") ==
                            "d99a3f78fb4b253b21efbd6818337a5ef1cac47768e54369f44d4c19b102fbf4",
                            "matches_current_exclusion_version": names["torch"].get("version") == "2.14.1+cpu"},
        "third_party_modules_imported": False, "source_application_started": False,
        "crypto_commands_executed": False,
        "relocated_runtime_verified": False, "distribution_qualified": False,
    }
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, subprocess.SubprocessError, ValueError):
        print(f"probe-failure={PHASE}", file=sys.stderr)
        sys.exit(1)
