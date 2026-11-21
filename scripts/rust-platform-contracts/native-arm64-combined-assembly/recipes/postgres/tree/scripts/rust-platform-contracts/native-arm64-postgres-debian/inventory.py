"""Read-only native ARM64 inventory for Dispatcharr's PostgreSQL client collector."""
from hashlib import file_digest
import json
import os
from pathlib import Path
import platform
import re
import struct
import subprocess
import sys


MANIFEST = "sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace"
CONFIG = "sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43"
CLIENTS = ("pg_dump", "pg_restore", "psql")
BINARY = Path("/usr/lib/postgresql/17/bin")
TRIPLET = "aarch64-linux-gnu"
INTERPRETER = "/lib/ld-linux-aarch64.so.1"
SHARE = Path("/usr/share/postgresql/17")
MAX_LIBRARIES = 128
MAX_SUPPORT_FILES = 2048
HEX = re.compile(r"[a-f0-9]{64}")


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def sha(path):
    need(0 < path.stat().st_size <= 64 * 1024**2, "bounded nonempty owned file")
    with path.open("rb") as data:
        return file_digest(data, "sha256").hexdigest()


def command(argv, default=None, maximum=65536):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=12, check=False)
    need(len(result.stdout) <= maximum, "bounded inventory helper output")
    if result.returncode == 1 and default is not None:
        return default
    need(result.returncode == 0, "installed tool inventory command failed")
    return result.stdout


def elf(path, require_interpreter=False):
    with path.open("rb") as stream:
        header = stream.read(64)
        need(len(header) == 64 and header[:6] == b"\x7fELF\x02\x01" and
             struct.unpack_from("<H", header, 18)[0] == 183, "native AArch64 ELF required")
        if not require_interpreter:
            return
        offset = struct.unpack_from("<Q", header, 32)[0]
        size, count = struct.unpack_from("<HH", header, 54)
        need(size == 56 and 0 < count <= 128 and offset <= 65536, "bounded ELF program header table")
        stream.seek(offset)
        table = stream.read(size * count)
        need(len(table) == size * count, "complete ELF program header table")
        segments = [table[i:i + size] for i in range(0, len(table), size)]
        [interp] = [segment for segment in segments if struct.unpack_from("<I", segment)[0] == 3]
        file_offset, _, _, filesz = struct.unpack_from("<QQQQ", interp, 8)
        need(file_offset < 65536 and filesz == len(INTERPRETER) + 1, "exact AArch64 interpreter size")
        stream.seek(file_offset)
        need(stream.read(filesz) == INTERPRETER.encode() + b"\0", "exact AArch64 interpreter bytes")


class Inventory:
    def __init__(self):
        self.packages = {}
        self.libraries = {}
        self.visited = set()

    def owners(self, path):
        source = [str(path.resolve(strict=True)), str(path)]
        options = list(dict.fromkeys(source + ["/usr" + item if item.startswith("/lib/") else item[4:]
                                             for item in source if item.startswith(("/lib/", "/usr/lib/"))]))
        for key in options:
            text = command(["dpkg-query", "--search", key], default="")
            matches = {name for line in text.splitlines() for owner, _, match in (line.rpartition(": "),)
                       if match == key for name in owner.split(", ")}
            if matches:
                return sorted(self.package(name) for name in matches)
        raise ValueError("collector input has no installed Debian package owner")

    def package(self, owner):
        need(re.fullmatch(r"[a-z0-9][a-z0-9+.-]*(?::(?:arm64|all))?", owner), "expected ARM64 Debian owner")
        package = owner.split(":", 1)[0]
        if owner not in self.packages:
            raw = command(["dpkg-query", "-W", "-f=${Architecture}\t${Version}\t${source:Package}\t${source:Version}", owner])
            arch, version, source, source_version = raw.strip().split("\t")
            need(arch in {"arm64", "all"} and bool(version and source and source_version), "complete installed package metadata")
            notice = Path("/usr/share/doc") / package / "copyright"
            self.packages[owner] = {"architecture": arch, "version": version, "source_package": source,
                                    "source_version": source_version, "copyright_sha256": sha(notice)}
        return owner

    def library(self, path):
        previous = self.libraries.get(path.name)
        value = {"sha256": sha(path), "owners": self.owners(path)}
        need(previous is None or previous == value, "conflicting library basename or owner")
        if previous is None:
            need(len(self.libraries) < MAX_LIBRARIES, "bounded selected shared library set")
            self.libraries[path.name] = value
        elf(path)
        self.walk(path)

    def walk(self, path):
        exact = path.resolve(strict=True)
        if exact in self.visited:
            return
        self.visited.add(exact)
        output = command(["ldd", str(path)])
        need("not found" not in output, "unresolved native dependency")
        for token in output.split():
            if token.startswith("/"):
                candidate = Path(token)
                need(candidate.is_file(), "native dependency path unavailable")
                self.library(candidate)

    def collect(self):
        binaries = {}
        for name in CLIENTS:
            path = BINARY / name
            elf(path, require_interpreter=True)
            version = command([str(path), "--version"]).strip()
            need(re.match(r"\S+ \(PostgreSQL\) 17\.", version) is not None, "actual PostgreSQL 17 client version")
            binaries[name] = {"sha256": sha(path), "owners": self.owners(path), "version": version}
            self.walk(path)
        self.library(Path(INTERPRETER))
        nss = sorted(path for path in (Path("/lib") / TRIPLET).glob("libnss_*.so*") if path.is_file())
        for path in nss:
            self.library(path)
        module_root = Path("/usr/lib") / TRIPLET / "ossl-modules"
        modules = {}
        if module_root.is_dir():
            for path in sorted(module_root.iterdir()):
                need(path.is_file(), "unexpected OpenSSL provider directory entry")
                modules[path.name] = {"sha256": sha(path), "owners": self.owners(path)}
                elf(path)
                self.walk(path)
        need(SHARE.is_dir(), "PostgreSQL public support directory required")
        support = sorted(path for path in SHARE.rglob("*") if path.is_file())
        need(0 < len(support) <= MAX_SUPPORT_FILES, "bounded public support input inventory")
        listing = command(["dpkg-query", "--search", str(SHARE) + "/*"], maximum=262144)
        node_owners = {name: owner.split(", ") for line in listing.splitlines()
                       for owner, separator, name in (line.rpartition(": "),)
                       if separator and name.startswith(str(SHARE) + "/")}
        support_owners = set()
        for path in support:
            matches = node_owners.get(str(path.resolve(strict=True))) or node_owners.get(str(path))
            need(bool(matches), "public support file has no Debian package owner")
            support_owners.update(self.package(name) for name in matches)
        return {"native_platform": "linux/arm64", "uid": os.getuid(), "donor_manifest": MANIFEST, "donor_config": CONFIG,
                "triplet": TRIPLET, "elf_interpreter": INTERPRETER, "binaries": binaries,
                "libraries": self.libraries, "nss_names": [path.name for path in nss], "openssl_providers": modules,
                "public_support_files": len(support), "public_support_owners": sorted(support_owners), "packages": self.packages,
                "server_binary_present": (BINARY / "postgres").is_file(),
                "package_runtime_verified": False, "distribution_qualified": False}


def validate(value):
    need((value["native_platform"], value["uid"], value["donor_manifest"], value["donor_config"],
          value["triplet"], value["elf_interpreter"]) == ("linux/arm64", 65532, MANIFEST, CONFIG, TRIPLET, INTERPRETER),
         "exact named native donor identity")
    libraries, packages = value["libraries"], value["packages"]
    need(set(value["binaries"]) == set(CLIENTS) and Path(INTERPRETER).name in libraries
         and 2 <= len(libraries) <= MAX_LIBRARIES and set(value["nss_names"]) <= libraries.keys()
         and 0 < value["public_support_files"] <= MAX_SUPPORT_FILES
         and bool(value["public_support_owners"]) and set(value["public_support_owners"]) <= packages.keys(),
         "expected PG client, loader and support coverage")
    need(type(value["server_binary_present"]) is bool and all(re.fullmatch(r"[a-zA-Z0-9+_.-]{1,120}", name)
         for name in libraries.keys() | value["openssl_providers"].keys()), "bounded named donor input metadata")
    for row in [*value["binaries"].values(), *libraries.values(), *value["openssl_providers"].values()]:
        need(bool(row["owners"]) and set(row["owners"]) <= packages.keys()
             and HEX.fullmatch(row["sha256"]), "installed owner attribution for each input")
    for row in packages.values():
        need(row["architecture"] in {"arm64", "all"} and HEX.fullmatch(row["copyright_sha256"])
             and all(row[key] for key in ("version", "source_package", "source_version")), "existing package notices")
    need(all(re.match(r"\S+ \(PostgreSQL\) 17\.", item["version"]) for item in value["binaries"].values()),
         "exact major PostgreSQL version")
    need(value["package_runtime_verified"] is value["distribution_qualified"] is False, "inventory claims only")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--validate":
        raw = Path(sys.argv[2]).read_bytes()
        need(0 < len(raw) <= 175000, "bounded owned inventory projection")
        projection = json.loads(raw)
    else:
        need(len(sys.argv) == 1 and os.getuid() == 65532 and platform.machine() == "aarch64", "exact isolated native donor")
        projection = Inventory().collect()
    validate(projection)
    print(json.dumps(projection, indent=2, sort_keys=True))
