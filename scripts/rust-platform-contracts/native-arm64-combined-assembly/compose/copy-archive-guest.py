"""Owned guest-only PostgreSQL archive transport for the private ARM64 fixture."""
from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from urllib.parse import unquote, urlsplit

SOURCE = "dispatcharr"
COPY = "dispatcharr_arm64_copy"
PG_BIN = Path("/usr/local/bin")


def endpoints(url):
    parsed = urlsplit(url)
    assert parsed.scheme == "postgresql" and parsed.hostname == "database"
    assert parsed.port == 5432 and parsed.username == "dispatch" and parsed.path == f"/{SOURCE}"
    assert parsed.password and not parsed.query and not parsed.fragment
    assert re.fullmatch(r"[0-9a-f]{48}", parsed.password)
    return {"PGHOST": "database", "PGPORT": "5432", "PGUSER": "dispatch",
            "PGPASSWORD": unquote(parsed.password), "PGCONNECT_TIMEOUT": "5"}


def tool(name, args, connection, database):
    assert name in {"pg_dump", "pg_restore"}
    assert database == (SOURCE if name == "pg_dump" else COPY)
    return subprocess.run([PG_BIN / name, "--no-password", "--dbname", database, *args],
                          env={**connection, "PATH": str(PG_BIN) + ":/usr/bin:/bin", "LC_ALL": "C"},
                          capture_output=True, timeout=55, check=False)


def main():
    assert os.uname().machine == "aarch64" and os.getuid() == 65532
    database = endpoints(os.environ["DATABASE_URL"])
    with TemporaryDirectory(dir="/tmp", prefix="arm64-owned-pg-copy-") as directory:
        artifact = Path(directory) / "owned.dump"
        export = tool("pg_dump", ["--format=custom", "--no-owner", "--no-privileges", "--file", artifact], database, SOURCE)
        if export.returncode != 0:
            print(json.dumps({"checkpoint": "dump", "exit": export.returncode}))
            raise SystemExit(1)
        size = artifact.stat().st_size
        assert 5 < size < 8 * 1024 * 1024
        with artifact.open("rb") as stream:
            assert stream.read(5) == b"PGDMP"
            stream.seek(0)
            sha = file_digest(stream, "sha256").hexdigest()
        restore = tool("pg_restore", ["--single-transaction", "--exit-on-error", "--no-owner",
                                      "--no-privileges", artifact], database, COPY)
        if restore.returncode != 0:
            print(json.dumps({"checkpoint": "restore", "exit": restore.returncode}))
            raise SystemExit(1)
    assert not artifact.exists()
    print(json.dumps({"dump_exit": 0, "restore_exit": 0, "custom_archive_sha256": sha,
                      "custom_archive_bytes": size, "archive_removed": True}, sort_keys=True))


if __name__ == "__main__":
    main()
