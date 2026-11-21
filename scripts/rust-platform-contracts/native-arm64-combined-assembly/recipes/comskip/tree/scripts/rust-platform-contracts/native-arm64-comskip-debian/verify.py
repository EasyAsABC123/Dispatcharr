"""Verify the exact Comskip closure and ordinary generated-media behavior."""

from hashlib import sha256
import json
from pathlib import Path
import re
import sys

PREFIX = "/opt/dispatcharr-comskip"
DOCS = "/usr/share/doc/dispatcharr-standalone-comskip"
LOADER = "ld-linux-aarch64.so.1"
LOCK_SHA = "cbfe929a644b4a1134b34af05c00dfe6b03e5b6ea4ceef83ef9f1a26f6aa75fd"


def verify(state, stdout, stderr, lock_bytes):
    assert state["Status"] == "exited" and state["ExitCode"] == 0 and state["OOMKilled"] is False
    assert stderr == ("private-loader-and-help=complete\nowned-audio-video-decoded=complete\n"
                      "owned-detector-exit=1\nempty-edl-and-input-unchanged=complete\n")
    assert sha256(lock_bytes).hexdigest() == LOCK_SHA
    lock = json.loads(lock_bytes)
    assert 0 < len(stdout) < 180000
    items = dict(re.findall(r"^BEGIN ([a-z-]+)\n(.*?)^END \1\n", stdout, re.MULTILINE | re.DOTALL))
    assert "".join(f"BEGIN {name}\n{value}END {name}\n" for name,value in items.items()) == stdout
    assert set(items) == {"manifest", "files", "loader", "version", "media", "media-digest"}
    manifest = json.loads(items["manifest"])
    identity = lock["original_runtime"]
    assert (manifest["input_OCI_manifest"], manifest["input_OCI_config"]) == (identity["manifest"],identity["config"])
    assert manifest["ABI_lock_sha256"] == LOCK_SHA and manifest["distribution_qualified"] is False
    assert (manifest["collector_input_files"],manifest["private_libraries"],manifest["installed_package_notices"]) == (6,138,126)
    known = manifest["path_sha256"]
    found = {}
    for line in items["files"].splitlines():
        digest, path = line.split("  ",1)
        assert path not in found and re.fullmatch("[a-f0-9]{64}",digest)
        found[path] = digest
    assert found == known
    for name,digest in lock["input_files_sha256"].items():
        output = { "comskip":f"{PREFIX}/bin/comskip", "comskip.ini":"/app/docker/comskip.ini" }.get(name,f"{DOCS}/{name}")
        assert found[output] == digest
    libraries = manifest["resolved_library_sha256"]
    assert len(libraries) == 138 and all(found[f"{PREFIX}/lib/{name}"] == digest for name,digest in libraries.items())
    links = set()
    for line in items["loader"].splitlines():
        parts = line.split()
        if not parts or parts[0] == "linux-vdso.so.1":
            continue
        if len(parts) == 2:
            assert parts[0] in (LOADER, f"/lib/{LOADER}", f"{PREFIX}/lib/{LOADER}")
            name = LOADER
        else:
            name = LOADER if parts[0] == f"/lib/{LOADER}" else parts[0]
            assert len(parts) == 4 and parts[1] == "=>" and parts[2] == f"{PREFIX}/lib/{name}"
        assert name not in links
        links.add(name)
    assert links == set(libraries)
    version = items["version"].strip()
    assert version.startswith("Comskip 0.83.001")
    shape = json.loads(items["media"])
    assert sorted(row["codec_name"] for row in shape["streams"]) == ["mp2", "mpeg2video"]
    duration = float(shape["format"]["duration"])
    assert 7.9 <= duration <= 8.2
    digest,name = items["media-digest"].strip().split("  ",1)
    assert name == "owned.ts" and digest == "562fa93972763130c3079330b5e028a1e248ff98d59202129164d20a8c67d0b1"
    return {"scope":"standalone Debian ARM64 source-built Comskip replacement",
            "uid":65532,"native_platform":"linux/arm64","private_libraries":len(links),
            "installed_package_notices":126,"verified_file_count":len(found),
            "version":version,"binary_sha256":found[f"{PREFIX}/bin/comskip"],
            "candidate_upstream_revision":lock["candidate_upstream_revision"],
            "ABI_lock_sha256":LOCK_SHA,"input_manifest_sha256":sha256(items["manifest"].encode()).hexdigest(),
            "media":{"sha256":digest,"duration_seconds":duration,"audio_video_decode_completed":True,
                     "empty_EDL":True,"input_unchanged":True,"detector_exit":1},
            "source_application_started":False,"distribution_qualified":False}


if __name__ == "__main__":
    state,stdout,stderr,lock = map(Path,sys.argv[1:])
    print(json.dumps(verify(json.loads(state.read_bytes()),stdout.read_text(),stderr.read_text(),lock.read_bytes()),
                     indent=2,sort_keys=True))
