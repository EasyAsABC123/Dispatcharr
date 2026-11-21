"""Check exact private GPG linker inputs and one owned literal packet."""

from hashlib import sha256
import json
from pathlib import Path
import re
import sys

PREFIX = "/opt/dispatcharr-crypto"
DOCS = "/usr/share/doc/dispatcharr"
LOADER = "ld-linux-aarch64.so.1"
INPUT_SHA = "f3dde1f5f83cb86fa98e6d509d9323e58b98ba92e24a8c0b43abb064ba8c5f5f"
LITERAL_SHA = "faddf54867aa88a01eb802140a3b45ecffb4e0eb01044727125f5ad55ff987dd"


def hash_bytes(raw):
    return sha256(raw).hexdigest()


def verify(state, projection, stderr, input_record):
    assert state["Status"] == "exited" and state["ExitCode"] == 0 and state["OOMKilled"] is False
    assert stderr == ("loader-and-version-completed\nexact-gpg-command-exit=0\n"
                       "synthetic-home=empty\npacket-bytes=unchanged\ncollector-tree-traversal=complete\n"
                      "fixed-read-and-hash=complete\nmanifest-read=complete\n")
    assert hash_bytes(input_record) == INPUT_SHA
    inputs = json.loads(input_record)
    assert inputs["original_OCI"]["native_platform"] == "linux/arm64"
    assert inputs["original_OCI"]["manifest"] == "sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace"
    crypto = inputs["crypto"]
    gpg = crypto["gpg"]["input"]
    observed = {}
    for match in re.finditer(r"^BEGIN ([a-z-]+)\n(.*?)^END \1\n", projection, re.MULTILINE | re.DOTALL):
        assert match[1] not in observed
        observed[match[1]] = match[2]
    assert set(observed) == {"files", "manifest", "loader", "version", "packet", "packet-digest"}
    assert "".join(f"BEGIN {name}\n{value}END {name}\n" for name,value in observed.items()) == projection
    manifest = json.loads(observed["manifest"])
    assert manifest["no_agent_autostart"] is True
    assert manifest["binary"]["sha256"] == gpg["sha256"]
    assert manifest["trust"]["sha256"] == crypto["trust"]["sha256"]
    private = {Path(source).name: row for source, row in gpg["libraries"].items()}
    assert len(private) == 12 and LOADER in private and set(private) == set(manifest["libraries"])
    assert all(manifest["libraries"][name]["sha256"] == row["sha256"] for name, row in private.items())
    wrapper = f'#!/bin/sh\nexec {PREFIX}/lib/{LOADER} --library-path {PREFIX}/lib {PREFIX}/bin/gpg "$@"\n'
    expected = {
        f"{PREFIX}/bin/gpg": gpg["sha256"], f"{PREFIX}/trust/cacert.pem": crypto["trust"]["sha256"],
        f"{DOCS}/LICENSE": "8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef",
        f"{DOCS}/ATTRIBUTION.md": "1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7",
        "/usr/local/bin/gpg": hash_bytes(wrapper.encode()),
    }
    for name,row in private.items():
        expected[f"{PREFIX}/lib/{name}"] = row["sha256"]
    notices = {}
    for installed in [gpg,*private.values()]:
        assert installed["owners"]
        for owner,row in installed["owners"].items():
            package = owner.partition(":")[0]
            assert row["notice"]["present"]
            notices[package] = row["notice"]["sha256"]
    for package, stamp in notices.items():
        expected[f"{DOCS}/crypto-copyright/{package}/copyright"] = stamp
    expected[f"{DOCS}/crypto-copyright/GPG-LICENSE"] = notices["gpg"]
    expected[f"{DOCS}/crypto-copyright/certifi-LICENSE"] = crypto["explicit_certifi_notice"]["sha256"]
    assert manifest["licenses"]["GPG"]["sha256"] == notices["gpg"]
    assert manifest["licenses"]["certifi"]["sha256"] == crypto["explicit_certifi_notice"]["sha256"]
    found = {}
    for line in observed["files"].splitlines():
        digest, name = line.split("  ", 1)
        assert name not in found
        found[name] = digest
    assert found == expected
    linked = set()
    for line in observed["loader"].splitlines():
        tokens = line.split()
        if not tokens or tokens[0] == "linux-vdso.so.1":
            continue
        if len(tokens) == 2:
            assert tokens[0] in (f"{PREFIX}/lib/{LOADER}", f"/lib/{LOADER}", LOADER)
            linked.add(LOADER)
            continue
        assert len(tokens) >= 4 and tokens[1] == "=>"
        assert tokens[0] in private and tokens[2] == f"{PREFIX}/lib/{tokens[0]}"
        linked.add(tokens[0])
    assert linked == set(private)
    version = observed["version"].strip()
    assert version == manifest["version"] == "gpg (GnuPG) 2.4.8"
    packet = observed["packet"]
    assert packet.count(":literal data packet:") == 1 and "tag=11" in packet
    assert "mode b (62), created 0, name=" in packet and "raw data: 26 bytes" in packet
    assert observed["packet-digest"].strip() == LITERAL_SHA
    return {"scope": "standalone Debian ARM64 GPG wrapper and owned literal packet parser",
            "native_platform": "linux/arm64", "uid": 65532, "private_gpg_links": len(linked),
            "verified_files": len(found), "verified_installed_package_notices": len(notices),
            "certifi_trust_sha256": crypto["trust"]["sha256"], "gpg_sha256": gpg["sha256"], "version": version,
            "literal_packet_sha256": LITERAL_SHA, "literal_packet_tag": 11, "literal_data_bytes": 26,
            "parser_exit": 0, "synthetic_home_empty": True, "signature_operation_verified": False,
            "source_application_started": False, "distribution_qualified": False}


if __name__ == "__main__":
    state, output, stderr, inputs = map(Path, sys.argv[1:])
    print(json.dumps(verify(json.loads(state.read_bytes()), output.read_text(), stderr.read_text(), inputs.read_bytes()),
                     indent=2, sort_keys=True))
