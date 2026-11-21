"""Write the exact ARM64-only exclusion into a private validation context."""

from hashlib import sha256
import json
from pathlib import Path
import sys

INPUT_SHA = "3dd2636717701c1397308bb98d49b1c3e764de21f0bad56231509704f5431ba5"
AMD_SHA = "78a7b60825611013c42932f481a145da87a3acb3d47ebf811c8cb453265ced03"
FILE = Path("rust/runtime/python_host_exclusions/manifest.json")


def stamp(data):
    return sha256(data).hexdigest()


def prepare(root, raw_input):
    assert stamp(raw_input) == INPUT_SHA
    evidence = json.loads(raw_input)
    assert evidence["native_platform"] == "linux/arm64" and evidence["source_run"] == 44876783005
    assert len(evidence["distributions"]) == 127 and len(evidence["torch_notices"]) == 90
    assert all(row["notice_inputs_present"] for row in evidence["distributions"].values())
    shim = evidence["torch_shim"]
    assert shim["wheel_record_sha256_matches"] is shim["wheel_record_size_present"] is shim["ELF64_AArch64"] is True
    path = root / FILE
    pre = path.read_bytes()
    assert stamp(pre) == AMD_SHA and not path.is_symlink()
    manifest = json.loads(pre)
    [row] = manifest["excluded"]
    assert (row["path"], row["version"], row["bytes"], row["primary_source_sha256"], row["sha256"]) == (
        shim["path"], shim["version"], 630280, shim["upstream_test_config_sha256"], shim["exact_AMD64_exclusion_sha256"])
    post = pre.replace(str(row["bytes"]).encode(), str(shim["bytes"]).encode(), 1)
    post = post.replace(row["sha256"].encode(), shim["sha256"].encode(), 1)
    row.update({"bytes": shim["bytes"], "sha256": shim["sha256"]})
    assert json.loads(post) == manifest
    path.write_bytes(post)
    return {"ARM64_exclusion_manifest_sha256": stamp(post), "AMD64_manifest_preimage_sha256": stamp(pre),
            "changed_fields": ["bytes", "sha256"], "scope": "private ARM64 validation context only"}


if __name__ == "__main__":
    root, inputs = map(Path, sys.argv[1:])
    print(json.dumps(prepare(root, inputs.read_bytes()), sort_keys=True))
