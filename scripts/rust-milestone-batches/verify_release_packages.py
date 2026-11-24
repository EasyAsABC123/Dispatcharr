"""Reconcile the two audited ordinary packages without calling either engine."""
import argparse
from hashlib import file_digest, sha256
import json
from pathlib import Path
import re

from coordinator import HERE, module


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    require(path.is_absolute() and path.resolve(strict=True) == path and path.is_file(),
            "expected an absolute, owned evidence file")
    with path.open("rb") as source:
        return file_digest(source, "sha256").hexdigest()


def read(path, expected):
    require(re.fullmatch(r"[a-f0-9]{64}", expected) and digest(path) == expected,
            "audited evidence digest differs")
    return json.loads(path.read_bytes())


def reconcile(revision, amd_binding, arm_audit):
    require(re.fullmatch(r"[a-f0-9]{40}", revision), "expected the exact frozen Git revision")
    candidate = amd_binding["candidate"]
    owner = module(HERE / "m478/batch.py", "release_pair_candidate")
    require(candidate["git"] == revision and owner.candidate_ready(candidate),
            "AMD64 ordinary package does not bind the frozen candidate")
    amd = read(Path(candidate["build_manifest_file"]), candidate["build_manifest_sha256"])
    require(amd["platform"] == "linux/amd64" and amd["worktree_status"] == ""
            and amd["source_input_mode"] == "committed-revision-snapshot"
            and amd["runtime_packages_frozen"] is True,
            "AMD64 package is outside the ordinary committed input scope")

    require(arm_audit["application_revision"] == revision
            and arm_audit["platform"] == "linux/arm64"
            and arm_audit["status"] == arm_audit["workspace_gate"] == "passed"
            and all(type(arm_audit[key]) is int and arm_audit[key] == 0
                    for key in ("actual_exit", "candidate_engine_exit"))
            and arm_audit["exact_cleanup"] is True
            and arm_audit["diagnostic_only"] is arm_audit["instrumentation_enabled"] is False,
            "ARM64 package audit is outside the ordinary frozen scope")
    phases = arm_audit["literal_phases"]
    require(set(phases) == {"owner", "package_run", "physical_build", "preflight", "proof", "signed_auth"}
            and all(type(exit_code) is int and exit_code == 0 for exit_code in phases.values()),
            "ARM64 package audit is missing a successful physical phase")
    require(arm_audit["cleanup"]["scoped_inventories_empty"] is True,
            "ARM64 package audit left an owned resource")

    source_tree = amd["source_tree_sha256"]
    require(re.fullmatch(r"[a-f0-9]{64}", source_tree)
            and source_tree == arm_audit["source_tree_sha256"],
            "ordinary AMD64 and ARM64 packages must come from the same source tree")
    mapping = read(Path(candidate["source_map_file"]), candidate["source_map_sha256"])
    if candidate.get("source_map_format") == "files_path_to_sha256":
        mapping = mapping["files"]
    # The ordinary build manifest uses this normalized digest for all inputs.
    tree = sha256(json.dumps(mapping, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    require(mapping == amd["source_sha256"] and tree == source_tree,
            "AMD64 build manifest and complete source map differ")
    protected = {name: value for name, value in mapping.items() if not name.startswith("rust/")}
    require({"LICENSE", "ATTRIBUTION.md"} <= protected.keys()
            and all(name in ("LICENSE", "ATTRIBUTION.md") or name.startswith("frontend/")
                    for name in protected)
            and (len(mapping), len(protected), len(mapping) - len(protected))
            == (arm_audit["application_inputs"], arm_audit["protected_inputs"], arm_audit["rust_inputs"]),
            "protected React, license, or attribution inputs differ")
    images = (candidate["image_id"], arm_audit["arm_config"])
    require(all(re.fullmatch(r"sha256:[a-f0-9]{64}", image) for image in images)
            and images[0] != images[1],
            "require two actual distinct ordinary platform images")
    arm_manifest = arm_audit["receipts_sha256"]["build.json"]
    require(re.fullmatch(r"[a-f0-9]{64}", arm_manifest), "ARM64 build receipt is absent")
    return {
        "verified_same_frozen_source": True,
        "revision": revision,
        "source_tree_sha256": source_tree,
        "protected_inputs": len(protected),
        "protected_map_sha256": sha256(json.dumps(protected, sort_keys=True).encode()).hexdigest(),
        "linux/amd64": {"image_id": images[0], "build_manifest_sha256": candidate["build_manifest_sha256"]},
        "linux/arm64": {"image_id": images[1], "build_manifest_sha256": arm_manifest},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--amd-binding", type=Path, required=True)
    parser.add_argument("--amd-binding-sha256", required=True)
    parser.add_argument("--arm-audit", type=Path, required=True)
    parser.add_argument("--arm-audit-sha256", required=True)
    args = parser.parse_args()
    report = reconcile(
        args.revision,
        read(args.amd_binding, args.amd_binding_sha256),
        read(args.arm_audit, args.arm_audit_sha256),
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
