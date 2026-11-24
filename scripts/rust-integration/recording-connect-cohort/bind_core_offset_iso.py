#!/usr/bin/env python3
"""Narrow an unused exact portable plan to the existing finite Core pair."""
import argparse
import hashlib
import json
from pathlib import Path
import re

PRODUCT = {
    "rust/crates/store/src/core_task_offsets.rs": "5d975cb1542da01f130e9580be9110e8c3a6b0bf78a34606a2f9e268342f90d5",
    "rust/crates/store/src/epg_grid_iso.rs": "92bd519f2ad635f51c6dd1b8124e33668ed781a0db9ca95169d381004e3cb6b5",
}
ORIGINAL_ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
OWNER_TOOLS = {
    'core_context.py': 'f73b1552553cd39b83fc647d3e1e1e76ad87bf531e4291dd8d698d8ca3ddc7db',
    'core_offset_iso_case.py': 'a8f8a830e78f99ab930189dd9628b1d505d7150d2b0599c5844dc3a86286e3e1',
    'workflow.py': 'aa011bad3187fa0b6abb7cd8f42352abfe043442ddc1b23376367760fa7c8cc7',
}
TOOLS = "scripts/rust-integration/recording-connect-cohort"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def need(value, label):
    if not value:
        raise ValueError(label)


def check_product(source):
    need(all(source.get(name) == expected for name, expected in PRODUCT.items()),
         "BothExactOffsetProductLeavesRequired")


def bind(plan_path, output, expected_build_sha256):
    plan_path = plan_path.resolve()
    output = output.resolve()
    plan = json.loads(plan_path.read_bytes())
    root = Path(plan["repo"]).resolve()
    directory = Path(plan["output"]).resolve()
    need(plan["state"] == "UNQUALIFIED_PUBLIC_EXPORT_PREPARED"
         and not plan.get("workflow") and plan_path == directory / "plan.json"
         and output.parent == directory and output.name == "core-offset-iso-plan.json"
         and not (directory / "run-started.json").exists(),
         "FreshStockPreparedPlanAndSiblingRequired")
    base = plan["base_binding"]
    need(base["status"] == "prepared_not_run" and base["performance"] is False
         and not base["retained_source_contexts"] and not base["retained_native_contexts"]
         and Path(base["support_root"]).resolve() == root,
         "FreshOwnedPortableBindingRequired")
    build_path = Path(base["candidate_build"])
    need(re.fullmatch(r"[0-9a-f]{64}", expected_build_sha256)
         and sha(build_path) == expected_build_sha256, "ExpectedFinalBuildManifestRequired")
    build = json.loads(build_path.read_bytes())
    need(build["passed"] is True and build["exit_code"] == 0
         and re.fullmatch(r"sha256:[0-9a-f]{64}", build["image_id"])
         and build["image_id"] == base["candidate_image_id"]
         and build["git_revision"] == base["candidate_git_revision"]
         and build["source_tree_sha256"] == base["candidate_source_tree_sha256"],
         "ExactPreparedFinalCandidate")
    check_product(build["source_sha256"])
    retained = {Path(item["path"]).resolve(): item["sha256"] for item in base["inputs"]}
    archive = Path(base["Original_archive"]).resolve()
    need(retained.get(archive) == ORIGINAL_ARCHIVE and sha(archive) == ORIGINAL_ARCHIVE,
         "ExactPinnedOriginalArchiveRequired")
    need(all(retained.get(root / name) == expected and sha(root / name) == expected
             for name, expected in PRODUCT.items()), "CheckoutHasBothExactOffsetProductLeaves")
    cohort = root / TOOLS / "cohort"
    for name in ("core_context", "workflow", "core_offset_iso_case"):
        path = cohort / (name + ".py")
        need(retained.get(path) == sha(path) == OWNER_TOOLS[path.name],
             "ExactPreparedScopeOverlay:" + name)
    for label, name in (("core_context_adapter", "core_context.py"),
                        ("core_offset_iso_case", "core_offset_iso_case.py")):
        path = cohort / name
        base[label] = str(path)
        base[label + "_sha256"] = retained[path]
    for name in ("broker_monitor.py", "core_cases.py"):
        path = cohort / name
        need(retained.get(path) == sha(path), "ExactExistingSettingsOwnerTool:" + name)
    base["core_offset_iso_pair"] = True
    base["core_monitor_budget"] = {"bytes": 1_048_576}
    plan.update(workflow="core-offset-iso-pair", source_contexts=["core-produced"],
                native_contexts=["core-produced"], source_seconds=600, native_seconds=600,
                qualification="Finite paired ordinary worker proof; broad Core-scale acceptance remains separate")
    with output.open("x") as stream:
        stream.write(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    output.chmod(0o600)
    print(json.dumps({"prepared": str(output), "applications_started": 0}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-sha256", required=True)
    args = parser.parse_args()
    bind(args.prepared_plan, args.output, args.build_sha256)
