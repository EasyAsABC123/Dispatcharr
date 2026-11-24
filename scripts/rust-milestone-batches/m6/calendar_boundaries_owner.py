"""Bind exact Original boundary observations and prepare scoped saved-calendar Native arms."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    import recording61_owner as common
    from prepare_recording61 import rebind_package
    import retained_scheduler_semantics as retained
finally:
    sys.path.pop(0)

NATIVE_TEMPLATE_SHA = "3f90be2e83d0940e69b6ff8b2787dfa597010ff791ce4a0fe831b5d54dbdb14b"
SCOPES = ("chicago", "newfoundland")
SUPPORT = ("run.py", "candidate.py", "campaign.sh")
SOURCE_ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
require, read, digest = common.require, common.read, common.digest


def original_boundaries(spring, fall=None):
    require(digest(spring) == retained.CALENDAR_SHA, "exact pinned Original spring observation required")
    document = read(spring)
    original = retained.calendar(document)
    scenarios = [row for row in document["cases"] if row["name"] == "ny-fall"]
    require(len(scenarios) == 1, "retained Original eager fall scenario required")
    fold, = scenarios
    first, second = fold["samples"]
    require(fold["input"]["timezone"] == "America/New_York"
            and fold["input"]["now"] == "2026-11-01T05:30:00+00:00"
            and len(fold["envelopes"]) == first["queue_count"] == second["queue_count"] == 1
            and first["state"] == second["state"] and type(first["state"]["count"]) is int
            and first["state"]["count"] == 1
            and first["state"]["last_run_at"] == "2026-11-01T05:30:00+00:00",
            "retained eager fall-fold SQL and single publication differ")
    frame, = fold["envelopes"]
    head = frame["headers"]
    require(UUID(head["id"]).version == 4 and head["id"] == head["root_id"] == frame["properties"]["correlation_id"]
            and head["parent_id"] is None and head["periodic_task_name"] == first["state"]["name"],
            "retained eager fall-fold broker identity differs")
    result = {"source_revision": common.ORIGINAL, "spring_real_beat": original,
              "fall_eager_sql_and_publication": True, "fall_real_beat": None,
              "canonical_calendar_full_scope_accepted": False}
    if fall is not None:
        report = read(fall / "integration.json")
        data = fall / "source-calendar-fall-fold-private.json"
        require(report["passed"] is True and report["original_revision"] == common.ORIGINAL
                and type(report["guest_actual_exit"]) is int and report["guest_actual_exit"] == 0
                and report["original_archive_sha256"] == SOURCE_ARCHIVE
                and report["source_report_sha256"] == digest(data)
                and report["default_database_unchanged"] is True and report["resources_retained"] is False
                and report["exact_cleanup"] == {
                    "owned_containers": [{"container_inspect_exit": 1, "generation_absent": True}] * 2,
                    "inventories": {"container": [], "network": [], "volume": []}},
                "exact Original real Beat fall fold host receipt and cleanup required")
        verifier = Path(__file__).resolve().parents[2] / "rust-job-contracts/source_calendar_fall_fold.py"
        require(report["tool_source_sha256"]["scripts/rust-job-contracts/source_calendar_fall_fold.py"]
                == digest(verifier), "exact fall-fold acquisition tools required")
        guest = common.load_module(verifier, "validated_fall_fold")
        raw = read(data)
        fold = guest.qualify(raw)
        require(report["clock_log_sha256"] == guest.clock_log(raw, fall / "calendar-fall-fold-clock-private.jsonl"),
                "host and guest Beat clock dependency receipts differ")
        require(raw["passed"] is True and report["controlled_fall_fold"] == fold,
                "literal Original controlled fall-fold report differs")
        result["fall_real_beat"] = fold
    return result


def prepare_native(template, candidate, stage):
    _, names = common.sealed_inputs(template, NATIVE_TEMPLATE_SHA)
    require(set(SUPPORT) | {"package-binding.json"} <= names, "exact saved-calendar template inputs required")
    old = read(template / "package-binding.json")
    manifest, package = rebind_package(old, candidate)
    files = {name: (template / name).read_bytes() for name in SUPPORT}
    adapter = files["run.py"].decode()
    adapter, count = re.subn(r"^BUILD = ART / .+$", lambda _: f"BUILD = Path({str(manifest)!r})",
                            adapter, flags=re.MULTILINE)
    require(count == 1, "saved calendar package path binding differs")
    files["run.py"] = adapter.encode()
    checker = files["candidate.py"].decode()
    for key in ("candidate", "image", "manifest"):
        require(checker.count(old[key]) == 1, "saved calendar package checker shape differs")
        checker = checker.replace(old[key], package[key])
    checker = checker.replace(f"=={old['source_files']}==", f"=={package['source_files']}==")
    files["candidate.py"] = checker.encode()
    files["package-binding.json"] = (json.dumps(package, sort_keys=True) + "\n").encode()
    require(stage.resolve() == stage and not stage.exists(), "fresh scoped saved-calendar stage required")
    stage.mkdir(mode=0o700)
    for name, body in files.items():
        (stage / name).write_bytes(body)
    sums = stage / "SHA256SUMS"
    sums.write_text("".join(f"{digest(stage / name)}  {name}\n" for name in sorted(files)))
    seal = digest(sums)
    (stage / "workload-seal.sha256").write_text(seal + "\n")
    receipts = []
    for scope in SCOPES:
        result = subprocess.run(["/usr/bin/python3", "-B", str(stage / "run.py"), "--preflight", "--scope", scope],
                                capture_output=True, check=True, timeout=45)
        row = json.loads(result.stdout)
        require(row["candidate"] == candidate["git"] and row["scope"] == scope and row["engine_calls"] == 0
                and row["physical_passed"] is False, "ordinary saved-calendar cold preflight differs")
        receipts.append(row)
    return {"stage": str(stage), "seal": seal, "scoped_native_preflights": receipts,
            "real_dst_transition_verified": False, "canonical_calendar_full_scope_accepted": False}


def native_partial(stage, seal, candidate):
    common.sealed_inputs(stage, seal)
    package = read(stage / "package-binding.json")
    require((package["candidate"], package["image"], package["manifest"])
            == (candidate["git"], candidate["image_id"], candidate["build_manifest_sha256"]),
            "saved-calendar scoped results must match the final package")
    ledger = stage / "owner-1"
    phases = ["actual-exit", *(f"{scope}-{phase}.actual-exit" for scope in SCOPES
                               for phase in ("preflight", "run", "reduce"))]
    require(all((ledger / name).read_bytes() == b"0\n" for name in phases),
            "literal saved-zone Native phases did not exit zero")
    receipts = []
    for scope, zone in zip(SCOPES, ("America/Chicago", "America/St_Johns"), strict=True):
        physical = subprocess.run(["/usr/bin/python3", "-B", str(stage / "run.py"), "--reduce", "--scope", scope],
                                  capture_output=True, check=True, timeout=45)
        row = json.loads(physical.stdout)
        require(row == read(ledger / f"{scope}-reduce.stdout") and row["scope"] == scope
                and row["candidate"] == candidate["git"] and row["image"] == candidate["image_id"]
                and row["manifest"] == candidate["build_manifest_sha256"]
                and row["original_revision"] == common.ORIGINAL and row["business"]["schedule_zone"] == zone
                and row["physical_passed"] is True and row["actual_native_lanes"] == 1
                and row["actual_original_lanes"] == 0 and row["canonical_calendar_full_scope_accepted"] is False,
                "ordinary saved-zone Native physical receipts differ")
        receipts.append(row)
    return {"scoped_native_saved_zones": receipts, "real_dst_transition_verified": False,
            "canonical_calendar_full_scope_accepted": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    original = modes.add_parser("original")
    original.add_argument("spring", type=Path)
    original.add_argument("--fall", type=Path)
    native = modes.add_parser("native-prepare")
    native.add_argument("template", type=Path)
    native.add_argument("candidate", type=Path)
    native.add_argument("stage", type=Path)
    partial = modes.add_parser("native-reduce")
    partial.add_argument("stage", type=Path)
    partial.add_argument("seal")
    partial.add_argument("candidate", type=Path)
    args = parser.parse_args()
    if args.mode == "original":
        result = original_boundaries(args.spring, args.fall)
    elif args.mode == "native-prepare":
        result = prepare_native(args.template, read(args.candidate), args.stage)
    else:
        result = native_partial(args.stage, args.seal, read(args.candidate))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
