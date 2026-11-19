#!/usr/bin/env python3
"""Prepare and run one paired Original/Native Main XC group PATCH scenario."""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time

from scoped_driver import patch_run

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / ".git").exists())
OVERLAYS = Path(__file__).resolve().parent
COORDINATOR = ROOT / "scripts/rust-milestone-batches/coordinator.py"
BUILD = ROOT / "scripts/rust-integration/build.py"
CASE = "xc-account-partial-group-patch"
GROUP_SOURCE = "rust/crates/store/src/provider_account_writes.rs"
DRIVERS = ("Original", "Native")
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ORIGINAL_SHA256 = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
SUPPORT_REVISION = "17fae6de0702835c5e94643cccc995df5622d6b1"
COHORT = Path("scripts/rust-integration/catalogue-language-cohort")
COMPANIONS = (
    "fixture_adapter.py", "cohort/fixture.py", "cohort/run.py", "cohort/workflow.py",
    "cohort/budget.py", "cohort/origin.py", "cohort/role_owner.py",
    "cohort/startup_hooks.py", "cohort/startup_observer.py", "cohort/channel_tools/journal.py",
)
SUPPORT = (
    "docker/compose.rust-playback.yml",
    "scripts/rust-integration/seed_accounts.py",
    "scripts/rust-integration/fixture_ports.py",
    "scripts/rust-playback-baseline",
    "scripts/rust-baseline",
)
WINDOW = 2400
CLEANUP = 90


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(path):
    with Path(path).open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def load(path):
    return Path(path).read_text()


def read(path):
    return json.loads(load(path))


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def ref(path, sha=None):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha or digest(path)}


def verify(inputs):
    for entry in inputs:
        if digest(entry["path"]) != entry["sha256"]:
            raise ValueError("frozen input changed: " + entry["path"])


def modules(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def archive(revision, destination, paths=()):
    with destination.open("xb") as stream:
        subprocess.run(["git", "archive", "--format=tar", revision, *paths],
                       cwd=ROOT, stdout=stream, check=True, timeout=90)


def replace_once(source, expected, replacement):
    require(source.count(expected) == 1, "pinned cohort source anchor changed")
    return source.replace(expected, replacement, 1)


def stage(output, docker):
    require(not output.exists(), "fresh staging directory required")
    output.mkdir(mode=0o700, parents=True)
    original = output / "Original.tar"
    archive(ORIGINAL, original)
    require(digest(original) == ORIGINAL_SHA256, "pinned Original archive hash differs")
    original_files = {}
    with tarfile.open(original) as reader:
        for member in reader:
            if member.isfile():
                with reader.extractfile(member) as stream:
                    original_files[member.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    support_archive = output / "support.tar"
    archive(SUPPORT_REVISION, support_archive, (*SUPPORT, *(str(COHORT / name) for name in COMPANIONS)))
    support = output / "support"
    support.mkdir()
    with tarfile.open(support_archive) as reader:
        reader.extractall(support, filter="data")

    driver = output / "driver"
    shutil.move(support / COHORT, driver)
    (driver / "cohort/fixture.py").write_text(replace_once(
        replace_once(load(driver / "cohort/fixture.py"),
                     "ROOT=Path(__file__).resolve().parents[4]", "ROOT=Path(" + repr(str(ROOT)) + ")"),
        "'RUST_EXPERIMENTAL_SELECTED_BEAT':'1'", "'RUST_EXPERIMENTAL_SELECTED_BEAT':'0'",
    ))
    runner = driver / "cohort/run.py"
    runner.write_text(patch_run(runner.read_text()))
    path = driver / "cohort/workflow.py"
    workflow = replace_once(path.read_text(), " def run(self):", """ def xc_account_group_patch(self):
  import xc_account_group_patch
  if self.fixture.backend == 'Original':self.fixture.role('beat',[],stop=True)
  path=self.out/(xc_account_group_patch.CASE+'.json')
  result=xc_account_group_patch.run(self.fixture,lambda report:save(path,report))
  if not result['passed']:raise AssertionError('XC partial group PATCH contract failed')
 def run(self):""")
    path.write_text(replace_once(workflow, "\n  by_name=dict(cases)",
                                 "\n  cases.append(('xc-account-partial-group-patch',self.xc_account_group_patch))\n"
                                 "  by_name=dict(cases)"))
    probe = OVERLAYS / "xc_account_group_patch.py"
    shutil.copyfile(probe, driver / "cohort" / probe.name)
    for path in [driver, *driver.rglob("*")]:
        path.chmod(0o755 if path.is_dir() or path.stat().st_mode & 0o111 else 0o644)

    # These are local generated artifacts, not historical execution authority.
    sources = [COORDINATOR, BUILD, Path(__file__), OVERLAYS / "scoped_driver.py", probe]
    files = [path for directory in (support, driver) for path in directory.rglob("*") if path.is_file()]
    inputs = [ref(path) for path in sorted(files)] + [ref(path) for path in [*sources, original, support_archive]]
    binding = {
        "docker": docker, "support_root": str(support), "functional_pids_limit": 1600,
        "Original_archive": str(original), "Original_source_root": str(output / "Original-source"),
        "Original_source_sha256": original_files,
    }
    write(output / "staged.json", {
        "case": CASE, "driver": str(driver), "inputs": inputs, "fixture": binding,
        "fixture_support_revision": SUPPORT_REVISION, "original_revision": ORIGINAL,
    })
    print("Paired driver staged from committed fixtures. Candidate package binding is pending.")


def bind(output, manifest_path, revision, mode):
    metadata = read(output / "staged.json")
    source_root = Path(metadata["fixture"]["Original_source_root"])
    require(source_root == output / "Original-source" and not source_root.exists(),
            "Original extraction path must be separate from backend directories and absent before binding")
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "supply the full Candidate Git revision")
    manifest = read(manifest_path)
    require(manifest["git_revision"] == revision and manifest["source_input_mode"] == "committed-revision-snapshot"
            and manifest["exit_code"] == 0 and manifest["runtime_packages_frozen"] is True
            and re.fullmatch(r"sha256:[0-9a-f]{64}", manifest["image_id"]),
            "exact successful Candidate package identity required")
    source_archive = manifest_path.parent / manifest["source_archive"]
    modules(BUILD, "candidate_build_validator").validate_replay_archive(source_archive, manifest_path.read_bytes())
    with tarfile.open(source_archive) as archive:
        packaged_source = archive.extractfile(GROUP_SOURCE).read()
    require(b"validate_group_references(&group_ids,&found)" in packaged_source
            and b'"channel_groups": errors' in packaged_source,
            "candidate package lacks sparse missing-group validation")
    if mode in {"duplicates", "orphans", "activity"}:
        require(b"keep_first_group_selections(&mut validated.group_selection)" in packaged_source,
                "candidate package lacks ordered duplicate group selection")
        require(b"let xc_preferences = !create" in packaged_source
                and b"&& !input.is_empty()\n        && input.iter().all(|(key, value)|" in packaged_source,
                "candidate package lacks group-filter preference PATCH support")
    if mode in {"orphans", "activity"}:
        require(b"let xc_orphan_cleanup = !create" in packaged_source
                and b"&& !xc_orphan_cleanup" in packaged_source,
                "candidate package lacks live group-filter orphan cleanup PATCH support")
    if mode == "activity":
        require(b"let xc_active_toggle = !create" in packaged_source
                and b"&& !xc_active_toggle" in packaged_source,
                "candidate package lacks playlist table XC activity toggle")
    if mode == "rename":
        require(b"let xc_label = !create" in packaged_source
                and b"&& !xc_label" in packaged_source,
                "candidate package lacks playlist editor XC rename")
    if mode == "interval":
        require(b"let xc_interval = !create" in packaged_source
                and b"&& !xc_interval" in packaged_source,
                "candidate package lacks playlist editor XC interval update")
    package = [ref(manifest_path), ref(source_archive, manifest["source_archive_sha256"])]
    verify(metadata["inputs"])
    for backend in DRIVERS:
        (output / backend).mkdir(mode=0o700)
        binding = {**metadata["fixture"]}
        driver = Path(metadata["driver"])
        adapter = driver / "fixture_adapter.py"
        binding.update(
            candidate_build=str(manifest_path), candidate_image_id=manifest["image_id"],
            candidate_git_revision=revision, candidate_source_tree_sha256=manifest["source_tree_sha256"],
            backend_order=[backend], execution_contexts={backend: [CASE]},
            fixture_output=str(output / backend / "fixtures"), adapter_sha256=digest(adapter),
            retained_source_contexts={}, retained_native_contexts={}, restore_callbacks={},
            execution_scope=f"{backend} Main XC group PATCH parity, {mode}",
            xc_group_patch_duplicates=mode in {"duplicates", "orphans", "activity"},
            xc_group_patch_orphan_modes=mode in {"orphans", "activity"},
            xc_group_patch_active_toggle=mode == "activity",
            xc_group_patch_label_edit=mode == "rename",
            xc_group_patch_interval_edit=mode == "interval",
            whole_cohort_seconds=WINDOW, cleanup_seconds=CLEANUP, performance=False,
            inputs=metadata["inputs"] + package,
        )
        verify(binding["inputs"])
        write(output / backend / "binding.json", binding)
    write(output / "bound.json", {
        "case": CASE, "mode":mode, "candidate_git_revision": revision, "candidate_image_id": manifest["image_id"],
        "candidate_source_tree_sha256": manifest["source_tree_sha256"],
        "staged": ref(output / "staged.json"),
        "bindings": {backend: ref(output / backend / "binding.json") for backend in DRIVERS},
    })
    print("Candidate paired drivers bound. No runtime has been launched.")


def generation(pid):
    try:
        return Path("/proc", str(pid), "stat").read_text().rsplit(")", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def actual_absence(docker, scopes):
    def inventory(*arguments):
        call = subprocess.run([docker, *arguments], capture_output=True, text=True, timeout=15)
        require(call.returncode == 0, "actual Docker inventory failed")
        return call.stdout.split()
    all_ids = set(inventory("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}"))
    for scope in scopes:
        project = scope["project"]
        require(re.fullmatch(r"dispatcharr-normal-dvr-[a-f0-9]+", project), "invalid owned Compose project")
        require(scope["exact_owned_absence"] and scope["all_original_process_generations_absent"]
                and scope["host_CLI_groups_settled"] and not scope["errors"], "owned cleanup incomplete")
        for item in scope["exact_CID_absence"]:
            require(re.fullmatch(r"[a-f0-9]{64}", item["Id"]) and item["Id"] not in all_ids,
                    "old owned container remains")
        for item in scope["captured_original_process_generations"]:
            require(generation(item["pid"]) != item["starttime"], "old process generation remains")
        for kind, all_flag, fmt in (("container", ["--all"], "{{.ID}}"), ("network", [], "{{.ID}}"), ("volume", [], "{{.Name}}")):
            require(not inventory(kind, "ls", *all_flag, "--filter", f"label=com.docker.compose.project={project}",
                                  "--format", fmt), "owned Compose resources remain")


def run(output):
    package = read(output / "bound.json")
    bindings = package["bindings"]
    verify([package["staged"], *bindings.values()])
    lock_path = ROOT / "scripts/rust-milestone-batches/.dispatcher.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cleanup_gate = ROOT / ".dispatcharr-recording-cohort-cleanup-required.json"
        require(not cleanup_gate.exists(), "verify the previous owned fixture cleanup first")
        staged = read(package["staged"]["path"])
        first = read(bindings["Original"]["path"])
        docker = first["docker"]
        actual = subprocess.run([docker, "image", "inspect", package["candidate_image_id"]],
                                capture_output=True, text=True, check=True, timeout=15)
        inspected = json.loads(actual.stdout)[0]
        require(inspected["Id"] == package["candidate_image_id"]
                and inspected["Config"]["Labels"]["org.opencontainers.image.source-tree-sha256"]
                == package["candidate_source_tree_sha256"], "Candidate image/package binding differs")
        coordinator = modules(COORDINATOR, "owned_candidate_coordinator")
        dispatch = output / "dispatch"
        dispatch.mkdir(mode=0o700)
        deadline = time.monotonic() + WINDOW
        global_context = dispatch / "context.json"
        write(global_context, {
            "heavy_exclusive": True, "active_functional_fixtures": 1, "attempt": 1,
            "claim_persisted": True, "deadline_monotonic": deadline, "cleanup_seconds": CLEANUP,
            "holder_pid": os.getpid(), "actual_local_lease": str(lock_path), "performance": False,
        })
        summaries = {}
        verdict = {"scope": CASE, "candidate_git_revision": package["candidate_git_revision"],
                   "candidate_image_id": package["candidate_image_id"], "results": summaries,
                   "paired_passed": False, "performance_verified": False}
        try:
            for backend in DRIVERS:
                binding = Path(bindings[backend]["path"])
                verify(read(binding)["inputs"])
                driver = Path(staged["driver"])
                results = output / backend / "results"
                argv = [sys.executable, "-B", str(driver / "cohort/run.py"),
                        "--adapter", str(driver / "fixture_adapter.py"), "--binding", str(binding),
                        "--global-context", str(global_context), "--output", str(results)]
                remaining = deadline - time.monotonic()
                require(remaining > 0, "paired fixture functional deadline exhausted")
                write(cleanup_gate, {"owned_run": str(output), "backend": backend,
                                     "reason": "Owned fixture cleanup has not been verified"})
                receipt = coordinator.command(argv, {"cwd": str(ROOT), "environment": dict(os.environ),
                                                      "output_bytes": 8_388_608},
                                              dispatch, backend, remaining + CLEANUP)
                write(dispatch / f"{backend}-command.json", receipt)
                require(coordinator.settled(receipt), f"{backend} fixture process group unsettled")
                report = read(results / "report.json")
                require(len(report["cleanup"]) == 1, f"{backend} owned cleanup missing")
                actual_absence(docker, report["cleanup"])
                cleanup_gate.unlink()
                probe = read(results / backend / f"{CASE}.json")
                summaries[backend] = {
                    "actual_exit": receipt["actual_exit"], "cleanup_verified": True,
                    "status": [item["status"] for item in probe["steps"]],
                    "missing_group_status": probe.get("missing_group_references", {}).get("status"),
                    "duplicate_group_status": probe.get("duplicate_group_selections", {}).get("status"),
                    "group_filter_preferences_status": probe.get("group_filter_preferences", {}).get("status"),
                    "orphan_cleanup_modes_status": [mode["status"] for mode in probe.get("orphan_cleanup_modes", [])],
                    "active_toggle_status": [step["status"] for step in probe.get("active_toggle", [])],
                    "label_edit_status": probe.get("label_edit", {}).get("status"),
                    "interval_edit_status": [step["status"] for step in probe.get("interval_edit", [])],
                    "passed": probe["passed"] and report["combined_selected_workflow_passed"]
                    and receipt["actual_exit"] == 0 and receipt["failure"] is None,
                    "sanitized_probe_sha256": digest(results / backend / f"{CASE}.json"),
                }
                require(summaries[backend]["passed"], f"{backend} Main group PATCH probe failed")
            verdict["paired_passed"] = len(summaries) == 2
        finally:
            write(output / "paired-result.json", verdict)
        print(json.dumps(verdict))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("stage", "bind", "run"):
        command = sub.add_parser(action)
        command.add_argument("--output", required=True, type=Path)
        if action == "stage":
            command.add_argument("--docker", default="docker")
        if action == "bind":
            command.add_argument("--manifest", required=True, type=Path)
            command.add_argument("--mode", required=True, choices=("references", "duplicates", "orphans", "activity", "rename", "interval"))
            command.add_argument("--candidate-revision", required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    os.umask(0o077)
    sys.dont_write_bytecode = True
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    if args.action == "stage":
        stage(output, args.docker)
    elif args.action == "bind":
        bind(output, args.manifest.resolve(), args.candidate_revision, args.mode)
    else:
        run(output)


if __name__ == "__main__":
    main()
