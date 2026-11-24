"""Own the ordinary Main six saved-default scheduler startup prerequisite."""
import argparse
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys

# This CLI also runs with Python isolated mode from the coordinator.
sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    import recording61_owner as common
    from prepare_recording61 import rebind_package
finally:
    sys.path.pop(0)

NODE = "m6/scheduler-install-known-defaults"
TEMPLATE_SHA = "07e2d49685c38f00531ba3d36628b7052526d78b0b29b38162270e717cf9a35b"
ORIGINAL_SHA = "a4c13b85f76de12d264520eb5b003627f69973d37f820c167d25383e03ad38e1"
ORIGINAL_IMAGE = "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
ORIGINAL_REL = "m6-source-native-scheduler-final-physical-1/source-scheduler-database-private.json"
SUPPORT = ("run.py", "candidate.py", "scheduler_scope.py", "external-pins.json")
SCOPE_SHA = "4dcb3abcc1e43d084f74b4bba7efdf32cd87e5ea1991bd992ad74a532d4c00a8"
SCRIPT = Path(__file__).resolve()
require, read, digest = common.require, common.read, common.digest

# Intercept the sealed donor's existing textual adaptation, replacing only
# its standalone acquisition. The lane still runs its real child claim check.
LEASE_OPEN = '    with lock.open("a+") as held:\n        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)'
LANE_SELECTOR = 'for side in ("Original", "Native"):'


def borrow_adapter(source):
    head = "def configured():"
    require(source.count(head) == 1, "scheduler template configuration differs")
    source = source.replace(head, "def configured(coordinator_lease=None):")
    mark = "  driver,donor,Class,cases,native=inherited(controller,owner,expected,context)"
    replacement = f'''  replace=controller.replace
  def adapt(text,before,after):
   updated=replace(text,before,after)
   if before=={LANE_SELECTOR!r}:
    updated=replace(updated,{LEASE_OPEN!r},"    with coordinator_lease(lock):")
   return updated
  controller.replace=adapt
  owner.coordinator_lease=coordinator_lease
  try:
   driver,donor,Class,cases,native=inherited(controller,owner,expected,context)
  finally:
   controller.replace=replace'''
    require(source.count(mark) == 1, "scheduler inherited owner differs")
    source = source.replace(mark, replacement)
    old = "if __name__=='__main__':"
    require(source.count(old) == 1, "scheduler fixture entrypoint differs")
    source = source.replace(old, "def main(coordinator_lease=None):")
    mark = "  candidate()\n  m.inspect_engine_image"
    require(source.count(mark) == 1, "scheduler fixture engine fence differs")
    source = source.replace(mark, "  assert coordinator_lease is not None, 'reviewed coordinator lease required'\n" + mark)
    return (source.replace(" raise SystemExit(configured().main())", " return configured(coordinator_lease).main()")
            + "\nif __name__=='__main__':\n raise SystemExit(main())\n")


def prepare(template, candidate, stage):
    _, names = common.sealed_inputs(template, TEMPLATE_SHA)
    require(set(SUPPORT) | {"package-binding.json"} <= names, "scheduler template input missing")
    manifest, package = rebind_package(read(template / "package-binding.json"), candidate)
    files = {name: (template / name).read_bytes() for name in SUPPORT}
    original = read(template / "package-binding.json")
    for name in ("run.py", "candidate.py"):
        source = files[name].decode()
        for key in ("candidate", "image", "manifest"):
            require(source.count(original[key]) == 1, "scheduler candidate binding changed shape")
            source = source.replace(original[key], package[key])
        if name == "run.py":
            source, count = re.subn(r"^PACKAGE=ART/.+$", lambda _: f"PACKAGE=Path({str(manifest.parent)!r})",
                                    source, flags=re.MULTILINE)
            require(count == 1, "scheduler package path changed shape")
            source = borrow_adapter(source)
        else:
            source = source.replace(f"=={original['source_files']}==", f"=={package['source_files']}==")
        compile(source, name, "exec")
        files[name] = source.encode()
    files["package-binding.json"] = (json.dumps(package, sort_keys=True) + "\n").encode()
    files["scheduler-coordinator.json"] = b'"saved-default-install-v1"\n'
    require(stage.resolve() == stage and not stage.exists(), "fresh absolute scheduler stage required")
    stage.mkdir(mode=0o700)
    for name, body in files.items():
        (stage / name).write_bytes(body)
    sums = stage / "SHA256SUMS"
    sums.write_text("".join(f"{digest(stage / name)}  {name}\n" for name in sorted(files)))
    return {"stage": str(stage), "seal": digest(sums)}


def stage_binding(stage, seal, candidate, source, *, coordinator_mode):
    inputs, names = common.sealed_inputs(stage, seal)
    require(set(SUPPORT) | {"package-binding.json"} <= names
            and inputs[str(stage / "scheduler_scope.py")] == SCOPE_SHA,
            "exact scheduler observer required")
    if coordinator_mode:
        require("scheduler-coordinator.json" in names
                and read(stage / "scheduler-coordinator.json") == "saved-default-install-v1",
                "stage must use the canonical scheduler lease")
    package = read(stage / "package-binding.json")
    manifest = Path(candidate["build_manifest_file"])
    require((package["candidate"], package["image"], package["manifest"], package["archive"], package["Original"])
            == (candidate["git"], candidate["image_id"], digest(manifest),
                candidate["source_archive_sha256"], common.ORIGINAL)
            and package["manifest"] == candidate["build_manifest_sha256"],
            "scheduler stage differs from the common ordinary package")
    require(digest(source) == ORIGINAL_SHA, "exact pinned Original scheduler rows required")
    return inputs | {str(manifest): package["manifest"], str(source): ORIGINAL_SHA,
                     str(SCRIPT): digest(SCRIPT), str(common.SCRIPT): digest(common.SCRIPT)}


def dispatch_spec(stage, seal, candidate, repository, entrypoint, output):
    source = repository / "artifacts/rust" / ORIGINAL_REL
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    coordinator_sha = digest(coordinator)
    inputs = stage_binding(stage, seal, candidate, source, coordinator_mode=True)
    output = Path(output)
    require(output.resolve() == output and not stage.is_relative_to(output)
            and not output.is_relative_to(stage), "separate canonical scheduler output required")
    prefix = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(stage), seal,
              json.dumps(candidate, sort_keys=True), str(source)]
    tooling = repository / "artifacts/rust/tooling"
    return {
        "argv": [*prefix, "run", entrypoint, str(output), str(coordinator), coordinator_sha],
        "cleanup_argv": [*prefix, "cleanup", str(repository)],
        "evaluator_argv": [*prefix, "evaluate", entrypoint, str(output), "@EXECUTION@"],
        "input_sha256": inputs | {str(coordinator): coordinator_sha},
        "cwd": str(repository), "environment": {"DOCKER_HOST": common.DAEMON,
                     "DOCKER_CONFIG": str(tooling / "docker-config"), "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
        "strip_inherited_prefixes": common.STRIP_ENV,
        "seconds": 1540, "cleanup_seconds": 60, "output_bytes": 65536, "exclusive_heavy": True,
    }


def require_clear_quarantine(repository):
    marker = ".dispatcharr-recording-cohort-cleanup-required.json"
    shared = repository / "scripts/rust-milestone-batches"
    require(not any(os.path.lexists(path) for path in
                    (repository / marker, shared / marker, shared / ".cleanup-required.json")),
            "scheduler fixture cleanup uncertainty marker remains")


def exact_cleanup(stage):
    actual = stage / "actual-native-1"
    lane = read(actual / "Native/lane.json")
    terminal = read(actual / "Native-execution.json")
    root = Path(lane["fixture_path"])
    require(root.resolve() == root and root.is_relative_to(actual)
            and lane["exact_cleanup"] is True
            and all(terminal[key] is True for key in ("process_group_settled", "stdout_EOF", "stderr_EOF"))
            and terminal["remaining_group_members"] == [] and terminal["output_bytes_discarded"] == 0,
            "scheduler lane cleanup and process settlement required")
    proof = read(root / "independent-cleanup.json")
    cids = proof["exact_CID_absence"]
    require(proof["inventories"] == {"container": [], "network": [], "volume": []}
            and proof["errors"] == [] and len({row["Id"] for row in cids}) == len(cids) == 3
            and all(proof[key] is True for key in ("exact_owned_absence",
                    "all_original_process_generations_absent", "host_CLI_groups_settled"))
            and all(row["actual_missing_object"] is row["original_host_generation_absent"] is True for row in cids),
            "scheduler copied datastore and fixture inventory did not retire")
    return root, digest(root / "independent-cleanup.json")


def evaluate_install(stage, seal, candidate, source):
    stage_binding(stage, seal, candidate, source, coordinator_mode=False)
    actual = stage / "actual-native-1"
    require(all((stage / "owner-1" / leaf).read_bytes() == b"0\n" for leaf in
                ("preflight.actual-exit", "run.actual-exit", "cleanup.actual-exit", "actual-exit"))
            and (actual / "actual-exit").read_bytes() == b"0\n"
            and common.settled(read(actual / "Native-execution.json")),
            "literal scheduler installation phases did not exit zero")
    require(read(actual / "binding.json") ==
            {"candidate": candidate["git"], "manifest_sha256": candidate["build_manifest_sha256"],
             "native_image": candidate["image_id"], "source": ORIGINAL_IMAGE},
            "scheduler owned fixture binding differs")
    root, cleanup_sha = exact_cleanup(stage)
    scope = read(root / "scheduler-scope-install.json")
    observer = common.load_module(stage / "scheduler_scope.py", "saved_default_observer")
    before, prepared, ready = (scope[key] for key in ("before", "prepared", "native_ready"))
    observer.validate_install(before, prepared, ready)
    require(read(root / "ordinary-aio-readiness.json") ==
            {"ordinary_aio": True, "core_saved_scheduler_observed": True, "both_lanes_accepting": True},
            "ordinary Core and both production lanes must be ready")
    original = read(source)
    rows = original["installed_rows"]
    require(original["passed"] is True and {row["name"] for row in rows} == set(ready),
            "installed defaults differ from retained Original roster")
    for row in rows:
        native = ready[row["name"]]
        interval = row["interval"] or {"every": None, "period": None}
        require((native["task"], native["every"], native["period"], native["one_off"])
                == (row["task"], interval["every"], interval["period"], row["one_off"]),
                "installed default task metadata differs from retained Original")
    observed = []
    for receipt_path in sorted(root.glob("command-*.json")):
        command = read(receipt_path)
        stdout = Path(command["stdout"])
        if (not common.settled(command) or stdout.parent not in (root, root / "private")
                or not stdout.is_file() or not 0 < stdout.stat().st_size <= 8000):
            continue
        try:
            value = read(stdout)
        except ValueError:
            continue
        if value == (before, prepared, ready)[len(observed)]:
            require(digest(stdout) == command["stdout_sha256"], "saved default SQL stdout custody differs")
            observed.append(command["stdout_sha256"])
            if len(observed) == 3:
                break
    require(len(observed) == 3, "ordered before, absent and installed SQL receipts required")
    return {"scope": NODE, "candidate": candidate, "stage_seal": seal,
            "known_default_count": len(ready), "reinstalled_default": observer.TARGET,
            "preserved_default_count": len(prepared), "ordered_sql_sha256": observed,
            "retained_original_revision": common.ORIGINAL,
            "retained_original_sha256": ORIGINAL_SHA, "cleanup_sha256": cleanup_sha}


def execute_campaign(stage, lock):
    @contextmanager
    def borrow(expected):
        require(expected == lock and common.parent_holds_lock(Path("/proc") / str(os.getppid()), lock),
                "same live coordinator lease required")
        yield

    sys.path.insert(0, str(stage))
    driver = common.load_module(stage / "run.py", "saved_default_production_fixture")
    require(driver.ART == lock.parents[2] / "artifacts/rust", "scheduler fixture repository differs")
    ledger = stage / "owner-1"
    ledger.mkdir(mode=0o700)
    status = 1

    def phase(name, action):
        code = 1
        try:
            with (ledger / f"{name}.stdout").open("x") as out, (ledger / f"{name}.stderr").open("x") as err:
                with redirect_stdout(out), redirect_stderr(err):
                    code = action()
            return code
        finally:
            (ledger / f"{name}.actual-exit").write_text(f"{code}\n")

    def fixture(mode):
        sys.argv = [str(stage / "run.py"), mode]
        return driver.main(coordinator_lease=borrow)

    def teardown():
        require_clear_quarantine(driver.ART.parents[1])
        _, cleanup_sha = exact_cleanup(stage)
        print(json.dumps({"exact_owned_absence": True, "cleanup_sha256": cleanup_sha}))
        return 0

    try:
        status = phase("preflight", lambda: fixture("--preflight"))
        if status == 0:
            try:
                status = phase("run", lambda: fixture("--run"))
            finally:
                cleanup = phase("cleanup", teardown)
            status = status or cleanup
        return status
    except BaseException:
        status = 1
        raise
    finally:
        (ledger / "actual-exit").write_text(f"{status}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("seal")
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("source", type=Path)
    parser.add_argument("mode", choices=("run", "cleanup", "check", "evaluate"))
    parser.add_argument("entrypoint", nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("authority", nargs="?")
    parser.add_argument("coordinator_sha", nargs="?")
    args = parser.parse_args()
    stage, seal, candidate, source = args.stage, args.seal, args.candidate, args.source
    if args.mode == "run":
        coordinator = Path(args.authority)
        repository = coordinator.parents[2]
        binding = dispatch_spec(stage, seal, candidate, repository, args.entrypoint, args.output)
        require(binding["argv"] == sys.orig_argv and Path.cwd() == repository,
                "exact reviewed scheduler controller invocation required")
        lock = common.coordinator_parent(coordinator, args.coordinator_sha, args.output, binding, candidate, node=NODE)
        require(all(os.environ.get(key) == value for key, value in binding["environment"].items())
                and not any(key.startswith(("DOCKER_CONTEXT", "USE_NGINX_ACCEL", "RUST_")) for key in os.environ)
                and Path(shutil.which("docker") or "").resolve() == repository / "artifacts/rust/tooling/docker/docker",
                "exact isolated daemon and ordinary scheduler feature settings required")
        require(not any(os.path.lexists(stage / name) for name in ("owner-1", "actual-native-1")),
                "scheduler stage must be fresh at dispatch")
        require_clear_quarantine(repository)
        os.umask(0o077)
        status = execute_campaign(stage, lock)
        if status:
            return status
    if args.mode == "cleanup":
        stage_binding(stage, seal, candidate, source, coordinator_mode=True)
        require_clear_quarantine(Path(args.entrypoint))
        _, cleanup_sha = exact_cleanup(stage)
        result = {"exact_owned_absence": True, "cleanup_sha256": cleanup_sha}
    else:
        result = evaluate_install(stage, seal, candidate, source)
    if args.mode == "evaluate":
        directory = common.attempt_directory(args.output, NODE)
        require(Path(args.authority) == directory / "execution.json", "scheduler execution receipt path differs")
        receipt = read(directory / "execution.json")
        require(receipt["id"] == NODE and common.settled(receipt["execution"])
                and common.settled(receipt["cleanup"]) and receipt["cleanup_verified"] is True
                and read(directory / "execution.stdout") == result,
                "scheduler exact coordinator attempt did not settle")
        result.update(entrypoint=args.entrypoint, semantic_assertions_passed=True, full_required_workflow=True)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
