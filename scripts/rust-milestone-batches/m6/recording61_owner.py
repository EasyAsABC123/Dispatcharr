"""Qualify the sealed ordinary Native recording61 against retained Original."""
import argparse
from base64 import b64decode
from datetime import datetime
from contextlib import contextmanager, redirect_stdout, redirect_stderr
import fcntl
from importlib.util import module_from_spec, spec_from_file_location
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys

NODE = "m6/recording61-video-audio"
ONEOFF = "m6/oneoff-beat-publication"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ORIGINAL_REDUCTION = "91b16d451a44a01bed2b86ea8180224a64fafa84ebde15853b9f47eca9814e9c"
PHASES = ("preflight", "run", "cleanup", "proof", "campaign")
SCRIPT = Path(__file__).resolve()
COORDINATOR_PROTOCOL = "recording61-coordinator-lease-v1"
DAEMON = "unix:///tmp/dispatcharr-docker.sock"
STRIP_ENV = ["DOCKER_", "COMPOSE_", "POSTGRES_", "REDIS_", "DATABASE_", "GIT_", "CARGO_",
             "RUST_", "DJANGO_", "CELERY_", "FIXTURE_", "PYTHON", "USE_NGINX_ACCEL"]
WORKFLOW_PINS = {
    "scenario.py": "62c5778943f83ccf52566322d619cfbf6da0193d899f3d17c91a835e5518cb2c",
    "reduce.py": "f97de99571b49f5f0b0a8147e8b10308b23873fcf2bb5156842d33624fa0692c",
    "retained.py": "0d05add379e3c53600bc05ee90532cb1ded5fcf1c4118a6463f920eff62d0629",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    require(path.resolve() == path and path.is_file(), "expected exact regular file")
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def read(path):
    require(path.resolve() == path and path.is_file()
            and path.stat().st_size <= 8 * 1024 * 1024, "expected exact bounded receipt")
    return json.loads(path.read_bytes())


def sealed_inputs(stage, seal):
    manifest = stage / "SHA256SUMS"
    require(digest(manifest) == seal, "recording61 stage seal differs")
    inputs = {str(manifest): seal, str(SCRIPT): digest(SCRIPT)}
    names = set()
    for line in manifest.read_text().splitlines():
        expected, name = line.split("  ", 1)
        relative = Path(name)
        require(not relative.is_absolute() and ".." not in relative.parts
                and str(relative) == name and name not in names, "invalid recording61 input name")
        path = stage / relative
        require(digest(path) == expected, "recording61 stage input changed")
        names.add(name)
        inputs[str(path)] = expected
    return inputs, names


def stage_inputs(stage, seal, candidate):
    inputs, names = sealed_inputs(stage, seal)
    require({"run.py", "scenario.py", "package-binding.json",
             "fixture_adapter.py", "candidate.py", "external-pins.json", "reduce.py", "retained.py"} <= names,
            "missing sealed recording61 input")
    require(all(inputs[str(stage / name)] == expected for name, expected in WORKFLOW_PINS.items()),
            "recording61 fixture or reducer differs from the reviewed workflow")
    # The stage's preflight also verifies its frozen support and archive.
    package = read(stage / "package-binding.json")
    require((package["candidate"], package["image"], package["manifest"])
            == (candidate["git"], candidate["image_id"], candidate["build_manifest_sha256"]),
            "recording61 stage differs from the common candidate")
    require(package["Original"] == ORIGINAL, "recording61 Original authority differs")
    require(package["archive"] == candidate["source_archive_sha256"],
            "recording61 stage archive differs from the common candidate")
    manifest_path = Path(candidate["build_manifest_file"])
    require(digest(manifest_path) == package["manifest"], "recording61 package manifest changed")
    inputs[str(manifest_path)] = package["manifest"]
    return inputs


def settled(receipt):
    return (type(receipt["actual_exit"]) is int and receipt["actual_exit"] == 0
            and receipt["failure"] is None and receipt["remaining_group_members"] == []
            and receipt["output_bytes_discarded"] == 0
            and all(receipt[key] is True for key in ("process_group_settled", "stdout_EOF", "stderr_EOF")))


def verify(stage, seal, candidate):
    stage_inputs(stage, seal, candidate)
    ledger = stage / "owner-1"
    actual = stage / "actual-1"
    require(all((ledger / f"{phase}.actual-exit").read_bytes() == b"0\n" for phase in PHASES),
            "recording61 campaign phase did not exit zero")
    summary = read(ledger / "proof.stdout")
    require(summary["candidate"] == candidate["git"]
            and summary["manifest_sha256"] == candidate["build_manifest_sha256"]
            and summary["scope"] == "recording61-video-audio"
            and set(summary["actual"]) == {"Native"}
            and read(actual / "admission.json")["native"] == candidate["image_id"],
            "recording61 proof differs from the admitted Native arm")
    close = read(actual / "independent-cleanup.json")
    require(close == read(ledger / "cleanup.stdout") == summary["physical_cleanup"]
            and [row["side"] for row in close["arms"]] == ["Native"]
            and close["exact_cleanup"] is True, "recording61 scoped retirement differs")
    require(settled(read(actual / "Native-execution.json")), "recording61 arm did not settle")
    business = actual / "Native/business"
    raw = read(business / "scenario.json")
    decoder = read(business / "declared-whole-EOF.json")
    require(decoder == raw["media"]["declared"] and type(decoder["actual_exit"]) is int
            and decoder["actual_exit"] == 0 and decoder["failure"] is None
            and all(decoder[key] is True for key in
                    ("stdout_EOF", "stderr_EOF", "process_group_settled", "full_progress_end")),
            "recording61 declared decode receipt differs")
    require(raw["clocked_broker_lineage"]["task_id"] == raw["task_terminal"]["task_id"]
            == raw["task_id"] != raw["route_task_name"]
            and type(raw["clock_broker_publications"]) is int
            and raw["clock_broker_publications"] == 1
            and raw["manual_task_frame"] is raw["early_operator_stop"] is False,
            "recording61 broker, natural completion and result identities differ")

    # The reviewed reducer replays the actual Redis journal, DB2 command bytes,
    # recorder/provider/origin observations and exact cleanup. It is read-only.
    process = subprocess.run([sys.executable, "-E", "-s", "-B", str(stage / "reduce.py")],
                             cwd=stage, capture_output=True, check=False, timeout=30)
    require(process.returncode == 0, "recording61 sealed raw reducer failed")
    reduced = json.loads(process.stdout)
    require((reduced["candidate"], reduced["image_id"], reduced["manifest_sha256"])
            == (candidate["git"], candidate["image_id"], candidate["build_manifest_sha256"])
            and reduced["retained_original_revision"] == ORIGINAL
            and reduced["retained_original_reduction_sha256"] == ORIGINAL_REDUCTION
            and type(reduced["actual_native_arms"]) is int and reduced["actual_native_arms"] == 1
            and type(reduced["actual_original_arms"]) is int and reduced["actual_original_arms"] == 0
            and reduced["scoped_functional_passed"] is True,
            "recording61 reduced package or retained Original authority differs")
    native = reduced["native"]
    require(native["private_business_sha256"] == digest(business / "scenario.json")
            and native["result"]["publication_uuid_matches"] is True
            and native["scoped_inventories_empty"] is True, "recording61 reduced custody differs")
    return {"candidate": candidate, "scope": NODE, "exact_owned_absence": True,
            "scope_results": native, "retained_original_reduction_sha256": ORIGINAL_REDUCTION,
            "stage_seal": seal}


def verify_publication(stage, seal, candidate):
    """Read the signed booking, stored clock and actual broker/DB2 observations."""
    stage_inputs(stage, seal, candidate)
    lane = stage / "actual-1/Native"
    require(read(stage / "actual-1/admission.json")["native"] == candidate["image_id"],
            "one-off Native package admission differs")
    meta = read(lane / "fixture.json")
    root = Path(meta["output"])
    require(meta["backend"] == "Native" and root.parent == lane / "fixtures"
            and root.resolve() == root, "one-off receipt ownership differs")
    raw = read(lane / "business/scenario.json")
    schedule = raw["clocked_task"]
    booking, *reads = raw["requests"]
    args = json.loads(schedule["args"])
    start, end = (datetime.fromisoformat(raw[key]) for key in ("scheduled_start", "scheduled_end"))
    require(booking["method"] == "POST" and booking["path"] == "/api/channels/recordings/"
            and booking["status"] == 201 and len(booking["sha256"]) == 64,
            "one-off signed booking receipt differs")
    require(len(args) == 4 and all(type(value) is int and value > 0 for value in args[:2])
            and [datetime.fromisoformat(value) for value in args[2:]] == [start, end]
            and start.utcoffset() is not None and end.utcoffset() is not None
            and (end - start).total_seconds() == 61
            and raw["manual_task_frame"] is raw["early_operator_stop"] is False,
            "one-off clocked task arguments differ")
    name = f"dvr-recording-{args[0]}"
    require(raw["route_task_name"] == schedule["name"] == name
            and schedule["task"] == "apps.channels.tasks.run_recording"
            and schedule["enabled"] is schedule["one_off"] is True
            and datetime.fromisoformat(schedule["clocked__clocked_time"]) == start
            and reads and all(row["status"] == 200 and row["method"] == "GET"
                              and row["path"] == f"/api/channels/recordings/{args[0]}/" for row in reads),
            "one-off signed recording and saved schedule differ")
    matches, creates = [], []
    for path in sorted(root.glob("command-*.json")):
        command = read(path)
        stdout = Path(command["stdout"])
        if (not stdout.is_relative_to(root) or not stdout.is_file() or stdout.stat().st_size > 16384
                or command["actual_exit"] != 0):
            continue
        try:
            value = read(stdout)
        except (ValueError, UnicodeError):
            continue
        if value == [schedule]:
            require(settled(command) and digest(stdout) == command["stdout_sha256"],
                    "one-off saved schedule SQL receipt did not settle")
            matches.append(command["stdout_sha256"])
        if isinstance(value, dict) and value.get("status") == 201 and isinstance(value.get("body"), str):
            body = b64decode(value["body"], validate=True)
            if sha256(body).hexdigest() != booking["sha256"]:
                continue
            created = json.loads(body)
            require(settled(command) and digest(stdout) == command["stdout_sha256"]
                    and created["id"] == args[0] and created["channel"] == args[1]
                    and created["task_id"] == name
                    and [datetime.fromisoformat(created[key]) for key in ("start_time", "end_time")] == [start, end],
                    "one-off actual signed create response differs from saved scheduler row")
            creates.append(command["stdout_sha256"])
    require(len(creates) == 1, "one-off requires exactly one actual signed create response")
    require(len(matches) == 1, "one-off requires exactly one actual saved schedule SQL receipt")
    journal = root / raw["clock_broker_journal_path"]
    require(journal.is_relative_to(root) and journal.resolve() == journal
            and digest(journal) == raw["clock_broker_journal_sha256"]
            and raw["clock_broker_observer_retired"] is True,
            "one-off exact broker journal differs")
    sys.path.insert(0, str(stage))
    try:
        scenario = load_module(stage / "scenario.py", "oneoff_recording_scenario")
        reducer = load_module(stage / "reduce.py", "oneoff_recording_result")
        frames = scenario.recording_frames_from_journal(journal)
        lineage = scenario.published_recording(frames, name, args[:2] + [start, end],
                                               datetime.fromisoformat(raw["first_recorder"]["upper_bound_utc"]))
        require(lineage is not None and lineage["task_id"] == raw["task_id"] != name,
                "one-off broker UUID differs from the owned execution")
        backend = reducer.results_command(root, meta["project"], lineage["task_id"])
        reference = reducer.original_reference()
        require(reference["actual_recording_broker_publications"] == 1
                and reference["broker_root_correlation_arguments_due_match"] is True
                and reference["result"]["terminal"] == backend["terminal"],
                "one-off differs from retained Original publication authority")
    finally:
        sys.path.remove(str(stage))
    require(backend["publication_uuid_matches"] is True and backend["terminal"] == "SUCCESS"
            and backend["result"] is None, "one-off authoritative DB2 terminal differs")
    return {"scope": ONEOFF, "candidate": candidate, "stage_seal": seal,
            "signed_create_status": booking["status"], "signed_create_sha256": booking["sha256"],
            "signed_transport_sha256": creates[0], "retained_original_reduction_sha256": ORIGINAL_REDUCTION,
            "saved_schedule_sha256": matches[0], "schedule_name": name,
            "broker_sha256": digest(journal), "single_clocked_root": lineage,
            "authoritative_result": backend}


def prior_publication(output, stage, seal, candidate, recording):
    directory = attempt_directory(output, ONEOFF)
    attempt = read(directory / "attempt.json")
    result_path = directory / "result.json"
    receipt = read(result_path)
    proof = receipt["qualification"]
    require(attempt["node"]["id"] == receipt["id"] == ONEOFF and attempt["candidate"] == candidate
            and receipt["status"] == "passed" and receipt["cleanup_verified"] is True
            and all(settled(receipt[key]) for key in ("execution", "cleanup", "evaluation"))
            and proof["full_required_workflow"] is True and proof["semantic_assertions_passed"] is True
            and proof["candidate"] == candidate and proof["stage_seal"] == seal
            and proof["scope"] == ONEOFF
            and proof["broker_sha256"] == recording["private_broker_sha256"]
            and proof["authoritative_result"]["private_result_sha256"] == recording["result"]["private_result_sha256"]
            and Path(attempt["binding"]["argv"][4]) == stage
            and read(directory / "evaluation.stdout") == proof,
            "recording61 requires the same owned passed one-off predecessor")
    return digest(result_path)


def attempt_directory(output, node=NODE):
    stem = node.replace("/", "__")
    return output / f"{stem}-{sha256(node.encode()).hexdigest()[:12]}"


def dispatch_spec(stage, seal, candidate, repository, entrypoint, output, *, node=NODE):
    require(node in (ONEOFF, NODE), "invalid recording family row")
    output = Path(output)
    require(output.resolve() == output
            and not stage.is_relative_to(output) and not output.is_relative_to(stage),
            "expected separate canonical coordinator output")
    inputs = stage_inputs(stage, seal, candidate)
    protocol = stage / "coordinator-mode.json"
    require(str(protocol) in inputs and read(protocol) == COORDINATOR_PROTOCOL,
            "recording61 stage must support the coordinator-held lease")
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    coordinator_sha = digest(coordinator)
    inputs[str(coordinator)] = coordinator_sha
    tooling = repository / "artifacts/rust/tooling"
    prefix = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(stage), seal,
              json.dumps(candidate, sort_keys=True)]
    return {
        "argv": ([*prefix, "run", entrypoint, str(output), str(coordinator), coordinator_sha]
                 if node == ONEOFF else [*prefix, "reuse", entrypoint, str(output)]),
        "cleanup_argv": [*prefix, "cleanup"],
        "evaluator_argv": [*prefix, "evaluate", entrypoint, str(output), "@EXECUTION@", node],
        "cwd": str(repository),
        "environment": {"DOCKER_HOST": DAEMON, "DOCKER_CONFIG": str(tooling / "docker-config"),
                        "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
        "strip_inherited_prefixes": STRIP_ENV,
        "input_sha256": inputs, "seconds": 660 if node == ONEOFF else 60, "cleanup_seconds": 60,
        "output_bytes": 65536, "exclusive_heavy": True,
    }


def parent_holds_lock(parent, path):
    """Require an exclusive flock on this exact inode in the live parent's fdinfo."""
    lock_stat = path.stat()
    for entry in (parent / "fdinfo").iterdir():
        try:
            descriptor = (parent / "fd" / entry.name).stat()
            if (descriptor.st_dev, descriptor.st_ino) != (lock_stat.st_dev, lock_stat.st_ino):
                continue
            for line in entry.read_text().splitlines():
                if not line.startswith("lock:"):
                    continue
                fields = line.split()
                # The open descriptor identifies the filesystem. Btrfs fdinfo
                # can report the superblock device instead of stat().st_dev.
                if (len(fields) == 9 and fields[2:6] == ["FLOCK", "ADVISORY", "WRITE", parent.name]
                        and fields[6].rpartition(":")[2] == str(lock_stat.st_ino)
                        and fields[7:] == ["0", "EOF"]):
                    return True
        except FileNotFoundError:
            continue
    return False


def coordinator_parent(coordinator, coordinator_sha, output, binding, candidate, *, node=ONEOFF):
    parent = Path("/proc") / str(os.getppid())
    supplied = [os.fsdecode(arg) for arg in (parent / "cmdline").read_bytes().split(b"\0") if arg]
    cwd = (parent / "cwd").resolve(strict=True)
    require(len(supplied) >= 2 and "--bindings" in supplied and "--check-bindings" not in supplied
            and supplied.count("--output") == 1, "actual dispatching coordinator parent required")
    index = supplied.index("--output")
    require(index + 1 < len(supplied)
            and any((cwd / arg).resolve() == coordinator for arg in supplied[1:min(index, supplied.index("--bindings"))])
            and (cwd / supplied[index + 1]).resolve() == output
            and digest(coordinator) == coordinator_sha, "actual reviewed coordinator parent required")
    directory = attempt_directory(output, node)
    attempt = read(directory / "attempt.json")
    require(attempt["node"]["id"] == node and type(attempt["attempt"]) is int and attempt["attempt"] == 1
            and attempt["candidate"] == candidate
            and attempt["binding"] == binding and not os.path.lexists(directory / "execution.json")
            and (directory / "execution.stdout").is_file()
            and (directory / "execution.stderr").is_file(), "actual exact recording61 coordinator attempt required")
    lock = coordinator.with_name(".dispatcher.lock")
    with lock.open("rb") as probe:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            fcntl.flock(probe, fcntl.LOCK_UN)
            raise ValueError("actual held coordinator lease required")
    require(parent_holds_lock(parent, lock) and str(os.getppid()) == parent.name,
            "dispatcher lease is held by a different process")
    return lock


def load_module(path, name):
    spec = spec_from_file_location(name, path)
    driver = module_from_spec(spec)
    spec.loader.exec_module(driver)
    return driver


def load_driver(stage):
    return load_module(stage / "run.py", "recording61_coordinator_driver")


def cleanup_receipt(stage, driver):
    close = driver.cleanup()
    require(close == read(stage / "actual-1/independent-cleanup.json")
            and [row["side"] for row in close["arms"]] == ["Native"]
            and close["exact_cleanup"] is True
            and not os.path.lexists(driver.MARKER), "exact recording61 scoped retirement required")
    return {"exact_owned_absence": True, "physical_cleanup": close}


def run_campaign(stage, driver, lock):
    @contextmanager
    def borrow(expected):
        require(expected == lock, "same dispatcher lock required by recording61 child")
        require(parent_holds_lock(Path("/proc") / str(os.getppid()), lock),
                "coordinator lease no longer held")
        yield

    ledger = stage / "owner-1"
    ledger.mkdir(mode=0o700)
    status = 1

    def step(phase):
        code = 1
        try:
            with (ledger / f"{phase}.stdout").open("x") as out, (ledger / f"{phase}.stderr").open("x") as err:
                with redirect_stdout(out), redirect_stderr(err):
                    code = driver.main([phase], coordinator_lease=borrow)
            return code
        finally:
            (ledger / f"{phase}.actual-exit").write_text(f"{code}\n")

    try:
        status = step("preflight")
        if status != 0:
            return status
        try:
            status = step("run")
        finally:
            cleanup_status = step("cleanup")
        if status == 0:
            status = cleanup_status or step("proof")
        return status
    except BaseException:
        status = 1
        raise
    finally:
        (ledger / "campaign.actual-exit").write_text(f"{status}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("seal")
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("mode", choices=("run", "reuse", "cleanup", "check", "publication-check", "evaluate"))
    parser.add_argument("entrypoint", nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("authority", nargs="?")
    parser.add_argument("coordinator_sha", nargs="?")
    args = parser.parse_args()
    stage, seal, candidate = args.stage, args.seal, args.candidate
    if args.mode == "run":
        coordinator = Path(args.authority)
        repository = coordinator.parents[2]
        binding = dispatch_spec(stage, seal, candidate, repository, args.entrypoint, args.output, node=ONEOFF)
        require(binding["argv"] == sys.orig_argv and Path.cwd() == repository,
                "exact reviewed recording61 controller invocation required")
        lock = coordinator_parent(coordinator, args.coordinator_sha, args.output, binding, candidate)
        require(all(os.environ.get(key) == value for key, value in binding["environment"].items())
                and not any(key in os.environ for key in ("DOCKER_CONTEXT", "USE_NGINX_ACCEL"))
                and all(not key.startswith("RUST_") for key in os.environ),
                "exact isolated recording61 daemon and ordinary feature settings required")
        docker = repository / "artifacts/rust/tooling/docker/docker"
        require(Path(shutil.which("docker") or "").resolve() == docker,
                "exact recording61 engine executable required")
        require(not any(os.path.lexists(stage / name) for name in ("owner-1", "actual-1")),
                "fresh recording61 owner directory required at dispatch")
        os.umask(0o077)
        sys.path.insert(0, str(stage))
        driver = load_driver(stage)
        require(driver.SHARED / ".dispatcher.lock" == lock
                and read(driver.DONOR / "base-binding.json")["docker"] == str(docker),
                "same dispatcher lock and isolated engine required by fixture support")
        status = run_campaign(stage, driver, lock)
        if status != 0:
            return status
    if args.mode == "cleanup":
        stage_inputs(stage, seal, candidate)
        sys.path.insert(0, str(stage))
        result = cleanup_receipt(stage, load_driver(stage))
    elif args.mode in ("run", "publication-check") or (args.mode == "evaluate" and args.coordinator_sha == ONEOFF):
        result = verify_publication(stage, seal, candidate)
    else:
        result = verify(stage, seal, candidate)
        if args.mode in ("reuse", "evaluate"):
            result["oneoff_predecessor_sha256"] = prior_publication(args.output, stage, seal, candidate, result["scope_results"])
    if args.mode == "evaluate":
        execution = Path(args.authority)
        node = args.coordinator_sha
        require(node in (ONEOFF, NODE), "invalid recording family evaluator row")
        directory = attempt_directory(args.output, node)
        require(execution == directory / "execution.json", "recording61 execution receipt path differs")
        receipt = read(execution)
        require(receipt["id"] == node and settled(receipt["execution"])
                and settled(receipt["cleanup"]) and receipt["cleanup_verified"] is True
                and Path(receipt["execution"]["stdout"]) == directory / "execution.stdout"
                and read(directory / "execution.stdout") == result,
                "recording61 coordinator execution did not settle")
        result.update(entrypoint=args.entrypoint, semantic_assertions_passed=True,
                      full_required_workflow=True)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
