"""Execute the reviewed ordinary-Main SQL held-Ready recording gate."""
import argparse
from contextlib import contextmanager, redirect_stdout
from importlib.util import module_from_spec, spec_from_file_location
from io import StringIO
import json
from math import isfinite
import os
from pathlib import Path
import shutil
import signal
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
try:
    import recording61_owner as common
finally:
    sys.path.pop(0)

SCRIPT = Path(__file__).resolve()
NODE = "m6/SQL-ErrorResponse-heldReady"
SUCCESSOR = "m6/cache-reply-held"
STAGE_SEAL = "e1666db1367ae6ca7610418e93ce7d40037b99dd60cb16c971d7bd222254a63e"
CAPTURE_LEAF = "rust/crates/store/src/recording_capture.rs"
CAPTURE_SHA = "5dd958c783db4ee02e4bdcc67dea5080911b65e033778b1bae99675c238f313c"
EXPECTED_PATHS = frozenset("""
external-pins.json origin61.py scenario.py recording_origin/origin61.py
recording_origin/input.ts run.py fixture_adapter.py recording_origin/guide_fixture.py
recording_origin/graph_origin.py coordinator-mode.json graph/guide_fixture.py
graph/graph_origin.py graph/live_phase.py graph/live_view_guest.py graph/phase.py
graph/signed_refresh_guest.py graph/package_intake.py graph/service_identity.py
graph/worker_epoch.py relay.py physical_pg_proxy.py owned_nodelay.py fault.py
""".split())
require, read, digest = common.require, common.read, common.digest


def package(inputs, candidate):
    path = Path(candidate["build_manifest_file"])
    require(digest(path) == candidate["build_manifest_sha256"]
            and path.stat().st_size <= 24 * 1024 * 1024, "exact bounded ordinary held-Ready manifest")
    with path.open("rb") as manifest_file:
        manifest = json.load(manifest_file)
    require(manifest["git_revision"] == candidate["git"] and manifest["image_id"] == candidate["image_id"]
            and manifest["platform"] == "linux/amd64"
            and manifest["source_sha256"].get(CAPTURE_LEAF) == CAPTURE_SHA,
            "exact current null-safe recording capture source in ordinary AMD64 package")
    inputs[str(path)] = candidate["build_manifest_sha256"]
    return path


def inputs_for(stage, receiving, seal, candidate, repository):
    artifacts = (repository / "artifacts/rust").resolve(strict=True)
    require(stage.resolve() == stage and stage.parent == artifacts and seal == STAGE_SEAL,
            "exact approved held-Ready fixture")
    require(receiving.resolve() == receiving
            and receiving == artifacts / "candidate54-isolated-publication-1/repo",
            "exact receiving Git for ordinary held-Ready fixture")
    inputs, names = common.sealed_inputs(stage, seal)
    require(names == EXPECTED_PATHS, "complete exact held-Ready fixture inputs")
    require(read(stage / "coordinator-mode.json") == {
        "schema": "dispatcharr-m6-sql-ready-recording-standalone/1",
        "layout": "owned-runtime-sibling",
    }, "known reviewed held-Ready fixture driver contract")
    inputs[str(SCRIPT)] = digest(SCRIPT)
    manifest = package(inputs, candidate)
    require(manifest.is_relative_to(artifacts), "held-Ready build manifest in exact artifact tree")
    return inputs, manifest


def dispatch_spec(stage, receiving, seal, candidate, repository, entrypoint, output):
    require(output.resolve() == output and not output.is_relative_to(stage) and not stage.is_relative_to(output),
            "separate fresh held-Ready dispatcher and runtime paths")
    inputs, _ = inputs_for(stage, receiving, seal, candidate, repository)
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    coordinator_sha = digest(coordinator)
    inputs[str(coordinator)] = coordinator_sha
    tooling = repository / "artifacts/rust/tooling"
    head = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(stage), str(receiving), seal,
            json.dumps(candidate, sort_keys=True), str(repository)]
    return {
        "argv": [*head, "run", entrypoint, str(output), str(coordinator), coordinator_sha],
        "cleanup_argv": [*head, "cleanup"],
        "evaluator_argv": [*head, "evaluate", entrypoint, str(output), "@EXECUTION@"],
        "cwd": str(repository),
        "environment": {"DOCKER_HOST": common.DAEMON, "DOCKER_CONFIG": str(tooling / "docker-config"),
                        "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
        "strip_inherited_prefixes": common.STRIP_ENV,
        "input_sha256": inputs,
        # The existing independent Original and Native arm deadlines are each
        # 910 s. This bound leaves room for the frozen controller and proof.
        "seconds": 1980, "cleanup_seconds": 90, "output_bytes": 65536,
        "exclusive_heavy": True,
    }


def load_driver(stage):
    sys.path.insert(0, str(stage))
    spec = spec_from_file_location("exact_canonical_sql_ready_runtime", stage / "run.py")
    driver = module_from_spec(spec)
    spec.loader.exec_module(driver)
    return driver


def phase(driver, mode, arguments, *, borrow=None):
    evidence = StringIO()
    with redirect_stdout(evidence):
        status = driver.main([mode, *arguments], coordinator_lease=borrow)
    require(type(status) is int, "literal held-Ready driver status")
    return status, evidence.getvalue()


def exact_cleanup(driver, arguments):
    status, stdout = phase(driver, "cleanup", arguments)
    close = json.loads(stdout)
    require(status == 0 and close["exact_cleanup"] is True
            and [arm["side"] for arm in close["arms"]] == ["Original", "Native"]
            and len({arm["project"] for arm in close["arms"]}) == 2,
            "both exact held-Ready fixture arms retired")
    return {"exact_owned_absence": True, "cleanup_sha256": digest(driver.OUT / "independent-cleanup.json")}


def scope_proof(driver, arguments, candidate):
    status, stdout = phase(driver, "proof", arguments)
    proof = json.loads(stdout)
    fault = proof["actual_native_sql_rejection_held_ready"]
    require(status == 0 and proof["candidate"] == candidate["git"]
            and proof["manifest_sha256"] == candidate["build_manifest_sha256"]
            and proof["functional_scope_passed"] is True and proof["same_long_lived_context"] is True
            and proof["same_owner_graph_not_whole_milestone"] is True and proof["performance_verified"] is False
            and proof["m6_formal_credit"] is False
            and type(fault["held_seconds"]) in (float, int) and isfinite(fault["held_seconds"])
            and fault["held_seconds"] >= 6.2
            and all(fault[key] is True for key in ("same_connection_ready", "two_live_samples_while_held",
                                                   "relay_retired", "ordinary_main_fresh_fault_scope"))
            and all(type(proof["actual"][side]["actual_exit"]) is int and proof["actual"][side]["actual_exit"] == 0
                    for side in ("Original", "Native")),
            "complete SQL ErrorResponse/held Ready row in actual ordinary Main")
    return {"scope": NODE, "candidate": candidate, "sql_ready": fault,
            "cleanup_sha256": exact_cleanup(driver, arguments)["cleanup_sha256"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("receiving", type=Path)
    parser.add_argument("seal")
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("repository", type=Path)
    parser.add_argument("mode", choices=("run", "cleanup", "evaluate"))
    parser.add_argument("rest", nargs="*")
    options = parser.parse_args()
    stage, receiving, candidate, repository = options.stage, options.receiving, options.candidate, options.repository
    _, manifest = inputs_for(stage, receiving, options.seal, candidate, repository)
    output = stage / "actual-1"
    arguments = ["--repo", str(receiving), "--manifest", str(manifest),
                 "--manifest-sha", candidate["build_manifest_sha256"], "--revision", candidate["git"],
                 "--run-output", str(output)]
    driver = load_driver(stage)
    if options.mode == "cleanup":
        require(not options.rest, "exact held-Ready cleanup invocation")
        result = exact_cleanup(driver, arguments)
    elif options.mode == "evaluate":
        entrypoint, parent_output, path = options.rest
        attempt = common.attempt_directory(Path(parent_output), NODE)
        receipt = read(Path(path))
        require(Path(path) == attempt / "execution.json" and receipt["id"] == NODE
                and receipt["cleanup_verified"] is True
                and all(common.settled(receipt[phase_name]) and receipt[phase_name]["actual_exit"] == 0
                        and receipt[phase_name]["failure"] is None for phase_name in ("execution", "cleanup"))
                and Path(receipt["execution"]["stdout"]) == attempt / "execution.stdout",
                "actual exact canonical held-Ready process and retirement")
        result = scope_proof(driver, arguments, candidate)
        require(read(attempt / "execution.stdout") == result, "independently replay exact current held-Ready output")
        result.update(entrypoint=entrypoint, semantic_assertions_passed=True, full_required_workflow=True)
    else:
        entrypoint, parent_output, coordinator_path, coordinator_sha = options.rest
        binding = dispatch_spec(stage, receiving, options.seal, candidate, repository, entrypoint, Path(parent_output))
        require(binding["argv"] == sys.orig_argv and Path.cwd() == repository
                and Path(coordinator_path) == repository / "scripts/rust-milestone-batches/coordinator.py",
                "exact canonical held-Ready invocation and coordinator")
        lock = common.coordinator_parent(Path(coordinator_path), coordinator_sha, Path(parent_output),
                                         binding, candidate, node=NODE)
        require(lock.samefile(driver.SHARED / ".dispatcher.lock")
                and not os.path.lexists(output) and all(os.environ.get(key) == expected
                    for key, expected in binding["environment"].items())
                and not any(key in os.environ for key in ("DOCKER_CONTEXT", "USE_NGINX_ACCEL"))
                and all(not key.startswith("RUST_") for key in os.environ),
                "borrow one live coordinator lease in fresh ordinary runtime")
        docker = repository / "artifacts/rust/tooling/docker/docker"
        require(Path(shutil.which("docker") or "").resolve() == docker,
                "exact owned isolated engine executable")
        os.umask(0o077)

        @contextmanager
        def borrow(expected):
            require(expected.samefile(lock) and common.parent_holds_lock(Path("/proc") / str(os.getppid()), lock),
                    "held-Ready run uses the live coordinator lease")
            yield

        status, cold = phase(driver, "preflight", arguments)
        qualification = json.loads(cold)
        require(status == 0 and qualification["engine_calls"] == 0 and qualification["final_package_bound"] is True
                and qualification["candidate"] == candidate["git"] and qualification["image_id"] == candidate["image_id"],
                "exact final package held-Ready fixture preflight")
        status, _ = phase(driver, "run", arguments, borrow=borrow)
        if status:
            return status
        result = scope_proof(driver, arguments, candidate)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signal, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    raise SystemExit(main())
