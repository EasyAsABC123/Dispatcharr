"""Exact grouped held-guide / recording owner. A successful scope stays partial."""
import argparse
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from hashlib import file_digest
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
NODE = "m6/playback-during-refresh"
ENTRY = "production-Main; owner full workflow evaluator required"
COMPOSE_DAEMON = "unix:///tmp/dispatcharr-docker.sock"
PHASES = ("preflight", "run", "cleanup", "proof")
WORKFLOW_PINS = {
    'coordinator-mode.json': 'f06b66bbd56a2aff0b57cdb7b64e9dae6c833c26a4f8e79a138d2b6aa793440b',
    'external-pins.json': '42593f8ce7628b3e56e7bf205a31de950fbc0dcbfe1a9de1a478e55634ac6dd4',
    'fixture_adapter.py': '3fddadbfb9b1a56e2dcf991b0c67e60db339a956931824a97981eca666ec68ce',
    'graph/graph_origin.py': 'dc09b1435c1f6deb47f18a5f722e8f3af068bbe5d214bbe7ca67542c510f1aab',
    'graph/guide_fixture.py': '00b3e1bd31baa3cb3abdbabb5d030ae84821d1e97210adb8da97495fac07b0e3',
    'graph/live_phase.py': '4add771fdf620451860267560fd89afdbcccf686bb89fe41e92a9c913855bab8',
    'graph/live_view_guest.py': 'b8dd896c584abf8c9e11f1880642809b13d81414415aa7cacc72dcca80929424',
    'graph/package_intake.py': '1ca7ea7543e346a187648d309232c44700364212ac46fd7ac1a7dabbb5d48468',
    'graph/phase.py': '55100154ddf310422a9b077d875ef73767e000e3fe2d9d7b01271e3e71d8f635',
    'graph/signed_refresh_guest.py': '362fd2357da45d8f19c8a8803b28c279ab86c43d08188ac16cab87706adf2572',
    'origin61.py': '7bcc35d1d57dcf78e97e599de719e928a641c4dfe273cdb30aada69b86df8355',
    'recording_origin/graph_origin.py': 'dc09b1435c1f6deb47f18a5f722e8f3af068bbe5d214bbe7ca67542c510f1aab',
    'recording_origin/guide_fixture.py': '00b3e1bd31baa3cb3abdbabb5d030ae84821d1e97210adb8da97495fac07b0e3',
    'recording_origin/input.ts': '8ca8ddcd4813c0ea6345d9c1b97d37357035b8cb46b4a6138142a938f4b4412c',
    'recording_origin/origin61.py': '5e66d647e733b3e3a9df044413696d215ad62e9818072c9b1512534715e9d1a4',
    'run.py': '5d06919bcab323788660c371df8135248e069b926cd73697fcd6dd1ec6e6ae5c',
    'scenario.py': '0569b4245e2b860fd889a6909e8edd31a40f36efba7c1123d7f5fa5209d2d6e8',
}
STRIP = ("DOCKER_", "RUST_", "CARGO_", "PYTHON", "USE_NGINX_ACCEL", "GIT_", "COMPOSE_", "CELERY_", "POSTGRES_",
         "DJANGO_", "REDIS_", "DATABASE_", "FIXTURE_", "BENCHMARK_")


def need(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    need(path.is_file() and path.resolve() == path and not path.is_symlink(), "exact regular bound input")
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def load(path, name):
    spec = spec_from_file_location(name, path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read(path):
    need(path.resolve() == path and path.is_file() and path.stat().st_size <= 8 * 1024 * 1024,
         "bounded exact owner input")
    return json.loads(path.read_bytes())


def inputs(stage, seal, candidate, repository, receiving):
    artifact_root = repository / "artifacts/rust"
    need(repository.resolve() == repository and stage.resolve() == stage and stage.parent == artifact_root
         and receiving.resolve() == receiving and receiving.is_dir()
         and receiving.is_relative_to(artifact_root) and receiving != stage,
         "explicit approved repository, receiving and owner paths")
    need(stage.is_dir() and stage.stat().st_mode & 0o222 == 0, "read-only reviewed fixture stage required")
    sums = stage / "SHA256SUMS"
    need(sha(sums) == seal and sums.stat().st_size <= 16384, "exact declared held overlap owner seal")
    files = {str(sums): seal}
    names = set()
    for row in sums.read_text().splitlines():
        digest, name = row.split("  ", 1)
        need(name in WORKFLOW_PINS and name not in names and digest == WORKFLOW_PINS[name]
             and sha(stage / name) == digest and (stage / name).stat().st_mode & 0o222 == 0,
             "exact reviewed held overlap workflow bytes")
        files[str(stage / name)] = digest
        names.add(name)
    need(names == WORKFLOW_PINS.keys(), "complete staged held overlap executable closure")
    manifest = Path(candidate["build_manifest_file"])
    need(manifest.is_relative_to(artifact_root) and manifest.name == "build.json"
         and sha(manifest) == candidate["build_manifest_sha256"], "exact shared final ordinary package")
    package = read(manifest)
    need(package["git_revision"] == candidate["git"] and package["image_id"] == candidate["image_id"]
         and package["passed"] is True and package["exit_code"] == 0
         and package["platform"] == "linux/amd64",
         "same passing final application and physical AMD64 image")
    files[str(manifest)] = candidate["build_manifest_sha256"]
    return files


def dispatch_spec(stage, seal, candidate, repository, receiving, entrypoint, output):
    stage, repository, receiving, output = map(Path, (stage, repository, receiving, output))
    need(entrypoint == ENTRY and output.resolve() == output and not output.is_relative_to(stage)
         and not stage.is_relative_to(output), "separate declared canonical output")
    bound = inputs(stage, seal, candidate, repository, receiving)
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    coordinator_sha = sha(coordinator)
    own_script = HERE / "playback_overlap_owner.py"
    bound[str(own_script)] = sha(own_script)
    bound[str(coordinator)] = coordinator_sha
    sibling = HERE / "recording61_owner.py"
    bound[str(sibling)] = sha(sibling)
    prefix = ["/usr/bin/python3", "-I", "-B", str(own_script), str(stage), seal,
              json.dumps(candidate, sort_keys=True), str(repository), str(receiving), str(output)]
    return {"argv": [*prefix, "run"], "cleanup_argv": [*prefix, "cleanup"],
            "evaluator_argv": [*prefix, "evaluate", "@EXECUTION@"],
            "cwd": str(repository),
            "environment": {"DOCKER_HOST": COMPOSE_DAEMON, "DOCKER_CONFIG": str(repository / "artifacts/rust/tooling/docker-config"),
                            "PATH": f"{repository / 'artifacts/rust/tooling/docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes": list(STRIP), "input_sha256": bound,
            "seconds": 1640, "cleanup_seconds": 75, "output_bytes": 65536,
            "exclusive_heavy": True}


def run_phases(runtime, driver, args, lock, actual_parent, parent_holds_lock):
    @contextmanager
    def borrow(expected):
        need(Path(expected) == lock and parent_holds_lock(actual_parent, lock)
             and str(os.getppid()) == actual_parent.name, "same actual root dispatcher lease")
        yield

    runtime.mkdir(mode=0o700)
    ledger = runtime / "owner-1"
    ledger.mkdir(mode=0o700)

    def step(name):
        code = 1
        try:
            with (ledger / f"{name}.stdout").open("x") as out, (ledger / f"{name}.stderr").open("x") as err:
                with redirect_stdout(out), redirect_stderr(err):
                    code = driver.main([name, *args], coordinator_lease=borrow)
            return code
        finally:
            (ledger / f"{name}.actual-exit").write_text(f"{code}\n")

    exit_code = 1
    try:
        exit_code = step("preflight")
        if exit_code:
            return exit_code
        try:
            exit_code = step("run")
        finally:
            cleanup = step("cleanup")
        if exit_code == 0:
            exit_code = cleanup or step("proof")
        return exit_code
    except BaseException:
        exit_code = 1
        raise
    finally:
        (ledger / "campaign.actual-exit").write_text(f"{exit_code}\n")


def teardown(driver):
    actual = driver.cleanup()
    need(actual == read(driver.OUT / "independent-cleanup.json")
         and actual["exact_cleanup"] is True and not os.path.lexists(driver.MARKER),
         "exact original cleanup receipts and retired marker")
    return {"exact_owned_absence": True, "physical_cleanup": actual}


def partial_evaluation(runtime, candidate, journal, driver, args):
    verified = teardown(driver)
    execution = read(journal)
    run = execution["execution"]
    need(execution["cleanup_verified"] is True and run["actual_exit"] == 0 and run["failure"] is None
         and all(run[k] is True for k in ("stdout_EOF", "stderr_EOF", "process_group_settled")),
         "actual settled partial execution and cleanup")
    ledger = runtime / "owner-1"
    need(all((ledger / f"{name}.actual-exit").read_bytes() == b"0\n" for name in (*PHASES, "campaign")),
         "typed owner retained actual successful phase exits")
    proof = read(ledger / "proof.stdout")
    import io
    replay = io.StringIO()
    with redirect_stdout(replay):
        need(driver.main(["proof", *args]) == 0, "replayed exact existing held overlap proof")
    need(proof == json.loads(replay.getvalue()), "actual owner reduction reverified from raw scope")
    need(proof["scope"] == "shared-context-held-live-two-guide-imports-and-recording61"
         and proof["same_long_lived_context"] is True and proof["held_refresh_same_viewers_decodes"] == 4
         and proof["full_strict_eof_matched"] is True and proof["m6_formal_credit"] is False
         and proof["physical_cleanup"] == verified["physical_cleanup"]
         and proof["candidate"] == candidate["git"] and proof["manifest_sha256"] == candidate["build_manifest_sha256"],
         "actual scoped behaviour and exact common candidate")
    return {"entrypoint": ENTRY, "candidate": candidate, "semantic_assertions_passed": True,
            "scope_receipt_sha256": sha(ledger / "proof.stdout"), "partial_result_observed": True,
            "full_required_workflow": False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("stage", type=Path)
    p.add_argument("seal")
    p.add_argument("candidate", type=json.loads)
    p.add_argument("repository", type=Path)
    p.add_argument("receiving", type=Path)
    p.add_argument("output", type=Path)
    p.add_argument("mode", choices=("run", "cleanup", "evaluate"))
    p.add_argument("journal", type=Path, nargs="?")
    a = p.parse_args()
    binding = dispatch_spec(a.stage, a.seal, a.candidate, a.repository, a.receiving, ENTRY, a.output)
    control = load(HERE / "recording61_owner.py", "m6_existing_recording_parent_guard")
    sys.path.insert(0, str(a.stage))
    driver = load(a.stage / "run.py", "m6_held_overlap_recording_driver")
    attempt = control.attempt_directory(a.output, NODE)
    runtime = attempt / "scoped-owner"
    driver.OUT = runtime / "actual-1"
    args = ["--repo", str(a.receiving), "--manifest", a.candidate["build_manifest_file"],
            "--manifest-sha", a.candidate["build_manifest_sha256"], "--revision", a.candidate["git"],
            "--run-output", str(driver.OUT)]
    if a.mode == "run":
        coordinator = a.repository / "scripts/rust-milestone-batches/coordinator.py"
        lock = control.coordinator_parent(coordinator, sha(coordinator), a.output, binding, a.candidate, node=NODE)
        return run_phases(runtime, driver, args, lock, Path("/proc") / str(os.getppid()), control.parent_holds_lock)
    if a.mode == "cleanup":
        result = teardown(driver)
    else:
        need(a.journal == attempt / "execution.json", "same actual scoped dispatch journal required")
        result = partial_evaluation(runtime, a.candidate, a.journal, driver, args)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
