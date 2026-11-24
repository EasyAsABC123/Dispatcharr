"""Late-bound API202 owner for the Root coordinator, with offline admission."""

import argparse
from contextlib import contextmanager, redirect_stdout
from hashlib import file_digest, sha256
import importlib.util
import io
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tarfile

ANCESTRY_FLOOR = "a2bd0d663a3ee8169ce80755f6752e87131d34f4"
SEAL = "7656787108fd7df5ccd75cd5c21d44183ea409f2d3c7af1c0fc676b1d14b0e54"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
CUSTODY_PINS = {
    "http": "d92268c5212686770d8f739cc25237efc4b6cfb179fc9024fa07c0a9de374841",
    "worker": "20de9069d142545ae8c662bb71e19b35df113f1bd8c93c3281be889f0f0c56bc",
    "fifo_reducer": "b38130ed5be888d8e5fd57508ea29b4845806ffa22dcac728c12381519612185",
    "positive_reducer": "e6b50e028f83fff993623d255a050cab5a9fe978a8f07fc11a9051028a7c0f7d",
}
HELPER_SHA = "f66fc1cbcbe48b39720e9163437608c96817ba7532b789cdbc590024568f247f"
NODE = "m6/api202-publication"
ENTRY = "production-Main; owner full workflow evaluator required"
KIND = "api202-exact-ordinary-owner-v1"
HERE = Path(__file__).resolve().parent
LEAVES_SHA = "5eb36222b29b02530c0d45f9f8ebd39433296519c0a2f803c5e695f3e13496c6"


def need(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    need(path.is_absolute() and path.resolve() == path and path.is_file(), "exact regular evidence file required")
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def read(path):
    digest(path)
    need(path.stat().st_size <= 16 * 1024 * 1024, "bounded evidence JSON required")
    return json.loads(path.read_bytes())


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git(*args, repo):
    return subprocess.check_output(
        ["git", "--no-optional-locks", "-C", str(repo), *args],
        timeout=20, stderr=subprocess.DEVNULL)


def product_ready(stage, receiving, revision):
    git("merge-base", "--is-ancestor", ANCESTRY_FLOOR, revision, repo=receiving)
    leaves = HERE / "api202-leaves.sha256"
    need(digest(leaves) == LEAVES_SHA, "exact reviewed product fingerprint list required")
    for spec in (stage / "product-postimages.sha256", leaves):
        for line in spec.read_text().splitlines():
            expected, name = line.split("  ", 1)
            need(sha256(git("show", f"{revision}:{name}", repo=receiving)).hexdigest() == expected,
                 "reviewed positive FIFO product leaf differs")


def receiving_ready(stage, receiving, candidate, authority, files, artifacts):
    revision = candidate["git"]
    need(re.fullmatch("[0-9a-f]{40}", revision) and receiving.is_absolute()
         and receiving.resolve() == receiving and receiving.is_relative_to(artifacts)
         and git("rev-parse", "HEAD", repo=receiving).decode().strip() == revision
         and git("status", "--porcelain", "--untracked-files=all", repo=receiving) == b"",
         "exact clean final receiving snapshot required")
    remote = Path(authority["remote_ref_receipt"])
    ref = authority["remote_ref"]
    need(re.fullmatch(r"refs/(heads|tags)/[A-Za-z0-9_./-]+", ref)
         and digest(remote) == authority["remote_ref_sha256"]
         and remote.read_bytes() == f"{revision}\t{ref}\n".encode(),
         "independently retained remote ref must equal receiving and candidate")
    files[str(remote)] = authority["remote_ref_sha256"]
    product_ready(stage, receiving, revision)


def pins(stage, seal, custody, artifacts):
    need(stage.parent == artifacts and seal == SEAL, "explicit exact positive FIFO owner required")
    sums = stage / "SHA256SUMS"
    need(digest(sums) == seal, "exact positive FIFO owner seal required")
    result = {str(sums): seal}
    for line in sums.read_text().splitlines():
        wanted, name = line.split("  ", 1)
        path = stage / name
        need(path.parent == stage and str(path) not in result and digest(path) == wanted,
             "exact positive FIFO owner leaf required")
        result[str(path)] = wanted
    need(len(result) == 27 and set(custody) == set(CUSTODY_PINS), "26 owner leaves and explicit Original custody required")
    paths = {key: Path(value) for key, value in custody.items()}
    for key, path in paths.items():
        wanted = CUSTODY_PINS[key]
        need(path.is_relative_to(artifacts) and digest(path) == wanted,
             "fixed Original or reducer custody differs")
        result[str(path)] = wanted
    worker = read(paths["worker"])
    need(worker["source"] == ORIGINAL and worker["scope_passed"] is True
         and worker["exact_owned_cleanup"] is True and worker["canonical_row_passed"] is False,
         "Original worker observations are scoped")
    producer = load(stage / "producer.py", "api202_route_source")
    producer.SOURCE = paths["http"]
    rows = producer.authority()
    need(len(rows) == 37 and sum(len(row["envelopes"]) for row in rows) == 16,
         "fixed Original wide roster required")
    return result


def driver_context(paths, coordinator, coordinator_sha):
    return paths | {"coordinator": str(coordinator), "coordinator_sha256": coordinator_sha,
                    "lock": str(coordinator.with_name(".dispatcher.lock"))}


def owner_layout(driver, paths, artifacts, coordinator):
    driver.configure(paths)
    need(driver.ART == artifacts and driver.SHARED == coordinator.parent
         and driver.missing_scenario.producer.SOURCE == Path(paths["http"])
         and driver.missing_scenario.fifo_gate.PROOF == Path(paths["fifo_reducer"])
         and driver.missing_scenario.positive_fixture.CHECKER == Path(paths["positive_reducer"]),
         "sealed owner requires its existing external custody and dispatcher layout")


def verify_owner_layout(stage, paths, artifacts, coordinator, *, fence=False):
    # Import only. Do not dispatch the external owner or open an engine.
    code = (
        "import json,sys\n"
        "def no_engine(event, args):\n"
        "    if event.startswith(('subprocess.', 'socket.', 'os.spawn', 'os.exec', 'os.system', 'os.posix_spawn')):\n"
        "        raise RuntimeError('layout preflight cannot start processes or access the engine')\n"
        "sys.addaudithook(no_engine)\n"
        "sys.path[:0]=sys.argv[1:3]\n"
        "import api202_owner as owner; "
        "driver=owner.load(owner.Path(sys.argv[1])/'run.py','api202_offline_layout'); "
        "owner.owner_layout(driver,json.loads(sys.argv[3]),owner.Path(sys.argv[4]),owner.Path(sys.argv[5])); "
        "driver.first_fence() if sys.argv[6]=='fence' else None"
    )
    subprocess.check_call(
        ["/usr/bin/python3", "-I", "-B", "-c", code, str(stage), str(HERE),
         json.dumps(paths, sort_keys=True), str(artifacts), str(coordinator), "fence" if fence else "layout"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)


def inspected_package(candidate, build, authority, files):
    audit = Path(authority["package_audit"])
    need(digest(audit) == authority["package_audit_sha256"], "independent package audit bytes differ")
    proof = read(audit)
    need(proof["passed"] is True and type(proof["actual_build_wait_exit"]) is int
         and proof["actual_build_wait_exit"] == 0 and proof["all_archive_files_byteexact_Git"] is True
         and proof["actual_ELF_magic"] is True and proof["actual_production_entrypoint"] is True
         and proof["exact_probe_cleanup"] is True
         and proof["git_revision"] == candidate["git"] and proof["image_id"] == candidate["image_id"]
         and proof["source_tree_sha256"] == build["source_tree_sha256"]
         and re.fullmatch("[0-9a-f]{64}", authority["production_binary_sha256"])
         and proof["production_binary_sha256"] == authority["production_binary_sha256"],
         "independent exact package and application binary audit required")
    files[str(audit)] = authority["package_audit_sha256"]


def verify_archive(archive, mapping):
    seen = set()
    with tarfile.open(archive, "r|*") as stream:
        for entry in stream:
            name = entry.name
            if entry.isdir():
                continue
            need(entry.isfile() and name in mapping and name not in seen,
                 "unique reviewed release archive file required")
            with stream.extractfile(entry) as leaf:
                need(file_digest(leaf, "sha256").hexdigest() == mapping[name],
                     "release archive leaf differs from complete source map")
            seen.add(name)
    need(seen == mapping.keys(), "complete release archive required")


def package(stage, candidate, files, receiving, authority, artifacts):
    manifest = Path(candidate["build_manifest_file"])
    archive = Path(candidate["source_archive_file"])
    source_map = Path(candidate["source_map_file"])
    need(manifest.is_relative_to(artifacts) and manifest.name == "build.json"
         and archive == manifest.with_name("source.tar"), "same-package manifest and archive required")
    for path, key in ((manifest, "build_manifest_sha256"), (archive, "source_archive_sha256"),
                      (source_map, "source_map_sha256")):
        need(digest(path) == candidate[key], "exact current package bytes required")
        files[str(path)] = candidate[key]
    build = read(manifest)
    need(build["git_revision"] == candidate["git"] and build["image_id"] == candidate["image_id"]
         and build["source_archive_sha256"] == candidate["source_archive_sha256"],
         "candidate manifest identity differs")
    mapping = read(source_map)
    if candidate.get("source_map_format") == "files_path_to_sha256":
        mapping = mapping["files"]
    else:
        need(candidate.get("source_map_format", "path_to_sha256") == "path_to_sha256", "unknown source map format")
    need(mapping == build["source_sha256"] and sha256(json.dumps(
        mapping, sort_keys=True, separators=(",", ":")).encode()).hexdigest() == build["source_tree_sha256"],
         "complete integrated release source map differs")
    verify_archive(archive, mapping)
    inspected_package(candidate, build, authority, files)
    role = manifest.parents[2] / "ordinary-launch-1"
    receipts = [manifest.parents[1] / "actual-exit", role / "actual-exit",
                role / "ordinary-build.actual-exit", role / "ordinary-verify.actual-exit",
                role / "ordinary-verify.stdout"]
    for path in receipts:
        files[str(path)] = digest(path)
    args = ["--repo", str(receiving), "--manifest", str(manifest),
            "--manifest-sha", candidate["build_manifest_sha256"], "--revision", candidate["git"],
            "--image", candidate["image_id"]]
    # The external owner uses bare sibling imports. Keep them out of the
    # shared coordinator process and make the receipt comparison offline.
    check = ("import json,sys; sys.path.insert(0,sys.argv[1]); "
             "import transition; print(json.dumps(transition.intake(sys.argv[2:])))")
    checked = subprocess.check_output(
        ["/usr/bin/python3", "-I", "-B", "-c", check, str(stage), *args],
        stderr=subprocess.DEVNULL, timeout=120)
    need(json.loads(checked) == build, "exact ordinary build and signed phase verification required")
    return args


def attempt_dir(output):
    return output / ("m6__api202-publication-" + sha256(NODE.encode()).hexdigest()[:12])


def dispatch_spec(context, candidate, repository, node, output):
    stage, receiving = (Path(context[key]) for key in ("stage", "receiving"))
    artifacts = Path(context["artifacts"])
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    helper = HERE / "recording61_owner.py"
    need(artifacts.is_dir() and artifacts.resolve() == artifacts and repository.resolve() == repository and HERE == coordinator.parent / "m6"
         and node["id"] == NODE and node["entrypoint"] == ENTRY
         and node["depends_on"] == [] and node["resources"] == ["m6/ingestion-api"],
         "exact first ingestion row, tracked dispatcher path and predecessor policy required")
    need(output is not None and output.is_absolute() and output.resolve() == output
         and output.is_relative_to(artifacts) and not output.is_relative_to(stage)
         and not stage.is_relative_to(output), "separate owned coordinator output required")
    git("ls-files", "--error-unmatch", "--", str(coordinator.relative_to(repository)),
        str(Path(__file__).resolve().relative_to(repository)), repo=repository)
    files = pins(stage, context["seal"], context["original_custody"], artifacts)
    receiving_ready(stage, receiving, candidate, context["authority"], files, artifacts)
    package(stage, candidate, files, receiving, context["authority"], artifacts)
    need(digest(helper) == HELPER_SHA, "exact existing dispatcher parent guard required")
    coordinator_sha = digest(coordinator)
    verify_owner_layout(stage, driver_context(context["original_custody"], coordinator, coordinator_sha),
                        artifacts, coordinator)
    for path in (Path(__file__).resolve(), coordinator, helper, HERE / "api202-leaves.sha256"):
        files[str(path)] = digest(path)
    prefix = ["/usr/bin/python3", "-I", "-B", str(Path(__file__).resolve()),
              json.dumps(context, sort_keys=True), json.dumps(candidate, sort_keys=True),
              str(repository), str(output)]
    return {"argv": [*prefix, "run", coordinator_sha], "cleanup_argv": [*prefix, "cleanup", coordinator_sha],
            "evaluator_argv": [*prefix, "evaluate", coordinator_sha, "@EXECUTION@"],
            "api202_binding": KIND, "cwd": str(repository),
            "environment": {"DOCKER_HOST": "unix:///tmp/dispatcharr-docker.sock",
                            "DOCKER_CONFIG": str(artifacts / "tooling/docker-config"),
                            "PATH": f"{artifacts / 'tooling/docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes": ["DOCKER_", "COMPOSE_", "RUST_EXPERIMENTAL_", "PYTHON"],
            "input_sha256": files, "seconds": 640, "cleanup_seconds": 120,
            "output_bytes": 65536, "exclusive_heavy": True}


def settled(result, directory, label, *, success=True):
    return (type(result["actual_exit"]) is int and (not success or result["actual_exit"] == 0)
            and result["failure"] is None and result["remaining_group_members"] == []
            and all(result[key] is True for key in ("stdout_EOF", "stderr_EOF", "process_group_settled"))
            and all(Path(result[key]) == directory / f"{label}.{key}" for key in ("stdout", "stderr")))


def semantic(proof, candidate):
    values = proof["native"]
    need(proof["candidate"] == candidate["git"] and proof["manifest_sha256"] == candidate["build_manifest_sha256"]
         and [values[k] for k in ("response_matches", "publish_matches", "nonpublish_matches",
                                   "settled_worker_roots", "matched_Original_typed_results",
                                   "scoped_local_failure_count")] == [37, 16, 21, 16, 4, 4]
         and values["fifo_gate"] == {"right_head_removed": True, "unchanged_younger_siblings": 2,
                                     "canonical_api202_passed": False}
         and sorted((row["case"], row["typed_terminal_count"], row["canonical_api202_passed"])
                    for row in values["positive_landings"]) == [("bool", 5, False), ("float", 5, False)]
         and values["canonical_api202_passed"] is False, "full scoped API202 evidence required")


def reduce(driver, mode, args):
    stream = io.StringIO()
    with redirect_stdout(stream):
        need(driver.main([mode, *args]) == 0, "owner offline reduction failed")
    return json.loads(stream.getvalue())


def retired(closure, retained):
    need(closure == retained and closure["exact_cleanup"] is True,
         "exact independently retired fixture required")


def install_stage(stage):
    if str(stage) not in sys.path:
        sys.path.insert(0, str(stage))


def route(context, candidate, repository, output, coordinator_sha, mode, journal):
    node = {"id": NODE, "entrypoint": ENTRY, "depends_on": [], "resources": ["m6/ingestion-api"]}
    bound = dispatch_spec(context, candidate, repository, node, output)
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    if mode == "binding":
        need(not os.path.lexists(output), "fresh exclusive coordinator output required")
        return {"candidate": candidate, "owner_bindings": {"m6": {"api202_publication": context}},
                "nodes": {NODE: bound}}
    need(bound["input_sha256"][str(coordinator)] == coordinator_sha, "same bound dispatcher bytes required")
    directory = attempt_dir(output)
    attempt_path = directory / "attempt.json"
    attempt = read(attempt_path)
    need(attempt["candidate"] == candidate and attempt["binding"] == bound
         and all(attempt["node"][key] == value for key, value in node.items())
         and type(attempt["attempt"]) is int and attempt["attempt"] == 1, "exact coordinator attempt required")
    runtime = directory / "actual-1"
    token = directory / "api202-dispatch-start.json"
    stage, receiving = (Path(context[key]) for key in ("stage", "receiving"))
    artifacts = Path(context["artifacts"])
    paths = driver_context(context["original_custody"], coordinator, coordinator_sha)
    args = ["--repo", str(receiving), "--manifest", candidate["build_manifest_file"],
            "--manifest-sha", candidate["build_manifest_sha256"], "--revision", candidate["git"],
            "--image", candidate["image_id"], "--run-output", str(runtime),
            "--owner-context", json.dumps(paths, sort_keys=True)]
    install_stage(stage)
    driver = load(stage / "run.py", "api202_ordinary_runtime")
    owner_layout(driver, paths, artifacts, coordinator)
    if mode == "run":
        driver.first_fence()
        control = load(HERE / "recording61_owner.py", "api202_dispatcher_guard")
        lock = control.coordinator_parent(coordinator, coordinator_sha,
                                          output, bound, candidate, node=NODE)
        need(driver.SHARED / ".dispatcher.lock" == lock and not os.path.lexists(runtime),
             "same existing dispatcher lock and fresh attempt required")
        with token.open("x") as stream:
            json.dump({"attempt": digest(attempt_path), "binding": KIND}, stream)
            stream.flush()
            os.fsync(stream.fileno())

        @contextmanager
        def borrow(expected):
            need(expected == lock and control.parent_holds_lock(Path("/proc") / str(os.getppid()), lock),
                 "actual coordinator-held lock required")
            yield

        return driver.main(["run", *args], coordinator_lease=borrow)

    need(read(token) == {"attempt": digest(attempt_path), "binding": KIND}, "same runtime start receipt required")
    child = read(runtime / "Native-execution.json")
    need(settled(child, runtime, "Native", success=False), "actual Native arm must have settled")
    closure = reduce(driver, "cleanup", args)
    retired(closure, read(runtime / "independent-cleanup.json"))
    if mode == "cleanup":
        return {"exact_owned_absence": True, "physical_cleanup": closure}
    need(journal == directory / "execution.json", "same coordinator execution journal required")
    record = read(journal)
    need(record["id"] == NODE and record["cleanup_verified"] is True and settled(child, runtime, "Native")
         and settled(record["execution"], directory, "execution")
         and settled(record["cleanup"], directory, "cleanup")
         and read(directory / "cleanup.stdout") == {"exact_owned_absence": True, "physical_cleanup": closure},
         "actual execution and cleanup receipts required")
    proof = reduce(driver, "proof", args)
    need(proof["physical_cleanup"] == closure, "same retired fixture proof required")
    semantic(proof, candidate)
    return {"candidate": candidate, "entrypoint": ENTRY, "semantic_assertions_passed": True,
            "full_required_workflow": True, "api202_binding": KIND,
            "attempt_sha256": digest(attempt_path)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context", type=json.loads)
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("repository", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("mode", choices=("binding", "run", "cleanup", "evaluate"))
    parser.add_argument("coordinator_sha", nargs="?")
    parser.add_argument("journal", type=Path, nargs="?")
    a = parser.parse_args()
    try:
        result = route(a.context, a.candidate, a.repository, a.output, a.coordinator_sha, a.mode, a.journal)
    except Exception:
        print("API202 owner admission or receipt refused", file=sys.stderr)
        return 1
    if type(result) is int:
        return result
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
