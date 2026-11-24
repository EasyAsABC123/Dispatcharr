"""One exact saved Version503 Native run against retained Original authority."""
import argparse
from base64 import b64decode
from datetime import datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import signal
import sys
from tempfile import TemporaryDirectory
import time
from types import FunctionType
from uuid import UUID

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
try:
    import version200_owner as version
finally:
    sys.path.pop(0)

common, scheduler = version.common, version.scheduler
require, read, digest = common.require, common.read, common.digest
SCRIPT = Path(__file__).resolve()
NODE = "m6/version503"
PREDECESSOR = "m6/account-warning-expiration"
STAGE_SEAL = "03e746758a701d1902557740aa56f6f5bbec5d7b7f74e856d56ff48b4f15a3b1"
OWNER_SHA = "5615802be1f5ece5fa2a991758366dc77957724cd8476bbd8c0a10ec53c06263"
ORIGINAL_PACKET = "m6-a8ea333d-core-version503-pair-1"
ORIGINAL_PROOF = "43e0bc415e02ba59432f91c8c004115013db72b3694c2d80c1c66d7402f59dab"
ORIGINAL_INPUTS = {
    "state.json": "5c1a7e24cf4d62d8eaeafedfaf84f8ebe587d69933325a8077dc003c63af6630",
    "prepared-daily-row.json": "8456543e0028b09f73d018b5740a0dc08b3a544325060b27d8093d9b11fdd9df",
    "armed-daily-row.json": "29c94235735a0f0e5e5a1abf7ba9d6d7e9b0ee4647aa68e7f3de6c7d44146ead",
    "terminal-daily-row.json": "878ef03da6f76731030beb393e05cc7c171fe8d90cf1f8b81d6ff782d1d552cc",
    "first-terminal-observation.json": "51659d1e12e248950286ba9589e934d30eaff6d6e9cad4370d9270db5e2dcd79",
    "events-stopped.json": "8f3f17f7807ce673ee769d60275a89b3deeb053a6682dbec35137682615a8cc9",
    "origin-stop.json": "41ca9dfcbc248fbe3b91d92235e8c0708e3ea99e71e9121da188e64ae0b167b0",
    "origin-requests.jsonl": "58c85341f7ac772f28af3fdaa9274edf26858dc97f7dc79a3ed5d8cf83efeb1c",
    "exact-host-identities.json": "5973179add62f8eaa5ecfbd1e9bbf4ee6333e98c5e34159fb884abb20e6159d7",
    "private-startup-monitor.jsonl": "f7cc0077da380562a240b9978974ad1b71328b5da5a82ceb89dd9b60cb774a18",
    "final.json": "b53d20eeac728968e8e5a7017f3ef2bcc92b91092e95a88ae0aa3049839b3c03",
    "bootstrap-stop-status": "9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa",
}
READINESS = """def selected_readiness():
    with urlopen("http://127.0.0.1:9192/health/ready", timeout=3) as response:
        app = json.load(response)
        graph = app.get("selected_workers", {})
        ordinary = response.status == 200 and app.get("status") == "ready" and app.get("backend") == "rust"
        core = graph.get("clocked_scheduler_scope") == "core-saved-families"
        lanes = all(str(graph.get(lane, "")).startswith(("Working(", "Observed(")) for lane in ("celery", "dvr"))
        return {"ready": ordinary and core and lanes and graph.get("ready") is True
                         and graph.get("clocked_scheduler_enabled") is True and graph.get("startup") == "Started",
                "ordinary_aio": ordinary, "core_saved_families": core, "both_lanes_accepting": lanes}

"""


def authority(stage):
    require(digest(stage / "run.py") == OWNER_SHA, "exact approved Version503 workload files")
    current = common.load_module(stage / "run.py", "reviewed_saved_version503")
    retained, daily, inputs, phase = current.authority()
    text = inputs["schedule.py"].decode()
    start = text.index("def selected_readiness():")
    end = text.index("\ndef daily(mode):", start)
    approved = text[:start] + READINESS + text[end:]
    compile(approved, "native_saved_version503_readiness.py", "exec")
    inputs["schedule.py"] = approved.encode()
    return current, retained, daily, inputs, phase


def static(stage, candidate):
    current, _, daily, _, phase = authority(stage)
    # The inherited role projector expects a tag. Runtime uses the verified
    # immutable image ID instead of resolving the build tag.
    with TemporaryDirectory(prefix="m6-version503-native-cold-") as scratch:
        root = Path(scratch)
        model, database = daily.composition(root, root / "tools", root / "Original",
                                             "dispatcharr-version503-daily-000000000001", "native", candidate, 1)
        daily.preflight_import_path(root, database)
        services = model["services"]
        worker = services["worker"]
        require(services["baseline"]["network_mode"] == "none"
                and set(services) == {"baseline", "worker", "origin", "journal", "events"}
                and all("ports" not in role for role in services.values())
                and worker["image"] == candidate["tag"] and worker["pull_policy"] == "never",
                "same disconnected saved Version fixture on immutable packaged Main")
        current.verify_environment(worker["environment"])
    require(phase.__globals__["composition"] is daily.composition, "same executed ordinary saved Version phase")


def wire(root):
    settled = read(root / "final.json")
    require(all(settled[k] is True for k in ("terminal_barrier_observed", "terminal_barrier_fsynced",
                                            "thread_settled", "writer_closed"))
            and settled["terminal_reader_error"] is settled["observer_error"] is None, "actual fenced monitor closure")
    path = root / "private-startup-monitor.jsonl"
    require(path.resolve() == path and 0 < path.stat().st_size <= 16 * 1024 * 1024, "bounded exact actual Version503 wire")
    roots = []
    with path.open() as raw:
        for line in raw:
            observed = json.loads(line)
            parts = observed["command"].split(" ", 2)
            if observed["db"] == 1 and len(parts) == 3 and parts[0].upper() in ("LPUSH", "RPUSH") and parts[1] in ("celery", "dvr"):
                frame = json.loads(parts[2])
                roots.append((parts[1], json.loads(frame) if isinstance(frame, str) else frame))
    (queue, frame), = roots
    headers, data = frame["headers"], frame["properties"]
    task = headers["id"]
    template = {"callbacks": None, "errbacks": None, "chain": None, "chord": None}
    require(queue == "celery" and str(UUID(task)) == task and UUID(task).version == 4
            and headers["task"] == version.TASK and headers["periodic_task_name"] == version.ROW
            and headers["root_id"] == data["correlation_id"] == task and headers["parent_id"] is None
            and headers["eta"] is None and headers["timelimit"] == [None, None]
            and data["delivery_info"] == {"exchange": "", "routing_key": "celery"}
            and json.loads(b64decode(frame["body"], validate=True)) == [[], {}, template],
            "one exact actual saved Version503 root and no startup publication")
    return task, digest(path)


def business(root, retained, side):
    task, journal = wire(root)
    state = read(root / "state.json")
    results = read(root / "first-terminal-observation.json")
    meta = results["backend"]
    rows = [read(root / (label + "-daily-row.json")) for label in ("prepared", "armed", "terminal")]
    initial, armed, last = rows
    require([version.daily(row, index, flag) for row, index, flag in zip(rows, (0, 0, 1), (False, True, True), strict=True)] == [True]*3
            and datetime.fromisoformat(last["last_run_at"]) > datetime.fromisoformat(armed["last_run_at"]),
            "one real 86400-second Version schedule advance")
    require(state["complete"] is True and state["backend"] == side and state["cleanup"] == version.CLEAN
            and all(state[k] == expected for k, expected in {"actual_origin_HTTP": 503, "version_roots": 1,
                "durable_run_count_delta": 1, "worker_exit": 0, "version_SQL_notices": 0,
                "version_notice_events": 0, "worker_ready_publications": 0}.items())
            and state["cache_guard_deadline_preserved"] is True
            and initial["name"] == version.ROW and meta["task_id"] == task
            and meta["status"] == "SUCCESS" and meta["result"] is None and meta["traceback"] is None
            and meta["children"] == [] and meta.get("parent_id") is None and 0 < results["backend_ttl"] <= 3600
            and results["notices"] == [] and not any(results["queues"].values()),
            "exact saved Version HTTP503 SQL/cache/result and process predicates")
    path = root / "origin-requests.jsonl"
    require(path.stat().st_size <= 4096 and path.resolve() == path, "bounded origin journal")
    [call] = [json.loads(line) for line in path.read_bytes().splitlines()]
    headers = {key.lower(): val for key, val in call["headers"]}
    require(call["completed"] is True and call["path"] == "/owned-version" and call["status"] == 503
            and call["body_base64"] == retained.BODY and headers["user-agent"] == "Dispatcharr/0.33.0"
            and headers["accept"] == "application/vnd.github.v3+json" and headers["accept-encoding"] == "gzip, deflate"
            and read(root / "events-stopped.json") == {"stopped": True, "version_counts": {}}
            and read(root / "origin-stop.json") == {"thread_settled": True, "actual_requests": 1},
            "one real loopback 503 and no Version notice event")
    return {"actual_http": 503, "guard_deadline_unchanged": True, "one_saved_run": True,
            "result": "SUCCESS/null", "task_sha256": sha256(task.encode()).hexdigest(),
            "actual_broker_sha256": journal}


def original(repository, retained):
    historic = repository / "artifacts/rust" / ORIGINAL_PACKET
    proof_path = historic / "owner-1/proof.stdout"
    root = historic / "actual-version503-1/original"
    require(digest(proof_path) == ORIGINAL_PROOF
            and all(digest(root / name) == stamp for name, stamp in ORIGINAL_INPUTS.items())
            and all((historic / "owner-1" / name).read_bytes() == b"0\n"
                    for name in ("actual-exit", "preflight.actual-exit", "run.actual-exit", "proof.actual-exit")),
            "exact separately retained and settled Original Version503 authority")
    roles = read(root / "exact-host-identities.json")
    require(set(roles) == {"baseline", "worker", "beat", "origin", "journal", "events"}
            and len({item["cid"] for item in roles.values()}) == 6 and all(item["generation"] for item in roles.values())
            and (root / "bootstrap-stop-status").read_bytes() == b"0\n", "all six exact retired Original roles")
    return business(root, retained, "original") | {"Original_exact_cleanup": True, "Original_proof_sha256": ORIGINAL_PROOF}


def dispatch_spec(stage, seal, output, candidate, repository, entrypoint, coordinator_output):
    require(stage.resolve() == stage and stage.parent == repository / "artifacts/rust"
            and seal == STAGE_SEAL, "exact known Version503 workload stage")
    require(output.resolve() == output and output.is_relative_to(repository / "artifacts/rust")
            and coordinator_output.resolve() == coordinator_output and not output.is_relative_to(coordinator_output)
            and not coordinator_output.is_relative_to(output) and not output.is_relative_to(stage),
            "distinct exact Version503 owner and coordinator paths")
    inputs, _ = common.sealed_inputs(stage, seal)
    require(inputs[str(stage / "run.py")] == OWNER_SHA, "exact Version503 fixture seal")
    build = version.package(candidate)
    manifest = Path(candidate["build_manifest_file"])
    require(build["git_revision"] == candidate["git"], "identical common ordinary package")
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    parent_sha = digest(coordinator)
    historic = repository / "artifacts/rust" / ORIGINAL_PACKET
    for item in (coordinator, manifest, SCRIPT, Path(version.__file__).resolve(), historic / "owner-1/proof.stdout"):
        inputs[str(item)] = digest(item)
    tools = repository / "artifacts/rust/tooling"
    head = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(stage), seal, str(output), str(repository),
            json.dumps(candidate, sort_keys=True)]
    return {"argv": [*head, "run", entrypoint, str(coordinator_output), parent_sha],
            "cleanup_argv": [*head, "cleanup"], "evaluator_argv": [*head, "evaluate", entrypoint,
                                                                  str(coordinator_output), "@EXECUTION@"],
            "cwd": str(repository), "environment": {"DOCKER_HOST": common.DAEMON,
                "DOCKER_CONFIG": str(tools / "docker-config"), "PATH": f"{tools / 'docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes": common.STRIP_ENV, "input_sha256": inputs, "seconds": 460,
            "cleanup_seconds": 60, "output_bytes": 65536, "exclusive_heavy": True}


def cleanup(output, candidate, repository):
    return version.cleanup(output / "native", candidate, repository)


def proof(stage, output, repository, candidate):
    _, inherited, _, _, _ = authority(stage)
    previous = original(repository, inherited)
    require((output / "actual-exit").read_bytes() == b"0\n"
            and read(output / "receipt.json") == {"candidate": candidate, "literal_exit": 0},
            "literal current matched Version503 phase and exact frozen package")
    root = output / "native"
    require(read(root / "native-graph-readiness.json") == version.READY
            and read(root / "native-default-owner.json") == {"aio": True, "worker_override_absent": True,
                                                            "owned_endpoint": True}, "actual ordinary Main with both lanes")
    physical = business(root, inherited, "native")
    values = [row for (row, _) in version.command_values(root) if "guard_deadline_redis_ms" in row]
    require(len(values) >= 3, "observed guard before and after real worker run")
    prior, terminal, final = values[0], read(root / "first-terminal-observation.json"), values[-1]
    require(prior["backend"] is None and prior["guard_value"] == terminal["guard_value"] == final["guard_value"] == "1"
            and prior["guard_deadline_redis_ms"] == terminal["guard_deadline_redis_ms"] == final["guard_deadline_redis_ms"]
            and 0 < final["guard_pttl_ms"] <= terminal["guard_pttl_ms"] <= prior["guard_pttl_ms"]
            and final["backend"] == terminal["backend"] and final["notices"] == []
            and physical["task_sha256"] != previous["task_sha256"],
            "actual exact guard lifetime and one fresh Native task, no qualified Source replay")
    return {"scope": NODE, "candidate": candidate, "current_native": physical, "retained_original": previous,
            "cleanup_sha256": cleanup(output, candidate, repository)}


def run(stage, output, repository, candidate, entrypoint, parent_output, parent_sha, seal):
    binding = dispatch_spec(stage, seal, output, candidate, repository, entrypoint, parent_output)
    require(sys.orig_argv == binding["argv"] and Path.cwd() == repository,
            "actual exact Version503 coordinator command")
    controller = repository / "scripts/rust-milestone-batches/coordinator.py"
    common.coordinator_parent(controller, parent_sha, parent_output, binding, candidate, node=NODE)
    require(not os.path.lexists(output)
            and all(os.environ.get(key) == value for key, value in binding["environment"].items())
            and not any(key.startswith(("DOCKER_CONTEXT", "RUST_", "USE_NGINX_ACCEL")) for key in os.environ)
            and Path(shutil.which("docker") or "").resolve() == repository / "artifacts/rust/tooling/docker/docker",
            "fresh owned ordinary Version fixture on exact isolated engine")
    scheduler.require_clear_quarantine(repository)
    os.umask(0o077)
    build = version.package(candidate) | {"tag": candidate["image_id"]}
    _, inherited, daily, inputs, phase = authority(stage)
    original(repository, inherited)
    output.mkdir(mode=0o700)
    ns = dict(inherited.ensure_output_inputs.__globals__, OUT=output)
    materialize = FunctionType(inherited.ensure_output_inputs.__code__, ns)
    tools, stock = materialize(daily, inputs)
    sys.path.insert(0, str(tools))
    marker = repository / ".dispatcharr-recording-cohort-cleanup-required.json"
    token = {"scope": NODE, "candidate": candidate["git"], "output": str(output)}
    with marker.open("x") as stream:
        json.dump(token, stream)
    code = 1
    try:
        live = common.load_module(controller, "version503_live_command")
        live.command = version.observed_command(live.command)
        result = phase(output / "native", "native", tools, stock, live, build, time.monotonic() + 345)
        if result["complete"] is True and result["cleanup"] == version.CLEAN:
            cleanup(output, candidate, repository)
            require(read(marker) == token, "retire exact Version503 quarantine")
            marker.unlink()
            code = 0
    finally:
        daily.save(output / "receipt.json", {"candidate": candidate, "literal_exit": code})
        (output / "actual-exit").write_text(f"{code}\n")
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("seal")
    parser.add_argument("output", type=Path)
    parser.add_argument("repository", type=Path)
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("mode", choices=("run", "cleanup", "evaluate", "preflight"))
    parser.add_argument("rest", nargs="*")
    args = parser.parse_args()
    stage, output, repo, candidate = args.stage, args.output, args.repository, args.candidate
    common.sealed_inputs(stage, args.seal)
    require(args.seal == STAGE_SEAL, "exact approved Version503 stage")
    version.package(candidate)
    if args.mode == "preflight":
        require(not args.rest and not output.exists(), "fresh current Version503 fixture")
        static(stage, {"tag": candidate["image_id"]})
        _, inherited, _, _, _ = authority(stage)
        summary = {"scope": NODE, "candidate": candidate, "original": original(repo, inherited), "engine_calls": 0}
    elif args.mode == "cleanup":
        require(not args.rest, "exact Version503 cleanup request")
        scheduler.require_clear_quarantine(repo)
        summary = {"exact_owned_absence": True, "cleanup_sha256": cleanup(output, candidate, repo)}
    else:
        entrypoint, parent, witness = args.rest
        parent_output = Path(parent)
        if args.mode == "run":
            status = run(stage, output, repo, candidate, entrypoint, parent_output, witness, args.seal)
            if status:
                return status
            summary = proof(stage, output, repo, candidate)
        else:
            attempt = common.attempt_directory(parent_output, NODE)
            require(Path(witness) == attempt / "execution.json", "exact owned Version503 journal")
            actual = read(Path(witness))
            require(actual["id"] == NODE and common.settled(actual["execution"])
                    and common.settled(actual["cleanup"]) and actual["cleanup_verified"] is True,
                    "literal healthy Version503 owner execution and retirement")
            summary = proof(stage, output, repo, candidate)
            require(read(attempt / "execution.stdout") == summary, "same actual current Version503 output")
            summary.update(entrypoint=entrypoint, semantic_assertions_passed=True, full_required_workflow=True)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signal, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    raise SystemExit(main())
