"""Replay four scheduled Version200 cases on the frozen ordinary Main package."""
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
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    import recording61_owner as common
    import retained_scheduler_semantics as retained
    import scheduler_install_owner as scheduler
finally:
    sys.path.pop(0)

NODE = "m6/version200-update-no-update-malformed"
SCRIPT = Path(__file__).resolve()
TEMPLATE = "m6-a5be97-core-saved-version-fixture-1/fixture.py"
TEMPLATE_SHA = "587db2e7c6eb6e7238cc1593c01ee2ee5d966e1fe8ef6a092db952df49c23406"
SOURCE = "scripts/rust-job-contracts/m6-missing-source/epoch2/m6-missing-domains/Source-extension.json"
TASK = "core.tasks.check_for_version_update"
ROW = "check-version-updates"
LABELS = ("no-update", "update", "repeat", "malformed")
READY = {"ready": True, "ordinary_aio": True, "core_saved_families": True, "both_lanes_accepting": True}
CLEAN = dict.fromkeys(("normal_bootstrap_stop", "exact_inventory_empty", "owned_cids_absent",
                       "owned_host_generations_absent", "passed"), True)
require, read, digest = common.require, common.read, common.digest


def package(candidate):
    path = Path(candidate["build_manifest_file"])
    build = read(path)
    require(digest(path) == candidate["build_manifest_sha256"]
            and (build["git_revision"], build["image_id"], build["platform"], build["source_archive_sha256"])
            == (candidate["git"], candidate["image_id"], "linux/amd64", candidate["source_archive_sha256"])
            and build["passed"] is True and type(build["exit_code"]) is int and build["exit_code"] == 0
            and build["worktree_status"] == "" and build["source_input_mode"] == "committed-revision-snapshot",
            "exact frozen ordinary AMD64 Version package required")
    launcher = path.parents[2] / "ordinary-launch-1"
    require(all((launcher / leaf).read_bytes() == b"0\n" for leaf in
                ("ordinary-build.actual-exit", "ordinary-verify.actual-exit", "actual-exit"))
            and read(launcher / "ordinary-verify.stdout") ==
            {"revision": candidate["git"], "image": candidate["image_id"],
             "manifest_sha256": candidate["build_manifest_sha256"], "feature_arg": 0},
            "ordinary package launcher must have physically verified feature-zero build")
    return build


def authority(repository):
    require(__debug__, "Version fixture requires inherited runtime assertions")
    path = repository / "artifacts/rust" / TEMPLATE
    source = repository / SOURCE
    require(digest(path) == TEMPLATE_SHA and digest(source) == retained.VERSION_SHA,
            "exact licensed Version fixture and Original authority required")
    require(retained.versions(read(source)) == ["version200-" + label for label in LABELS],
            "retained four Original outcomes required")
    core = common.load_module(path, "version200_retained_core_fixture")
    version, base, owner, inputs, previous = core.authority()
    require(owner.composition is previous.__globals__["composition"], "exact retained Version composition")
    compose = owner.composition

    def ordinary(out, tools, original, project, backend, image, deadline):
        require(backend == "native", "fresh version owner starts Native only")
        # Use the immutable local ID even if the original build tag was retagged.
        model, database = compose(out, tools, original, project, backend,
                                  image | {"tag": image["image_id"]}, deadline)
        services = model["services"]
        flags = services["worker"]["environment"]
        require(flags.pop("RUST_CORE_SCHEDULE_OWNER") == "rust"
                and flags.pop("RUST_EPG_REFRESH_ENABLED") == "0"
                and flags["RUST_VERSION_HTTP_ORIGIN"] == "http://127.0.0.1:9193",
                "exact ordinary scheduler feature boundary")
        flags["DISPATCHARR_ENV"] = "aio"
        require("RUST_VERSION_TASKS_ENABLED" not in flags and "RUST_VERSION_STARTUP_OWNER" not in flags
                and "RUST_EXPERIMENTAL_SELECTED_JOBS" not in flags and "RUST_EXPERIMENTAL_SELECTED_BEAT" not in flags,
                "version ownership must follow ordinary AIO composition")
        return model, database

    owner.composition = ordinary
    old = inputs["schedule.py"].decode()
    start = old.index("def core_readiness():")
    end = old.index("\ndef daily(mode):", start)
    readiness = '''
def core_readiness():
    with urlopen("http://127.0.0.1:9192/health/ready", timeout=3) as response:
        app = json.load(response)
        graph = app.get("selected_workers", {})
        core = graph.get("clocked_scheduler_scope") == "core-saved-families"
        lanes = all(str(graph.get(lane, "")).startswith(("Working(", "Observed(")) for lane in ("celery", "dvr"))
        ordinary = response.status == 200 and app.get("status") == "ready" and app.get("backend") == "rust"
        return {"ready": ordinary and core and lanes and graph.get("ready") is True
                         and graph.get("clocked_scheduler_enabled") is True and graph.get("startup") == "Started",
                "ordinary_aio": ordinary, "core_saved_families": core, "both_lanes_accepting": lanes}

'''
    adapted = old[:start] + readiness + old[end:]
    compile(adapted, "ordinary_aio_version_readiness", "exec")
    inputs["schedule.py"] = adapted.encode()
    phase = version.phase(base, owner, version.stock(base), previous.__globals__["startup_reference"])
    cases = phase.__globals__["ordinary_four"].__globals__
    return base, owner, inputs, phase, cases


def preflight(repository, candidate):
    build = package(candidate)
    _, owner, _, phase, _ = authority(repository)
    with TemporaryDirectory(prefix="m6-ordinary-version200-cold-") as folder:
        path = Path(folder)
        model, database = owner.composition(path, path / "tools", path / "Original",
                                            "dispatcharr-version200-daily-000000000001", "native", build, 1)
        worker = model["services"]["worker"]
        require(worker["image"] == candidate["image_id"] and worker["pull_policy"] == "never"
                and worker["environment"]["DISPATCHARR_ENV"] == "aio"
                and model["services"]["baseline"]["network_mode"] == "none"
                and all("ports" not in item for item in model["services"].values()),
                "immutable Native worker and isolated loopback origin required")
        owner.preflight_import_path(path, database)
        require(phase.__globals__["composition"] is owner.composition,
                "actual owner must use approved ordinary composition")
    return {"scope": NODE, "candidate_git": candidate["git"], "engine_calls": 0, "cases": list(LABELS)}


def command_values(root):
    values = []
    for command in read(root / "command-executions.json"):
        stdout = Path(command["stdout"])
        if not common.settled(command) or stdout.parent != root or not stdout.is_file():
            continue
        if not 0 < stdout.stat().st_size <= 131072:
            continue
        try:
            value = read(stdout)
        except ValueError:
            continue
        if isinstance(value, dict):
            values.append((value, digest(stdout)))
    return values


def roots_from_wire(root):
    final = read(root / "final.json")
    require(all(final[key] is True for key in ("thread_settled", "terminal_barrier_observed",
                    "terminal_barrier_fsynced", "writer_closed")) and not final["terminal_reader_error"]
            and final["observer_error"] is None, "actual Version broker observer must settle")
    wire = root / "private-startup-monitor.jsonl"
    require(wire.stat().st_size <= 16 * 1024 * 1024, "bounded Version journal")
    frames = []
    with wire.open() as lines:
        for line in lines:
            row = json.loads(line)
            parts = row["command"].split(" ", 2)
            if row["db"] != 1 or len(parts) != 3 or parts[0].upper() not in ("LPUSH", "RPUSH") or parts[1] not in ("dvr", "celery"):
                continue
            frame = json.loads(parts[2])
            frames.append((parts[1], json.loads(frame) if isinstance(frame, str) else frame))
    require(len(frames) == 4, "exactly four actual broker publications required")
    roots = []
    for queue, frame in frames:
        head, props = frame["headers"], frame["properties"]
        identity = head["id"]
        body = json.loads(b64decode(frame["body"], validate=True))
        require(queue == "celery" and str(UUID(identity)) == identity and UUID(identity).version == 4
                and head["task"] == TASK and head["periodic_task_name"] == ROW
                and head["root_id"] == props["correlation_id"] == identity and head["parent_id"] is None
                and head["eta"] is None and head["timelimit"] == [None, None]
                and props["delivery_info"] == {"exchange": "", "routing_key": "celery"}
                and body == [[], {}, {"callbacks": None, "errbacks": None, "chain": None, "chord": None}],
                "exact saved daily Version producer envelope required")
        roots.append(identity)
    require(len(set(roots)) == 4, "Version producer UUIDs must be distinct")
    return roots, digest(wire)


def daily(row, count, enabled):
    return (row.get("name") == ROW and row.get("task") == TASK and row.get("every") == 86400
            and row.get("period") == "seconds" and row.get("one_off") is False
            and type(row.get("run_count")) is int and row["run_count"] == count
            and row.get("enabled") is enabled and row.get("enabled_names") == ([ROW] if enabled else []))


def ordered_cases(root, roots, source, cases):
    calls = [json.loads(line) for line in (root / "origin-requests.jsonl").read_text().splitlines()]
    require(len(calls) == 4 and read(root / "origin-stop.json") == {"thread_settled": True, "actual_requests": 4},
            "exactly four settled Version origin requests required")
    values = command_values(root)
    cursor, observations, events = 0, [], [{"type": "notifications_cleared"}]
    baseline = next(value for value, _ in values if "notice_sequence" in value)
    original = read(source)["report"]["cases"]
    for index, (identifier, label, call, pinned) in enumerate(zip(roots, LABELS, calls, original, strict=True)):
        headers = {key.lower(): value for key, value in call["headers"]}
        require(call["attempted"] is True and call["completed"] is True and call["path"] == "/owned-version" and type(call["status"]) is int
                and call["status"] == 200 and headers["user-agent"] == "Dispatcharr/0.33.0"
                and headers["accept"] == "application/vnd.github.v3+json" and headers["accept-encoding"] == "gzip, deflate"
                and json.loads(b64decode(call["body_base64"], validate=True)) == cases["PAYLOADS"][index],
                "exact owned HTTP 200 request and body required")
        while cursor + 1 < len(values):
            if (cursor > 0 and "notice_sequence" in values[cursor - 1][0]
                    and daily(values[cursor][0], index, index > 0)
                    and daily(values[cursor + 1][0], index, True)):
                break
            cursor += 1
        require(cursor and cursor + 1 < len(values), "missing ordered saved Version row")
        before, before_sha = values[cursor - 1]
        current = values[cursor][0]
        armed, arm_sha = values[cursor + 1]
        require("notice_sequence" in before and not any(before["queues"].values())
                and daily(armed, index, True), "missing pre-arm SQL and exact saved Version arm")
        if current["last_run_at"] is not None:
            require(datetime.fromisoformat(armed["last_run_at"]) < datetime.fromisoformat(current["last_run_at"]),
                    "Version due arm must rewind the same saved row")
        cursor += 2
        while cursor < len(values) and (values[cursor][0].get("backend") or {}).get("task_id") != identifier:
            cursor += 1
        require(cursor < len(values), "missing actual matching Version DB2 result")
        after, after_sha = values[cursor]
        cases["result"](after, identifier, baseline)
        cases["state_change"](label, before, after, pinned)
        while cursor < len(values) and not daily(values[cursor][0], index + 1, True):
            cursor += 1
        require(cursor < len(values), "saved Version row did not advance once")
        final, count_sha = values[cursor]
        require(datetime.fromisoformat(final["last_run_at"]) > datetime.fromisoformat(armed["last_run_at"]),
                "saved Version row last_run_at did not advance")
        cursor += 1
        if index == 0:
            events.append({"success": True, "type": "notifications_cleared", "count": 1})
        elif index == 1:
            events.append(cases["event_model"](cases["version_notice_rows"](after)))
        observations.append({"case": label, "http_status": 200, "task_id": identifier,
                             "pre_sql_sha256": before_sha, "arm_sha256": arm_sha,
                             "terminal_sql_sha256": after_sha, "count_sha256": count_sha})
    observed = read(root / "events-stopped.json")
    require(observed["stopped"] is True, "Channels notification observer did not settle")
    cases["assert_events"](observed["events"], events)
    return observations


def cleanup(root, candidate, repository, *, historical=False):
    state = read(root / "state.json")
    roles = read(root / "exact-host-identities.json")
    require(state["cleanup"] == CLEAN and type(state["worker_exit"]) is int and state["worker_exit"] == 0
            and set(roles) == {"baseline", "worker", "origin", "journal", "events"}
            and len({role["cid"] for role in roles.values()}) == 5
            and all(role["generation"] for role in roles.values())
            and roles["worker"]["image"] == candidate["image_id"],
            "exact ordinary Native worker and all owned Version fixture generations must retire")
    require((root / "bootstrap-stop-status").read_bytes() == b"0\n", "literal baseline stop exit 0 required")
    if not historical:
        commands = read(root / "command-executions.json")
        require(all("argv_sha256" in row for row in commands), "actual owned Version CLI command custody required")
        docker = str(repository / "artifacts/rust/tooling/docker/docker")
        exits, absent, live, inventories = set(), set(), set(), {}
        overrides = {"RUST_EXPERIMENTAL_SELECTED_JOBS", "RUST_EXPERIMENTAL_SELECTED_BEAT",
                     "RUST_EXPERIMENTAL_SELECTED_CORE_BEAT", "RUST_CORE_SCHEDULE_OWNER",
                     "RUST_VERSION_TASKS_ENABLED", "RUST_VERSION_STARTUP_OWNER", "RUST_EPG_REFRESH_ENABLED"}
        cids = {row["cid"]: name for name, row in roles.items()}
        project = None
        for row in commands:
            stdout = Path(row["stdout"])
            require(stdout.parent == root and stdout.resolve() == stdout and stdout.is_file()
                    and digest(stdout) == row["stdout_sha256"], "exact bounded Version command output custody differs")
            if row["kind"] == "inspect":
                cid = row["cid"]
                if cid not in cids:
                    continue
                require(row["argv_sha256"] == sha256(json.dumps([docker, "container", "inspect", cid]).encode()).hexdigest(),
                        "Version inspect argv differs from exact owned CID")
                if common.settled(row):
                    observation, = read(stdout)
                    config, observed = observation["Config"], observation["State"]
                    require(observation["Id"] == cid and config["Labels"]["com.docker.compose.service"] == cids[cid],
                            "Version role inspection belongs to a different CID or service")
                    if observed["Running"] is True:
                        require(observation["Image"] == roles[cids[cid]]["image"]
                                and observed["Pid"] == roles[cids[cid]]["pid"],
                                "Version role running generation differs")
                        if cids[cid] == "worker":
                            env = dict(item.split("=", 1) for item in config["Env"])
                            require(env.get("DISPATCHARR_ENV") == "aio" and not overrides.intersection(env)
                                    and env.get("RUST_VERSION_HTTP_ORIGIN") == "http://127.0.0.1:9193",
                                    "inspected exact live worker must use ordinary AIO and owned Version HTTP")
                        live.add(cid)
                    elif observed["Running"] is False:
                        require(observation["Id"] == cid and observed["OOMKilled"] is False
                                and type(observed["ExitCode"]) is int and observed["ExitCode"] == 0
                                and config["Labels"]["com.docker.compose.service"] == cids[cid],
                                "settled exact owned Version role exit 0 required")
                        this_project = config["Labels"]["com.docker.compose.project"]
                        require(project in (None, this_project), "Version shutdown project identity differs")
                        project = this_project
                        exits.add(cid)
                elif (type(row["actual_exit"]) is int and row["actual_exit"] == 1 and row["failure"] is None
                      and all(row[key] is True for key in ("process_group_settled", "stdout_EOF", "stderr_EOF"))
                      and row["remaining_group_members"] == [] and row["output_bytes_discarded"] == 0):
                    absent.add(cid)
            if row["kind"] == "inventory" and common.settled(row):
                resource = row["resource"]
                options = ["-aq", "--no-trunc"] if resource == "container" else ["-q"]
                argv = [docker, resource, "ls", *options, "--filter", "label=com.docker.compose.project=" + row["project"]]
                require(row["argv_sha256"] == sha256(json.dumps(argv).encode()).hexdigest(),
                        "Version inventory argv differs from exact project label")
                inventories[(row["project"], resource)] = stdout.read_bytes()
        require(live == exits == absent == set(cids) and project is not None
                and all(inventories.get((project, resource)) == b"" for resource in ("container", "network", "volume")),
                "literal owned Version exits, CID absence and empty scoped inventories required")
    return digest(root / "state.json")


def observed_command(command):
    def capture(argv, *args, **kwargs):
        result = command(argv, *args, **kwargs)
        stdout = Path(result["stdout"])
        result.update(argv_sha256=sha256(json.dumps(argv).encode()).hexdigest(),
                      stdout_sha256=digest(stdout), kind="other")
        if len(argv) == 4 and argv[1:3] == ["container", "inspect"]:
            result.update(kind="inspect", cid=argv[3])
        if (len(argv) >= 5 and argv[1] in ("container", "network", "volume")
                and argv[2] == "ls" and argv[-2] == "--filter"
                and argv[-1].startswith("label=com.docker.compose.project=")):
            result.update(kind="inventory", resource=argv[1], project=argv[-1].split("=", 2)[2])
        return result
    return capture


def evaluate(output, repository, candidate, *, historical=False):
    root = output / "native"
    require((output / "actual-exit").read_bytes() == b"0\n", "literal Version fixture exit 0 required")
    receipt = read(output / "receipt.json")
    require(receipt["candidate"] == candidate["git"] and receipt["image"] == candidate["image_id"]
            and receipt["manifest_sha256"] == candidate["build_manifest_sha256"]
            and type(receipt["literal_exit"]) is int and receipt["literal_exit"] == 0,
            "Version receipt differs from the common candidate")
    graph = read(root / "native-graph-readiness.json")
    expected = {"ready": True, "shared_Core": True, "selected_enabled": False} if historical else READY
    require(graph == expected, "fresh ordinary AIO production scheduler and both live lanes required")
    _, _, _, _, cases = authority(repository)
    roots, journal = roots_from_wire(root)
    observations = ordered_cases(root, roots, repository / SOURCE, cases)
    return {"scope": NODE, "candidate": candidate, "source_sha256": retained.VERSION_SHA,
            "broker_sha256": journal, "cases": observations, "cleanup_sha256": cleanup(root, candidate, repository, historical=historical)}


def dispatch_spec(output, candidate, repository, entrypoint, coordinator_output):
    require(output.resolve() == output and coordinator_output.resolve() == coordinator_output
            and not output.is_relative_to(coordinator_output) and not coordinator_output.is_relative_to(output),
            "exact distinct Version fixture and coordinator paths required")
    package(candidate)
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    digest_parent = digest(coordinator)
    source, template = repository / SOURCE, repository / "artifacts/rust" / TEMPLATE
    require(digest(source) == retained.VERSION_SHA and digest(template) == TEMPLATE_SHA,
            "exact frozen Version authority required")
    prefix = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(repository), str(output),
              json.dumps(candidate, sort_keys=True)]
    tooling = repository / "artifacts/rust/tooling"
    return {"argv": [*prefix, "run", entrypoint, str(coordinator_output), digest_parent],
            "cleanup_argv": [*prefix, "cleanup"],
            "evaluator_argv": [*prefix, "evaluate", entrypoint, str(coordinator_output), "@EXECUTION@"],
            "cwd": str(repository), "environment": {"DOCKER_HOST": common.DAEMON,
                "DOCKER_CONFIG": str(tooling / "docker-config"), "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes": common.STRIP_ENV, "seconds": 460, "cleanup_seconds": 60,
            "output_bytes": 65536, "exclusive_heavy": True,
            "input_sha256": {str(path): digest(path) for path in
                (SCRIPT, coordinator, source, template, common.SCRIPT, Path(retained.__file__).resolve(),
                 Path(candidate["build_manifest_file"]))}}


def run(repository, output, candidate, coordinator_output, entrypoint, coordinator_sha):
    binding = dispatch_spec(output, candidate, repository, entrypoint, coordinator_output)
    require(binding["argv"] == sys.orig_argv and Path.cwd() == repository, "actual exact Version coordinator argv required")
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    common.coordinator_parent(coordinator, coordinator_sha, coordinator_output, binding, candidate, node=NODE)
    require(not os.path.lexists(output)
            and all(os.environ.get(key) == value for key, value in binding["environment"].items())
            and not any(key.startswith(("DOCKER_CONTEXT", "RUST_", "USE_NGINX_ACCEL")) for key in os.environ)
            and Path(shutil.which("docker") or "").resolve() == repository / "artifacts/rust/tooling/docker/docker",
            "fresh owned fixture and explicit isolated engine required")
    scheduler.require_clear_quarantine(repository)
    os.umask(0o077)
    image = package(candidate)
    base, daily_owner, inputs, phase, _ = authority(repository)
    output.mkdir(mode=0o700)
    # Borrow the live coordinator's one lease. Use only the legacy tool materializer and Native phase.
    ns = dict(base.ensure_output_inputs.__globals__, OUT=output)
    from types import FunctionType
    materialize = FunctionType(base.ensure_output_inputs.__code__, ns)
    tools, original = materialize(daily_owner, inputs)
    sys.path.insert(0, str(tools))
    code = 1
    marker = repository / ".dispatcharr-recording-cohort-cleanup-required.json"
    token = {"scope": NODE, "candidate": candidate["git"], "output": str(output)}
    with marker.open("x") as marker_stream:
        json.dump(token, marker_stream)
    try:
        live = common.load_module(coordinator, "version200_live_command")
        live.command = observed_command(live.command)
        report = phase(output / "native", "native", tools, original, live, image, time.monotonic() + 345)
        if report["complete"] is True and report["cleanup"] == CLEAN:
            cleanup(output / "native", candidate, repository)
            require(read(marker) == token, "exact retiring Version owner required")
            marker.unlink()
            code = 0
    finally:
        daily_owner.save(output / "receipt.json", {"candidate": candidate["git"], "image": candidate["image_id"],
                         "manifest_sha256": candidate["build_manifest_sha256"], "literal_exit": code})
        (output / "actual-exit").write_text(f"{code}\n")
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("mode", choices=("run", "cleanup", "evaluate", "preflight", "historical-check"))
    parser.add_argument("entrypoint", nargs="?")
    parser.add_argument("coordinator_output", type=Path, nargs="?")
    parser.add_argument("witness", nargs="?")
    a = parser.parse_args()
    if a.mode == "run":
        status = run(a.repository, a.output, a.candidate, a.coordinator_output, a.entrypoint, a.witness)
        if status:
            return status
    if a.mode == "preflight":
        proof = preflight(a.repository, a.candidate)
    elif a.mode == "cleanup":
        scheduler.require_clear_quarantine(a.repository)
        proof = {"exact_owned_absence": True, "cleanup_sha256": cleanup(a.output / "native", a.candidate, a.repository)}
    else:
        proof = evaluate(a.output, a.repository, a.candidate, historical=a.mode == "historical-check")
        if a.mode == "evaluate":
            folder = common.attempt_directory(a.coordinator_output, NODE)
            require(Path(a.witness) == folder / "execution.json", "exact Version execution journal required")
            receipt = read(folder / "execution.json")
            require(receipt["id"] == NODE and common.settled(receipt["execution"])
                    and common.settled(receipt["cleanup"]) and receipt["cleanup_verified"] is True
                    and read(folder / "execution.stdout") == proof, "actual Version coordinator attempt must settle")
            proof.update(entrypoint=a.entrypoint, semantic_assertions_passed=True, full_required_workflow=True)
    print(json.dumps(proof, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
