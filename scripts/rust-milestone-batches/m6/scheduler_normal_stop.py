"""Read-only predicates for the ordinary Main scheduler stop boundary."""
from hashlib import sha256
import json
from pathlib import Path

NODE = "m6/scheduler-normal-stop"
ORIGINAL_STOP_SHA = "15ab39f80f72aa00b5b36114b0c5133ed976018e4ac12ca72dbe371b00ea524d"
SUPPORT_COMPOSE = "image54-package-plan-1/support/docker/compose.rust-playback.yml"
QUEUES = """local n=redis.call('LLEN','celery')+redis.call('LLEN','dvr')
for _,priority in ipairs({'3','6','9'}) do
  for _,queue in ipairs({'celery','dvr'}) do
    n=n+redis.call('LLEN',queue..string.char(6,22)..priority)
  end
end
return n+redis.call('HLEN','unacked')+redis.call('ZCARD','unacked_index')"""


def require(condition, message):
    if not condition:
        raise ValueError(message)


def argv_digest(argv):
    return sha256(json.dumps(argv).encode()).hexdigest()


def original_stop(lane, read, digest):
    root = Path(lane["fixture_path"])
    paths = list(root.glob("role-beat-stop-*.json"))
    require(len(paths) == 1 and digest(paths[0]) == ORIGINAL_STOP_SHA,
            "exact pinned Original Beat stop receipt required")
    path, = paths
    receipt = read(path)
    require(receipt["role"] == "beat" and type(receipt["actual_exit"]) is int
            and receipt["actual_exit"] == 0 and receipt["forced"] is False
            and all(receipt[key] is True for key in ("actual_master_SIGTERM_sent", "shutdown_settled",
                    "actual_wait_completed", "stdout_EOF", "stderr_EOF", "all_original_process_generations_absent")),
            "Original Beat shutdown did not settle normally")
    return path


def stop_commands(root, scope, artifact_root, read, digest, settled):
    exits = list(root.glob("*-native-*-exit.json"))
    require(len(exits) == 1, "one owned Main shutdown exit required")
    exit_file, = exits
    container = exit_file.name.removesuffix("-exit.json")
    recorded = read(exit_file)
    require(type(recorded["actual_exit"]) is int and type(recorded["expected_exit"]) is int
            and recorded == scope["main_exit"] == {"actual_exit": 0, "expected_exit": 0, "oom_killed": False},
            "literal ordinary Main stop exit 0 required")
    docker = str(artifact_root / "tooling/docker/docker")
    close = read(root / "independent-cleanup.json")
    project = close["project"]
    require(container.startswith(project + "-native-"), "stop command must target the exact owned Main")
    compose = [docker, "compose", "-p", project, "-f", str(artifact_root / SUPPORT_COMPOSE),
               "-f", str(root / "ports.json"), "-f", str(root / "compose.json")]
    queue = argv_digest([*compose, "exec", "-T", "baseline", "redis-cli", "-n", "1", "--json", "EVAL", QUEUES, "0"])
    stop = argv_digest([docker, "stop", "--time", "55", container])
    inspect = argv_digest([docker, "inspect", container])
    stops, inspections, zeros, snapshots = [], [], [], []
    for path in sorted(root.glob("command-*.json")):
        receipt = read(path)
        if not settled(receipt):
            continue
        stdout = Path(receipt["stdout"])
        if stdout.parent not in (root, root / "private") or not stdout.is_file():
            continue
        if receipt["argv_sha256"] == stop:
            require(0 < stdout.stat().st_size < 4096 and digest(stdout) == receipt["stdout_sha256"]
                    and stdout.read_text().splitlines()[-1:] == [container],
                    "actual Main stop stdout does not identify the owned container")
            stops.append((path.name, receipt["stdout_sha256"]))
            continue
        is_inspect = receipt["argv_sha256"] == inspect
        is_queue = receipt["argv_sha256"] == queue
        if not (is_inspect or is_queue) and not 0 < stdout.stat().st_size <= 8000:
            continue
        try:
            value = read(stdout)
        except ValueError:
            continue
        if is_queue and type(value) is int and value == 0:
            zeros.append((path.name, receipt["stdout_sha256"]))
        elif is_inspect:
            state, = value
            status = state["State"]
            if (status["Running"] is False and status["OOMKilled"] is False
                    and type(status["ExitCode"]) is int and status["ExitCode"] == 0
                    and state["Name"] == "/" + container
                    and any(item["Id"] == state["Id"] for item in close["exact_CID_absence"])):
                inspections.append((path.name, receipt["stdout_sha256"]))
            else:
                continue
        elif value == scope["before_main_stop"]:
            snapshots.append((path.name, receipt["stdout_sha256"]))
        else:
            continue
        require(digest(stdout) == receipt["stdout_sha256"], "scheduler shutdown command stdout custody differs")
    require(len(stops) == len(inspections) == 1 and len(snapshots) == 2,
            "one actual stop and exit inspection between two SQL snapshots required")
    before, after = snapshots
    def adjacent(path, offset):
        ordinal = int(Path(path).stem.removeprefix("command-"))
        return f"command-{ordinal + offset:04d}.json"

    by_name = dict(zeros)
    before_queue, after_queue = adjacent(before[0], -1), adjacent(after[0], 1)
    require(before[0] < stops[0][0] < inspections[0][0] < after[0]
            and before_queue in by_name and after_queue in by_name,
            "SQL and empty ready/reserved queue receipts must straddle actual Main stop")
    return {"literal_main_exit": recorded["actual_exit"], "main_oom_killed": recorded["oom_killed"],
            "stop_stdout_sha256": stops[0][1], "exit_inspection_sha256": inspections[0][1],
            "pre_post_sql_sha256": [before[1], after[1]], "ready_reserved_before_after": [0, 0],
            "queue_command_receipts": [before_queue, after_queue],
            "queue_stdout_sha256": [by_name[before_queue], by_name[after_queue]]}
