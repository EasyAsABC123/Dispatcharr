"""Private receipt evaluator for the bounded Schedules Direct guide cohort."""
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from uuid import UUID

CASE = "schedules-direct"
TASK = "apps.epg.tasks.fetch_sd_mapped_guide_batch"
SOURCE = "scripts/rust-integration/milestone-batch34/Source-qualification/groups/schedules-direct.json"
SOURCE_SHA = "c3347fd2a5bd90e38e5676800f35c184d9957b3cd945edfb8ee4d56d42e86bcf"


def need(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    need(path.is_file() and not path.is_symlink() and path.stat().st_size <= 1048576,
         "bounded owned receipt required")
    return json.loads(path.read_bytes())


def cases(report, original):
    expected = original["cases"]
    actual = report["cases"]
    need(len(actual) == len(expected) == 9 and report["status"] == "passed"
         and report["unrun_required"] == original["unrun_required"],
         "exact nine Schedules Direct cases and unresolved scope required")
    for index, (result, source) in enumerate(zip(actual, expected, strict=True)):
        want = dict(source["actual"])
        if index == 8:
            identity = result["actual"]["child"]
            need(str(UUID(identity)) == identity and UUID(identity).version == 4,
                 "actual retry child UUID required")
            want["child"] = identity
        need(result == (source | {"actual": want}), "Schedules Direct case differs from retained Original")
    return actual[-1]["actual"]["child"]


def retry(owned, identity):
    private = owned / CASE / "private"
    delivery, payload = read(private / "SD-durable-receipt.json")
    frame, _, queue = json.loads(payload)
    parent = read(private / "held-batch-defer-backend.json")
    child = read(private / "deferred-guide-backend.json")
    head = frame["headers"]
    eta = datetime.fromisoformat(head["eta"])
    done = datetime.fromisoformat(child["date_done"])
    started = datetime.fromisoformat(parent["date_done"])
    need(isinstance(delivery, str) and bool(delivery)
         and queue == "celery" and head["id"] == identity and head["task"] == TASK
         and frame["properties"]["correlation_id"] == identity
         and frame["properties"]["delivery_info"] == {"exchange": "", "routing_key": "celery"}
         and type(head["retries"]) is int and head["retries"] == 0
         and head["root_id"] == head["parent_id"] == parent["task_id"] == child["parent_id"]
         and parent["children"] == [[[identity, None], None]]
         and parent["status"] == child["status"] == "SUCCESS" and child["task_id"] == identity
         and parent["result"] == "Deferred - batch already in progress"
         and child["result"] == "SD mapped guide batch complete"
         and all(clock.utcoffset() is not None for clock in (eta, started, done))
         and 60 < (eta - started).total_seconds() <= 95 and done >= eta,
         "kept reservation, parent, retry ETA and due result differ")
    return delivery


def capture_retirement(fixture, folder, identity, delivery):
    source = fixture._sd_guide["source"]
    lock = "task_lock_sd_mapped_guide_fetch_" + str(source)
    broker = fixture.redis(1,
        "return {ARGV[1], redis.call('HEXISTS','unacked',ARGV[1]), redis.call('ZSCORE','unacked_index',ARGV[1]) or false}",
        delivery)
    broker_receipt = fixture.cli[-1]["stdout"]
    lease = fixture.redis(0, "return {ARGV[1],redis.call('EXISTS',ARGV[1])}", lock)
    lease_receipt = fixture.cli[-1]["stdout"]
    need(broker == [delivery, 0, None] and lease == [lock, 0], "owned retry reservation and lease must retire")
    stops = {role: fixture.role(role, [], stop=True) for role in ("sd-guide-publications", "sd-guide-origin")}
    (folder / "sd-retirement.json").write_text(json.dumps({"task_id": identity, "broker": broker, "lease": lease,
        "broker_stdout": broker_receipt, "lease_stdout": lease_receipt, "roles": stops}))


def validate(owned, folder, original_path):
    need(sha256(original_path.read_bytes()).hexdigest() == SOURCE_SHA, "exact Original SD receipt required")
    identity = cases(read(owned / CASE / "report.json"), read(original_path))
    delivery = retry(owned, identity)
    retire = read(folder / "sd-retirement.json")
    source = read(owned / "SD-declared-inputs.json")["source"]
    need(retire["task_id"] == identity and retire["broker"] == [delivery, 0, None]
         and retire["lease"] == ["task_lock_sd_mapped_guide_fetch_" + str(source), 0]
         and type(retire["lease"][1]) is type(retire["broker"][1]) is int,
         "exact reservation and lease retirement required")
    for probe in ("broker", "lease"):
        stdout = Path(retire[probe + "_stdout"])
        need(stdout.resolve() == stdout and stdout.is_relative_to(owned) and read(stdout) == retire[probe],
             "actual Redis command stdout required")
        receipt = read(owned / (stdout.name.split(".", 1)[0] + ".json"))
        need(receipt["stdout"] == str(stdout) and receipt["stdout_sha256"] == sha256(stdout.read_bytes()).hexdigest()
             and type(receipt["actual_exit"]) is int and receipt["actual_exit"] == 0
             and receipt["failure"] is None
             and all(receipt[key] is True for key in ("stdout_EOF", "stderr_EOF", "process_group_settled")),
             "settled Redis proof required")
    need(set(retire["roles"]) == {"sd-guide-publications", "sd-guide-origin"}, "exact owned role retirement")
    for exit_state in retire["roles"].values():
        need(type(exit_state["actual_exit"]) is int and exit_state["actual_exit"] == 0
             and exit_state["forced"] is False and all(exit_state[key] is True for key in
             ("actual_wait_completed", "stdout_EOF", "stderr_EOF", "all_original_process_generations_absent")),
             "literal SD role exit and absence required")
    return {"cases": 9, "same_live_worker_retry": True, "retired": True, "source_sha256": SOURCE_SHA}
