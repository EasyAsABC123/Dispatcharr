"""Capture and independently qualify two actual due ticks on one saved hourly row."""
from base64 import b64decode
from datetime import datetime
import json
from math import isfinite
from pathlib import Path
import re
import time
from uuid import UUID

NODE = "m6/scheduler-tick-publication"
NAME = "maintain-recurring-recordings"
ROOT = "apps.channels.tasks.maintain_recurring_recordings"
CHILD = "apps.channels.tasks.prefetch_recording_artwork"
ARM = "root.last_run_at=timezone.now()-timedelta(hours=1,seconds=10)"
OPTIONS = {"callbacks": None, "errbacks": None, "chain": None, "chord": None}
ORIGINAL_LANE = "m6-d608ff75-scheduled-recurring-repeat-1/actual-pair-1/Original/lane.json"
ORIGINAL_LANE_SHA = "4074837cb45fad496ff93e511b1e32e24f84347cb79b077835e022bbc130f08d"
ORIGINAL_EXEC_SHA = "9e680ad626214c39ce909fc7e17c795bbcd405b06f3b061d5ffbf86606f4a1b1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def instrument(base):
    class ObservedTick(base):
        def sql(self, script, secret=False):
            if ARM not in script:
                return super().sql(script, secret=secret)
            expected = re.search(r"root.total_run_count==([01])\b", script)
            require(expected and "root.save(update_fields=['enabled','last_run_at'])" in script
                    and "PeriodicTasks.update_changed()" in script, "exact stock due-arm SQL required")
            previous = int(expected[1])
            started = time.time()
            result = super().sql(script, secret=secret)
            finished = time.time()
            require(result is True and started <= finished, "actual due-arm SQL must succeed")
            command = self.cli[-1]
            receipt = {"prior_count": previous, "started": started, "finished": finished,
                       "command": Path(command["stdout"]).with_suffix(".json").name,
                       "argv_sha256": command["argv_sha256"], "stdout_sha256": command["stdout_sha256"]}
            with (self.output / f"scheduler-tick-arm-{previous}.json").open("x") as stream:
                json.dump(receipt, stream)
            return result

    return ObservedTick


def publications(root, lane, read, digest):
    matches = [path for path in root.glob("data/ordinary-enabled-recurring/dvr-domain-observer-*/private-startup-monitor.jsonl")
               if digest(path) == lane["result"]["wire_sha256"]]
    require(len(matches) == 1, "exact recurring broker journal required")
    journal, = matches
    final = read(journal.with_name("final.json"))
    require(all(final[key] is True for key in ("terminal_barrier_observed", "terminal_barrier_fsynced",
                                               "thread_settled", "writer_closed"))
            and not final["observer_error"] and not final["terminal_reader_error"],
            "recurring broker observer did not settle")
    frames = []
    with journal.open() as stream:
        for line in stream:
            row = json.loads(line)
            command = row["command"].split(" ", 2)
            if row["db"] != 1 or len(command) != 3 or command[0].upper() not in ("LPUSH", "RPUSH"):
                continue
            if command[1] not in ("celery", "dvr"):
                continue
            frame = json.loads(command[2])
            frames.append((row["time"], command[1], json.loads(frame) if isinstance(frame, str) else frame))
    require(len(frames) == 3 and [frame["headers"]["task"] for _, _, frame in frames] == [ROOT, CHILD, ROOT],
            "expected two actual due roots and one artwork child")
    ids = [frame["headers"]["id"] for _, _, frame in frames]
    require(len(set(ids)) == 3 and all(UUID(value).version == 4 and str(UUID(value)) == value for value in ids),
            "each scheduler publication must have a distinct canonical UUID")
    times = [time for time, _, _ in frames]
    require(all(type(value) in (int, float) and isfinite(value) for value in times)
            and times[0] <= times[1] < times[2], "scheduler journal causal order differs")
    for index, (_, queue, frame) in enumerate(frames):
        head, props = frame["headers"], frame["properties"]
        child = index == 1
        args, kwargs, options = json.loads(b64decode(frame["body"], validate=True))
        require(queue == "celery" and props["delivery_info"] == {"exchange": "", "routing_key": "celery"}
                and props["correlation_id"] == ids[index] and head["root_id"] == ids[0 if child else index]
                and head["parent_id"] == (ids[0] if child else None) and head["timelimit"] == [None, None]
                and head.get("periodic_task_name") == (None if child else NAME)
                and kwargs == {} and options == OPTIONS,
                "scheduler envelope timing, delivery or ancestry fields differ")
        if child:
            eta = datetime.fromisoformat(head["eta"])
            require(eta.utcoffset() is not None and times[0] <= eta.timestamp() < times[2]
                    and len(args) == 1 and type(args[0]) is int and args[0] > 0,
                    "artwork ETA and recording argument differ from the owned first tick")
        else:
            require(head["eta"] is None and args == [], "elapsed Beat roots must publish immediately")
    return ids, times, digest(journal)


def armed_windows(arms, times):
    first, second = arms
    require([row["prior_count"] for row in arms] == [0, 1]
            and all(type(row[key]) in (int, float) and isfinite(row[key]) for row in arms
                    for key in ("started", "finished"))
            and all(row["started"] <= row["finished"] for row in arms),
            "exact successful due-arm sequence required")
    # The inherited Original owner allows 65 seconds for each observed tick.
    require(first["started"] <= times[0] <= first["finished"] + 65
            and times[1] <= second["started"] <= times[2] <= second["finished"] + 65,
            "actual root publication is outside its owned due-arm window")


def verify_arm_receipts(root, arms, read, digest, settled):
    require(arms[0]["command"] < arms[1]["command"], "due-arm SQL command sequence differs")
    for arm in arms:
        name = arm["command"]
        require(re.fullmatch(r"command-[0-9]{4}\.json", name), "exact due-arm SQL receipt path required")
        command = read(root / name)
        stdout = Path(command["stdout"])
        require(stdout.parent in (root, root / "private") and settled(command)
                and command["argv_sha256"] == arm["argv_sha256"]
                and digest(stdout) == command["stdout_sha256"] == arm["stdout_sha256"]
                and read(stdout) is True, "actual due-arm SQL settlement or stdout differs")


def terminals(root, ids, read, digest, settled):
    records = {}
    for path in root.glob("command-*.json"):
        receipt = read(path)
        stdout = Path(receipt["stdout"])
        if (not settled(receipt) or stdout.parent != root / "private"
                or receipt["secret_output_excluded"] is not True
                or not stdout.is_file() or not 0 < stdout.stat().st_size <= 8000):
            continue
        try:
            value = read(stdout)
            if isinstance(value, str):
                value = json.loads(value)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("task_id") in ids and value.get("status") == "SUCCESS":
            identity = value["task_id"]
            require(digest(stdout) == receipt["stdout_sha256"] and
                    (identity not in records or records[identity][0] == value),
                    "durable terminal result custody or identity differs")
            records[identity] = value, receipt["stdout_sha256"]
    require(set(records) == set(ids), "matching actual durable terminal for each publication required")
    for index, expected in enumerate((1, "ok", 0)):
        value = records[ids[index]][0]
        require(type(value["result"]) is type(expected) and value["result"] == expected
                and value["traceback"] is None
                and value.get("parent_id") == (ids[0] if index == 1 else None)
                and value["children"] == ([[[ids[1], None], None]] if index == 0 else []),
                "typed durable scheduler terminal or child ancestry differs")
    return [records[key][1] for key in ids]
