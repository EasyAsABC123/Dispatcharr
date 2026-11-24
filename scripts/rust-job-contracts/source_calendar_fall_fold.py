"""Acquire the missing real Original Beat fall-fold observation in an owned copy.

Original Dispatcharr, Celery and django-celery-beat retain authorship and credit.
Only the clock dependency is controlled. No task worker is started.
"""
import argparse
from datetime import datetime
from base64 import b64decode
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from uuid import UUID

BEFORE = "2026-11-01T05:29:59+00:00"
FIRST = "2026-11-01T05:30:00+00:00"
SECOND = "2026-11-01T06:30:00+00:00"
NAME = "rust-owned-calendar-real-fall-fold"
TASK = "apps.epg.tasks.refresh_epg_data"
TZIF_SHA = "e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def qualify(report):
    require(report["new_york_tzif_sha256"] == TZIF_SHA
            and type(report["source_id"]) is int and report["source_id"] > 0,
            "pinned time zone and owned source identity required")
    stages = report["stages"]
    require(len(stages) == 3 and [step["clock"] for step in stages] == [BEFORE, FIRST, SECOND]
            and [len(step["frames"]) for step in stages] == [0, 1, 1]
            and all(type(step["clock_reads"]) is int and step["clock_reads"] >= 2 for step in stages)
            and [step["local_fold"] for step in stages] == [0, 0, 1],
            "actual Beat must observe both fold instants and only publish at the first")
    first, = stages[1]["frames"]
    require(stages[2]["frames"] == [first], "fall fold must keep the exact same publication")
    head, props = first["headers"], first["properties"]
    value = head["id"]
    payload = json.loads(b64decode(first["body"], validate=True))
    require(str(UUID(value)) == value
            and UUID(value).version == 4 and head["root_id"] == props["correlation_id"] == value
            and head["task"] == TASK and head["periodic_task_name"] == NAME and head["parent_id"] is None
            and head["eta"] is None and payload[:2] == [[report["source_id"]], {}]
            and props["delivery_info"] == {"exchange": "", "routing_key": "celery"},
            "exact saved fall-fold task identity and payload differ")
    before, after = report["before"], report["after"]
    require(before["name"] == NAME and before["timezone"] == "America/New_York"
            and before["minute"] == "30" and before["hour"] == "1"
            and before["last_run_at"] == "2026-10-31T05:30:00+00:00"
            and before["enabled"] is True and before["one_off"] is False
            and type(before["count"]) is int and before["count"] == 0
            and after == (before | {"count": 1, "last_run_at": FIRST}),
            "fall-fold durable SQL row must record only the first fold")
    require(type(report["beat_actual_exit"]) is int and report["beat_actual_exit"] == 0
            and report["beat_generation_retired"] is True and report["queue_and_results_after_cleanup"] == [0, 0],
            "literal Beat exit and copied queue cleanup required")
    return {"controlled_fall_fold_first": FIRST, "controlled_fall_fold_second": SECOND,
            "periodic_name": NAME, "actual_publications": 1, "durable_count": 1}


def clock_log(report, path):
    require(path.is_file() and path.stat().st_size <= 131072, "bounded real Beat clock dependency log required")
    raw = path.read_bytes()
    require(sha256(raw).hexdigest() == report["clock_log_sha256"]
            and type(report["beat_pid"]) is int and report["beat_pid"] > 0
            and report["beat_starttime"].isdigit(), "exact observed Beat clock log custody required")
    reads = [json.loads(line) for line in raw.splitlines()]
    for stage in report["stages"]:
        fact = {"pid": report["beat_pid"], "clock": stage["clock"], "dependency": "entry-now"}
        require(reads.count(fact) >= stage["clock_reads"], "real owned Beat clock calls missing")
    return report["clock_log_sha256"]


def generation(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def run():
    require(os.environ.get("JOB_DISPOSABLE_FIXTURE") == "playback-fixture-only"
            and re.fullmatch(r"rust_probe_backup_jobs_[a-f0-9]{10}", os.environ.get("POSTGRES_DB", "")),
            "exact copied calendar database required")
    sys.path.insert(0, "/app")
    sys.argv.append("shell")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
    import django
    django.setup()
    import redis
    from apps.epg.models import EPGSource
    from django.db import connection
    from django_celery_beat.models import CrontabSchedule, PeriodicTask, PeriodicTasks
    from zoneinfo import ZoneInfo

    with connection.cursor() as cursor:
        cursor.execute("select current_database()")
        require(cursor.fetchone() == (os.environ["POSTGRES_DB"],), "calendar DB ownership differs")
    broker = redis.Redis.from_url(os.environ["CELERY_BROKER_URL"])
    backend = redis.Redis.from_url(os.environ["CELERY_RESULT_BACKEND"])
    for peer, index in ((broker, 8), (backend, 9)):
        options = peer.connection_pool.connection_kwargs
        require(options["db"] == index and options["host"] in ("localhost", "127.0.0.1")
                and peer.dbsize() == 0, "empty local calendar-only Redis databases required")
    root = Path("/evidence")
    clock = root / "calendar-clock-fall-fold.json"
    reads = root / "calendar-fall-fold-clock-private.jsonl"
    ready = root / "calendar-beat-ready.json"
    zone = Path("/usr/share/zoneinfo/America/New_York")
    zone_sha = sha256(zone.read_bytes()).hexdigest()
    require(zone_sha == TZIF_SHA, "Original New York TZif differs from the retained spring authority")
    report = {"passed": False, "stages": [], "beat_actual_exit": None, "new_york_tzif_sha256": zone_sha}
    children, source, cron, row = None, None, None, None

    def clock_set(value):
        temporary = clock.with_suffix(".next")
        temporary.write_text(json.dumps({"now": value, "mode": "zoneinfo"}))
        temporary.replace(clock)

    def frames():
        result = broker.lrange("celery", 0, 2)
        require(len(result) <= 1 and all(len(frame) <= 65536 for frame in result),
                "unexpected duplicate or oversized fall-fold publication")
        return [json.loads(frame) for frame in result]

    def state():
        row.refresh_from_db()
        return {"id": row.pk, "name": row.name, "enabled": row.enabled, "one_off": row.one_off,
                "count": row.total_run_count, "last_run_at": row.last_run_at.isoformat(),
                "timezone": row.crontab.timezone, "hour": row.crontab.hour, "minute": row.crontab.minute}

    def observe(expected, queue_count):
        deadline = time.monotonic() + 20
        observed_at = None
        while time.monotonic() < deadline:
            require(children.poll() is None, "actual Original Beat exited before observation")
            actual = frames()
            require(len(actual) <= queue_count, "unexpected fall-fold publication")
            require(not reads.exists() or reads.stat().st_size <= 131072, "fall-fold clock log bound")
            hits = []
            if reads.is_file():
                with reads.open() as stream:
                    for line in stream:
                        if not line.endswith("\n"):
                            continue
                        entry = json.loads(line)
                        if entry == {"pid": children.pid, "clock": expected, "dependency": "entry-now"}:
                            hits.append(entry)
            if hits and observed_at is None:
                observed_at = time.monotonic()
            if (ready.exists() and len(hits) >= 2 and len(actual) == queue_count
                    and time.monotonic() - observed_at >= 1.2):
                local = datetime.fromisoformat(expected).astimezone(ZoneInfo("America/New_York"))
                report["stages"].append({"clock": expected, "local_fold": local.fold,
                                        "frames": actual, "clock_reads": len(hits)})
                return
            time.sleep(0.05)
        raise TimeoutError(f"actual Beat fold observation missing: {expected}")

    try:
        require(not any(os.path.lexists(path) for path in (clock, reads, ready)), "fresh private calendar outputs required")
        PeriodicTask.objects.update(enabled=False)
        PeriodicTasks.update_changed()
        source = EPGSource(name=NAME, source_type="xmltv", is_active=False, custom_properties={})
        EPGSource.objects.bulk_create([source])
        report["source_id"] = source.pk
        cron = CrontabSchedule.objects.create(timezone="America/New_York", minute="30", hour="1",
                                               day_of_week="*", day_of_month="*", month_of_year="*")
        row = PeriodicTask.objects.create(name=NAME, task=TASK, args=json.dumps([source.pk]), kwargs="{}",
                                          crontab=cron, last_run_at=datetime.fromisoformat("2026-10-31T05:30:00+00:00"))
        report["before"] = state()
        clock_set(BEFORE)
        env = dict(os.environ, C_FORCE_ROOT="true", PYTHONPATH="/job-tools:/app",
                   CALENDAR_DISPOSABLE_CLOCK=str(clock), CALENDAR_BEAT_READY=str(ready),
                   CALENDAR_BEAT_CLOCK_READS=str(reads))
        with (root / "calendar-fall-fold-beat-private.log").open("xb") as logs:
            children = subprocess.Popen(["/dispatcharrpy/bin/celery", "-A", "periodic_timer_observer:app", "beat",
                                         "--scheduler", "calendar_clock_observer:ClockScheduler",
                                         "--max-interval", "1", "--pidfile=", "--loglevel=WARNING"],
                                        env=env, stdout=logs, stderr=logs, start_new_session=True)
        birth = generation(children.pid)
        require(birth is not None, "live Original Beat generation required")
        report.update(beat_pid=children.pid, beat_starttime=birth)
        observe(BEFORE, 0)
        clock_set(FIRST)
        observe(FIRST, 1)
        clock_set(SECOND)
        observe(SECOND, 1)
        os.killpg(children.pid, signal.SIGTERM)
        report["beat_actual_exit"] = children.wait(timeout=15)
        report["beat_generation_retired"] = generation(children.pid) != birth
        report["after"] = state()
        report["clock_log_sha256"] = sha256(reads.read_bytes()).hexdigest()
        require(frames() == report["stages"][-1]["frames"] and backend.dbsize() == 0,
                "real graceful stop must preserve sole publication and no worker results")
    finally:
        if children is not None and children.poll() is None:
            os.killpg(children.pid, signal.SIGKILL)
            children.wait(timeout=5)
        for owned in (row, cron, source):
            if owned is not None:
                owned.delete()
        broker.delete("celery")
        report["queue_and_results_after_cleanup"] = [broker.dbsize(), backend.dbsize()]
        path = root / "source-calendar-fall-fold-private.json"
        path.write_text(json.dumps(report, indent=2) + "\n")
    report["qualification"] = qualify(report)
    clock_log(report, reads)
    report["passed"] = True
    path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
