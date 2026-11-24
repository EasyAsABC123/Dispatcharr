"""First saved scanner tick in the owned ordinary Native SQL/Redis fixture."""
from base64 import b64decode
from contextlib import contextmanager
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import time
from uuid import UUID

NAME = "scan-files"
SCAN = "core.tasks.scan_and_process_files"
INDEX = "apps.epg.tasks.build_programme_index_task"
CONTEXT = "ordinary-missing-programme-index"
SOURCES = (("missing", "xmltv", True, "absent"), ("sqlnull", "xmltv", True, "sql-null"),
           ("jsonnull", "xmltv", True, "json-null"), ("empty", "xmltv", True, "empty"),
           ("dummy", "dummy", True, "absent"), ("sd", "schedules_direct", True, "absent"),
           ("inactive", "xmltv", False, "absent"))
QUEUES = """local n=redis.call('LLEN','celery')+redis.call('LLEN','dvr')
for _,priority in ipairs({'3','6','9'}) do
  for _,queue in ipairs({'celery','dvr'}) do
    n=n+redis.call('LLEN',queue..string.char(6,22)..priority)
  end
end
return n+redis.call('HLEN','unacked')+redis.call('ZCARD','unacked_index')"""
DB = """
import re
with connection.cursor() as cur:
    cur.execute("select current_database(),current_setting('data_directory')")
    name,location=cur.fetchone()
assert re.fullmatch(r'normal_dvr_[a-f0-9]{16}',name) and location=='/data/db'
"""


def need(value, message):
    if not value:
        raise ValueError(message)


def write(path, value):
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


def prepare_context(fixture, output):
    from dvr_domain_cases import observer_start
    sql = DB + """
from pathlib import Path
import time
from django_celery_beat.models import IntervalSchedule,PeriodicTask,PeriodicTasks
from apps.epg.models import EPGSource,EPGSourceIndex
from core.models import CoreSettings
stock=list(EPGSource.objects.order_by("id").values("id","name","source_type","is_active")[:9])
assert len(stock)<=8 and not any(row["name"].startswith("rust-owned-first-scan-") for row in stock)
EPGSource.objects.filter(id__in=[row["id"] for row in stock]).update(is_active=False)
owned=[]
for label,kind,active,state in __SOURCES__:
    path=Path('/data/epgs/rust-owned-first-scan-'+label+'.xml')
    assert path.parent.resolve()==Path('/shared/__CONTEXT__/epgs') and not path.exists() and not path.is_symlink()
    row=EPGSource(name='rust-owned-first-scan-'+label,source_type=kind,is_active=active,file_path=str(path),custom_properties={})
    EPGSource.objects.bulk_create([row])
    if state!='absent':
        EPGSourceIndex.objects.bulk_create([EPGSourceIndex(source=row,data={})])
        with connection.cursor() as cur:
            if state=='sql-null':cur.execute('UPDATE epg_epgsourceindex SET data=NULL WHERE source_id=%s',[row.pk])
            if state=='json-null':cur.execute("UPDATE epg_epgsourceindex SET data='null'::jsonb WHERE source_id=%s",[row.pk])
    owned.append(row.pk)
PeriodicTask.objects.update(enabled=False)
interval,_=IntervalSchedule.objects.get_or_create(every=20,period='seconds')
scan,_=PeriodicTask.objects.get_or_create(name='scan-files',defaults={'task':__SCAN__,'interval':interval,'enabled':False})
assert scan.interval_id==interval.pk and scan.task==__SCAN__ and not scan.one_off and scan.total_run_count==0
settings,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}})
CoreSettings.objects.filter(pk=settings.pk).update(value={**(settings.value or {}),'schedule_enabled':False})
PeriodicTasks.update_changed()
print(json.dumps({'database':name,'stock':stock,'ids':owned,'schedule_id':scan.pk}))
"""
    seed = fixture.sql(sql.replace("__SOURCES__", repr(SOURCES)).replace("__CONTEXT__", CONTEXT)
                       .replace("__SCAN__", repr(SCAN)), secret=True)
    fixture.scanner_first_seed = seed
    fixture.scanner_first_observer = observer_start(fixture, "scanner-first-exact", database=1)
    write(output / "scanner-first-seed.json", seed)


def snapshot(fixture, seed):
    sql = DB + """
from pathlib import Path
import time
from django.db import transaction
from apps.epg.models import EPGData,EPGSource,ProgramData
from apps.channels.models import Logo
from apps.m3u.models import M3UAccount
from django_celery_beat.models import PeriodicTask
with transaction.atomic():
    with connection.cursor() as cur:
        cur.execute('SET TRANSACTION READ ONLY')
        cur.execute("select current_setting('transaction_read_only')")
        assert cur.fetchone()==('on',)
        cur.execute("SELECT source_id,id,data,data IS NULL,updated_at FROM epg_epgsourceindex ORDER BY source_id LIMIT 32")
        indexes=[{'source_id':a,'id':b,'data':c,'sql_null':d,'updated_at':e}for a,b,c,d,e in cur.fetchall()]
    sources=list(EPGSource.objects.order_by('id').values()[:32])
    assert len(sources)<32 and len(indexes)<32
    related=list(EPGData.objects.order_by('id').values()[:256])
    programmes=list(ProgramData.objects.order_by('id').values()[:256])
    logos=list(Logo.objects.order_by('id').values()[:256])
    playlists=list(M3UAccount.objects.order_by('id').values()[:256])
    assert all(len(rows)<256 for rows in (related,programmes,logos,playlists))
    task=PeriodicTask.objects.get(pk=__ROW__)
    schedule={'id':task.pk,'name':task.name,'task':task.task,'enabled':task.enabled,'count':task.total_run_count,
              'last_run_at':task.last_run_at,'every':task.interval.every,'period':task.interval.period,
              'one_off':task.one_off,'args':json.loads(task.args),'kwargs':json.loads(task.kwargs)}
    files={key:sorted(p.name for p in (Path('/data')/key).iterdir())for key in ('m3us','epgs','logos')}
    print(json.dumps({'database':name,'read_only':True,'sources':sources,'indexes':indexes,'parsers':related,
                      'programmes':programmes,'logos':logos,'playlists':playlists,'schedule':schedule,'files':files},default=str))
"""
    return fixture.sql(sql.replace("__ROW__", repr(seed["schedule_id"])), secret=True)


def eligibility(seed, before):
    need(before["read_only"] is True and before["database"] == seed["database"]
         and before["files"] == {"m3us": [], "epgs": [], "logos": []}, "empty owned scanner input and copied DB required")
    ids = seed["ids"]
    need(len(ids) == len(set(ids)) == len(SOURCES) and all(type(value) is int and value > 0 for value in ids),
         "distinct owned scanner source rows required")
    sources = {row["id"]: row for row in before["sources"]}
    indexes = {row["source_id"]: row for row in before["indexes"]}
    need(set(sources) == set(ids) | {row["id"] for row in seed["stock"]}, "complete copied source roster differs")
    for row in seed["stock"]:
        need(all(sources[row["id"]][key] == row[key] for key in ("name", "source_type"))
             and sources[row["id"]]["is_active"] is False, "only copied non-test sources are disabled")
    for ident, (label, kind, active, state) in zip(ids, SOURCES, strict=True):
        row, index = sources[ident], indexes.get(ident)
        need((row["name"], row["source_type"], row["is_active"], row["file_path"])
             == ("rust-owned-first-scan-" + label, kind, active, "/data/epgs/rust-owned-first-scan-" + label + ".xml")
             and row["is_active"] is active and not row["extracted_file_path"], "exact seven Original source preconditions required")
        if state == "absent":
            need(index is None, "expected absent index row")
        else:
            need(index is not None and index["data"] == ({} if state == "empty" else None)
                 and index["sql_null"] is (state == "sql-null") and index["updated_at"],
                 "SQL NULL, JSON null and empty map must remain distinct")
    schedule = before["schedule"]
    need(schedule["id"] == seed["schedule_id"] and schedule["name"] == NAME and schedule["task"] == SCAN
         and schedule["enabled"] is False and schedule["count"] == 0
         and schedule["one_off"] is False and schedule["args"] == [] and schedule["kwargs"] == {}
         and (schedule["every"], schedule["period"]) == (20, "seconds"), "saved stock scanner row must be cold")
    return set(ids[:2])


def publications(entries, expected):
    roots, children = [], []
    for entry in entries:
        frame = entry["frame"]
        head, props = frame["headers"], frame["properties"]
        task = head["task"]
        if task not in (SCAN, INDEX):
            continue
        identity = head["id"]
        args, kwargs, _ = json.loads(b64decode(frame["body"], validate=True))
        need(entry["queue"] == "celery" and str(UUID(identity)) == identity and UUID(identity).version == 4
             and head["eta"] is None and props["correlation_id"] == identity and kwargs == {}
             and props["delivery_info"] == {"exchange": "", "routing_key": "celery"},
             "exact first scanner frame UUID and delivery")
        when = float(entry["actual_redis_time"])
        if task == SCAN:
            need(head["periodic_task_name"] == NAME and head["parent_id"] is None
                 and head["root_id"] == identity and args == [], "scanner publication must come from saved Beat row")
            roots.append({"id": identity, "when": when})
        else:
            need(len(args) == 1 and type(args[0]) is int and args[0] in expected
                 and head["root_id"] == head["parent_id"] and head.get("periodic_task_name") is None,
                 "scanner selected an excluded or unrelated Index source")
            children.append({"id": identity, "source_id": args[0], "parent": head["parent_id"], "when": when})
    need(len(roots) <= 1 and len(children) <= len(expected)
         and len({row["source_id"] for row in children}) == len(children), "duplicate first-scan publication")
    if len(roots) != 1 or len(children) != len(expected):
        return None
    root, = roots
    need(len({root["id"], *(row["id"] for row in children)}) == len(expected) + 1
         and all(row["parent"] == root["id"] and row["when"] >= root["when"] for row in children),
         "actual scanner root must precede the exact two parented publications")
    return {"root": root, "children": children}


def validate_pass(report, wire, number, expected):
    before, after = report["before"], report["after"]
    need(before["schedule"]["count"] == number - 1, "ordered scanner saved run count")
    projected = publications(wire, expected)
    need(projected is not None and projected == report["jobs"], "closed broker journal differs from first scanner report")
    root = projected["root"]
    arm = report["arm"]
    need(arm["id"] == report["seed"]["schedule_id"] and arm["enabled"] is True and type(arm["count"]) is int and arm["count"] == number - 1
         and report["arm_started"] <= root["when"] <= report["arm_finished"] + 35
         and all(float(entry["actual_redis_time"]) < report["arm_started"]
                 or entry["frame"]["headers"]["task"] in (SCAN, INDEX) for entry in wire),
         "one real scanner publication must follow the exact saved due arm")
    children = projected["children"]
    metas = report["terminals"]
    need(set(metas) == {root["id"], *(child["id"] for child in children)}, "exact affected DB2 jobs required")
    for ident, meta in metas.items():
        need(meta["task_id"] == ident and meta["status"] == "SUCCESS" and meta["result"] is None
             and meta.get("traceback") is None
             and meta.get("parent_id") == (None if ident == root["id"] else root["id"]),
             "actual matching scanner/child terminal differs")
    need(metas[root["id"]]["children"] == [[[child["id"], None], None] for child in children],
         "scanner report child order differs from real broker publication order")
    need({key: value for key, value in after.items() if key != "schedule"}
         == {key: value for key, value in before.items() if key != "schedule"},
         "scanner changed source/index/parser/programme/playlist/logo or file state")
    need(after["schedule"] == (before["schedule"] | {"count": number, "last_run_at": after["schedule"]["last_run_at"]})
         and datetime.fromisoformat(after["schedule"]["last_run_at"]) > datetime.fromisoformat(arm["last_run_at"])
         and datetime.fromisoformat(after["schedule"]["last_run_at"]).utcoffset() is not None
         and type(report["ready_or_reserved"]) is int and report["ready_or_reserved"] == 0
         and report["events"] == {"actual_receiver": report["receiver"], "events": [], "raw_events_base64": []},
         "first saved scan must advance once, drain and emit no file summary")
    return {"root": root["id"], "eligible_source_count": len(expected), "excluded_source_count": 5,
            "scanner_count": number, "empty_file_events": True}


def validate(report, wire):
    expected = eligibility(report["seed"], report["before"])
    return validate_pass(report, wire, 1, expected)


LOCK_STATE = DB + """
with connection.cursor() as cur:
    cur.execute("""
LOCK_STATE += repr("""
SELECT a.pid, a.backend_start::text
FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
WHERE l.relation='epg_epgsourceindex'::regclass
  AND l.granted AND l.mode='AccessExclusiveLock'
  AND a.datname=current_database() AND a.application_name='owned-scanner-index-contract'
""") + """)
    locks=cur.fetchall()
    assert len(locks)<=1
    cur.execute("""
LOCK_STATE += repr("""
SELECT a.pid
FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
WHERE l.relation='epg_epgsourceindex'::regclass AND NOT l.granted
  AND a.datname=current_database() AND a.wait_event_type='Lock'
  AND a.query LIKE 'SELECT %epg_epgsourceindex%' ORDER BY a.pid
""") + """)
    print(json.dumps({'held':list(locks[0]) if locks else None,'waiters':[row[0] for row in cur.fetchall()]}))
"""

LOCK_ROLE = """
import os, signal, time
import psycopg
stop = False
def terminate(*args):
    global stop
    stop = True
signal.signal(signal.SIGTERM, terminate)
with psycopg.connect(dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'],
                     password=os.environ['POSTGRES_PASSWORD'], host='/var/run/postgresql',
                     application_name='owned-scanner-index-contract') as conn:
    conn.execute('LOCK TABLE epg_epgsourceindex IN ACCESS EXCLUSIVE MODE NOWAIT')
    deadline = time.monotonic() + 110
    while not stop and time.monotonic() < deadline:
        time.sleep(.05)
    assert stop
"""


def lock_state(fixture):
    return fixture.sql(LOCK_STATE, secret=True)


@contextmanager
def index_lock(fixture, enabled):
    if not enabled:
        yield None
        return
    role = "scanner-index-lock"
    fixture.role(role, ["/dispatcharrpy/bin/python", "-c", LOCK_ROLE])
    try:
        state = fixture.wait(lambda: state if (state := lock_state(fixture))["held"] else None,
                             10, "actual exclusive Index table lock")
        need(state["waiters"] == [], "Index lock must begin on idle copied SQL")
        yield state
    finally:
        fixture.role(role, [], stop=True)


def run_pass(fixture, seed, observer, number, expected, before):
    import owned_update_receiver
    from scanner_wire_support import broker_snapshot, receive

    receiver = owned_update_receiver.begin(fixture)
    report = {"seed": seed, "before": before, "receiver": receiver}

    def journal_entries():
        return [entry for entry in broker_snapshot(observer) if entry["queue"] == "celery"]

    initial_entries = journal_entries()
    cursor = len(initial_entries)
    need(number != 1 or not any(entry["frame"]["headers"]["task"] in (SCAN, INDEX)
                               for entry in initial_entries), "no scanner publication before first arm")

    def finish():
        jobs = fixture.wait(lambda: publications(journal_entries()[cursor:], expected), 35, "saved scanner publications")
        report["jobs"] = jobs
        report["terminals"] = {ident: fixture.wait(lambda: receive(fixture, ident), 35, "saved scanner DB2 terminal")
                               for ident in [jobs["root"]["id"], *(child["id"] for child in jobs["children"])]}
        need(fixture.sql(DB + "from django_celery_beat.models import PeriodicTasks;PeriodicTasks.update_changed();print(json.dumps(True))") is True,
             "sync the actual saved run through the scheduler marker")
        stop = DB + """
from django_celery_beat.models import PeriodicTask,PeriodicTasks
row=PeriodicTask.objects.get(pk=__ROW__)
assert row.name=='scan-files'
if row.total_run_count==__NEXT__:
 row.enabled=False;row.save(update_fields=['enabled']);PeriodicTasks.update_changed()
print(json.dumps(row.total_run_count))
"""
        fixture.wait(lambda: fixture.sql(stop.replace("__ROW__", repr(seed["schedule_id"])).replace("__NEXT__", str(number))) == number, 20,
                     "one durable saved scanner run")

    with index_lock(fixture, number in (2, 3)) as lock:
        if lock:
            report["lock_before_arm"] = lock
        script = DB + """
from datetime import timedelta
from django.utils import timezone
from django_celery_beat.models import PeriodicTask,PeriodicTasks
row=PeriodicTask.objects.get(pk=__ROW__)
assert row.name=='scan-files' and not row.enabled and row.total_run_count==__COUNT__
row.enabled=True;row.last_run_at=timezone.now()-timedelta(seconds=21)
row.save(update_fields=['enabled','last_run_at'])
PeriodicTasks.update_changed()
print(json.dumps({'id':row.pk,'count':row.total_run_count,'enabled':row.enabled,'last_run_at':row.last_run_at},default=str))
"""
        report["arm_started"] = time.time()
        report["arm"] = fixture.sql(script.replace("__ROW__", repr(seed["schedule_id"])).replace("__COUNT__", str(number - 1)), secret=True)
        report["arm_finished"] = time.time()
        if number == 3:
            root = fixture.wait(lambda: publications(journal_entries()[cursor:], set()), 35,
                                "saved scanner root while the Index table is held")["root"]["id"]
            report["blocked_first_helper"] = fixture.wait(
                lambda: state if (state := lock_state(fixture))["waiters"] else None,
                15, "cold Main helper waits on the actual Index table lock")
            key = "celery-tasks:celery-task-meta-" + root
            report["blocked_root_result"] = fixture.redis(
                2, "return {ARGV[1],redis.call('EXISTS',ARGV[1]),redis.call('TIME')}", key)
            need(report["blocked_root_result"][:2] == [key, 0], "held cold scanner root must lack a DB2 terminal")
        else:
            finish()
        if number == 2:
            report["lock_after_root"] = lock_state(fixture)
            need(report["lock_after_root"]["held"] and report["lock_after_root"]["waiters"] == [],
                 "repeat root completed while the Index table lock stayed held")
    if number == 3:
        finish()
    report["after"] = snapshot(fixture, seed)
    report["ready_or_reserved"] = fixture.wait(lambda: fixture.redis(1, QUEUES) == 0, 15,
                                                "saved scanner queue drain") and fixture.redis(1, QUEUES)
    queue_receipt = fixture.cli[-1]
    report["queue_command"] = Path(queue_receipt["stdout"]).with_suffix(".json").name
    events = owned_update_receiver.read(fixture, receiver)
    report["events"] = events

    report["journal_cursor"] = cursor
    return report


def validate_lifecycle(report, wire):
    expected = eligibility(report["seed"], report["before"])
    passes = report["passes"]
    need(len(passes) == 3 and passes[0]["before"] == report["before"]
         and passes[2]["after"] == report["after"]
         and all(part["seed"] == report["seed"] for part in passes), "complete scanner lifecycle snapshots required")
    first, repeat, restarted = passes
    need(first["after"] == repeat["before"] and repeat["after"] == restarted["before"]
         and report["restart"]["after_stop"] == report["restart"]["after_start"] == repeat["after"],
         "same copied SQL and roots across cold Main replacement required")
    need(first["main"] == repeat["main"] != restarted["main"]
         and report["restart"]["old"] == repeat["main"] and report["restart"]["new"] == restarted["main"]
         and report["restart"]["exit"] == {"actual_exit": 0, "oom_killed": False, "expected_exit": 0},
         "same process repeat and clean cold Main replacement required")
    held = repeat["lock_before_arm"]
    need(held == repeat["lock_after_root"] and held["waiters"] == []
         and isinstance(held["held"], list) and len(held["held"]) == 2
         and type(held["held"][0]) is int and held["held"][0] > 0 and held["held"][1],
         "repeat must complete with the same actual Index lock held")
    cold_lock, waiter = restarted["lock_before_arm"], restarted["blocked_first_helper"]
    need(cold_lock["held"] and cold_lock["held"] == waiter["held"] and cold_lock["waiters"] == []
         and len(waiter["waiters"]) == 1 and type(waiter["waiters"][0]) is int and waiter["waiters"][0] > 0,
         "fresh Main must actually wait on the Index table")
    key, present, clock = restarted["blocked_root_result"]
    cold_jobs = restarted["jobs"]
    need(key == "celery-tasks:celery-task-meta-" + cold_jobs["root"]["id"]
         and type(present) is int and present == 0
         and len(clock) == 2 and all(isinstance(value, str) and value.isascii() and value.isdecimal() for value in clock)
         and 0 <= int(clock[1]) < 1000000, "held cold scanner root must lack its own DB2 result")
    observed = int(clock[0]) + int(clock[1]) / 1000000
    need(cold_jobs["root"]["when"] <= observed
         and all(child["when"] >= observed for child in cold_jobs["children"]),
         "cold Index publications must follow the held SQL observation")
    cuts = [part["journal_cursor"] for part in passes] + [len(wire)]
    need(all(type(a) is int and 0 <= a < b for a, b in zip(cuts, cuts[1:]))
         and not any(entry["frame"]["headers"]["task"] in (SCAN, INDEX) for entry in wire[:cuts[0]]),
         "exact ordered scanner journal partitions required")
    result = [validate_pass(part, wire[cuts[i]:cuts[i+1]], i + 1, ids)
              for i, (part, ids) in enumerate(zip(passes, (expected, set(), expected), strict=True))]
    identities = [ident for part in passes for ident in part["terminals"]]
    need(len(identities) == len(set(identities)) == 7
         and report["retained_results"] == first["terminals"] | repeat["terminals"],
         "distinct saved runs and first generation DB2 retained through restart required")
    return {"passes": result, "index_children": [2, 0, 2], "repeat_completed_under_index_table_lock": True,
            "cold_main_replaced": True, "cold_scanner_helper_held_by_index_sql": True, "retained_original_scope": "scanner-logo-only"}


def run(fixture, out):
    from dvr_domain_cases import producer_observer_stop
    from scanner_wire_support import from_journal, receive

    out.mkdir()
    seed = fixture.scanner_first_seed
    observer = fixture.scanner_first_observer
    try:
        before = snapshot(fixture, seed)
        expected = eligibility(seed, before)
        old = fixture.native
        first = run_pass(fixture, seed, observer, 1, expected, before) | {"main": old}
        repeat = run_pass(fixture, seed, observer, 2, set(), first["after"]) | {"main": old}
        exit_state = fixture.stop_native(expected=0)
        stopped_sql = snapshot(fixture, seed)
        fixture.start_native()
        new = fixture.native
        restarted_sql = snapshot(fixture, seed)
        retained = {ident: receive(fixture, ident) for part in (first, repeat) for ident in part["terminals"]}
        restarted = run_pass(fixture, seed, observer, 3, expected, restarted_sql) | {"main": new}
        closing, observer = observer, None
        journal = producer_observer_stop(fixture, closing)
        stream = Path(journal["raw_journal_path"])
        report = {"seed": seed, "before": before, "after": restarted["after"], "passes": [first, repeat, restarted],
                  "restart": {"old": old, "new": new, "exit": exit_state,
                              "after_stop": stopped_sql, "after_start": restarted_sql},
                  "retained_results": retained, "journal_path": str(stream.relative_to(fixture.output)),
                  "journal_sha256": journal["raw_journal_sha256"], "observer_stopped": journal["observer_stop"],
                  "passed": True}
        validate_lifecycle(report, from_journal(stream))
        write(out / "scenario.json", report)
        return report
    finally:
        if observer is not None:
            producer_observer_stop(fixture, observer)
