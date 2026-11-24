"""Owned loopback PostgreSQL hold for the ordinary Index write failure."""
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import re
import time

PROXY = ("scripts/rust-job-contracts/recording-artwork/followup/controllers/physical/"
         "tools/pg_reply_proxy.py")
PROXY_SHA = "2f880970b0703e6e72ff0feac2975b184ef51bc5250c7f5b3f97d52d443cbdd3"
WRITE = ("INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES($1,$2,$3) "
         "ON CONFLICT(source_id) DO UPDATE SET data=EXCLUDED.data,updated_at=EXCLUDED.updated_at")
MESSAGE = "owned-index-sql-write-failure"
ROLE = "index-pg-held-ready"
PORT = 61651
RESULT = "celery-tasks:celery-task-meta-"
LEASE = "task_lock_epg_source_file_"
SAMPLE = "local v=redis.call('GET',ARGV[1]);return {ARGV[1],v and redis.sha1hex(v) or false,redis.call('PTTL',ARGV[1])}"


def need(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    return json.loads(path.read_bytes())


def replace(text, old, new):
    need(text.count(old) == 1, "exact reviewed PostgreSQL proxy boundary required")
    return text.replace(old, new)


def guest_source(repository):
    raw = (repository / PROXY).read_bytes()
    need(sha256(raw).hexdigest() == PROXY_SHA, "exact retained PostgreSQL framing gate required")
    text = raw.decode()
    policy = {"update": WRITE, "known_owned_trigger_error_code": "P0001",
              "known_owned_trigger_error_message": MESSAGE}
    text = replace(text, 'POLICY=json.loads(Path(__file__).with_name("sql-policy.json").read_text())',
                   f"POLICY={policy!r}")
    # Use only the reviewed exchange recognizer and socket relay. Retain no raw
    # authentication or application payloads. The ordinary pool needs more than four connections.
    start, end = text.index("  def capture("), text.index("  class Handler")
    text = text[:start] + "  def capture(*args): pass\n" + text[end:]
    text = replace(text, "BoundedSemaphore(4)", "BoundedSemaphore(16)")
    text = replace(text, ">=64:owner.errors.append({'error':'PGHistoricalConnectionCapacity64'})",
                   ">=128:owner.errors.append({'error':'PGHistoricalConnectionCapacity128'})")
    text = replace(text, "if matching is not None:owner.subject_complete_count+=1",
                   """if matching is not None:
        if payload != b'E':raise ValueError('ExpectedFailedTransactionReady')
        owner.subject_complete_count+=1""")
    text = replace(text, "       with owner.lock:exchanges.forwarded_ready()",
                   """       with owner.lock:
        exchanges.forwarded_ready()
        if hold:
         owner.timeline.append({'stage':'actual-Ready-forwarded','connection':ident,'backend_pid':backend_pid,
                                'monotonic':time.monotonic()})""")
    text = replace(text, "'actual_error':matching['error'],'executions'", """'actual_error':matching['error'],
       'sqlstate':matching['error_fields']['C'],'message_sha256':hashlib.sha256(matching['error_fields']['M'].encode()).hexdigest(),
       'ready_status':payload.decode(),'executions'""")
    text = replace(text, "'handler_or_upstream_threads_alive':alive,",
                   "'matched_error_ready_count':self.subject_complete_count,'handler_or_upstream_threads_alive':alive,")
    return text + GUEST


GUEST = """
if __name__ == '__main__':
 import signal,sys
 root=Path(sys.argv[1])
 if not root.is_dir() or root.is_symlink() or not root.as_posix().startswith('/shared/ordinary-missing-programme-index/'):
  raise ValueError('ExactOwnedProxyRoot')
 stopping=threading.Event()
 signal.signal(signal.SIGTERM,lambda *args:stopping.set())
 proxy=Proxy('update-error-Ready')
 proxy.insert_release.set()
 def put(name, value):
  temp=root/(name+'.tmp')
  temp.write_text(json.dumps(value))
  temp.replace(root/name)
 put('listening.json',{'loopback':True,'port':61651})
 try:
  while not stopping.wait(.025):
   with proxy.lock:
    timeline=list(proxy.timeline)
    errors=list(proxy.errors)
   if timeline:
    put('state.json',{'timeline':timeline,'errors':errors})
   if (root/'release').exists():
    if (root/'release').is_symlink() or (root/'release').read_bytes()!=b'release\\n':
     raise ValueError('ExactOwnedReadyRelease')
    proxy.release.set()
 finally:
  put('closed.json',proxy.close())
"""


def start(fixture):
    root = "/shared/" + fixture.case + "/" + ROLE
    local = fixture.case_data / ROLE
    fixture.python(f"import os;os.mkdir({root!r},0o755);os.chown({root!r},65532,65532)", secret=True)
    guest = Path(__file__).with_name("index_pg_gate.py").read_text()
    # Process argv contains only the gate source and its disposable path.
    fixture.role(ROLE, ["/dispatcharrpy/bin/python", "-c", guest, root])
    fixture.wait(lambda: (local / "listening.json").is_file()
                 and read(local / "listening.json") == {"loopback": True, "port": PORT},
                 10, "owned PG gate listening")
    fixture.pg_gate = local
    fixture.pg_gate_guest = root
    fixture._plugin_environment["DATABASE_URL"] = (
        f"postgresql://dispatch:playback-fixture-only@127.0.0.1:{PORT}/{fixture.database}?sslmode=disable")


def last_stdout(fixture):
    return str(Path(fixture.cli[-1]["stdout"]).relative_to(fixture.output))


def observe(fixture, out, child, target, baseline, snapshot):
    if fixture.backend != "Native":
        return
    need(child["source_id"] == target["source_id"], "source identity for held Index result")
    key, lock = RESULT + child["task_id"], LEASE + str(target["source_id"])
    state_path = fixture.pg_gate / "state.json"
    def held():
        if not state_path.is_file():
            return None
        state = read(state_path)
        need(not state["errors"], "PG gate framing failure")
        return state if state["timeline"] and len(state["timeline"]) == 1 else None

    hold = fixture.wait(held, 15, "actual Index P0001 trailing Ready held")
    samples = []
    for _ in range(2):
        at = time.monotonic()
        result = fixture.redis(2, "return {ARGV[1],redis.call('EXISTS',ARGV[1])}", key)
        result_stdout = last_stdout(fixture)
        owner = fixture.redis(0, SAMPLE, lock)
        samples.append({"started": at, "finished": time.monotonic(), "result": result, "lease": owner,
                        "result_stdout": result_stdout, "lease_stdout": last_stdout(fixture)})
        if len(samples) == 1:
            time.sleep(.3)
    unchanged = snapshot(fixture)
    sql_stdout = last_stdout(fixture)
    need(unchanged == baseline and held() == hold, "same owned SQL and held Ready before release")
    verify_hold(hold, samples, key, lock)
    released = time.monotonic()
    release_file = fixture.pg_gate_guest + "/release"
    fixture.python(f"from pathlib import Path; p=Path({release_file!r}); p.open('xb').write(b'release\\n')",
                   secret=True)
    (out / "held-ready.json").write_text(json.dumps({
        "held": hold, "samples": samples, "target": target["source_id"], "task_id": child["task_id"],
        "baseline": unchanged, "sql_stdout": sql_stdout, "release_requested": released}))


def observe_after(fixture, out, child):
    if fixture.backend != "Native":
        return
    key = LEASE + str(child["source_id"])
    value = fixture.redis(0, "return {ARGV[1],redis.call('EXISTS',ARGV[1])}", key)
    need(value == [key, 0], "Index file lease must be absent after terminal")
    (out / "held-ready-lease-exit.json").write_text(json.dumps({"value": value, "stdout": last_stdout(fixture)}))


def verify_hold(held, samples, key, lock):
    event, = held["timeline"]
    need(held["errors"] == [] and event["stage"] == "actual-matching-Ready-held"
         and event["identity"] == WRITE and event["actual_error"] is True
         and event["sqlstate"] == "P0001" and event["ready_status"] == "E"
         and event["message_sha256"] == sha256(MESSAGE.encode()).hexdigest()
         and event["executions"] == event["subject_complete_occurrence"] == 1
         and event["frontend_Sync_or_simpleQuery_observed"] is True
         and event["matching_rows"] == 0 and event["matching_commands_hex"] == []
         and type(event["backend_pid"]) is int and event["backend_pid"] > 0,
         "exact PostgreSQL write error followed by held Ready")
    first, second = samples
    need(event["monotonic"] <= first["started"] <= first["finished"]
         and first["finished"] + .25 <= second["started"] <= second["finished"],
         "two ordered held reply observations required")
    for sample in samples:
        owner_key, owner_hash, ttl = sample["lease"]
        need(sample["result"] == [key, 0] and type(sample["result"][1]) is int
             and owner_key == lock and isinstance(owner_hash, str)
             and re.fullmatch(r"[0-9a-f]{40}", owner_hash) and type(ttl) is int and ttl > 0,
             "held Index must have no result and retain its actual Redis file lease")
    need(first["lease"][1] == second["lease"][1], "same Redis file lease must survive held reply")


def stop(fixture):
    receipt = fixture.role(ROLE, [], stop=True)
    (fixture.output / "index-pg-gate-exit.json").write_text(json.dumps(receipt))


def verify_stdout(owned, name, value):
    output = owned / name
    need(output.resolve() == output and output.is_relative_to(owned)
         and re.fullmatch(r"command-[0-9]{4}", output.name.split(".", 1)[0])
         and output.stat().st_size <= 65536, "bounded owned command output required")
    receipt = read(owned / (output.name.split(".", 1)[0] + ".json"))
    need(receipt["stdout"] == str(output) and receipt["failure"] is None
         and type(receipt["actual_exit"]) is int and receipt["actual_exit"] == 0
         and all(receipt[field] is True for field in ("process_group_settled", "stdout_EOF", "stderr_EOF"))
         and receipt["stdout_sha256"] == sha256(output.read_bytes()).hexdigest()
         and read(output) == value, "exact settled SQL or Redis probe required")
    return int(output.name.split(".", 1)[0].rsplit("-", 1)[1])


def verify(owned, business, terminal, expected):
    proof = read(business / "held-ready.json")
    matching = read(business / "scenario.json")
    child = matching["actual_scanner_and_index_publications"]["children"][1]
    need(child["source_id"] == proof["target"] and child["task_id"] == proof["task_id"] == terminal["task_id"]
         and terminal["status"] == "FAILURE" and terminal["result"] == expected
         and proof["baseline"] == matching["before"],
         "same owned Index write, read-only SQL and exact Original terminal required")
    verify_hold(proof["held"], proof["samples"], RESULT + child["task_id"], LEASE + str(child["source_id"]))
    ordinals = [verify_stdout(owned, sample[field + "_stdout"], sample[field])
                for sample in proof["samples"] for field in ("result", "lease")]
    ordinals.append(verify_stdout(owned, proof["sql_stdout"], proof["baseline"]))
    lease_exit = read(business / "held-ready-lease-exit.json")
    need(lease_exit["value"] == [LEASE + str(child["source_id"]), 0], "exact settled Index lease absent")
    ordinals.append(verify_stdout(owned, lease_exit["stdout"], lease_exit["value"]))
    need(ordinals == sorted(set(ordinals)), "exact order of held Redis and SQL probes required")
    closed = read(owned / "data/ordinary-missing-programme-index" / ROLE / "closed.json")
    held, released, forwarded = closed["timeline"]
    need(closed["errors"] == closed["frames"] == closed["active_connections"] == []
         and type(closed["matched_error_ready_count"]) is int and closed["matched_error_ready_count"] == 1
         and closed["authentication_bytes_retained"] is False and closed["CancelKey_secret_retained"] is False
         and closed["observer_thread_settled"] is closed["physical_close_verified"] is True
         and type(closed["handler_or_upstream_threads_alive"]) is int
         and closed["handler_or_upstream_threads_alive"] == 0
         and held == proof["held"]["timeline"][0]
         and [released["stage"], forwarded["stage"]]
         == ["actual-release-observed-before-Ready-forward", "actual-Ready-forwarded"]
         and all(row["connection"] == held["connection"] and row["backend_pid"] == held["backend_pid"]
                 for row in (released, forwarded))
         and proof["samples"][-1]["finished"] <= proof["release_requested"]
         <= released["monotonic"] <= forwarded["monotonic"]
         <= read(business / "terminal-2-timing.json")["observed"][1] / 1e9,
         "same actual Ready released before DB2 terminal and all relay sockets settled")
    completed = datetime.fromisoformat(terminal["date_done"])
    need(completed.utcoffset() is not None and completed.timestamp() >= released["epoch"],
         "Index terminal must be created after the actual release boundary")
    retired = read(owned / "index-pg-gate-exit.json")
    need(type(retired["actual_exit"]) is int and retired["actual_exit"] == 0 and retired["forced"] is False
         and all(retired[key] is True for key in ("actual_wait_completed", "stdout_EOF", "stderr_EOF",
                                                  "all_original_process_generations_absent"))
         and matching["scope_locks_absent"] is True,
         "owned PG gate process and Index Redis file lease must retire")
    return {"exact_held_ready": True, "postgres_backend_pid": held["backend_pid"],
            "pre_release_result_absent": True, "same_index_lease_held": True,
            "exact_original_terminal": True, "observer_and_index_lease_retired": True}
