"""Verify retained Original Source16 and its two independently owned supplements."""
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re

KEY = "source/backup-restore-schedules"
STATUS = "retained_source_baseline"
AUTHORITY = "retained_source_authority"
MEDIA_SCOPE = "M8 Original Source16 with detached synthetic recording-reference and PostgreSQL diagnostic observation"
WARNING_SCOPE = "M8 Original same-archive PostgreSQL ACL raw stderr warning portability diagnostic"
WARNING_OWNER_SEAL = "a163d53a4e2e2a794a026c14d3212faeb70c0bf2e0b78a36b1a63f3308636eb6"
WARNING_LINE = "pg_restore: warning: errors ignored on restore: 1\n"
MEDIA_RANGE = {"status": 206, "bytes": 564,
               "sha256": "1f8921ff01109efbf94c9bf0806411bc8999f25b9584de00b7f273efc1d3c710"}
BASELINE_PINS = {"receipt": "141756ce0e2cc047e2bfbf51d0aa56f18470de40730a86288f52d558c3197569",
                 "evaluator": "7844cf3db5bf31123c587c678aa935bab0fd156fa13d7ce426caea2adf60243b"}
MEDIA_PINS = {"receipt": "a67af25af5804c8d267aadd0314fe1b87293fbae03f3a70df6647eb785b791bb",
              "evaluator": "20e7e9cf75e75722182a2c003bee4e37c0b22928de61e70703fbd2c6424b7901"}
SUPPLEMENT_COMMON = {"controller_exit", "owner_exit", "run_exit", "evaluator_exit", "receipt",
                     "evaluator", "worker_journal", "cleanup", "owned_network", "network_absence"}
SCOPE = "M8 Original common Source16 schedule and restore workflow"
GIT = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
IMAGE = "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
DECLARATION = "37747abfb929a8d8cc301d57661a41730181f5d1bc34ad51920aff85a12110fa"
STATUSES = [200, 200, 200, 400, 400, 200, 200, 404, 202, 401, 202, 401, 202, 401, 200, 202]
FILES = {
    "definition", "controller_exit", "owner_exit", "run_exit", "evaluator_exit", "receipt", "evaluator",
    "calls", "worker_journal", "cleanup", "owned_network", "network_absence", "worker_stdout_summary",
    "worker_stderr_summary",
}


def need(value, label):
    if not value:
        raise ValueError(label)


def checked_files(expected, names):
    need(type(expected) is dict and set(expected) == names, "exact retained Source16 evidence surface")
    result = {}
    for name, entry in expected.items():
        need(type(entry) is dict and set(entry) == {"path", "sha256"}
             and type(entry["path"]) is str and type(entry["sha256"]) is str
             and re.fullmatch("[a-f0-9]{64}", entry["sha256"]), "typed retained Source16 reference: " + name)
        path = Path(entry["path"])
        need(path.is_absolute() and path.resolve() == path and path.is_file() and path.stat().st_size <= 1024 * 1024,
             "bounded regular retained Source16 file: " + name)
        data = path.read_bytes()
        need(sha256(data).hexdigest() == entry["sha256"], "retained Source16 file hash differs: " + name)
        result[name] = data
    return result


def check_calls(definition, calls, schedule):
    targets = definition["cases"]
    need(len(targets) == len(calls) == 16 and
         [row["index"] for row in targets] == [row["index"] for row in calls] == list(range(1, 17))
         and all(type(row["status"]) is int for row in calls) and [row["status"] for row in calls] == STATUSES,
         "retained literal 16-case order and statuses")
    for row, spec in zip(calls, targets, strict=True):
        body = bytes.fromhex(spec["body_hex"])
        if row["index"] == 2:
            utc = datetime.fromtimestamp(schedule["due_epoch"], timezone.utc)
            need(body.count(b"RECIPE_NEXT_MINUTE_UTC") == 1, "exact scheduled UTC input recipe")
            body = body.replace(b"RECIPE_NEXT_MINUTE_UTC", utc.strftime("%H:%M").encode("ascii"))
        need(row["method"] == spec["method"] and row["path"] == spec["path"]
             and row["media"] == "application/json" and row["request_body_sha256"] == sha256(body).hexdigest()
             and re.fullmatch("[a-f0-9]{64}", row["body_sha256"]), "retained Source16 literal wire")
    for index in (10, 12, 14):
        need(calls[index - 1]["body"] == {"detail": "Authentication required"}
             and "?task_token={" in calls[index - 1]["path"] and targets[index - 1]["actor"] == "signed-anonymous",
             "frozen literal signed query is distinct from ordinary token controls")
    need(targets[14]["actor"] == "restored-archive-admin", "frozen archived administrator case")


def check_worker(raw, result):
    events = [json.loads(line) for line in raw.splitlines()]
    need(len(events) == 15 and events[0] == {"event": "observer_ready", "task_id": None}, "exact settled worker event count")
    starts = [row for row in events if row["event"] in ("schedule_prerun", "prerun")]
    need(len(starts) == 5 and len({row["task_id"] for row in starts}) == 5
         and len({(row["worker_pid"], row["worker_generation"]) for row in starts}) == 1,
         "five real distinct jobs in the same prefork generation")
    private_receipts = [(result["schedule"]["worker_receipt_sha256"], "completed", [])]
    for item in result["restore"]:
        status = item["result"]["status"]
        private_receipts.append((item["worker_receipt_sha256"], status, ["cleanup", "restore"] if status == "completed" else []))
    for start, (expected_hash, expected_status, command_types) in zip(starts, private_receipts, strict=True):
        task_id = start["task_id"]
        group = [row for row in events if row["task_id"] == task_id]
        last = group[-1]
        need(start["request_id"] == task_id and start["exact_owned_database"] is True and start["eager"] is False
             and last["state"] == "SUCCESS" and last["result"]["status"] == expected_status
             and sha256(json.dumps(group, sort_keys=True).encode()).hexdigest() == expected_hash,
             "retained actual matching non-eager worker terminal")
        commands = [row for row in group if row["event"] == "command"]
        need([row["action"] for row in commands] == command_types and all(row["exact_owned_target"] is True for row in commands),
             "inert refusals have no restore command and positive jobs retain the scoped command targets")


def check_cleanup(files):
    cleanup, network, absence = (json.loads(files[name]) for name in ("cleanup", "owned_network", "network_absence"))
    observed = cleanup["exact_CID_absence"]
    captured = cleanup["captured_original_process_generations"]
    need(cleanup["exact_owned_absence"] is cleanup["all_original_process_generations_absent"] is
         cleanup["host_CLI_groups_settled"] is True and not cleanup["errors"]
         and cleanup["inventories"] == {"container": [], "network": [], "volume": []}
         and len(observed) == len(captured) == 2
         and len({row["Id"] for row in observed}) == 2 and {row["Id"] for row in observed} == {row["Id"] for row in captured}
         and all(row["actual_missing_object"] is row["original_host_generation_absent"] is True for row in observed)
         and {row["image"] for row in captured} == {IMAGE}
         and network["name"] == cleanup["project"] + "_default"
         and absence == [{"identity": network[key], "missing": True, "actual_exit": 1} for key in ("name", "id")],
         "actual stock Original container, process generation and network closure")
    return cleanup["project"], network["id"]


def same_pins(files, pins):
    need(all(sha256(files[name]).hexdigest() == digest for name, digest in pins.items()),
         "reviewed retained Original evidence hash")


def read_supplement(refs, names, scope):
    files = checked_files(refs, SUPPLEMENT_COMMON | names)
    need(all(files[name] == b"0\n" for name in ("controller_exit", "owner_exit", "run_exit", "evaluator_exit")),
         "actual supplemental controller, owner, business and evaluator exits")
    receipt, proof = (json.loads(files[name]) for name in ("receipt", "evaluator"))
    need(receipt["scope"] == scope and receipt["passed"] is receipt["exact_cleanup"] is True
         and receipt["formal_Source16_accepted"] is False
         and re.fullmatch(r"Original-[a-f0-9]{8}", receipt["fixture"])
         and proof["private_receipt_sha256"] == sha256(files["receipt"]).hexdigest(),
         "actual supplemental Original business and independent evaluation join")
    report_path = Path(refs["receipt"]["path"])
    fixture = report_path.parent / "fixtures" / receipt["fixture"]
    for key, relative in {"controller_exit": "actual-exit", "cleanup": "independent-cleanup.json",
                          "owned_network": "owned-network.json", "network_absence": "exact-network-absence.json",
                          "worker_journal": "data/normal-backup/source16-restore-worker.jsonl"}.items():
        base = report_path.parent if key == "controller_exit" else fixture
        need(Path(refs[key]["path"]) == base / relative, "supplemental run custody: " + key)
    owner = Path(refs["owner_exit"]["path"]).parent
    phase = "proof" if scope == WARNING_SCOPE else "evaluate"
    for name, relative in (("owner_exit", "actual-exit"), ("run_exit", "run.actual-exit"),
                           ("evaluator_exit", phase + ".actual-exit"), ("evaluator", phase + ".stdout")):
        need(Path(refs[name]["path"]) == owner / relative, "same supplemental owner ledger and literal phase")
    return files, receipt["result"], proof, check_cleanup(files), fixture


def read_events(files, key):
    return [json.loads(line) for line in files[key].splitlines()]


def worker_restore(events, task):
    group = [row for row in events if row["task_id"] == task]
    need([row["event"] for row in group] == ["prerun", "command", "command", "postrun"],
         "one real prefork restore group")
    start, first, second, terminal = group
    need(start["request_id"] == task and start["eager"] is False and start["exact_owned_database"] is True
         and first["action"] == "cleanup" and second["action"] == "restore"
         and first["exact_owned_target"] is second["exact_owned_target"] is True
         and terminal["state"] == "SUCCESS" and terminal["result"]["status"] == "completed"
         and all(start[field] == terminal[field] for field in ("worker_pid", "worker_generation")),
         "exact authorized stock restore subprocess and terminal lineage")
    return group


def check_media(refs, definition):
    files, result, proof, ownership, fixture = read_supplement(refs, {"calls"}, MEDIA_SCOPE)
    same_pins(files, MEDIA_PINS)
    need(Path(refs["calls"]["path"]) == fixture / "common-source16/source16-calls.json",
         "media owner literal source calls custody")
    need(type(result["actual_calls"]) is int and result["actual_calls"] == 16 and result["publication_count"] == 5
         and result["case_statuses"] == STATUSES and result["common_source16_workload_passed"] is True
         and result["root_m478_formal_acceptance"] is False
         and proof["actual_calls"] == 16 and proof["actual_backup_task_publications"] == 5
         and proof["supplemental_signed_http_calls"] == 7 and proof["supplemental_actual_backup_task_publications"] == 2
         and proof["common_source16_workload_passed"] is proof["exact_cleanup"] is True
         and proof["restored_media_reference"] is proof["owned_media_bytes_unchanged"] is True
         and proof["root_m478_formal_acceptance"] is False, "media supplemental proof and case count")
    check_calls(definition, json.loads(files["calls"]), result["schedule"])
    continuity = result["continuity"]
    need(continuity["worker_exit"] == 0 and continuity["copied_database_reused"] is
         continuity["same_prefork_generation_all_five"] is continuity["independent_controls_unchanged"] is True,
         "one copied database, worker and immutable controls throughout the media supplement")
    media = continuity["recording_reference_supplement"]
    need(media["http"] == [206, 202, 200, 404, 202, 200, 206] and media["signed_range"] == MEDIA_RANGE
         and media["typed_reference_restored"] is media["owned_media_unchanged"] is
         media["detached_channel_confirmed"] is media["archive_has_no_media_members"] is True
         and media["queue_after"] == [0, 0, 0, 0], "exact restored detached recording read and unchanged media bytes")
    events = read_events(files, "worker_journal")
    starts = [row for row in events if row["event"] in ("schedule_prerun", "prerun")]
    need(len(starts) == 6 and len({(row["worker_pid"], row["worker_generation"]) for row in starts}) == 1,
         "the five literal jobs and supplemental media restore ran in the same worker generation")
    requests = [row["task_id"] for row in starts if row.get("filename") == "owned-media-reference-restore.zip"]
    need(len(requests) == 1 and type(requests[0]) is str
         and re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", requests[0]),
         "one actual media-supplement restore job")
    group = worker_restore(events, requests[0])
    need(group[0]["filename"] == "owned-media-reference-restore.zip"
         and sha256(json.dumps(group, sort_keys=True).encode()).hexdigest() == media["worker_receipt_sha256"],
         "same worker journal joins the actual signed media-restore result")
    return ownership, sha256(files["receipt"]).hexdigest()


def check_warning(refs):
    files, result, proof, ownership, fixture = read_supplement(refs, {"diagnostics", "owner_seal"}, WARNING_SCOPE)
    stage = Path(refs["receipt"]["path"]).parents[1]
    need(Path(refs["owner_seal"]["path"]) == stage / "SHA256SUMS"
         and Path(refs["owner_exit"]["path"]) == stage / "owner-1/actual-exit",
         "exact raw-warning reviewed owner directory")
    same_pins(files, {"owner_seal": WARNING_OWNER_SEAL})
    need(Path(refs["diagnostics"]["path"]) == fixture / "data/normal-backup/source16-pg-restore-diagnostics.jsonl",
         "ACL captured restore diagnostics custody")
    need(result["stock_acl_warning_scoped_passed"] is result["pre_decode_stderr_bytes_captured"] is
         result["decoded_child_stderr_observed"] is True
         and result["formal_Source16_accepted"] is False and result["original_http"] == [202, 200, 202, 200, 202, 200]
         and result["broker_publications"] == 3 and result["worker_exit"] == 0
         and result["both_restored_exact_synthetic_row"] is result["exact_role_retirement"] is
         result["exact_default_template_unchanged"] is result["same_worker_for_both_restores"] is True
         and re.fullmatch(r"[0-9a-f]{64}", result["unchanged_archive_sha256"])
         and re.fullmatch(r"[0-9a-f]{64}", result["private_broker_journal_sha256"]),
         "real paired same-archive retention, unmodified controls and bounded raw output")
    expected_warning = WARNING_LINE.encode("ascii").hex()
    need(proof["scope"] == WARNING_SCOPE and proof["independently_verified_closure"] is
         proof["stock_acl_diagnostic_observed"] is proof["pre_decode_stderr_bytes_captured"] is True
         and proof["formal_Source16_accepted"] is False
         and proof["raw_warning_line_hex"] == expected_warning and proof["verbatim_decoded_warning_line"] == WARNING_LINE,
         "independent actual raw and decoded warning evidence")
    rows = read_events(files, "diagnostics")
    events = read_events(files, "worker_journal")
    starts = [row for row in events if row["event"] == "prerun"]
    need(len(rows) == len(starts) == 2 and len({row["task_id"] for row in starts}) == 2
         and len(events) == 9 and events[0] == {"event": "observer_ready", "task_id": None}
         and len({(row["worker_pid"], row["worker_generation"]) for row in starts}) == 1,
         "exact ordered genuine warning and control job observations")
    for row, start, label, lines in zip(rows, starts, ("baseline", "renamed_reader"), ([], [WARNING_LINE]), strict=True):
        terminal = worker_restore(events, row["task_id"])[-1]
        need(row["task_id"] == start["task_id"] and start["filename"] == "owned-acl-warning-restore.zip"
             and terminal["result"]["filename"] == start["filename"]
             and all(row[field] == start[field] for field in ("worker_pid", "worker_generation"))
             and row["all_warning_lines_preserved"] is True and row["preserved_warning_lines"] == lines
             and row["raw_warning_lines_hex"] == [line.encode("ascii").hex() for line in lines]
             and row["warning_line_count"] == row["sql_error_line_count"] == len(lines)
             and row["stock_missing_role_only"] is row["stock_acl_statement_matches"] is bool(lines)
             and type(row["returncode"]) is int and (row["returncode"] != 0) is bool(lines)
             and type(row["raw_stderr_size"]) is int and 0 < row["raw_stderr_size"] == row["stderr_bytes"] <= 262144
             and row["raw_stderr_sha256"] == row["stderr_sha256"] and re.fullmatch(r"[0-9a-f]{64}", row["raw_stderr_sha256"])
             and result[label] == {key: value for key, value in row.items()
                                  if key not in {"task_id", "worker_pid", "worker_generation", "all_warning_lines_preserved"}},
             "exact pre-decode stderr joins stock return, worker result and proof")
    return ownership, sha256(files["receipt"]).hexdigest()


def resolve(node, binding):
    need(node["id"] == KEY and node["declared_calls"] == 16
         and node["case_definition"]["sha256"] == DECLARATION, "exact declared M478 Source16 node")
    need(type(binding) is dict and set(binding) <= {"Original_git", "Original_image", "files", "supplements"}
         and binding["Original_git"] == GIT and binding["Original_image"] == IMAGE,
         "exact Original-only retained Source16 subject")
    files = checked_files(binding["files"], FILES)
    same_pins(files, BASELINE_PINS)
    need(sha256(files["definition"]).hexdigest() == DECLARATION
         and all(files[name] == b"0\n" for name in ("controller_exit", "owner_exit", "run_exit", "evaluator_exit")),
         "real declared Original and four literal scoped owner exits")
    receipt, evaluator = (json.loads(files[name]) for name in ("receipt", "evaluator"))
    result = receipt["result"]
    need(receipt["scope"] == SCOPE and receipt["passed"] is receipt["exact_cleanup"] is True
         and receipt["formal_Source16_accepted"] is False and result["common_source16_workload_passed"] is True
         and result["root_m478_formal_acceptance"] is False and type(result["actual_calls"]) is int
         and result["actual_calls"] == 16 and result["publication_count"] == 5 and result["case_statuses"] == STATUSES,
         "one actual Original workload without preexisting Root qualification")
    need(evaluator == {"actual_backup_task_publications": 5, "actual_calls": 16, "common_source16_workload_passed": True,
                       "exact_cleanup": True, "root_m478_formal_acceptance": False,
                       "private_receipt_sha256": sha256(files["receipt"]).hexdigest()},
         "actual independent scope evaluation joins the physical receipt")
    schedule, continuity = result["schedule"], result["continuity"]
    need(schedule["total_runs"] == [0, 1] and schedule["retention"] == 2 and len(schedule["removed"]) == 3
         and len(schedule["retained"]) == 2 and schedule["beat_exit"] == 0 and schedule["beat_retired_before_case4"] is True
         and schedule["queues_after_beat_retirement"] == [0, 0, 0, 0]
         and -1 <= schedule["published_epoch"] - schedule["due_epoch"] <= 30, "retained due task and Beat retirement")
    need(continuity["worker_exit"] == 0 and continuity["same_prefork_generation_all_five"] is True
         and continuity["copied_database_reused"] is continuity["independent_controls_unchanged"] is True
         and re.fullmatch("[a-f0-9]{64}", continuity["archived_actor_hash"]), "retained database and archived actor continuity")
    restore = result["restore"]
    need([(row["index"], row["result"]["status"]) for row in restore] ==
         [(9, "failed"), (11, "failed"), (13, "completed"), (16, "completed")]
         and all(row["sql_unchanged"] is True for row in restore[:2])
         and all(row["sentinel_removed"] is True for row in restore[2:]), "retained inert and positive job effects")
    check_calls(json.loads(files["definition"]), json.loads(files["calls"]), schedule)
    check_worker(files["worker_journal"], result)
    ownership = check_cleanup(files)
    for name in ("worker_stdout_summary", "worker_stderr_summary"):
        summary = json.loads(files[name])
        need(summary["read_completed"] is True and summary["raw_log_payload_copied"] is False,
             "observed bounded worker log summary scope")
    supplements = binding.get("supplements", {})
    need(type(supplements) is dict and set(supplements) <= {"media", "raw_warning"}, "known Source16 evidence supplements")
    own = [ownership]
    evidence = {"common": sha256(files["receipt"]).hexdigest()}
    if "media" in supplements:
        identity, evidence["media"] = check_media(supplements["media"], json.loads(files["definition"]))
        own.append(identity)
    if "raw_warning" in supplements:
        identity, evidence["raw_warning"] = check_warning(supplements["raw_warning"])
        own.append(identity)
    need(all(len(set(column)) == len(own) for column in zip(*own, strict=True)),
         "separate exact retired owners for historical common and supplemental Source16 captures")
    reasons = []
    if "media" not in evidence:
        reasons.append("The scoped restored media-reference capture is missing.")
    if "raw_warning" not in evidence:
        reasons.append("The scoped pre-decode pg_restore warning capture is missing.")
    complete = not reasons
    return {
        "id": KEY, "status": AUTHORITY if complete else STATUS,
        "actual_scoped_source_calls": 16, "actual_scoped_worker_jobs": 5,
        "scoped_source_exact_cleanup": True, "Original_git": GIT, "Original_image": IMAGE,
        "execution_performed": False, "historical_scoped_execution_performed": True,
        "satisfies_dependency": complete, "ready": False, "reasons": reasons,
        "retained_source_receipt_sha256": evidence["common"], "reviewed_scope_receipts": evidence,
    }
