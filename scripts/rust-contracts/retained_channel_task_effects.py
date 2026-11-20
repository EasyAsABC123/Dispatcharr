"""Bind four ordinary Original task effects to existing Celery registrations.

Original Dispatcharr, Celery, Django and PostgreSQL contributors retain full credit.
"""
import base64
from copy import deepcopy
import hashlib
import json
from uuid import UUID

import retained_task_registry_contracts
import source_contracts

CAPTURE = "scripts/rust-milestone-batches/m6/channel-failure-source-v1"
ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
TABLES = ("channel", "logo", "channelstream", "channelprofilemembership")
TASKS = {
    "normal_names": ("candidate-5fb6aee2109e9610a953", "set_channels_names_from_epg",
                     [[1]], {}, "epg_name_setting_progress", "updated_count"),
    "normal_tvg": ("candidate-7b8d35e77b886d865574", "set_channels_tvg_ids_from_epg",
                   [[1]], {}, "epg_tvg_id_setting_progress", "updated_count"),
    "normal_logos": ("candidate-dbc05325b5fec6965cf2", "set_channels_logos_from_epg",
                     [], {"channel_ids": [1]}, "epg_logo_setting_progress", "updated_count"),
    "normal_bulk": ("candidate-b6d3e5196b9fdf4d22f4", "bulk_create_channels_from_streams",
                    [[1], [], 0], {}, "bulk_channel_creation_progress", "created_count"),
}

SOURCE_REPORT = f"{CAPTURE}/attempt3/source-report.json"
CONTROLLER = f"{CAPTURE}/attempt3/report.json"
CLEANUP = f"{CAPTURE}/attempt3/independent-cleanup.json"
GUEST = f"{CAPTURE}/driver3/tools/guest.py"
MANIFEST = f"{CAPTURE}/driver3/input-sha256.json"
INPUT_SHA256 = {
    SOURCE_REPORT: "57f54204594c9f71c4fd12633818b145d96f3965e45aa2e5e8a982a082a90b62",
    CONTROLLER: "481bde98368d9ee2d0b996a1f2dcec0a288a171f1cf25262bedccc4cfa13ec0b",
    CLEANUP: "91cf44aa94c63b31ba154a78aa9bc6ca59a2768ce463b180344a50eb3d263bd2",
    GUEST: "3415cfe7880be835e3975c9665b72c72877120bbebbf6b3c80fb6dc7f60a9685",
    MANIFEST: "df225d03bce1b985252c88db55102eccf80b8bf04134c8aca4d17ab5a0fa4fd8",
}
FIELD = "supplemental_Original_task_observation"


def exact_input(root, path):
    value = (root / path).read_bytes()
    assert hashlib.sha256(value).hexdigest() == INPUT_SHA256[path], f"Original task evidence changed: {path}"
    return value



def rows(raw):
    assert set(raw) == {f"dispatcharr_channels_{table}" for table in TABLES}
    return {table: [json.loads(value) for value in raw[f"dispatcharr_channels_{table}"]]
            for table in TABLES}


def effect(case_name, before, after):
    expected = deepcopy(before)
    expected_channel = expected["channel"][0]
    assert expected_channel["id"] == 1 and expected_channel["epg_data_id"] == 1
    if case_name == "normal_names":
        assert expected_channel["name"] == "Old owned name"
        expected_channel["name"] = "Owned EPG name"
    elif case_name == "normal_tvg":
        assert expected_channel["tvg_id"] == "old.owned.id"
        expected_channel["tvg_id"] = "owned.epg.id"
    elif case_name == "normal_logos":
        assert expected_channel["logo_id"] is None and not expected["logo"]
        expected_channel["logo_id"] = 1
        expected["logo"].append({"id": 1, "url": "http://owned.invalid/logo.png", "name": "Owned EPG name"})
    else:
        created = after["channel"][-1]
        assert all(row["id"] != created["id"] for row in before["channel"])
        assert [created[key] for key in ("name", "tvg_id", "logo_id", "epg_data_id", "user_level")] == [
            "Owned stream", "owned.epg.id", 2, 1, 0
        ]
        expected["channel"].append(created)
        expected["logo"].append({"id": 2, "url": "http://owned.invalid/stream.png", "name": "Owned stream"})
        expected["channelstream"].append({"id": 1, "order": 0, "stream_id": 1,
                                          "channel_id": created["id"]})
    assert after == expected, "scoped SQL effects"
    return {table: {"before": len(before[table]), "after": len(after[table]),
                    "changed": before[table] != after[table]} for table in TABLES}


def observations(entries, capture, control, cleanup):
    assert control["source_archive_sha256"] == ARCHIVE
    assert control["actual_controller_exit"] == control["actual_guest_exit"] == 0
    assert all(control[key] is True for key in ("passed", "source_passed", "guest_settled", "default_unchanged",
                                               "exact_owned_absence", "all_CLI_groups_settled"))
    assert control["Source_API_calls"] == capture["Source_API_calls"] == 0
    assert cleanup["exact_owned_absence"] is True
    assert capture["passed"] is True and len(capture["cases"]) == control["cases"] == 9
    worker = capture["worker"]
    assert worker["actual_wait_exit"] == 0 and worker["leader_absent"] is True
    assert worker["remaining_group"] == [] and "forced_kill" not in worker

    candidates = {row["candidate_id"]: row for row in entries}
    assert list(TASKS) == [case["name"] for case in capture["cases"][:4]]
    selected = []
    seen_task_ids = set()
    for position, case in enumerate(capture["cases"][:4]):
        name = case["name"]
        candidate_id, callable_name, args, kwargs, event_type, counter = TASKS[name]
        wire_name = f"apps.channels.tasks.{callable_name}"
        candidate = candidates[candidate_id]
        registration = candidate["executable_Source_contract"]
        assert candidate["state"] == retained_task_registry_contracts.STATE
        assert candidate["source"] == registration["source"]
        assert candidate_id == "candidate-" + source_contracts.digest(candidate["source"])[:20]
        assert candidate["source"]["kind"] == "task_candidate"
        assert candidate["source"]["path"] == "apps/channels/tasks.py"
        assert candidate["source"]["name"] == callable_name
        assert registration["observation"]["task_name"] == wire_name
        assert candidate["result"] == registration["result"]
        assert candidate["result"]["task_execution_verified"] is candidate["fully_verified"] is False
        assert case["task"] == wire_name and case["args"] == args and case["kwargs"] == kwargs
        assert case["expected_state"] == "SUCCESS" and case["expected_state_match"] is True
        assert not {"capture_error", "controlled_owned_PG_error"} & case.keys()
        identifier = case["actual_task_id"]
        assert str(UUID(identifier)) == identifier and identifier not in seen_task_ids
        seen_task_ids.add(identifier)
        metadata = case["metadata"]
        raw = base64.b64decode(case["raw_backend_base64"], validate=True)
        assert json.loads(raw) == metadata and metadata["task_id"] == identifier
        assert metadata["status"] == "SUCCESS" and metadata["traceback"] is None and metadata["children"] == []
        assert 0 < case["ttl"] <= 3600
        result = {"status": "completed", counter: 1, "error_count": 0, "errors": []}
        if name == "normal_logos":
            result["created_logos_count"] = 1
        assert metadata["result"] == result
        events = case["Channels"]["decoded"]
        blobs = case["Channels"]["raw_base64"]
        assert len(events) == len(blobs) == (6 if name == "normal_bulk" else 3)
        assert all(base64.b64decode(blob, validate=True) for blob in blobs)
        if name == "normal_bulk":
            assert events[-1]["data"] == {"type": "channels_created", "count": 1}
        progress = events[:-1] if name == "normal_bulk" else events
        assert all(event["type"] == "update" and event["data"]["type"] == event_type
                   and event["data"]["task_id"] == identifier for event in progress)
        assert [event["data"]["status"] for event in progress] == (
            ["starting", "processing", "creating_logos", "creating_channels", "completed"]
            if name == "normal_bulk" else ["running", "running", "completed"]
        )
        assert progress[-1]["data"]["progress"] == progress[-1]["data"]["total"] == 1
        assert all(progress[-1]["data"][key] == result[key] for key in (counter, "error_count", "errors"))
        selected.append({
            "candidate_id": candidate_id, "ordinary_case": name,
            "capture_case_index": position, "task": wire_name, "arguments": args,
            "keyword_arguments": kwargs, "backend_status": "SUCCESS", "backend_result": result,
            "backend_record_sha256": hashlib.sha256(raw).hexdigest(),
            "matched_backend_and_progress_task_id": True, "channels_event_count": len(events),
            "channels_progress_event_type": event_type, "SQL_effects": effect(name, rows(case["before"]), rows(case["after"])),
            "full_task_input_coverage": False,
            "fully_verified": False, "Native_coverage_increased": False,
        })
    return selected


def derive(root, entries):
    exact_input(root, GUEST)
    manifest = json.loads(exact_input(root, MANIFEST))
    assert manifest["artifacts/rust/m6-channel-source-failure-prep-3/tools/guest.py"] == INPUT_SHA256[GUEST]
    records = [json.loads(exact_input(root, path)) for path in (SOURCE_REPORT, CONTROLLER, CLEANUP)]
    return observations(entries, *records)


def apply(root, entries):
    evidence = derive(root, entries)
    found = {entry["candidate_id"]: entry for entry in entries}
    for observed in evidence:
        entry = found[observed["candidate_id"]]
        assert FIELD not in entry, "duplicate Original task observation"
        entry[FIELD] = {
            **observed,
            "capture": SOURCE_REPORT,
            "Original_source_archive_sha256": ARCHIVE,
            "Original_controller_exit": 0,
            "Original_guest_exit": 0,
            "Original_worker_exit": 0,
            "Original_exact_owned_absence": True,
        }


def restore_before(root, entries):
    """Remove only verified behavioral supplements before checking historical digests."""
    if not any(FIELD in entry for entry in entries):
        return

    prior = deepcopy(entries)
    for entry in prior:
        entry.pop(FIELD, None)
    apply(root, prior)
    for entry, verified in zip(entries, prior, strict=True):
        assert entry.pop(FIELD, None) == verified.get(FIELD)
