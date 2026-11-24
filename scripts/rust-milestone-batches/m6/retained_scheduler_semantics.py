"""Replay the retained version200 and controlled-clock Original observations."""
import argparse
from base64 import b64decode
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from uuid import UUID

from recording61_owner import digest, read, require

VERSION_SHA = "ab0163e6608ef2bb94ad38ada1b383ca9ad9499077840ad7082e0ed4cf1e1ba5"
CALENDAR_SHA = "d4c5f54f20c481e2db5a2ed6deeb49ea4744edc6e2f71511b16c1a50b7a87219"
VERSION_PAIR_SHA = {
    "owner-version200-2/proof.stdout": "d48e5fe3ee2de7c09e59b524c9734e86d4b22826afdfa4f6a1a00105f8246bf1",
    "actual-version200-2/receipt.json": "485711a53bab6001800d10587474ed788afc66bcba1b010b49039f141fb75bf5",
    "actual-version200-2/original/state.json": "1598f926e2787752cfb784de7b3b92ebecb49fb6ad43be6e8afd141b49214b64",
    "actual-version200-2/native/state.json": "7551406156fbc31d752ec659a7892c267f8712c2029fd53bd3fd8de3a67a7774",
}
NOTIFICATIONS = "core_systemnotification"


def versions(document):
    cases = document["report"]["cases"]
    expected = [
        ("version200-no-update", ["version-0.33.0"], []),
        ("version200-update", [], ["version-0.34.0"]),
        ("version200-repeat", ["version-0.34.0"], ["version-0.34.0"]),
        ("version200-malformed", ["version-0.34.0"], ["version-0.34.0"]),
    ]
    require(len(cases) == len(expected), "version200 case count differs")
    require(len({case["task_id"] for case in cases}) == len(expected), "version200 task reused")
    for case, (name, before_keys, after_keys) in zip(cases, expected):
        require(case["name"] == name, "version200 case order differs")
        before, after = case["before"], case["after"]
        original, updated = (snapshot["tables"][NOTIFICATIONS] for snapshot in (before, after))
        require([row["notification_key"] for row in original] == before_keys
                and [row["notification_key"] for row in updated] == after_keys,
                "version200 notification transition differs")
        if original and before_keys == after_keys:
            require(original[0]["id"] == updated[0]["id"], "version200 replaced the existing notification")
        prior, observed = (snapshot["origin"]["requests"] for snapshot in (before, after))
        require(len(observed) == len(prior) + 1 and observed[:-1] == prior
                and observed[-1]["method"] == "GET" and observed[-1]["status"] == 200,
                "version200 requires one actual Origin GET")
        actual = case["actual"]
        terminal, = actual["postrun"]
        task_id = case["task_id"]
        require(terminal["id"] == terminal["root_id"] == task_id
                and terminal["parent_id"] is None and terminal["state"] == "SUCCESS"
                and terminal["result"] is None and terminal["task"] == "core.tasks.check_for_version_update"
                and actual["Original_AsyncResult"] == {"state": "SUCCESS", "value": None},
                "version200 worker and backend terminal differ")
        backend = json.loads(b64decode(actual["backend"]["raw_base64"], validate=True))
        require(backend["task_id"] == task_id and backend["status"] == "SUCCESS"
                and backend["result"] is None, "version200 raw backend identity differs")
    return [name for name, _, _ in expected]


def scheduled_version_pair(stage):
    for leaf, expected in VERSION_PAIR_SHA.items():
        require(digest(stage / leaf) == expected, "historical scheduled version200 receipt differs")
    ledger = stage / "owner-version200-2"
    require(all((ledger / name).read_bytes() == b"0\n" for name in
                ("actual-exit", "preflight.actual-exit", "physical.actual-exit", "proof.actual-exit")),
            "historical scheduled version200 phases did not exit zero")
    proof = read(ledger / "proof.stdout")
    require(proof["raw_receipt_sha256"] == VERSION_PAIR_SHA["actual-version200-2/receipt.json"],
            "historical scheduled version200 receipt custody differs")
    labels = ["no-update", "update", "repeat", "malformed"]
    for side in ("original", "native"):
        state = read(stage / "actual-version200-2" / side / "state.json")
        require(state["backend"] == side and state["complete"] is True
                and state["scheduled_roots"] == state["durable_run_count_delta"] == 4
                and type(state["worker_exit"]) is int and state["worker_exit"] == 0
                and all(value is True for value in state["cleanup"].values()),
                "historical scheduled version200 lineage or cleanup differs")
        require([row["label"] for row in state["cases"]] == labels
                and all(row["actual_result"] == "SUCCESS/null" and row["upstream_HTTP"] == 200
                        and row["one_saved_run"] is True and row["pinned_notice_result_matched"] is True
                        for row in state["cases"]), "historical scheduled version200 outcomes differ")
    return {"historical_native_revision": proof["candidate"], "case_labels": labels}


def calendar(document):
    gap = document["beat_dst"]
    require(gap["input"]["timezone"] == "America/New_York"
            and (gap["input"]["hour"], gap["input"]["minute"]) == ("2", "30"),
            "calendar controlled spring-gap fixture differs")
    now = datetime.fromisoformat(gap["input"]["now"])
    fired = datetime.fromisoformat(gap["state"]["last_run_at"])
    envelope, = gap["envelopes"]
    headers = envelope["headers"]
    require(now == datetime(2026, 3, 8, 7, tzinfo=timezone.utc)
            and fired - now == timedelta(minutes=30)
            and type(gap["state"]["count"]) is int and gap["state"]["count"] == 1
            and gap["state"]["enabled"] is True and gap["state"]["one_off"] is False,
            "calendar Beat did not persist the first spring-gap execution")
    body = json.loads(b64decode(envelope["body"], validate=True))
    require(headers["task"] == "apps.epg.tasks.refresh_epg_data"
            and UUID(headers["id"]).version == 4 and headers["root_id"] == headers["id"]
            and headers["parent_id"] is None and body[:2] == [[2], {}],
            "calendar spring-gap broker publication differs")

    expected = {"ny_spring_gap": (False, 1800), "ny_fall_first": (True, 86400),
                "ny_fall_second": (False, 86400), "lord_howe_gap": (False, 900),
                "quarter_offset": (True, 86400)}
    samples = [row for row in document["vectors"] if row["mode"] == "legacy-fixed-offset"
               and row["input"]["name"] in expected]
    require(len(samples) == len(expected)
            and {row["input"]["name"]: (row["actual"]["due"], row["actual"]["next_seconds"])
                 for row in samples} == expected
            and all(type(row["actual"]["due"]) is bool for row in samples),
            "calendar calculation boundary observations differ")
    return {"controlled_original_beat_first_publication_offset_seconds": 1800,
            "separate_calculation_observations": sorted(expected)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", type=Path)
    parser.add_argument("calendar", type=Path)
    parser.add_argument("--historical-version-pair", type=Path)
    args = parser.parse_args()
    version, clock = (path.resolve() for path in (args.version, args.calendar))
    require(digest(version) == VERSION_SHA and digest(clock) == CALENDAR_SHA,
            "retained Original observation bytes differ")
    result = {"version200_original": versions(read(version)), "calendar_original": calendar(read(clock)),
              "current_native_verified": False, "full_required_workflow": False}
    if args.historical_version_pair:
        result["version200_historical_pair"] = scheduled_version_pair(args.historical_version_pair)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
