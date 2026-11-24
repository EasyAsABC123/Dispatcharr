"""Three owned signed DVR rows through one real Core settings callback.

The same core-produced context, Redis journal and result evaluator own this
finite paired case. Original Dispatcharr and dependency authors retain credit.
"""
import base64
import datetime as dt
import json

import core_cases

TASK = "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change"
ARTWORK = "apps.channels.tasks.prefetch_recording_artwork"
READBACK = """
from django_celery_beat.models import PeriodicTask
records = {}
for pk in owned:
    row = Recording.objects.get(pk=pk)
    schedule = PeriodicTask.objects.select_related('clocked').get(name='dvr-recording-'+str(pk))
    records[str(pk)] = {
        'channel': row.channel_id,
        'start': row.start_time.isoformat(), 'end': row.end_time.isoformat(),
        'program': row.custom_properties.get('program'),
        'stored_task_id': row.task_id,
        'periodic': {
            'id': schedule.pk, 'name': schedule.name, 'task': schedule.task,
            'args': json.loads(schedule.args), 'clocked': schedule.clocked.clocked_time.isoformat(),
            'enabled': schedule.enabled, 'one_off': schedule.one_off,
        },
    }
print(json.dumps(records))
"""
UTC = dt.timezone.utc


def require(condition, label):
    if not condition:
        raise AssertionError(label)


def rows_to_book(now):
    """Full seconds plus naive/offset minutes and compact offset minutes."""
    anchor = (now + dt.timedelta(hours=2)).replace(second=0, microsecond=0)
    east = dt.timezone(dt.timedelta(hours=5, minutes=30))
    west = dt.timezone(dt.timedelta(hours=-1))
    for index, format_ in enumerate(("seconds", "minute", "compact"), start=1):
        start = anchor + dt.timedelta(minutes=30 * index)
        if format_ == "seconds":
            start += dt.timedelta(seconds=27)
        end = start + dt.timedelta(minutes=10)
        if format_ == "seconds":
            programme = start.isoformat(), end.isoformat()
        elif format_ == "minute":
            programme = (start.replace(tzinfo=None).isoformat(timespec="minutes"),
                         end.astimezone(east).isoformat(timespec="minutes"))
        else:
            programme = (start.strftime("%Y%m%dT%H%MZ"),
                         end.astimezone(west).strftime("%Y%m%dT%H%M%z"))
        yield format_, start, end, programme


def readback(fixture, identifiers):
    return fixture.sql("owned=" + repr(tuple(identifiers)) + "\n" + READBACK)


def as_utc_argument(raw):
    value = dt.datetime.fromisoformat(raw)
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def verify_row(fixture, row, ident, program, start, end):
    require(row["program"] == program, "ExactPersistedProgrammeMetadata")
    require(row["channel"] == fixture.channel_id, "ExactOwnedRecordingChannel")
    require(dt.datetime.fromisoformat(row["start"]) == start
            and dt.datetime.fromisoformat(row["end"]) == end, "ExactDurableRecordingWindow")
    periodic = row["periodic"]
    require(periodic["name"] == row["stored_task_id"] == f"dvr-recording-{ident}",
            "ExactRecordingScheduleIdentity")
    require(periodic["task"] == "apps.channels.tasks.run_recording"
            and periodic["one_off"] is True and periodic["enabled"] is True,
            "ActualEnabledClockedOneOff")
    args = periodic["args"]
    require(args[:2] == [ident, fixture.channel_id] and len(args) == 4
            and as_utc_argument(args[2]) == start and as_utc_argument(args[3]) == end,
            "ExactClockedTaskArguments")
    require(dt.datetime.fromisoformat(periodic["clocked"]) == start, "ExactClockedDueTime")


def verify_artwork(children, identifiers):
    selected = []
    for row in children:
        if row["headers"]["task"] != ARTWORK or not row["headers"].get("eta"):
            continue
        payload = json.loads(base64.b64decode(json.loads(row["raw_frame"])["body"], validate=True))
        if isinstance(payload, list) and len(payload) == 3 and isinstance(payload[0], list):
            args = payload[0]
            if len(args) == 1 and type(args[0]) is int and args[0] in identifiers:
                selected.append(args[0])
    require(sorted(selected) == sorted(identifiers), "OneActualParentedArtworkPerOwnedRecording")


def run(fixture):
    require(fixture.case == "core-produced" and fixture.backend in ("Original", "Native")
            and fixture.binding.get("core_offset_iso_pair") is True, "ExactFiniteCoreOffsetContext")
    module = core_cases.base(fixture)
    report = {"status": "running", "backend": fixture.backend,
              "scope": "one signed settings write and real worker, three persisted programme time spellings",
              "full_milestone_passed": False}
    output = module.directory(fixture) / "core-offset-iso.json"
    module.save(output, report)
    ledger = "/shared/" + fixture.case + "/m6-core-publications.jsonl"
    fixture.role("m6-core-publications",
                 ["/dispatcharrpy/bin/python", "/normal-dvr-tools/broker_monitor.py"],
                 extra={"NORMAL_DVR_LEDGER": ledger})
    try:
        fixture.wait(lambda: (fixture.case_data / "m6-core-publications.ready").is_file(),
                     10, "ActualCoreOffsetISOObserver")
        fixture.sql("from apps.channels.models import Channel;Channel.objects.filter(pk="
                    + str(fixture.channel_id) + ").update(logo_id=None,epg_data_id=None);print(json.dumps(True))")
        authored = {}
        for label, start, end, (raw_start, raw_end) in rows_to_book(dt.datetime.now(UTC)):
            program = {"title": fixture.channel_name, "start_time": raw_start, "end_time": raw_end}
            reply = fixture.api("POST", "/api/channels/recordings/", {
                "channel": fixture.channel_id, "start_time": start.isoformat(), "end_time": end.isoformat(),
                "custom_properties": {"program": program},
            })
            require(reply["status"] == 201, "ActualSignedBookingHTTP201")
            pk = reply["body"]["id"]
            require(type(pk) is int and pk > 0 and pk not in authored, "ExactDistinctBookingIDs")
            authored[pk] = (label, start, end, program)
        before = readback(fixture, authored)
        for ident, (_, start, end, program) in authored.items():
            verify_row(fixture, before[str(ident)], ident, program, start, end)
        report.update(bookings={ident: row[0] for ident, row in authored.items()}, before=before)
        module.save(output, report)
        subscription = module.subscribe(fixture)
        began, patch = core_cases.core_patch(fixture, "dvr_settings", {
            "pre_offset_minutes": 2, "post_offset_minutes": 3,
        })
        observed = core_cases.actual_task(fixture, module, TASK, began)
        metadata = observed["backend"]["metadata"]
        result = metadata["result"]
        require(type(result["changed"]) is int and result["changed"] >= 3
                and result["pre"] == 2 and result["post"] == 3, "ObservedCoreOffsetsAllRows")
        report.update(actual_patch_status=patch["status"], task=observed)
        module.save(output, report)
        parent = observed["publication"]["headers"]["id"]
        publications = [row["headers"]["id"] for row in core_cases.frames(fixture)
                        if row["headers"]["task"] == TASK and row["observed_epoch"] >= began]
        require(publications == [parent], "OneObservedOffsetPublicationForTheSignedWrite")
        after = readback(fixture, authored)
        report["after"] = after
        module.save(output, report)
        for ident, (_, start, end, program) in authored.items():
            saved = after[str(ident)]
            verify_row(fixture, saved, ident, program, start - dt.timedelta(minutes=2),
                       end + dt.timedelta(minutes=3))
            require(saved["periodic"]["id"] != before[str(ident)]["periodic"]["id"],
                    "ActualPreviousOneOffWasReplaced")
        expected_ids = {item[0][0] for item in metadata["children"]}

        def actual_children():
            rows = [row for row in core_cases.frames(fixture) if row["headers"].get("parent_id") == parent]
            return rows if {row["headers"]["id"] for row in rows} == expected_ids else None

        children = fixture.wait(actual_children, 10, "ActualCoreOffsetChildren")
        verify_artwork(children, authored)
        outcomes = core_cases.child_results(fixture, children, parent, expected_ids)

        def actual_event():
            events = module.events(fixture, subscription)
            return next((item for item in events if
                         item.get("data", {}).get("type") == "recordings_refreshed"
                         and item["data"].get("success") is True
                         and item["data"].get("rescheduled") == result["changed"]), None)

        event = fixture.wait(actual_event, 10, "ActualRecordingsRefreshedEvent")
        report.update(children=outcomes, observed_event=event)
    finally:
        try:
            stopped = fixture.role("m6-core-publications", [], stop=True)
            require(stopped and stopped["all_original_process_generations_absent"],
                    "ActualCoreObserverRetired")
            report["actual_observer_retired"] = True
        finally:
            module.save(output, report)
    report["status"] = "passed"
    module.save(output, report)
    return report
