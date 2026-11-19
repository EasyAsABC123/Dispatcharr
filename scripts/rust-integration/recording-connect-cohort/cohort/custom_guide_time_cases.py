"""Stock UTC Time Pattern on public XMLTV, React grid and XC short EPG."""
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode
import base64
import json
import xml.etree.ElementTree as ET

ORIGIN = "http://127.0.0.1:18097"
DURATION = timedelta(minutes=91)
CASES = (
    ("3 Owned timed morning @ 08:15 AM", "Owned timed morning", (8, 15),
     "8:15", "8:15 AM", "9:46", "9:46 AM", "08", "3"),
    ("4 Owned timed late @ 22:45", "Owned timed late", (22, 45),
     "22:45", "10:45 PM", "0:16", "12:16 AM", "22", "4"),
)
CONFIG = {
    "title_pattern": r"^(?<hour>\d+) (?<title>.+?) @",
    "time_pattern": r"@ (?<hour>\d+):(?<minute>\d+)(?: (?<ampm>[AP]M))?",
    "timezone": "UTC",
    "output_timezone": "UTC",
    "program_duration": 91,
    "title_template": "Live {title} {starttime_long} - {endtime24}",
    "description_template": "Live on {date}",
    "subtitle_template": "Time {hour}:{minute}",
    "upcoming_title_template": "Upcoming {starttime}",
    "upcoming_description_template": "Before {starttime24}",
    "ended_title_template": "Ended {endtime}",
    "ended_description_template": "After {endtime_long}",
    "channel_logo_url": ORIGIN + "/time-logo/{hour}.png",
    "program_poster_url": ORIGIN + "/time-poster/{hour}/{starttime}.png",
    "category": "Sport,Live",
    "include_live": True,
    "include_new": True,
}


def expected(case, day):
    _, name, (hour, minute), start24, start12, end24, end12, raw_hour, _ = case
    event = day.replace(hour=hour, minute=minute)
    return {
        "event": event,
        "upcoming": ("Upcoming " + start12, "Before " + start24, None),
        "main": (f"Live {name} {start12} - {end24}", "Live on " + day.date().isoformat(),
                 f"Time {raw_hour}:{minute}"),
        "ended": ("Ended " + end12, "After " + end12, None),
    }


def observe(expected, start, stop, title, description, subtitle, check_subtitle=True):
    main_start = expected["event"]
    main_end = main_start + DURATION
    if stop <= main_start:
        kind = "upcoming"
    elif start >= main_end:
        kind = "ended"
    else:
        kind = "main"
        assert start == main_start and stop == main_end
    labels = expected[kind]
    assert title == labels[0] and description == labels[1]
    if check_subtitle:
        assert subtitle == labels[2]
    assert timedelta(0) < stop - start <= DURATION
    return kind


def run(fixture, setup, private, request):
    case = setup["time"]
    day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    profile = quote(case["profile"]["name"], safe="")
    xml = ET.fromstring(request(
        fixture, "/output/epg/" + profile + "/?days=1&prev_days=0&tvg_id_source=tvg_id",
        "time-XML", private,
    ))
    headers = xml.findall("channel")
    assert [row.findtext("display-name") for row in headers] == [case[0] for case in CASES]
    lower = day + timedelta(hours=5, minutes=12, microseconds=123456)
    upper = day + timedelta(days=1, hours=2, minutes=2, microseconds=234567)
    query = urlencode({"channel_profile_id": case["profile"]["id"],
                       "start": lower.isoformat(), "end": upper.isoformat()})
    grid = json.loads(request(fixture, "/api/epg/grid/?" + query, "time-grid", private))
    xml_observed, grid_observed, xc_observed = [], [], []
    for item, channel, identity in zip(CASES, headers, case["channels"]):
        plan = expected(item, day)
        poster = ORIGIN + "/time-poster/" + item[-2] + "/{starttime}.png"
        assert channel.find("icon").attrib["src"] == ORIGIN + "/time-logo/" + item[-1] + ".png"
        xml_kinds = []
        for row in (row for row in xml.findall("programme") if row.attrib["channel"] == identity["tvg_id"]):
            start = datetime.strptime(row.attrib["start"], "%Y%m%d%H%M%S %z")
            stop = datetime.strptime(row.attrib["stop"], "%Y%m%d%H%M%S %z")
            kind = observe(plan, start, stop, row.findtext("title"), row.findtext("desc"), row.findtext("sub-title"))
            assert row.findtext("date") == start.date().isoformat()
            assert row.find("icon").attrib["src"] == poster
            assert (row.find("live") is not None) == (kind == "main")
            assert (row.find("new") is not None) == (kind == "main")
            assert [entry.text for entry in row.findall("category")] == (["Sport", "Live"] if kind == "main" else [])
            xml_kinds.append(kind)
        assert xml_kinds and xml_kinds.count("main") <= 1
        xml_observed.append(xml_kinds)

        grid_kinds = []
        previous_stop = lower
        for row in (row for row in grid["data"] if row["tvg_id"] == identity["uuid"]):
            start = datetime.fromisoformat(row["start_time"])
            stop = datetime.fromisoformat(row["end_time"])
            assert start == previous_stop
            previous_stop = stop
            kind = observe(plan, start, stop, row["title"], row["description"], row["sub_title"])
            assert row["is_new"] == row["is_live"] == (kind == "main")
            assert row["custom_properties"]["icon"] == poster
            grid_kinds.append(kind)
        assert previous_stop == upper
        assert grid_kinds[0] == "upcoming" and grid_kinds[-1] == "ended" and grid_kinds.count("main") == 1
        grid_observed.append(grid_kinds)

        params = urlencode({"username": setup["xc_username"], "password": setup["xc_password"],
                            "action": "get_short_epg", "stream_id": identity["id"], "limit": 2,
                            "days": 1, "prev_days": 0})
        xc = json.loads(request(fixture, "/player_api.php?" + params, "time-XC-" + str(identity["id"]),
                                private, False))["epg_listings"]
        assert len(xc) == 2
        kinds = []
        for row in xc:
            start = datetime.fromtimestamp(int(row["start_timestamp"]), timezone.utc)
            stop = datetime.fromtimestamp(int(row["stop_timestamp"]), timezone.utc)
            title = base64.b64decode(row["title"], validate=True).decode()
            desc = base64.b64decode(row["description"], validate=True).decode()
            kinds.append(observe(plan, start, stop, title, desc, None, check_subtitle=False))
        xc_observed.append(kinds)
    assert datetime.now(timezone.utc).date() == day.date(), "paired timed cases must stay within one observed UTC date"
    return {"xml_phases": xml_observed, "grid_phases": grid_observed, "xc_phases": xc_observed,
            "two_exact_UTC_kickoffs": True, "grid_microsecond_window_edges": True,
            "event_unclipped_and_filler_clipped": True, "logo_and_poster_capture_order": True}
