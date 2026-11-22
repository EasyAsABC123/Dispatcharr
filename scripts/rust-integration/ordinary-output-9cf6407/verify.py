"""Verify the bounded M3U and XMLTV observation packets without Docker."""
from argparse import ArgumentParser
from hashlib import sha256
import json
from pathlib import Path
import re
from urllib.parse import quote, urlencode

HERE = Path(__file__).resolve().parent
SUMMARY = "m7-m8-9cf6407-ordinary-compose.json"
PACKET = "22f8d59e38bd9d7096f55f659e68c1f52909c18ee8c9e17ac29b30c748734507"
PUBLISHED = {
    "commit": "9c8fa60f3a7ecf6fbfc31b7cd8a076ffcd938791",
    "summary_sha256": "5800df5a1469d2febdfbee856417251e3a13f37e8b574e7eba31249a933cc1a1",
    "docs_sha256": "8deee5731cf6adca06c7774d662ed836dafef7bfd781392de18ddafacbe485d1",
    "register_sha256": "c84e049dc2aa05df8b35e6fac1b09799790fe89c8ad16115d278feaaaff80998",
}
APP = {
    "application_revision": "9cf640701dacea07ea5f394ce7b18a6fa7c8c088",
    "image": "sha256:fd1a839e1fce4835039b7b079cd0929cd3c658261374cea2c7bbebf813fae3a9",
    "manifest_sha256": "19d618bfe72657a0adfc65fd93094ad42cbaffbe8b609bdc68ba8eddfb7e17a1",
}
ORIGINAL = {
    "revision": "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430",
    "archive_sha256": "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff",
    "historical_authorities": {
        "M3U": "3750d1f048d905cc8611db81aede4c1beccc72f25432af7e14581f983b9f28e8",
        "XMLTV": "7628f3cfadf16b67421805e668ffd93849d0f8e933dbf5b2c3f7bf63941116d4",
    },
}
PARTIAL_RECEIPT = "d9bbda65b22a1656cbd87f58f093b973532e2b8f6fa0c977af50a46e11e4d678"
GENERATIONS = ["baseline", "candidate", "data-init", "origin"]
BASE_M3U, BASE_XMLTV = "/output/m3u", "/output/epg"
PROFILE, XMLTV_PROFILE = "rust-export-profile", BASE_XMLTV + "/XC%20assigned%20profile"
GET_M3U = (
    ("anonymous-basic", "", ()),
    ("anonymous-trailing", "", ()),
    ("profile", PROFILE, ()),
    ("profile-trailing", PROFILE, ()),
    ("profile-ampersand", "Kids & News", ()),
    ("profile-plus", "Plus+News", ()),
    ("profile-unicode", "Café 東京", ()),
    ("profile-percent-once", "Percent%26News", ()),
    ("uncached-logos", PROFILE, (("cachedlogos", "false"),)),
    ("tvg-id", PROFILE, (("tvg_id_source", "tvg_id"),)),
    ("gracenote", PROFILE, (("tvg_id_source", "gracenote"),)),
    ("output-legacy", PROFILE, (("output", "hls"), ("output_profile", "23"))),
    ("output-format-wins", PROFILE, (("output", "hls"), ("output_format", "fmp4"), ("output_profile", "a*b~ c"))),
    ("epg-params", PROFILE, (("prev_days", "2"), ("tvg_id_source", "tvg_id"), ("cachedlogos", "false"), ("days", "3"))),
    ("duplicates", PROFILE, (("output_format", "ts"), ("output_format", "hls"),
                             ("tvg_id_source", "bad"), ("tvg_id_source", "gracenote"))),
)
METHODS = (
    ("named-get", "GET", "absent", 200),
    ("named-empty-post", "POST", "empty", 200),
    ("named-object-post", "POST", "empty-json-object", 200),
    ("named-body-post", "POST", "ordinary-nonempty-json", 403),
)
XMLTV = (
    ("normal HEAD cold", "HEAD", BASE_XMLTV, ()),
    ("normal first", "GET", BASE_XMLTV, ()),
    ("normal replay", "GET", BASE_XMLTV, ()),
    ("normal HEAD cached", "HEAD", BASE_XMLTV, ()),
    ("path " + BASE_XMLTV + "/", "GET", BASE_XMLTV + "/", ()),
    ("path " + XMLTV_PROFILE, "GET", XMLTV_PROFILE, ()),
    ("path " + XMLTV_PROFILE + "/", "GET", XMLTV_PROFILE + "/", ()),
    *((f"query {list(params)!r}", "GET", BASE_XMLTV, params) for params in (
        (("cachedlogos", "false"),), (("cachedlogos", "0"),), (("tvg_id_source", "tvg_id"),),
        (("tvg_id_source", "gracenote"),), (("days", "1"),), (("days", "1"), ("prev_days", "1")),
    )),
)
FORBIDDEN = re.compile(rb'(?i)(/(?:home|var/run|tmp|proc|root)/|file://|'
                       rb'"(?:raw_body|cookie|authorization|token|password|stderr|stdout)"\s*:|'
                       rb'\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|'
                       rb'\bBearer\s+[A-Za-z0-9_-]{20,})')


def stamp(raw):
    return sha256(raw).hexdigest()


def digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def scope(section, summary, ordinal, route_id, handler):
    assert section["role"] == "anonymous"
    assert section["resolver"] == {"ordinal": ordinal, "contract_id": route_id + ":METHOD_UNRESOLVED",
                                  "handler": "apps.output.views." + handler, "declared_method": None,
                                  "registry_pattern": r"output/^" + ("m3u" if ordinal == 362 else "epg") +
                                  r"(?:/(?P<profile_name>[^/]+))?/?$"}
    assert type(section["literal_actual_exit"]) is int and section["literal_actual_exit"] == 0
    assert section["exact_roles_retired"] == GENERATIONS
    assert section["remaining_owned"] == dict.fromkeys(("containers", "networks", "volumes"), 0)
    assert section["proof_sha256"] == summary["proof_sha256"] and digest(section["proof_sha256"])
    result = summary["result"]
    assert section["physical_receipt_sha256"] == result["physical_receipt_sha256"] and digest(section["physical_receipt_sha256"])
    assert result["literal_actual_exit"] == 0 and result["exact_cleanup"] is True
    assert (result["revision"], result["image_id"], result["manifest_sha256"]) == (
        APP["application_revision"], APP["image"], APP["manifest_sha256"])


def compare(row, bound):
    assert row["Original_sha256"] == row["Rust_sha256"] and digest(row["Original_sha256"])
    assert type(row["body_bytes"]) is int and 0 <= row["body_bytes"] <= bound


def get_target(case, profile, query):
    path = BASE_M3U + ("/" + quote(profile, safe="+&") if profile else "")
    if case.endswith("-trailing"):
        path += "/"
    return path + "?" + urlencode((*query, ("_fixture_case", case)))


def m3u(section, summary):
    scope(section, summary, 362, "route-b4a3f38e76ae962a071f", "m3u_endpoint")
    assert section["exact_started_roster_verified"] is True
    assert section["event_deltas"] == dict.fromkeys(("Original", "Rust"), {
        "get_exports": {"m3u_download": 15, "m3u_blocked": 0}, "named_methods": {"m3u_download": 3, "m3u_blocked": 0},
    })
    found = section["get_exports"]
    assert len(found) == summary["result"]["get_cells"] == 15
    for row, (name, profile, query) in zip(found, GET_M3U, strict=True):
        assert (row["case"], row["method"], row["target"], row["status"]) == (name, "GET", get_target(name, profile, query), 200)
        assert row["entries"] == (5 if profile else 17) and row["body_bytes"] > 0
        assert row["observed_wire_fields_matched"] is True
        compare(row, 128 * 1024)
    found = section["named_methods"]
    assert len(found) == summary["result"]["named_method_cells"] == 4
    for row, (name, method, shape, status) in zip(found, METHODS, strict=True):
        target = f"{BASE_M3U}/{PROFILE}?_fixture_case={name}"
        assert (row["case"], row["method"], row["request_body_shape"], row["status"], row["target"]) == (name, method, shape, status, target)
        content_type = "audio/x-mpegurl" if status == 200 else "text/html; charset=utf-8"
        disposition = ['attachment; filename="channels.m3u"'] if status == 200 else []
        assert row["entries"] == (5 if status == 200 else 0) and row["body_bytes"] > 0
        assert row["headers"] == {"content-type": [content_type], "content-disposition": disposition,
                                  "content-length": [str(row["body_bytes"])], "x-content-type-options": ["nosniff"]}
        compare(row, 128 * 1024)
    assert len({row["Original_sha256"] for row in found[:3]}) == 1


def xmltv(section, summary):
    scope(section, summary, 363, "route-a41fa134f8e99a70d993", "epg_endpoint")
    assert section["shared_utc_half_hour"] is True
    assert section["headers"] == {"content-type": "application/xml", "cache-control": "no-cache",
                                  "content-disposition": 'attachment; filename="Dispatcharr.xml"'}
    found = section["cases"]
    assert len(found) == summary["result"]["compared_cases_per_arm"] == 13
    for row, (name, method, base, query) in zip(found, XMLTV, strict=True):
        path = base + ("?" + urlencode(query) if query else "")
        assert (row["case"], row["method"], row["path"], row["status"]) == (name, method, path, 200)
        compare(row, 512 * 1024)
        if method == "HEAD":
            assert row["body_bytes"] == 0 and row["Original_sha256"] == stamp(b"")
            assert "channel_ids" not in row and "programmes" not in row
        else:
            assert row["body_bytes"] > 0 and row["programmes"] > 0 and len(row["channel_ids"]) == len(set(row["channel_ids"]))
    assert found[1]["Original_sha256"] == found[2]["Original_sha256"] == found[4]["Original_sha256"]
    assert found[5]["Original_sha256"] == found[6]["Original_sha256"]
    assert set(found[5]["channel_ids"]) < set(found[1]["channel_ids"])
    assert 0 < found[11]["programmes"] <= found[12]["programmes"] <= found[1]["programmes"]


def validate(packet, summary):
    assert packet["schema"] == 1 and packet["publication"] == PUBLISHED
    assert packet["application"] == APP and all(summary[key] == value for key, value in APP.items())
    assert packet["original"] == ORIGINAL and summary["milestones_closed"] == [] and summary["performance_measured"] is False
    failed = packet["initial_M3U_observer_failure"]
    assert failed == summary["M3U_first_observer_exit"] | {
        "exact_started_roles_retired": ["baseline", "origin"], "original_get_cases_passed": 15,
        "Rust_started": False, "receipt_sha256": PARTIAL_RECEIPT,
    }
    assert failed["literal_exit"] == 1 and digest(failed["independent_cleanup_sha256"])
    m3u(packet["M3U"], summary["M7_M3U"])
    xmltv(packet["XMLTV"], summary["M7_XMLTV"])


def main():
    assert __debug__
    cli = ArgumentParser(description=__doc__)
    cli.add_argument("--published-summary", type=Path, default=HERE.parent / SUMMARY)
    args = cli.parse_args()
    raw = (HERE / "evidence.json").read_bytes()
    summary = args.published_summary.read_bytes()
    assert stamp(raw) == PACKET and not FORBIDDEN.search(raw)
    assert stamp(summary) == PUBLISHED["summary_sha256"]
    validate(json.loads(raw), json.loads(summary))
    print("9cf6407: 19 M3U and 13 XMLTV cases per backend. The first M3U observer exit remains 1.")


if __name__ == "__main__":
    main()
