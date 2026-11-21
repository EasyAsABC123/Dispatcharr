"""Attach exactly the qualified Original plain recording HLS HTTP cases."""
import hashlib
import json
import re

KIND = "captured_original_plain_recording_hls_admin_HTTP"
ROUTE = "route-37da04e4eae7ba9df918:GET"
EXPLICIT = "route-0b392188d7e4e96171af:GET"
PATTERN = r"api/channels/^recordings/(?P<pk>[^/.]+)/hls/(?P<seg_path>.+)/$"
HANDLER = "apps.channels.api_views.RecordingViewSet"
RECEIPT = "scripts/rust-integration/m0-recording-hls-plain-get-original.json"
BINDING = "scripts/rust-contracts/recording-hls-plain-get-bindings.json"
FIXTURE = "pinned-Original-recording-hls-plain-GET"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
IMAGE = "ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
CASES = ("canonical-missing", "zero-padded-missing", "alphabetic-missing", "signed-positive-missing",
         "signed-negative-missing", "unicode-decimal-missing")
CLEANUP = {"compose_down_passed": True, "remaining_owned": dict.fromkeys(("containers", "volumes", "networks"), 0),
           "exact_container_and_process_generations_absent": dict.fromkeys(("baseline", "origin"), True)}


def dependencies():
    return {RECEIPT, BINDING}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def case_hash(value):
    return digest(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def assert_wire(wire):
    assert type(wire["status"]) is int and 100 <= wire["status"] <= 599
    assert set(wire) == {"status", "mime", "allow", "vary", "body_bytes", "content_length",
                         "body_sha256", "has_redirect_location", "has_auth_challenge"}
    assert wire["mime"] in (None, "application/json", "text/html", "text/plain", "application/x-mpegurl", "other")
    assert wire["allow"] in (None, "GET, HEAD, OPTIONS", "GET, OPTIONS", "other")
    assert wire["vary"] in (None, "Accept", "origin", "Accept, origin", "Accept, origin, Cookie", "other")
    assert type(wire["body_bytes"]) is int and 0 <= wire["body_bytes"] <= 32768
    assert wire["content_length"] in (None, wire["body_bytes"])
    assert re.fullmatch("[a-f0-9]{64}", wire["body_sha256"])
    assert type(wire["has_redirect_location"]) is bool and wire["has_auth_challenge"] is False


def projected_binding(payload):
    proof = json.loads(payload)
    observed = proof["observed"]
    assert proof["Original_revision"] == ORIGINAL
    assert proof["Original_image"] == IMAGE and proof["Original_archive_sha256"] == ARCHIVE
    assert proof["literal_exit"] == 0 and proof["independent_reducer_exit"] == 0
    assert proof["register_declaration"] == ROUTE and proof["source_only"] is True
    assert proof["exact_cleanup"] == CLEANUP and proof["formal_M0_accepted"] is False
    assert proof["existing_canonical_precedence_preserved"] is True
    assert re.fullmatch("[a-f0-9]{64}", proof["terminal_receipt_sha256"])
    assert observed["authenticated_recording_list_control"] is observed["same_hls_action_and_authenticators"] is True
    assert observed["confirmed_missing_numeric_targets"] is observed["recording_and_actor_rows_unchanged"] is True
    assert observed["redirects_followed"] is observed["raw_bodies_credentials_or_SQL_values_retained"] is False
    assert observed["Django"] == "6.0.9" and observed["DRF"] == "3.18.3"
    scope = {"id": FIXTURE, "baseline_revision": ORIGINAL, "baseline_image": IMAGE,
             "baseline_archive_sha256": ARCHIVE,
             "receipt": {"path": RECEIPT, "sha256": digest(payload)},
             "setup": ["Start exact stock Original in its isolated Compose fixture.",
                       "Verify admin Bearer credentials through the owned recording-list route.",
                       "Check Django resolver metadata and verify both numeric recording keys are absent.",
                       "Observe both ordinary numeric controls and the four matched plain HLS reads.",
                       "Verify unchanged Recording/User rows and retire all owned role generations."],
             "roles": {"admin": {"authenticated": True, "credential": "owned_Bearer",
                                 "user_level": 10, "active": True, "staff": True, "superuser": True}}}
    assert observed["HTTP_GETs"] == 7 and len(observed["cases"]) == len(CASES)
    cases = []
    for index, (actual, label) in enumerate(zip(observed["cases"], CASES, strict=True)):
        assert actual["case"] == label and actual["first_route"] == (EXPLICIT if index < 2 else ROUTE)
        assert_wire(actual["wire"])
        if index >= 2:
            cases.append({"label": label, "source_index": index, "record_json_pointer": f"/observed/cases/{index}",
                          "record_sha256": case_hash(actual), "HTTP_method_observed": "GET", "role": "admin",
                          "response": actual["wire"]})
    contract = {
        "contract_id": ROUTE, "method": "GET", "source_action": "hls", "source_handler": HANDLER,
        "pattern_exact": PATTERN, "owning_milestone": "M5", "fixture_id": FIXTURE,
        "observed_roles": ["admin"], "source_role_specs": scope["roles"], "fully_verified": False,
        "cases": cases, "assertions": [
            "Django resolved all four fixed examples to the exact declared plain DRF GET.",
            "The same bounded requests returned the captured status and wire metadata.",
            "The recording and actor rows and the final owned resource inventory matched the expected state.",
        ],
        "remaining": ["Other roles and input shapes", "Native runtime parity"],
        "result": {"state": "fresh_Original_scoped_HTTP_observed_Native_unmeasured", "Source_cases_passed": 4,
                   "new_Source_cases_this_task": 4, "complete_role_matrix": False,
                   "Native_application_qualified": False, "performance_verified": False},
    }
    return {"format_version": 1, "binding_kind": KIND, "operation_count": 1, "unique_Source_cases": 4,
            "fixtures": {FIXTURE: scope}, "contracts": [contract], "fully_verified_operations": 0,
            "Source_only": True, "Native_coverage_increased": False}


def validate(root, value, _bindings):
    assert value == projected_binding((root / RECEIPT).read_bytes()), "exact plain recording HLS Source assignment"


def restore_before(root, routes, summary):
    """Recover the exact prior register projection for historical checks."""
    reference = summary["inputs"].pop()
    assert reference["kind"] == KIND and reference["path"] == BINDING
    recorded = (root / BINDING).read_bytes()
    assert digest(recorded) == reference["sha256"]
    binding = json.loads(recorded)
    validate(root, binding, None)
    item, = binding["contracts"]
    route, = (row for row in routes if row["route_id"] + ":GET" == ROUTE)
    operation, = route["operations"]
    assert operation.pop("executable_Source_contract") == item
    assert operation["setup_fixture"] == binding["fixtures"][FIXTURE]
    assert operation["result"] == item["result"] and operation["assertions"] == item["assertions"]
    assert operation["role_matrix"] == {
        "state": "partial_Original_real_HTTP_capture_assignment", "required_roles": [],
        "observed_fixture_definitions": item["source_role_specs"], "complete": False,
        "observed_roles": ["admin"],
    }
    operation.update(setup_fixture=None, assertions=[], result=None, coverage_state="not_yet_mapped",
                     role_matrix={"state": "unmapped", "required_roles": []})
    assert case_hash(operation) == "d44ee96e8365b116e9b10a703f6dcbdfca82cd482908463a0dbda8a94c31c720"
    summary["operations"] -= 1
