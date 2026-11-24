"""Bind scoped passing signed stock Original GET evidence to existing operation IDs."""
from hashlib import sha256
import json
import re


PACKET = "scripts/rust-contracts/retained-original-catalogue-get.json"
PACKET_SHA256 = "f03f98b9c83b90efe8d477c2c694603b5982d9fe26d566557fc956b354d5ce3b"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ARCHIVE_SHA = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
IMAGE = "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
RECEIPT_SHA = "e9b7372e916ad40cc30747f3c2233effa36f5223f0c9968cd1a49d66a6fde5e5"
ROLES = ["standard", "admin"]
STATE = "retained_Original_signed_catalogue_GET_observations"


def sha_format(value):
    return isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value) is not None


def packet(root):
    raw = (root/PACKET).read_bytes()
    assert sha256(raw).hexdigest() == PACKET_SHA256
    report = json.loads(raw)
    assert report["schema"] == "Original_signed_stock_catalogue_GET_v1"
    assert (report["original_revision"], report["original_archive_sha256"], report["original_image_id"]) == (
        ORIGINAL, ARCHIVE_SHA, IMAGE)
    assert type(report["literal_exit"]) is int and report["literal_exit"] == 0
    assert report["case_counts"] == {"GET": 68, "HEAD": 17, "OPTIONS": 17}
    assert report["actual_owner_exits"] == dict.fromkeys(("preflight", "run", "cleanup", "proof", "campaign"), 0)
    assert report["actual_cleanup_passed"] is report["all_GET_owned_SQL_projections"] is True
    assert report["exact_container_and_process_generations_absent"] == dict.fromkeys(("baseline", "origin"), True)
    assert report["empty_owned_inventories"] == dict.fromkeys(("containers", "networks", "volumes"), 0)
    fingerprints = report["private_artifact_fingerprints"]
    assert fingerprints["receipt.json"] == RECEIPT_SHA
    assert set(fingerprints) == {"receipt.json", "proof.stdout", "cleanup.stdout", "seed.json",
                                 "dependencies.json", "case-plan.json", "schedule_guest.py"}
    assert all(sha_format(digest) for digest in [*fingerprints.values(), report["state_sha256"]])
    assert len(report["selected_operations"]) == 16
    return report


def checked_cases(cases):
    assert [case["role"] for case in cases] == ROLES
    for case in cases:
        assert case["status"] == 200 and case["media"] == "application/json"
        assert set(case["allow"]) <= {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
        assert {"GET", "HEAD", "OPTIONS"} <= set(case["allow"]) and case["vary"] == ["Accept", "origin"]
        assert type(case["content_length"]) is int and 0 < case["content_length"] <= 4194304
        assert sha_format(case["raw_sha256"]) and sha_format(case["typed_JSON_sha256"])
        assert case["owned_SQL_entity_matches"] is case["scoped_state_unchanged"] is True


def apply(root, rows):
    report = packet(root)
    lookup = {op["contract_id"]: (route,op) for route in rows for op in route["operations"]}
    for key, authority in report["selected_operations"].items():
        route, op = lookup[key]
        assert route["declared_methods_resolved"] is True and op["fully_verified"] is False
        assert all(route[field] == authority[field] for field in ("pattern_exact", "source_handler", "resolver_ordinal", "name"))
        assert op["method"] == authority["method"] == "GET"
        assert all(op[field] == authority[field] for field in ("source_action", "owning_milestone"))
        existing = op["executable_Source_contract"]
        assert existing["contract_id"] == key and existing["result"] == op["result"]
        assert "signed_stock_catalogue_GET" not in existing and op["result"].get("state") is None
        cases = authority["cases"]
        checked_cases(cases)
        existing["signed_stock_catalogue_GET"] = {
            "report": PACKET, "report_sha256": PACKET_SHA256,
            "source_receipt_sha256": RECEIPT_SHA, "source_runtime": "Original direct uWSGI",
            "resolver_ordinal": route["resolver_ordinal"], "observed_roles": ROLES,
            "method": "GET", "cases": cases, "Source_case_observations": 2,
            "owned_SQL_and_broker_hash_unchanged": report["state_sha256"],
            "previous_setup_assertions_and_cases_preserved": True,
            "complete_role_matrix": False, "Native_coverage_increased": False,
            "fully_verified": False,
        }
        result = {**op["result"], "state": STATE, "additional_signed_catalogue_GET_observations": 2}
        existing["result"] = op["result"] = result
