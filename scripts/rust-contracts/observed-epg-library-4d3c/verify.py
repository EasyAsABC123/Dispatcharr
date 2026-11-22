"""Verify a published scoped EPG library attachment and project four GET bindings."""
from argparse import ArgumentParser
from hashlib import sha256
import json
from pathlib import Path
import re

REGISTER = "c84e049dc2aa05df8b35e6fac1b09799790fe89c8ad16115d278feaaaff80998"
BASELINE = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
SOURCE = "09969484794184a2b8754b27935e75e7c60af3529bd569dee5cb941a99aefedc"
CANDIDATE = {
    "revision": "4d3cfbb15e8390e1cd205830b07d4eb76fcb258e",
    "review_sha256": "1b14ce31a1c304aaa86735b3f9f9913dce04e1677e0082df279e2367d0712c27",
    "package_seal_sha256": "edd3094c92ac78a8e9cff26296fa238105739798092ce540c8515933de42580f",
}
EXPECTED_ALLOW = {
    "/api/epg/sources/": "GET, POST, HEAD, OPTIONS",
    "/api/epg/sources/{id}/": "GET, PUT, PATCH, DELETE, HEAD, OPTIONS",
    "/api/epg/epgdata/": "GET, HEAD, OPTIONS",
    "/api/epg/epgdata/{id}/": "GET, HEAD, OPTIONS",
}
EVIDENCE_PATH = "observed-epg-library-4d3c/epg-library.json"
TARGETS = {
    "route-e8763dfe777286fc6b57:GET": (149, "sources", "list", 0, 16),
    "route-2bb81f30c5e580e289f8:GET": (153, "sources", "detail", 8, 17),
    "route-dfd94c84789ce0429d99:GET": (167, "epgdata", "list", 31, 51),
    "route-d64394eade0a18a43d8b:GET": (169, "epgdata", "detail", 39, 52),
}
HANDLERS = {"sources": "apps.epg.api_views.EPGSourceViewSet", "epgdata": "apps.epg.api_views.EPGDataViewSet"}
ROLES = {"admin": 10, "standard": 1}
RETIREMENT = dict.fromkeys(("baseline", "origin", "data-init", "candidate"), True)
FORBIDDEN = re.compile(
    rb'(?i)(?:file://|/(?:home|tmp|var/run|proc|mnt|root)/|"(?:token|password|authorization|raw_body|seed|stderr|stdout)"\s*:|'
    rb'\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}|\bBearer\s+[A-Za-z0-9_\-]{20,})')


def digest(value):
    return isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value) is not None


def roster(rows):
    indexed = {(route["resolver_ordinal"], op["contract_id"]): (route, op)
               for route in rows for op in route["operations"]}
    result = {}
    for contract, (ordinal, kind, surface, admin, standard) in TARGETS.items():
        route, op = indexed[ordinal, contract]
        expected_path = f"/api/epg/{kind}/" + ("{id}/" if surface == "detail" else "")
        assert route["pattern_exact"] == f"api/epg/^{kind}/" + (r"(?P<pk>[^/.]+)/$" if surface == "detail" else "$")
        assert route["source_handler"] == HANDLERS[kind]
        assert (op["method"], op["owning_milestone"], op["source_action"]) == (
            "GET", "M5", "retrieve" if surface == "detail" else "list")
        assert op["fully_verified"] is op["role_matrix"]["complete"] is False
        assert "case_bindings" not in op
        source = op["executable_Source_contract"]
        assert source["contract_id"] == contract and source["method"] == "GET"
        for role, index in (("admin", admin), ("standard", standard)):
            assert source["source_role_specs"][role] == {"credential": "real fixture JWT",
                                                        "user_level": ROLES[role], "is_active": True}
            captured, = (case for case in source["cases"] if case["source_index"] == index)
            assert (captured["method"], captured["role"], captured["path"], captured["HTTP_status"]) == (
                "GET", role, expected_path.replace("{id}", "1"), 200)
            assert captured["record_json_pointer"] == f"/cases/{index}"
            assert digest(captured["expected_projection_sha256"])
            result[contract, role] = {
                "method": "GET", "role": role, "accept": "application/json", "query": "",
                "contract_id": contract, "resolver_ordinal": ordinal, "source_handler": HANDLERS[kind],
                "route_template": expected_path, "historical_source_index": index,
                "historical_source_projection_sha256": captured["expected_projection_sha256"],
            }
    return result


def validate_packet(packet, expected):
    assert set(packet) == {"schema", "cohort", "register_sha256", "candidate", "original_revision",
        "framework_capture_sha256", "actual", "publication", "observations", "full_operations_verified",
        "performance_verified"}
    assert packet["schema"] == 1 and packet["cohort"] == "canonical EPG library positive reads"
    assert packet["register_sha256"] == REGISTER
    candidate = packet["candidate"]
    assert set(candidate) == set(CANDIDATE) | {"image_id", "manifest_sha256"}
    assert {key: candidate[key] for key in CANDIDATE} == CANDIDATE
    assert candidate["image_id"].startswith("sha256:") and digest(candidate["image_id"][7:])
    assert digest(candidate["manifest_sha256"])
    assert packet["framework_capture_sha256"] == SOURCE and packet["original_revision"] == BASELINE
    assert packet["full_operations_verified"] == 0 and packet["performance_verified"] is False
    actual = packet["actual"]
    assert set(actual) == {"physical_exit", "proof_exit", "receipt_sha256", "proof_sha256",
                          "exact_retired_roles", "remaining_owned", "unchanged_library_SQL"}
    assert actual["physical_exit"] == actual["proof_exit"] == 0
    assert all(digest(actual[name]) for name in ("receipt_sha256", "proof_sha256"))
    assert actual["exact_retired_roles"] == RETIREMENT
    assert actual["remaining_owned"] == dict.fromkeys(("containers", "volumes", "networks"), 0)
    assert actual["unchanged_library_SQL"] == dict.fromkeys(("original", "native"), True)
    publication = packet["publication"]
    assert set(publication) == {"remote_and_receiving_commit", "exact_application_inputs",
                                "protected_inputs", "verification_digests"}
    assert re.fullmatch("[a-f0-9]{40}", publication["remote_and_receiving_commit"])
    assert publication["exact_application_inputs"] == 2159
    assert publication["protected_inputs"] == 497
    assert set(publication["verification_digests"]) == {"archive", "remote_ref", "receiving_checkout", "verification"}
    assert all(digest(item) for item in publication["verification_digests"].values())
    assert len(packet["observations"]) == len(expected) == 8
    seen = set()
    for row in packet["observations"]:
        coordinate = row["contract_id"], row["role"]
        assert coordinate not in seen and coordinate in expected
        seen.add(coordinate)
        assert {k: v for k, v in row.items() if k != "response"} == expected[coordinate]
        wire = row["response"]
        assert set(wire) == {"status", "headers", "body_bytes", "body_sha256", "entity_count", "owned_entity_fields"}
        assert wire["status"] == 200 and 0 < wire["body_bytes"] <= 128 * 1024 and digest(wire["body_sha256"])
        assert type(wire["entity_count"]) is int and wire["entity_count"] > 0
        assert "{id}" not in row["route_template"] or wire["entity_count"] == 1
        assert wire["headers"] == {
            "allow": [EXPECTED_ALLOW[row["route_template"]]],
            "content-type": ["application/json"],
            "content-length": [str(wire["body_bytes"])],
            "vary": ["Accept, origin"],
        }
        fields = wire["owned_entity_fields"]
        assert fields == sorted(set(fields)) and {"id", "name"} <= set(fields)
        assert all(re.fullmatch("[a-z_]+", name) for name in fields)
    assert seen == set(expected)


def project(rows, packet, evidence_path, evidence_sha):
    expected = roster(rows)
    validate_packet(packet, expected)
    assert digest(evidence_sha)
    observations = packet["observations"]
    result = {}
    for contract in TARGETS:
        cases = [row | {"evidence_pointer": f"/observations/{index}"} for index, row in enumerate(observations)
                 if row["contract_id"] == contract]
        assert len(cases) == 2
        result[contract] = {"contract_id": contract, "method": "GET", "runtime": packet["candidate"],
                            "packaged_report": evidence_path, "packaged_report_sha256": evidence_sha,
                            "packaged_report_schema": "paired_epg_library_canonical_get_v1",
                            "fully_verified": False, "observed_roles": sorted(ROLES), "cases": cases}
    return result


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path)
    parser.add_argument("sha256")
    parser.add_argument("register", type=Path)
    args = parser.parse_args()
    assert __debug__
    raw, register = args.packet.read_bytes(), args.register.read_bytes()
    assert sha256(raw).hexdigest() == args.sha256 and not FORBIDDEN.search(raw)
    assert sha256(register).hexdigest() == REGISTER
    registry = json.loads(register)
    observed = project(registry["runtime_routes"], json.loads(raw), EVIDENCE_PATH, args.sha256)
    print(json.dumps({"operations": len(observed), "compiled_partial_cases": sum(len(x["cases"]) for x in observed.values()),
                      "full_operations_verified": 0, "Register_mutated": False}, sort_keys=True))


if __name__ == "__main__":
    main()
