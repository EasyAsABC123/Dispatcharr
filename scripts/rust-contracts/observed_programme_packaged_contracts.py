"""Attach only the sixteen published, live paired Programme Guide projections."""
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from itertools import product
import json

BASE = "scripts/rust-contracts/observed-http-9cf6407"
PACKET = f"{BASE}/programme.json"
VALIDATOR = f"{BASE}/verify.py"
INPUT_SHA256 = {
    PACKET: "0736bbee00d57377ce42952e1fb7f042535634d7336500ec2c22bbea6846d0bc",
    VALIDATOR: "da8983349edaf5f24bb071696803531e02801658c4e04194cddbd6e2223f4c87",
}
HANDLER = "apps.epg.api_views.ProgramViewSet"
TARGETS = {
    "route-55aadc5558ca3b8248b1:GET": (159, "api/epg/^programs/$", "/api/epg/programs/", "list"),
    "route-82723694fa5ab6ee5f1e:GET": (163, r"api/epg/^programs/(?P<pk>[^/.]+)/$", "/api/epg/programs/{pk}/", "retrieve"),
}
BROWSER = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
    "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
)
WEIGHTED_JSON = "application/json;q=0.9, text/html;q=0.5"
NEGOTIATIONS = ((BROWSER, ""), ("text/*", ""), (BROWSER, "?format=api"), (WEIGHTED_JSON, ""))
ROLES = {"admin": 10, "standard": 1}


def load_packet(root):
    assert all(sha256((root / path).read_bytes()).hexdigest() == stamp for path, stamp in INPUT_SHA256.items())
    spec = spec_from_file_location("published_9cf_programme_packet_verifier", root / VALIDATOR)
    verified = module_from_spec(spec)
    spec.loader.exec_module(verified)
    assert verified.verify("programme", INPUT_SHA256[PACKET]) == 16
    packet = json.loads((root / PACKET).read_bytes())
    assert set(packet["original_receipt_digests"]) == {"positive programme GET"}
    assert packet["original_receipt_digests"]["positive programme GET"] == (
        "49bec7c86e90997b6811ca9fc3ccb258d5606b4c2590ee3730e734d7ba9af38d")
    return packet


def project(rows, packet):
    registry = {(route["resolver_ordinal"], op["contract_id"]): (route, op)
                for route in rows for op in route["operations"]}
    allowed = {(contract, role, accept, query) for contract, role, (accept, query) in product(TARGETS, ROLES, NEGOTIATIONS)}
    observed = {}
    for index, row in enumerate(packet["observations"]):
        coordinate = (row["operation"], row["role"], row["accept"], row["format_query"])
        assert coordinate in allowed and coordinate not in observed and row["method"] == "GET"
        source = row["Original"]
        assert source == row["Rust"]
        wire = source["observed_HTTP"]
        renderer = "application/json" if row["accept"] == WEIGHTED_JSON else "text/html; charset=utf-8"
        assert wire["status"] == 200 and wire["content_type"] == renderer
        assert source["stable_repeat"] is True and (renderer == "application/json") == ("template_sha256" not in source)
        observed[coordinate] = {
            "method": "GET", "role": row["role"], "accept": row["accept"], "format_query": row["format_query"],
            "result": "passed_scoped_live_response_projection",
            "response": source,
            "evidence_pointer": f"/observations/{index}",
        }
    assert set(observed) == allowed

    result = []
    for contract, (ordinal, pattern, path, action) in TARGETS.items():
        route, op = registry[ordinal, contract]
        assert route["pattern_exact"] == pattern and route["source_handler"] == HANDLER
        assert route["route_id"] + ":GET" == contract
        assert (op["method"], op["source_action"], op["owning_milestone"]) == ("GET", action, "M5")
        assert op["fully_verified"] is False and op["role_matrix"]["complete"] is False
        assert "case_bindings" not in op
        source_contract = op["executable_Source_contract"]
        assert source_contract["contract_id"] == contract and source_contract["method"] == "GET"
        for role, level in ROLES.items():
            assert source_contract["source_role_specs"][role] == {
                "credential": "real fixture JWT", "user_level": level, "is_active": True,
            }
        cases = [observation for coordinate, observation in observed.items() if coordinate[0] == contract]
        assert len(cases) == 8
        result.append((op, {
            "contract_id": contract, "method": "GET", "pattern_exact": pattern, "literal_path_template": path,
            "source_handler": HANDLER, "runtime": packet["candidate"],
            "packaged_report": PACKET, "packaged_report_sha256": INPUT_SHA256[PACKET],
            "packaged_report_schema": "paired_programme_response_projection_v1",
            "observed_roles": sorted(ROLES), "fully_verified": False, "cases": cases,
        }))
    return result


def apply(root, rows):
    for operation, binding in project(rows, load_packet(root)):
        operation["case_bindings"] = binding
