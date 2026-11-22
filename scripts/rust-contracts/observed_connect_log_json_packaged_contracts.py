"""Attach the published paired cases for one formatted Connect delivery-log GET."""
from hashlib import sha256
import json


PACKET = "scripts/rust-integration/m0-e7dda7f-connect-log-json.json"
INPUT_SHA256 = {PACKET: "20d844e836d0cc224276a3a2ab38c98ff04b76fd2dd6721c55f94b61d729cb8e"}
CONTRACT = "route-d52d5a62d4e324405b85:GET"
ROLES = {"anonymous": 401, "standard": 403, "viewer": 403}
REVISION = "e7dda7fec25918644a29de56454bad7f9a63fd83"
IMAGE = "sha256:abcbbe219c822e8320d77810f4305831d832a0be0f0c72600af809c90fb4535d"
MANIFEST = "a07614a0d6b42efa27f3ee229fc61d28e331cbd81d51c829d6a26655ea0743cf"
SOURCE_RECEIPT = "46c4e7103d653f80220849bc5da631263ed60d9370dc70c7291ea094ae7b1de6"
NATIVE_RECEIPT = "6548bf3a60dc06edc7266f4d852d478247b2c0656a829fb20cc5c86654d76941"
PAIRED_PROOF = "3e00305740eff877e67d893d607d67b4dee8b17ed4d5260b962b04356686f38a"


def assemble(packet):
    assert packet["package"] == {
        "feature_arg": 0, "image": IMAGE, "revision": REVISION, "manifest_sha256": MANIFEST,
    }
    pair = packet["successor"]
    assert pair["actual_exit"] == 0 and pair["same_typed_formatted_pagination"] is True
    assert pair["original_contract_id"] == CONTRACT and pair["original_resolver_ordinal"] == 347
    assert pair["formal_nodes_promoted"] == 0 and pair["proof_sha256"] == PAIRED_PROOF
    runtime = pair["candidate"]
    assert runtime["expected"] == "successor" and (
        runtime["revision"], runtime["image_id"], runtime["manifest_sha256"]
    ) == (REVISION, IMAGE, MANIFEST)
    assert pair["evidence"] == {
        "Original": {"receipt_sha256": SOURCE_RECEIPT, "exact_retired_roles": 2},
        "Native": {"receipt_sha256": NATIVE_RECEIPT, "exact_retired_roles": 4},
    }
    report = pair["business"]
    assert set(report) == {"Original", "Native"}
    cases = []
    # The canonical comparison resolved a different ordinal and is deliberately
    # absent here. Each typed observation below was asserted by both real arms.
    for label, path in (("json", "/api/connect/logs.json"), ("json_slash", "/api/connect/logs.json/")):
        fixed = {"count": 3, "page1": [3, 2], "page2": [1], "roles": ROLES, "state_unchanged": True,
                 "status": 200, "typed_next_and_previous_stay_on_request_path": True}
        assert report["Original"][label] == report["Native"][label] == fixed
        for page, order in enumerate((fixed["page1"], fixed["page2"]), start=1):
            cases.append(case(label, path, "admin", page, 200) | {
                "matching_log_count": 3, "selected_row_order": order,
                "typed_next_and_previous_stay_on_request_path": True,
            })
        for role, code in ROLES.items():
            cases.append(case(label, path, role, 1, code))
    assert len(cases) == 10
    return {"contract_id": CONTRACT, "method": "GET", "runtime": runtime,
            "packaged_report": PACKET, "packaged_report_sha256": INPUT_SHA256[PACKET],
            "packaged_report_schema": "paired_connect_delivery_log_filtered_pagination_v1",
            "fully_verified": False,
            "observed_roles": ["admin", "anonymous", "standard", "viewer"], "cases": cases}


def case(label, path, role, page, status):
    return {"request_path": path, "method": "GET", "role": role, "page": page, "status": status,
            "content_type": "application/json", "authentication_challenge_present": status == 401,
            "owned_SQL_and_queues_unchanged": True, "body_comparison": "typed",
            "fixture_query": "selected integration, webhook, page_size 2",
            "evidence_pointer": f"/successor/business/Original/{label}",
            "paired_evidence_pointer": f"/successor/business/Native/{label}"}


def apply(root, rows):
    payload = (root / PACKET).read_bytes()
    assert sha256(payload).hexdigest() == INPUT_SHA256[PACKET]
    row, = [item for item in rows if item["route_id"] == "route-d52d5a62d4e324405b85"]
    assert (row["resolver_ordinal"], row["name"], row["source_handler"]) == (
        347, "api:connect:delivery-log-list", "apps.connect.api_views.DeliveryLogViewSet",
    )
    assert row["pattern_exact"] == r"api/connect/^logs\.(?P<format>[a-z0-9]+)/?$"
    operation, = [item for item in row["operations"] if item["contract_id"] == CONTRACT]
    assert (operation["method"], operation["source_action"], operation["owning_milestone"]) == ("GET", "list", "M5")
    assert operation["fully_verified"] is False and "case_bindings" not in operation
    assert operation["executable_Source_contract"]["contract_id"] == CONTRACT
    operation["case_bindings"] = assemble(json.loads(payload))
