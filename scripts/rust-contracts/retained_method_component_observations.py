"""Attach narrow Original and Rust component observations to stable route identities."""
from hashlib import sha256
import json


INPUT = "scripts/rust-contracts/retained-method-component-observations.json"
INPUT_SHA256 = "2f145e88bcc9f4a0dcee6cdb386ddb91a2b3c4628139fc03592d6ed15b664e46"
ROUTE_KEY = "scoped_method_component_observations"
SUMMARY_KEY = "retained_method_component_observations"
ORDINALS = (4, 190, 215, 216)
PHASES = dict(initdb=0, outer=0, ownership=0, postgres=0, test=0)


def attach(root, report):
    raw = (root / INPUT).read_bytes()
    assert sha256(raw).hexdigest() == INPUT_SHA256
    packet = json.loads(raw)
    assert packet["schema"] == "scoped_method_component_observations_v1"
    assert packet["original_revision"] == report["baseline_ref"]
    assert packet["baseline_runtime_roster_sha256"] == report["input_sha256"]["docs/rust-runtime-contracts.json"]
    assert INPUT not in report["input_sha256"] and SUMMARY_KEY not in report
    assert packet["runtime_operations_promoted"] == 0
    assert packet["packaged_acceptance"] is packet["release_acceptance"] is False
    rows = report["runtime_routes"]
    assert len(rows) == packet["roster_count"] == 592
    assert [entry["route_identity"]["resolver_ordinal"] for entry in packet["routes"]] == list(ORDINALS)

    checked = []
    for entry in packet["routes"]:
        identity = entry["route_identity"]
        route = rows[identity["resolver_ordinal"]]
        assert all(route[key] == value for key, value in identity.items())
        assert ROUTE_KEY not in route
        keys = ("contract_id", "method", "owning_milestone")
        assert entry["historical_operations"] == [
            {key: op[key] for key in keys} for op in route["operations"]
        ]
        assert all(op["fully_verified"] is False for op in route["operations"])
        original, native = entry["original"], entry["native"]
        assert original["literal_exit"] == 0 and original["vary"] == "Accept"
        ordinal = identity["resolver_ordinal"]
        expected_count = {4: (20, 17), 190: (40, 34), 215: (20, 0), 216: (24, 0)}
        methods = {"GET", "HEAD", "OPTIONS"}
        if ordinal == 216:
            methods.add("POST")
        assert set(original["role_matrix_by_method"]) == methods
        assert (original["cases"], native["signed_router_cases"]) == expected_count[ordinal]
        if native["signed_router_cases"]:
            assert native["actual_exits"] == PHASES and native["exact_private_postgres_retirement"] is True
        elif ordinal == 215:
            assert native["test_exit"] == 0 and native["exact_successful_entity_digests"] == 3
        else:
            assert native["test_exit"] == 0 and native["focused_suite_passed"] == 5
            assert [item["status"] for item in original["unicode_pagination_cases"]] == [200, 200, 200, 200, 500, 200, 500]
        checked.append((route, {"original": original, "native": native, "packaged_acceptance": False}))

    for route, evidence in checked:
        route[ROUTE_KEY] = evidence
    report["input_sha256"][INPUT] = INPUT_SHA256
    report[SUMMARY_KEY] = {
        "attestation": {"path": INPUT, "sha256": INPUT_SHA256},
        "routes": len(checked),
        "new_operation_ids": 0,
        "new_packaged_acceptance": 0,
        "scope": "Pinned Original DRF and scoped Rust component observations",
    }
    return report
