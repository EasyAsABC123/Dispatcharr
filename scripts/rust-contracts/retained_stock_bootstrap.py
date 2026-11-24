"""Attach the retained ordinary Source bootstrap requests to existing routes."""
from collections import defaultdict
from hashlib import sha256
import json

INPUT = "scripts/rust-contracts/retained-stock-bootstrap-source.json"
INPUT_SHA256 = "a85583d7eae0c3249df891435133d340e32f83d7f885077b848266b3b03b956a"
ATTACHMENTS = "additional_executable_Source_contracts"


def attach(root, report):
    path = root / INPUT
    if not path.exists():
        return report
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != INPUT_SHA256:
        raise ValueError("retained Original stock bootstrap changed")
    packet = json.loads(raw)
    cases = packet["Source_observations"]
    if (packet["baseline_ref"] != report["baseline_ref"]
            or packet["current_Rust_execution"] is not False
            or packet["complete_role_matrix"] is not False
            or packet["actual_requests"] != 9 or len(cases) != 9):
        raise ValueError("retained Original stock bootstrap authority differs")

    routes = {route["route_id"]: route for route in report["runtime_routes"]}
    grouped = defaultdict(list)
    operations = {}
    for position, case in enumerate(cases):
        observed = case["binding"]
        route_data = observed["route"]
        identity = observed["operation"]
        route = routes[route_data["route_id"]]
        if any(route.get(key) != value for key, value in route_data.items()):
            raise ValueError("retained Original bootstrap route differs")
        matches = [
            operation for operation in route["operations"]
            if all(operation.get(key) == value for key, value in identity.items())
        ]
        if (len(matches) != 1 or ATTACHMENTS in matches[0] or case["status"] != 200
                or case["sequence_index"] != position):
            raise ValueError("retained Original bootstrap operation differs")
        operation = matches[0]
        operations[operation["contract_id"]] = operation
        grouped[operation["contract_id"]].append(case)

    if len(grouped) != packet["register_operation_work_ids"] or len(grouped) != 7:
        raise ValueError("retained Original bootstrap operation roster differs")
    packet_bytes = json.dumps(packet, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False, allow_nan=False).encode()
    for contract_id, observations in grouped.items():
        operations[contract_id][ATTACHMENTS] = [{
            "kind": "additional retained actual Original stock bootstrap authority",
            "packet_schema": packet["schema"],
            "authority_packet_canonical_sha256": sha256(packet_bytes).hexdigest(),
            "authority_packet_relative_path": INPUT,
            "owner_project": packet["owner_project"],
            "receipt_sha256": packet["receipt_sha256"],
            "Source_case_ids": [case["case_id"] for case in observations],
            "method_observations": [case["method_observed"] for case in observations],
            "current_Rust_execution": False,
            "complete_role_matrix": False,
        }]
    report["input_sha256"][INPUT] = INPUT_SHA256
    return report
