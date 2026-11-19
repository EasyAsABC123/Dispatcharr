"""One reviewed Original XC11 refresh reference.

Original Dispatcharr and dependency authors retain all application, algorithm
and protocol credit. No Source/Native runtime or URL resolution is performed.
"""
import copy
import hashlib
import json
from pathlib import Path

BASE = "scripts/rust-contracts/retained-xc-refresh-source11/"
REFERENCE = BASE + "reference.json"
BINDINGS = BASE + "bindings.json"
KIND = "retained_original_xc_Source11_single_refresh_HTTP"
REFERENCE_SHA256 = "bd2a25ff3ae52d4d5f0155df66f3e69a8e741987126e702b973117b6885f1ad9"
CONTRACT_ID = "route-c7d88007daf59544f9ea:METHOD_UNRESOLVED"


def dependencies():
    return {REFERENCE, BINDINGS}


def reference(root):
    path = Path(root) / REFERENCE
    assert path.is_file() and not path.is_symlink(), "Exact regular XC11 reference"
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == REFERENCE_SHA256, "Exact reviewed refresh authority"
    return json.loads(raw)


def generate(root):
    observed = reference(root)
    prior = observed["prior_operation"]
    assert prior["contract_id"] == CONTRACT_ID and prior["method"] is None
    assert prior["fully_verified"] is False and prior["source_action"] is None
    case = copy.deepcopy(observed["case"])
    assert case["capture_qualified_and_business_reviewed"]
    assert case["observed_method"] == "POST" and case["status"] == 202
    case["method"] = case.pop("observed_method")
    fixture_id = "retained-XC11:original-single-refresh"
    fixture = {
        "baseline_revision": observed["baseline_revision"],
        "actual_Source_controller_exit": observed["actual_source_controller_exit"],
        "verified_cleanup": observed["verified_cleanup"],
        "Source_HTTP_replayed": False,
        "qualified_owned_account": 6,
        "preceding_fixture_actions": "Normal account create and group selection before first refresh",
        "capture_reference": {"path": REFERENCE, "sha256": REFERENCE_SHA256},
    }
    role = {"admin": {"actual_fixture_role": "admin", "complete_role_matrix": False}}
    contract = {
        **{key: prior[key] for key in ("contract_id", "method", "source_action", "owning_milestone")},
        **{key: observed[key] for key in ("pattern_exact", "source_handler", "resolver_ordinal")},
        "fixture_id": fixture_id,
        "cases": [case],
        "observed_roles": ["admin"],
        "source_role_specs": role,
        "assertions": [
            "Retained admin POST to the explicit single-account refresh view returned exact status 202 and a captured JSON response.",
            "The actual refresh task published its actual profile child, with root/parent lineage bound by the retained journal.",
            "The reviewed capture includes 61 SQL tables and 59 literal sequence observations, provider, broker, cache and ordered WebSocket evidence.",
            "Pinned Source URL declaration and handler match the existing route identity. The observed POST does not resolve the full method declaration.",
        ],
        "result": {
            "state": "retained_original_XC11_scoped_qualified_refresh_observation",
            "new_Source_cases_this_task": 0,
            "retained_observations": 1,
            "Native_coverage_increased": False,
            "fully_verified": False,
        },
        "remaining": ["Full method and effective-role matrix, Native wire/state parity, and full milestone acceptance."],
    }
    return {
        "format_version": 1,
        "binding_kind": KIND,
        "credit": observed["Original_credit"],
        "contracts": [contract],
        "fixtures": {fixture_id: fixture},
        "new_assignments": 1,
        "retained_Source_observations": 1,
        "new_Source_HTTP": 0,
        "fully_verified_added": 0,
    }


def validate(root, value, bindings=None):
    assert value == generate(root), "Exact retained XC11 refresh definition"


def restore_pre_xc(root, rows):
    """Reverse exactly this assignment for earlier whole-row custody proofs."""
    packet = generate(root)
    contract, = packet["contracts"]
    prior = reference(root)["prior_operation"]
    updated = copy.deepcopy(prior)
    updated.update(
        setup_fixture=packet["fixtures"][contract["fixture_id"]],
        assertions=contract["assertions"],
        result=contract["result"],
        executable_Source_contract=contract,
        coverage_state="partial_executable_Source_contract",
    )
    updated["role_matrix"].update(
        observed_fixture_definitions=contract["source_role_specs"],
        observed_roles=contract["observed_roles"],
        state="partial_Original_real_HTTP_capture_assignment",
        complete=False,
    )
    operation, = [op for route in rows for op in route["operations"] if op["contract_id"] == CONTRACT_ID]
    assert operation == updated, "Only the exact post-XC operation may be restored"
    operation.clear()
    operation.update(prior)
