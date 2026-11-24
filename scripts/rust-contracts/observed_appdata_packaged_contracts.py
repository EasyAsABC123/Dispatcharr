"""Attach signed AppData observations from the published comparison."""
from hashlib import sha256
import json

PACKET = "scripts/rust-integration/m0-2d3ffe53-numeric-appdata-parity.json"
HISTORY = "scripts/rust-integration/m0-0c5133b-appdata-methods.json"
DIFFERENTIAL = "scripts/rust-integration/m0-63ee09fc-numeric-appdata-method-differential.json"
INPUT_SHA256 = {
    PACKET: "aa331ed4511aa1d62f4452958da07075b65b25854082b858d52da7a972f371c6",
    HISTORY: "c515e093ce8e0e4876ce2a7b5bc3143a830dc2cb4ed88717eb090abb4246c34d",
    DIFFERENTIAL: "d49f3f5e68000f9beae1d9041d3800d4c4b1e51be6e291959ec8069089aeb635",
}
REVISION = "2d3ffe5386a7d09b72cda387d0ec9e4f4ab0abc1"
MANIFEST = "ebcae7bc9c03c00fb21d9ee4a653b9ac3261bf83c81fe1198cbaa16bfb293b75"
RECEIPT = "350ab0bb4359faec95cd600e41b4a7ab01440c4243da11ff93a9f771c17a3d1c"
HISTORICAL = "0c5133b62dc9f2df0900f2228e25b75baecb6754"
DIFFERENT = "63ee09fcba02f79300ee249b77f1fc881d05657c"
ROLES = {"owner": 200, "admin": 200, "other_standard": 403}
ROUTES = {
    "collection": ("route-4a50fcc0d000739bc60c", 9, "api:accounts:user-app-data",
                   "api/accounts/users/<int:user_id>/app-data/", "apps.accounts.api_views.UserAppDataCollectionView",
                   ("DELETE", "GET", "HEAD", "OPTIONS"), "User App Data Collection"),
    "key": ("route-f7f70739debd2202db5e", 10, "api:accounts:user-app-data-key",
            "api/accounts/users/<int:user_id>/app-data/<str:key>/", "apps.accounts.api_views.UserAppDataKeyView",
            ("DELETE", "GET", "HEAD", "OPTIONS", "PUT"), "User App Data Key"),
}
METHODS = ("GET", "HEAD", "OPTIONS")
EXPECTED = [(route, role, method) for route in ROUTES for role in ROLES for method in METHODS]


def json_input(root, path):
    raw = (root / path).read_bytes()
    assert sha256(raw).hexdigest() == INPUT_SHA256[path]
    return json.loads(raw)


def expected_metadata(route, method):
    if method != "OPTIONS":
        return None
    return {"name": ROUTES[route][-1], "renders": ["application/json", "text/html"],
            "parses": ["application/json"], "actions": None}


def identity(row):
    return row["route"], row["role"], row["method"]


def checked_package(packet, historical, prior):
    assert (packet["candidate"], packet["manifest_sha256"], packet["fresh_receipt_sha256"]) == (
        REVISION, MANIFEST, RECEIPT)
    assert packet["initial_materializer_exit"] == 1 and packet["corrected_runtime_owner_exit"] == 0
    for key in ("client_visible_HEAD_only", "exact_NO_SQL_write_and_same_scoped_state", "exact_cleanup",
                "fresh_original_and_native", "valid_Bearer_identities_verified"):
        assert packet[key] is True
    assert packet["formal_rows_promoted"] == []
    assert packet["canonical_routes"] == {key: row[0] + ":METHOD_UNRESOLVED" for key, row in ROUTES.items()}
    assert prior["candidate"] == DIFFERENT and prior["exact_cleanup"] is True
    observed, authority = packet["wire_differential"], prior["wire_differential"]
    assert list(map(identity, observed)) == list(map(identity, authority)) == EXPECTED
    cases = {route: [] for route in ROUTES}
    for index, (entry, stock) in enumerate(zip(observed, authority, strict=True)):
        route, role, method = identity(entry)
        status = 200 if method == "OPTIONS" else ROLES[role]
        assert entry["original_status"] == entry["native_status"] == stock["original_status"] == status
        assert entry["body_equal"] is True and not entry["headers_differ"]
        metadata = expected_metadata(route, method)
        assert entry["original_options_metadata"] == entry["native_options_metadata"] == stock["original_options_metadata"] == metadata
        case = {"method": method, "role": role, "status": status,
                "body_digest_matches": True, "selected_headers_match": True,
                "evidence_pointer": f"/wire_differential/{index}"}
        if method == "HEAD":
            case["client_body_empty"] = True
        if metadata is not None:
            case["options_metadata"] = metadata
        cases[route].append(case)
    assert historical["campaign_actual_exit"] == 0
    for phase in ("read", "write"):
        report = historical[phase]
        assert report["candidate"] == HISTORICAL and report["exact_cleanup"] is True
        assert report["preflight_run_cleanup_proof_exits"] == [0, 0, 0, 0]
        assert report["selected_AppData_header_parity"] is True
        assert len(report["wire_cases"]) == 6 and all(row["literal_body_equal"] is True for row in report["wire_cases"])
    framing = historical["write"]["known_204_framing_differences"]
    assert len(framing) == 2 and {row["role"] for row in framing} == {"owner", "admin"}
    assert all((row["method"], row["status"], row["Original_Content_Length"], row["Native_Content_Length"])
               == ("DELETE", 204, "0", None) for row in framing)
    return cases


def apply(root, rows):
    packet = json_input(root, PACKET)
    cases = checked_package(packet, json_input(root, HISTORY), json_input(root, DIFFERENTIAL))
    indexed = {row["route_id"]: row for row in rows}
    for label, (route, ordinal, name, pattern, handler, methods, _) in ROUTES.items():
        row = indexed[route]
        assert (row["resolver_ordinal"], row["name"], row["pattern_exact"], row["source_handler"]) == (
            ordinal, name, pattern, handler)
        assert row["declared_methods_resolved"] is False and tuple(row["runtime_handler_candidates"]) == methods
        op, = row["operations"]
        assert op["contract_id"] == route + ":METHOD_UNRESOLVED"
        assert (op["method"], op["source_action"], op["fully_verified"]) == (None, None, False)
        assert op["executable_Source_contract"]["contract_id"] == op["contract_id"] and "case_bindings" not in op
        op["case_bindings"] = {
            "contract_id": op["contract_id"], "method": None, "fully_verified": False,
            "packaged_report": PACKET, "packaged_report_sha256": INPUT_SHA256[PACKET],
            "packaged_report_schema": "signed_numeric_AppData_JSON_methods_v1",
            "runtime": {"git_revision": REVISION, "manifest_sha256": MANIFEST, "platform": "linux/amd64"},
            "cases": cases[label],
            "previous_observations": {"path": HISTORY, "revision": HISTORICAL,
                                      "methods": ["GET"] if label == "collection" else ["GET", "PUT", "DELETE"]},
            "coverage_limits": ["Format variants, browsable HTML and unmeasured roles and inputs remain open.",
                                "Collection DELETE remains unmeasured." if label == "collection"
                                else "Prior DELETE 204 Content-Length framing differs. Current PUT and DELETE remain unmeasured."],
        }
