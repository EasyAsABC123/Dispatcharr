"""Bind the packaged Original React root while preserving its declaration."""
import hashlib
import json

KIND = "retained_original_packaged_Root7_HTTP"
RECEIPT = "scripts/rust-contracts/original-aio-root7.json"
RECEIPT_SHA256 = "54e02d91c7ed04c5abaa8464a0c7805edadc00d8631952c701ebeae656aecdbf"
INPUT = "scripts/rust-contracts/retained-root7-binding.json"
CONTRACT = "route-a21d90b60c8d5e9c908b:METHOD_UNRESOLVED"
FIXTURE = "Original-packaged-root7-C.UTF-8"
HANDLER = "django.views.generic.base.TemplateView"
HTML = "b48dbb556b1bbe0ac3af50a9afbc528fc438eac114f4c924c5699a366d19d10a"
ASSETS = {
    "/assets/index-msm8qEOi.js": (
        "module", "application/javascript", 2654224,
        "010b81d1bb1557f5374d6eb2c0bf3f61fd122833029dbd98b321eb5dd1cfd058",
    ),
    "/assets/index-Ds5qLkMK.css": (
        "stylesheet", "text/css", 227833,
        "5dafb4056775922dd4dc8423bb3e2e50d00170e2bc1ab73c22cda50321fbb33c",
    ),
}
PHASES = {"cleanup", "create", "probe", "readiness", "startup"}
LOCALE_PHASES = {
    "copy-base-os-environment", "copy-utf8-os-environment",
    "verify-os-environment-round-trip",
}


def dependencies():
    return {INPUT, RECEIPT}


def zero_exit(value):
    return (type(value["actual_exit"]) is int and value["actual_exit"] == 0
            and value["settled"] is True and value["stdout_stderr_EOF"] is True)


def materialize(receipt):
    original = receipt["Original"]
    assert original["revision"] == "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
    assert original["archive_sha256"] == "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
    assert original["selected_OCI_manifest"] == "sha256:7d435cbd2d8d68d68fffe76cd1a4594af9c31a8fa989e87c25392f66cf1844e3"
    assert original["network_mode"] == "none" and original["original_source_directory_mode"] == "0755"
    assert original["stock_entrypoint"] is True and original["unmounted_application_and_assets"] is True
    assert zero_exit(original["local_build_actual"])

    locale = receipt["login_fixture"]
    assert (locale["OS_file"], locale["single_added_entry"]) == ("/etc/environment", "LANG=C.UTF-8")
    assert locale["fresh_data_tmpfs"] is True and locale["default_login_locale_qualified"] is False
    assert locale["actual_phase_results"].keys() == LOCALE_PHASES
    assert all(zero_exit(phase) for phase in locale["actual_phase_results"].values())

    actual = receipt["actual"]
    assert (actual["attempt"], actual["controller_exit"]) == (7, 0)
    assert actual["phase_results"].keys() == PHASES
    assert all(zero_exit(phase) for phase in actual["phase_results"].values())
    assert actual["same_container_generation_before_and_after_probe"] is True
    assert actual["exact_owned_Original_container_and_generation_retired"] is True
    assert actual["owned_resources_after_cleanup"] == dict.fromkeys(("container", "network", "volume"), 0)
    assert all(receipt[key] is False for key in ("fully_verified", "M0_accepted", "performance_verified"))

    wire = actual["root_wire_and_resolver"]
    assert (wire["method"], wire["path"], wire["role"], wire["contract_id"]) == ("GET", "/", "anonymous", CONTRACT)
    assert (wire["actual_status"], wire["actual_content_type"], wire["actual_html_bytes"]) == (200, "text/html", 858)
    assert wire["actual_location"] is None and wire["actual_location_present"] is False
    assert wire["actual_html_sha256"] == wire["stock_index_sha256"] == HTML
    assert wire["actual_resolver"] == {
        "ordinal": 589, "pattern": "", "handler": HANDLER, "view_name": HANDLER, "action": None,
    }
    assert wire["resolver_url_name"] is None and wire["resolver_method_declaration"] is None
    assert wire["django_version"] == "6.0.9"
    assert all(wire[key] is True for key in (
        "wire_passed", "resolver_passed", "resolver_declared_name_empty", "resolver_kwargs_empty", "resolver_packaged_index",
    ))
    assets = wire["exact_related_assets"]
    assert len(assets) == len(ASSETS) and {asset["path"] for asset in assets} == ASSETS.keys()
    for asset in assets:
        assert asset["actual_status"] == 200 and asset["same_stock_file"] is True and asset["redirect_present"] is False
        assert tuple(asset[key] for key in ("kind", "actual_content_type", "actual_bytes", "actual_sha256")) == ASSETS[asset["path"]]

    fixture = {
        "id": FIXTURE,
        "description": "Stock Original packaged React root with an explicit disposable OS login locale.",
        "receipt": {"path": RECEIPT, "sha256": RECEIPT_SHA256},
        "Original": original,
        "login_fixture": locale,
        "actual_teardown": {
            "cleanup_sha256": actual["cleanup_sha256"],
            "exact_owned_Original_container_and_generation_retired": True,
            "owned_resources_after_cleanup": actual["owned_resources_after_cleanup"],
        },
    }
    contract = {
        "contract_id": CONTRACT, "fixture_id": FIXTURE, "resolver_ordinal": 589,
        "resolver_name": "", "pattern_exact": "", "source_handler": HANDLER,
        "owning_milestone": "M8", "method": None, "source_action": None,
        "observed_methods": ["GET"], "observed_roles": ["anonymous"],
        "source_role_specs": {"anonymous": {"fixture_role_or_supplied_credential": "anonymous", "complete": False}},
        "assertions": [
            "Exact anonymous HTTP 200 root and linked JavaScript and stylesheet match the baked Original image.",
            "Full Django resolver selected the unnamed TemplateView at ordinal 589 with no keyword arguments.",
            "The explicitly qualified disposable login locale, exact container generation and cleanup were verified.",
        ],
        "result": {
            "state": "partial_exact_Original_observations", "Source_case_observations": 1,
            "new_Source_cases_this_task": 0, "Native_coverage_increased": False,
        },
        "fully_verified": False, "cases": [wire],
    }
    return {
        "format_version": 1, "binding_kind": KIND, "credit": receipt["credit"],
        "fixtures": {FIXTURE: fixture}, "contracts": [contract],
        "operation_count": 1, "fully_verified_operations": 0, "performance_verified": False,
    }


def build(root):
    raw = (root / RECEIPT).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == RECEIPT_SHA256, "exact Root-7 physical receipt"
    return materialize(json.loads(raw))


def validate(root, value):
    assert value == build(root), "Root-7 projection differs from the actual receipt"


def restore_before(root, rows):
    """Reverse the one proven assignment when checking an older register."""
    operation, = (op for route in rows for op in route["operations"] if op["contract_id"] == CONTRACT)
    binding = build(root)
    contract, = binding["contracts"]
    assert operation.pop("executable_Source_contract") == contract
    assert operation["setup_fixture"] == binding["fixtures"][FIXTURE]
    assert operation["assertions"] == contract["assertions"] and operation["result"] == contract["result"]
    assert operation["role_matrix"] == {
        "state": "partial_Original_real_HTTP_capture_assignment", "required_roles": [],
        "observed_fixture_definitions": contract["source_role_specs"], "complete": False,
        "observed_roles": ["anonymous"],
    }
    assert operation["coverage_state"] == "partial_executable_Source_contract" and not operation["related_evidence"]
    operation.update(setup_fixture=None, assertions=[], result=None, coverage_state="not_yet_mapped",
                     role_matrix={"state": "unmapped", "required_roles": []})
