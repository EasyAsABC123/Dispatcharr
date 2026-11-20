"""Assign only exact Original Django model registration observations."""
from collections import Counter
import hashlib
import json
from pathlib import PurePosixPath

import source_contracts

KIND = "retained_original_django_model_registry_non_HTTP"
FOLDER = "scripts/rust-contracts/retained-model-registry/"
INPUT = FOLDER + "bindings.json"
DECLARATIONS = FOLDER + "declarations.json"
OBSERVATIONS = FOLDER + "observations.json"
EXECUTION = FOLDER + "execution.json"
PROBE = FOLDER + "probe.py"
FIXTURE = "Original-1241b3-Django-model-registry"
STATE = "partial_executable_Original_Django_class_registration"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
IMAGE = "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
EXPECTED_SHA256 = {
    DECLARATIONS: "c522ab2ff66d767d003af04c13462c4ea268d9675d75605005e4bb38cbeed046",
    OBSERVATIONS: "645a2c3bf63f55578ef0241c1d2770580f00647cad9c53d7896d6114335a3a45",
    EXECUTION: "fa66cf71289a1ee6671c6e79c90b79ecd333b006856053a90548523abc67d8a4",
    PROBE: "2695f73a2e8d843300eb126bef848216e619c4d7795e4a905dfed85b7d2815f8",
}
REGISTRY_KINDS = {"django-registered-model": 45, "django-manager": 3, "django-queryset": 2}


def dependencies():
    return {INPUT, *EXPECTED_SHA256}


def exact_bytes(root, path):
    raw = (root / path).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == EXPECTED_SHA256[path], path
    return raw


def materialize(declarations, observations, execution):
    assert execution == {
        "actual_exit": 0, "candidate_count": 50, "cleanup": True,
        "django": "6.0.9", "image_id": IMAGE,
        "probe_output_sha256": EXPECTED_SHA256[OBSERVATIONS],
    }
    assert set(observations) == {"django", "database_connections", "observations"}
    assert observations["django"] == execution["django"] and observations["database_connections"] == 0
    rows = observations["observations"]
    assert len(declarations) == len(rows) == 50
    assert Counter(row["kind"] for row in rows) == REGISTRY_KINDS
    assert sum(len(row["fields"]) for row in rows if row["kind"] == "django-registered-model") == 424
    fixture = {
        "id": FIXTURE, "baseline_revision": ORIGINAL, "original_image_id": IMAGE,
        "django": execution["django"], "database_connections": 0,
        "actual_exit": execution["actual_exit"], "exact_owned_cleanup": execution["cleanup"],
        "network_mode": "none",
        "runtime_compose_sha256": "261e60aca27a7d7b405a3722e65870005a0f8e72cd6fd792729a052199c69818",
        "files_sha256": EXPECTED_SHA256,
        "scope": "Django registry metadata and Manager or QuerySet class bindings from pinned Original.",
    }
    contracts = []
    identities = set()
    paths = {}
    for declaration, row in zip(declarations, rows, strict=True):
        coordinate = {key: declaration[key] for key in ("kind", "name", "line", "path")}
        path = PurePosixPath(coordinate["path"])
        assert not path.is_absolute() and ".." not in path.parts and path.name == "models.py"
        assert coordinate["kind"] == "model_candidate" and type(coordinate["line"]) is int and coordinate["line"] > 0
        assert coordinate["name"].isidentifier()
        file_sha256 = declaration["source_sha256"]
        assert isinstance(file_sha256, str) and len(file_sha256) == 64 and all(c in "0123456789abcdef" for c in file_sha256)
        assert paths.setdefault(path, file_sha256) == file_sha256
        identity = declaration["candidate_id"]
        assert row["candidate_id"] == identity == "candidate-" + source_contracts.digest(coordinate)[:20]
        assert identity not in identities
        identities.add(identity)
        projection = {"kind": row["kind"]}
        if row["kind"] == "django-registered-model":
            projection.update({key: row[key] for key in ("label", "table", "primary_key")})
            projection["field_count"] = len(row["fields"])
        else:
            projection.update({key: row[key] for key in ("bound_models", "declared_methods")})
        contracts.append({
            "candidate_id": identity, "source": coordinate, "original_file_sha256": file_sha256,
            "fixture_id": FIXTURE, "observation": projection,
            "observation_sha256": source_contracts.digest(row),
            "assertions": [
                "Original runtime verified the exact source file bytes, class name and line before reading metadata.",
                "The observed class is registered as this Django model or bound to the listed models as this Manager or QuerySet.",
                "The probe opened zero database connections and did not exercise ORM methods.",
            ],
            "result": {"state": STATE, "fully_verified": False, "Native_coverage_increased": False,
                       "model_data_parity_verified": False, "runtime_behavior_verified": False},
            "fully_verified": False,
        })
    assert len(paths) == 10
    return {
        "format_version": 1, "kind": KIND, "credit": "Original Dispatcharr and Django contributors retain full credit.",
        "contracts": [], "fixtures": {FIXTURE: fixture}, "non_HTTP_contracts": contracts,
        "fully_verified_added": 0, "performance_verified": False,
    }


def derive(root):
    exact_bytes(root, PROBE)
    return materialize(*(json.loads(exact_bytes(root, path)) for path in (DECLARATIONS, OBSERVATIONS, EXECUTION)))


def validate(root, value):
    assert value == derive(root)


def restore_before(root, entries):
    binding = derive(root)
    selected = {row["candidate_id"]: row for row in binding["non_HTTP_contracts"]}
    for entry in entries:
        if entry["candidate_id"] not in selected:
            continue
        contract = selected.pop(entry["candidate_id"])
        assert entry.pop("executable_Source_contract") == contract
        assert entry.pop("setup_fixture") == binding["fixtures"][FIXTURE]
        assert entry.pop("assertions") == contract["assertions"]
        assert entry["state"] == STATE and entry["result"] == contract["result"]
        entry.update(state="static_candidate_requires_runtime_contract", result=None)
    assert not selected
