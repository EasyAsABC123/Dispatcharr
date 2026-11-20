"""Bind Original per-migration recorder observations without asserting schema parity."""
import hashlib
import json
from pathlib import PurePosixPath
import re

import source_contracts

KIND = "retained_original_fresh_migration_recorder_non_HTTP"
FOLDER = "scripts/rust-contracts/retained-migration-recorder/"
INPUT = FOLDER + "bindings.json"
OBSERVATIONS = FOLDER + "observations.json"
PG = "rust/crates/store/schema/original-1241b3-manifest.json"
FIXTURE = "pinned-Original-fresh-migration-recorder"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
EXPECTED_SHA256 = {
    OBSERVATIONS: "f40b47c324bec748a156d7bb5161d2836f5e4e4359d5531baaa29f968d9cd242",
    PG: "3b9ba205ec5b6df613c2a0737324f1271948a30dc8216050671a7ea305bce210",
}
STATE = "partial_executable_Original_migration_recorder_observations"


def dependencies():
    return {INPUT, *EXPECTED_SHA256}


def checked(root, path):
    raw = (root / path).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == EXPECTED_SHA256[path], path
    return json.loads(raw)


def digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def materialize(observations, pg):
    assert observations["Original_git"] == pg["source_baseline"] == ORIGINAL
    assert observations["archive_sha256"] == ARCHIVE
    assert observations["source_image_id"] == pg["source_image"].rsplit("@", 1)[1]
    pg_applied = {tuple(row) for row in pg["migrations"]}
    assert len(pg_applied) == len(pg["migrations"]) == 184
    assert all(pg[key] == 0 for key in ("users", "channels", "streams", "epg_sources"))

    sqlite = observations["sqlite"]
    assert sqlite["command"] == ["/dispatcharrpy/bin/python", "manage.py", "migrate", "--noinput"]
    assert sqlite["effective_settings"] == {
        "phase": "effective_production_settings", "module": "dispatcharr.settings",
        "DB_ENGINE": "sqlite", "engine": "django.db.backends.sqlite3", "path": "/data/dispatcharr.db",
        "skip_redis_cache": "1",
    }
    assert sqlite["CLI_exit"] == sqlite["container_exit"] == 1 and sqlite["readonly_observer_exit"] == 0
    assert sqlite["OOMKilled"] is False and sqlite["output_bytes_discarded"] == 0
    assert all(sqlite[key] is True for key in ("stdout_EOF", "stderr_EOF", "process_group_settled",
                                              "final_labelled_CID_absence"))
    assert sqlite["integrity_check"] == [["ok"]]
    prefix = [tuple(row) for row in sqlite["recorded_applied"]]
    sqlite_applied = set(prefix)
    failed = ("dispatcharr_channels", "0038_add_catchup_fields")
    assert len(prefix) == len(sqlite_applied) == 158
    assert tuple(sqlite["last_applied"]) == prefix[-1] == ("dispatcharr_channels", "0037_auto_sync_overhaul")
    assert failed not in sqlite_applied and sqlite["first_failed_migration"] == ".".join(failed)
    assert (sqlite["first_failure_class"], sqlite["first_failure_message"], sqlite["source_frame"]) == (
        "django.db.utils.OperationalError", 'near "~": syntax error',
        "apps/channels/migrations/0038_add_catchup_fields.py:9 backfill_stream_catchup cursor.execute",
    )
    label_overrides = {"apps/channels": "dispatcharr_channels", "apps/connect": "dispatcharr_connect"}
    for directory, definition in observations["app_labels"].items():
        assert definition["label"] == label_overrides.get(directory, PurePosixPath(directory).name)
        assert digest(definition["config_sha256"])

    fixture = {
        "id": FIXTURE, "baseline_revision": ORIGINAL, "source_archive_sha256": ARCHIVE,
        "source_image_id": observations["source_image_id"],
        "historical_proof_scope": "Fresh Original migration recorder on separate PostgreSQL and SQLite data.",
        "postgreSQL_manifest": {"path": PG, "sha256": EXPECTED_SHA256[PG], "migration_rows": 184},
        "sqlite_receipt": {"observations": OBSERVATIONS, "sha256": EXPECTED_SHA256[OBSERVATIONS],
                           "recorded_applied": 158, "actual_migrate_exit": 1, "exact_owned_cleanup": True},
        "populated_upgrade_verified": False, "Native_datastore_verified": False,
    }
    contracts = []
    keys = set()
    candidates = set()
    for row in observations["declarations"]:
        source = row["source"]
        path = PurePosixPath(source["path"])
        identity = row["candidate_id"]
        assert identity == "candidate-" + source_contracts.digest(source)[:20]
        assert identity not in candidates
        candidates.add(identity)
        assert source["kind"] == "migration" and source["name"] == "Migration"
        assert path.parent.name == "migrations" and type(source["line"]) is int and source["line"] > 0
        assert digest(row["original_file_sha256"]) and digest(row["definition_ast_sha256"])
        app = observations["app_labels"][str(path.parents[1])]["label"]
        key = (app, path.stem)
        assert key not in keys and key in pg_applied
        keys.add(key)
        sqlite_state = "applied_before_failure" if key in sqlite_applied else "observed_failed" if key == failed else "not_reached"
        outcome = {"state": sqlite_state, "whole_command_actual_exit": 1}
        if key == failed:
            outcome.update({k: sqlite[k] for k in ("first_failure_class", "first_failure_message", "source_frame")})
        contracts.append({
            "candidate_id": identity, "source": source, "owning_milestone": "M8",
            "definition_sha256": {k: row[k] for k in ("original_file_sha256", "definition_ast_sha256")},
            "fixture_ids": [FIXTURE], "django_recorder_key": list(key),
            "Original_PostgreSQL17": "recorded_applied_on_fresh_database",
            "Original_SQLite": outcome,
            "assertions": ["Pinned Original PostgreSQL recorder contains the exact application and migration key.",
                           "The fresh SQLite command and read-only prefix determine the separate SQLite outcome."],
            "result": {"state": STATE, "Native_coverage_increased": False, "datastore_parity_passed": False,
                       "new_Source_cases_this_task": 0, "fully_verified": False},
            "fully_verified": False,
        })
    assert len(contracts) == 145 and len(pg_applied - keys) == 39
    assert len(sqlite_applied & keys) == 120 and failed in keys
    assert len(keys - sqlite_applied - {failed}) == 24
    return {
        "format_version": 1, "kind": KIND,
        "credit": "Original Dispatcharr and dependency authors retain full application and migration credit.",
        "contracts": [], "fixtures": {FIXTURE: fixture}, "non_HTTP_contracts": contracts,
        "fully_verified_added": 0, "performance_verified": False,
    }


def derive(root):
    return materialize(checked(root, OBSERVATIONS), checked(root, PG))


def validate(root, value):
    assert json.dumps(value, sort_keys=True) == json.dumps(derive(root), sort_keys=True)


def restore_before(root, entries):
    """Restore the exact old typed rows for immutable historical tests."""
    binding = derive(root)
    selected = {row["candidate_id"]: row for row in binding["non_HTTP_contracts"]}
    for entry in entries:
        if entry["candidate_id"] not in selected:
            continue
        contract = selected.pop(entry["candidate_id"])
        assert entry.pop("executable_Source_contract") == contract
        assert entry.pop("setup_fixture") == binding["fixtures"]
        assert entry.pop("owning_milestone") == "M8" and entry.pop("assertions") == contract["assertions"]
        assert entry["state"] == STATE and entry["result"] == contract["result"]
        entry.update(state="static_candidate_requires_runtime_contract", result=None)
    assert not selected
