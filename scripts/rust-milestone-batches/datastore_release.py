"""Opt-in release profile for the exact Root-recorded datastore policy."""
import hashlib
import json
import subprocess

PROFILE = "postgresql-copied-sqlite-v1"
COMMIT = "152d96dfe554448031ee70e18ba757dc52172301"
OLD_GRAPH = "64a2d89935509a9f51279b6ddbdfec325b495b59"
GRAPH_SHA256 = "989b2b9017fd07633434378a70d6b3e79ed40c03246443533adb709581fc92ab"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
CANDIDATE = "3d8f96725ed90f5d37e406492e264e7c4d37f89a"
MANIFEST = "4127965d9e780ceb2b459f3b2feff0e80035f0d4768dc7f4ea966315207ee164"
IMAGE = "sha256:3cb8dff78119b1d0a40740a9f31f5ab8814a063a509f1891be9fef93ee2d39a6"
ARCHIVE = "be82700e942b1df0fffe00bcf58f03d8b25a3ce29ea56b2cd5a0e6ac4b374f4a"
PROOF_COMMIT = "7cda686b8ab705c3362ef1956055b152b20745f7"
RECORD = "docs/rust-m8-m9-datastore-scope-3d8f9672.json"
DOCUMENT = "docs/rust-m9-datastore-release-scope.md"
PROOF = "docs/rust-m8-amd64-copied-sqlite-operator-3d8f9672.json"
PINS = {
    RECORD: "3276943072c6200be13560809cd146d09de6e93e2cb102a7cdc2ca263b42f318",
    DOCUMENT: "f18f06cee998ae2193608dc5ee92f8f29f4377ae0c58818bca75316a2fb3c3dc",
    PROOF: "d2872d45a6a76f3d1bb458381bbff18ecec66a85e7b4d7d9f6dc1be18c0e0411",
}
EXTERNAL = "external/M9/datastore-scope"
UTILITY = "native/copied-SQLite-migration-utility"
OMIT = {
    "source/backup-SQLite-upgrade": "live Original SQLite and same-engine backup/restore are outside the selected release",
    "native/backup-SQLite-upgrade": "live Native SQLite and same-engine backup/restore are outside the selected release",
    "Main/SQLite-compatibility": "ordinary service uses PostgreSQL under the selected release",
}


def need(value, reason):
    if not value:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def graph_hash(nodes):
    return sha(json.dumps(nodes, sort_keys=True, separators=(",", ":")).encode())


def git(repo, *argv):
    return subprocess.check_output(["git", "--no-optional-locks", "-C", str(repo), *argv], timeout=20)


def verified_blob(repo, revision, path):
    raw = git(repo, "show", f"{revision}:{path}")
    leaf = repo / path
    need(sha(raw) == PINS[path] and leaf.resolve() == leaf
         and leaf.is_file() and leaf.read_bytes() == raw, "committed datastore record missing or altered: " + path)
    return raw


def project(repo, nodes, decision_commit):
    need(decision_commit == COMMIT, "exact Root decision commit required")
    need(graph_hash(nodes) == GRAPH_SHA256, "historical graph differs from the approved snapshot")
    need(git(repo, "rev-parse", COMMIT + "^").decode().strip() == OLD_GRAPH,
         "decision commit parent differs from the historical graph revision")
    need(git(repo, "merge-base", COMMIT, "HEAD").decode().strip() == COMMIT,
         "Root decision commit is not an ancestor of the running checkout")
    need(git(repo, "merge-base", PROOF_COMMIT, COMMIT).decode().strip() == PROOF_COMMIT,
         "copied operator proof is outside the recorded decision lineage")
    verified_blob(repo, COMMIT, DOCUMENT)
    record = json.loads(verified_blob(repo, COMMIT, RECORD))
    approval, contract = record["approval"], record["graph_projection"]
    need((approval["decision_maker"], approval["original_revision"], approval["application_revision"],
          approval["application_manifest_sha256"], approval["fork_graph_snapshot"],
          approval["evidence_publication_revision"], approval["ordinary_service_database"]) ==
         ("Justin", ORIGINAL, CANDIDATE, MANIFEST, OLD_GRAPH, PROOF_COMMIT, "postgresql")
         and approval["full_postgresql_workflow_gate"] == "pending"
         and approval["copied_sqlite_utility"] == dict(in_scope=True, opt_in=True, copied_data_only=True,
                                                        input_must_remain_unchanged=True)
         and all(approval[key] is False for key in ("live_sqlite_service_required_in_selected_release",
                 "live_sqlite_same_engine_backup_restore_required_in_selected_release", "production_or_cutover_authorized")),
         "Root approval does not match the current candidate and supported domain")
    proof = json.loads(verified_blob(repo, PROOF_COMMIT, PROOF))
    verify_operator(proof)
    updates = {row["id"]: row for row in contract["proposed_direct_dependency_updates"]}
    need(set(contract["historical_rows"]) == set(OMIT)
         and contract["base_graph_revision"] == OLD_GRAPH
         and contract["proposed_new_utility_row"]["id"] == UTILITY,
         "unexpected approved applicability projection")
    revised = []
    for old in nodes:
        key = old["id"]
        if key in OMIT:
            continue
        row = dict(old)
        if key in updates:
            patch = updates[key]
            need(row["depends_on"] == patch["before"], "approved dependency preimage changed: " + key)
            row["depends_on"] = patch["proposed_after"]
        elif key == "root/M9-workflow-parity":
            deps = [dep for dep in row["depends_on"] if dep not in OMIT]
            row["depends_on"] = list(dict.fromkeys([*deps, EXTERNAL, UTILITY]))
        revised.append(row)
    revised.append({"id": UTILITY, "owner": "m478", "depends_on": [EXTERNAL],
                    "heavy": False, "resources": [], "entrypoint": "published exact detached copied SQLite five-case proof"})
    need(not any(set(row["depends_on"]) & OMIT.keys() for row in revised), "dangling live SQLite dependency")
    meta = {
        "profile": PROFILE, "historical_graph_revision": OLD_GRAPH, "historical_graph_sha256": GRAPH_SHA256,
        "historical_exclusions": OMIT, "projected_graph_sha256": graph_hash(revised),
        "decision_commit": COMMIT, "decision_sha256": PINS[RECORD], "decision_maker": "Justin",
        "recorder": "Root coordinator", "original_revision": ORIGINAL, "application_revision": CANDIDATE,
        "application_manifest_sha256": MANIFEST, "operator_proof_commit": PROOF_COMMIT,
        "operator_proof_sha256": PINS[PROOF],
        "m235_m5_criteria": contract["proposed_m235_m5_criteria"]["after"],
    }
    return revised, meta


def verify_operator(proof):
    need((proof["revision"], proof["default_manifest_sha256"], proof["source_archive_sha256"],
          proof["architecture"], proof["migration_record_count"], proof["application_table_count"]) ==
         (CANDIDATE, MANIFEST, ARCHIVE, "linux/amd64", 184, 62)
         and all(proof[key] is True for key in ("exact_cleanup", "engine_executed", "engine_empty_after",
                                                  "copied_input_unchanged", "identical_main_binary"))
         and proof["sqlite_main_or_performance_verified"] is False and proof["canonical_rows_added"] == 0,
         "published copied operator scope does not match the candidate")
    cases = proof["cases"]
    expected = {"default-help": 0, "opt-in-help": 0, "staged-collision": 1, "unknown-history": 1, "upgrade": 0}
    need({key: case["actual_exit"] for key, case in cases.items()} == expected
         and all(case["exact_cleanup"] is True and case["network"] == "none"
                 and case["runtime_user"] == "65532:65532" for case in cases.values()),
         "exact five case exits and teardown required")
    companion = cases["opt-in-help"]["image_id"]
    need(cases["default-help"]["image_id"] == IMAGE and companion != IMAGE
         and all(cases[key]["image_id"] == companion
                 and cases[key]["input_before_sha256"] == cases[key]["input_after_sha256"]
                 for key in ("upgrade", "staged-collision", "unknown-history")),
         "ordinary/companion image and copied input binding differs")


def authorities(meta, candidate, bindings):
    need((candidate.get("git"), candidate.get("build_manifest_sha256"), candidate.get("image_id")) ==
         (CANDIDATE, MANIFEST, IMAGE), "datastore policy candidate is stale or mismatched")
    need(not bindings.keys() & {EXTERNAL, UTILITY}, "datastore release uses committed authority only")
    common = {"satisfies_dependency": True, "cleanup_verified": True, "counts_as_new_execution": False}
    return {
        EXTERNAL: common | {"id": EXTERNAL, "status": "recorded_scope_authority", "authority": meta},
        UTILITY: common | {"id": UTILITY, "status": "retained_copied_utility_authority",
                           "authority": {"commit": PROOF_COMMIT, "sha256": PINS[PROOF],
                                         "exact_five_case_scope": True}},
    }
