"""Explicitly pinned published successor datastore receipts."""
from hashlib import sha256
import json
import re

PROFILE = "postgresql-copied-sqlite-successor"
EXTERNAL = "external/M9/datastore-scope"
UTILITY = "native/copied-SQLite-migration-utility"
RELEASE_METADATA = {
    b"scripts/rust-milestone-batches/coordinator.py",
    b"scripts/rust-milestone-batches/datastore_release.py",
    b"scripts/rust-milestone-batches/datastore_successor.py",
}

EXPECTED = {"default-help": 0, "opt-in-help": 0, "upgrade": 0, "unknown-history": 1, "staged-collision": 1}


def need(condition, stage):
    if not condition:
        raise ValueError(stage)


def hashes(*values):
    return all(isinstance(v, str) and re.fullmatch("[a-f0-9]{64}", v) for v in values)


def verify(candidate, pg, utility, *, pg_receipt_sha, utility_receipt_sha):
    rev, image, manifest, archive = (candidate[k] for k in (
        "git_revision", "image_id", "manifest_sha256", "source_archive_sha256"))
    need((pg["candidate_revision"], pg["candidate_image_id"], pg["physical_receipt_sha256"]) ==
         (rev, image, pg_receipt_sha) and hashes(pg_receipt_sha)
         and pg["scoped_workflow_passed"] is pg["exact_cleanup"] is True
         and pg["formal_M8_native_backup_workflow_accepted"] is False
         and pg["native_original_matching_refusal_messages"] == 2
         and pg["actual_owned_empty_HTTP204_deletions"] == 2
         and all(pg["restore_readback"][key] is True for key in (
             "same_password_hash", "same_recording_reference", "same_owned_media_bytes")),
         "current signed PostgreSQL workflow and owned receipt required")
    need((utility["revision"], utility["default_manifest_sha256"], utility["source_archive_sha256"],
          utility["receipt_sha256"]) == (rev, manifest, archive, utility_receipt_sha)
         and hashes(utility_receipt_sha)
         and utility["engine_executed"] is utility["exact_cleanup"] is True
         and utility["sqlite_main_or_performance_verified"] is False
         and (utility["migration_record_count"], utility["application_table_count"]) == (184, 62),
         "current detached migration and physical receipt required")
    binding = utility["ordinary_bind_preflight"]
    need((binding["ordinary_image"], binding["ordinary_manifest_sha256"], binding["ordinary_archive_sha256"],
          binding["source_tree_sha256"], binding["source"]["revision"]) ==
         (image, manifest, archive, candidate["source_tree_sha256"], rev)
         and binding["historical_status_used_as_current"] is False
         and binding["companion_only_build_flags"] ==
             {"DISPATCHARR_BUILD_SQLITE_OPERATOR": 1, "DISPATCHARR_BUILD_COPIED_SQLITE_RUNTIME": 0},
         "ordinary and companion exact-source binding")
    cases = utility["cases"]
    need({k: v["actual_exit"] for k, v in cases.items()} == EXPECTED
         and all(v["exact_cleanup"] is True and v["network"] == "none"
                 and v["runtime_user"] == "65532:65532" for v in cases.values()),
         "five literal detached case exits and cleanup required")
    companion = cases["opt-in-help"]["image_id"]
    binary = cases["default-help"]["files_sha256"]["dispatcharr-rs"]
    need(cases["default-help"]["image_id"] == image and companion != image
         and isinstance(companion, str) and re.fullmatch("sha256:[a-f0-9]{64}", companion)
         and hashes(binary) and binary == cases["opt-in-help"]["files_sha256"]["dispatcharr-rs"]
         and all(cases[k]["image_id"] == companion for k in EXPECTED if k != "default-help")
         and all(hashes(cases[k]["input_before_sha256"])
                 and cases[k]["input_before_sha256"] == cases[k]["input_after_sha256"]
                 for k in ("upgrade", "unknown-history", "staged-collision")),
         "separate exact companion and unchanged copy inputs")
    return {"candidate": candidate, "postgres_receipt_sha256": pg_receipt_sha,
            "operator_receipt_sha256": utility_receipt_sha, "companion_image_id": companion,
            "ordinary_main_binary_sha256": binary, "scoped_group_passed": True,
            "canonical_activation": False, "historical_passes_transferred": 0}


PHASES = ("pg-run", "pg-proof", "operator-run", "operator-proof", "group-proof")


def json_sha(value):
    return sha256((json.dumps(value, sort_keys=True) + "\n").encode()).hexdigest()


def verify_record(record, application):
    need(set(record) == {"schema", "decision_commit", "original_revision", "graph_sha256",
                         "application_publication", "group", "pg_proof", "operator_proof", "literal_exits"}
         and record["schema"] == "datastore-successor-group/v1",
         "exact reviewed successor receipt shape")
    need(set(record["literal_exits"]) == set(PHASES)
         and all(type(v) is int and v == 0 for v in record["literal_exits"].values()),
         "all actual group and independent proof exits must be zero")
    group, pg, operator = (record[k] for k in ("group", "pg_proof", "operator_proof"))
    candidate = group["candidate"]
    need(candidate["git_revision"] == application and candidate["platform"] == "linux/amd64"
         and hashes(candidate["manifest_sha256"], candidate["source_archive_sha256"], candidate["source_tree_sha256"])
         and re.fullmatch("sha256:[a-f0-9]{64}", candidate["image_id"])
         and group["source_tree_sha256"] == candidate["source_tree_sha256"]
         and group["pg_independent_proof_sha256"] == json_sha(pg)
         and group["operator_independent_proof_sha256"] == json_sha(operator),
         "independent proof bytes and ordinary package identity differ")
    expected = verify(candidate, pg, operator, pg_receipt_sha=group["postgres_receipt_sha256"],
                      utility_receipt_sha=group["operator_receipt_sha256"])
    need(all(group.get(k) == v for k, v in expected.items()),
         "group receipt disagrees with independent proofs")
    need(pg["restore_readback"]["fresh_password_HTTP"] == [200, 200, 401]
         and pg["restore_readback"]["recording_range_HTTP"] == [206, 404, 206]
         and all(pg["sql_boundary"][key] is True for key in (
             "immediate_restore_equals_pre_backup", "post_schedule_copied_business_unchanged",
             "separate_template_unchanged_after_readback_and_schedule"))
         and pg["postgres_runtime"]["architecture"] == "linux/amd64",
         "signed PostgreSQL restore and retention boundary is absent")
    return candidate


def admit(repo, policy, meta, commit, wanted_sha, application):
    need(all(isinstance(v, str) and re.fullmatch("[a-f0-9]{40}", v) for v in (commit, application))
         and hashes(wanted_sha) and application != policy.CANDIDATE and commit != policy.COMMIT,
         "explicit successor application and evidence pins required")
    lineage = policy.git(repo, "rev-list", "--first-parent", "HEAD").decode().splitlines()
    need(commit in lineage and policy.COMMIT in lineage and lineage.index(commit) < lineage.index(policy.COMMIT),
         "evidence must be published on the Root release lineage")
    relative = "docs/rust-datastore-release-evidence-" + application + ".json"
    leaf = repo / relative
    raw = policy.git(repo, "show", f"{commit}:{relative}")
    need(sha256(raw).hexdigest() == wanted_sha and leaf.resolve() == leaf
         and leaf.is_file() and leaf.read_bytes() == raw,
         "published successor record differs from the explicit pin")
    record = json.loads(raw)
    pub = record["application_publication"]
    need(pub in lineage and lineage.index(commit) < lineage.index(pub) < lineage.index(policy.COMMIT)
         and record["decision_commit"] == policy.COMMIT and record["original_revision"] == policy.ORIGINAL
         and record["graph_sha256"] == policy.GRAPH_SHA256,
         "application publication, recorded scope or graph lineage differs")
    parents = policy.git(repo, "show", "-s", "--format=%P", pub).decode().split()
    need(pub == application or (len(parents) == 2 and parents[1] == application),
         "published application is not the exact packaged candidate")
    for revision in (pub, commit, "HEAD"):
        changed = policy.git(repo, "diff", "--no-ext-diff", "--name-only",
                             "-z", "--no-renames", application, revision, "--").split(b"\0")[:-1]
        need(all(path in RELEASE_METADATA or path.startswith((b"docs/", b"scripts/rust-contracts/"))
                 for path in changed),
             "protected application files differ from the actual package source")
    candidate = verify_record(record, application)
    clean = {k: v for k, v in meta.items() if k not in ("operator_proof_commit", "operator_proof_sha256")}
    return clean | {
        "profile": PROFILE, "application_revision": application,
        "application_manifest_sha256": candidate["manifest_sha256"], "successor_candidate": candidate,
        "successor_evidence_commit": commit, "successor_evidence_sha256": wanted_sha,
        "application_publication": pub, "physical_group_passed": True,
    }


def authorities(meta, candidate, bindings):
    expected = meta["successor_candidate"]
    need(tuple(candidate.get(k) for k in ("git", "build_manifest_sha256", "image_id", "source_archive_sha256")) ==
         tuple(expected[k] for k in ("git_revision", "manifest_sha256", "image_id", "source_archive_sha256")),
         "successor package binding differs from the physical evidence")
    need(not bindings.keys() & {EXTERNAL, UTILITY}, "committed scope or utility cannot be replaced by argv")
    common = {"satisfies_dependency": True, "cleanup_verified": True, "counts_as_new_execution": False}
    return {
        EXTERNAL: common | {"id": EXTERNAL, "status": "recorded_scope_authority", "authority": meta},
        UTILITY: common | {"id": UTILITY, "status": "retained_copied_utility_authority",
                           "authority": {"commit": meta["successor_evidence_commit"],
                                         "sha256": meta["successor_evidence_sha256"], "exact_five_case_scope": True}},
    }
