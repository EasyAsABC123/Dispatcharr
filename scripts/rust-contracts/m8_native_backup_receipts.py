"""Qualify exact ordinary backup workflow from a committed reviewed receipt index."""
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import re
import subprocess

KEY = "native/backup-workflow"
SOURCE = "source/backup-restore-schedules"
STATUS = "verified_current_receipt"
PROFILE = "postgresql-copied-sqlite-successor"
BRANCH = "refs/remotes/rust-fork/rust-backend-migration"
SOURCE_FIELDS = ("Original_git", "Original_image", "actual_scoped_source_calls",
                 "actual_scoped_worker_jobs", "retained_source_receipt_sha256")
SCHEMA = "native-backup-workflow-receipts/v1"


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(path):
    require(path.is_absolute() and path.is_file() and path.resolve() == path,
            "expected exact unaliased current backup artifact")
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def owner_zero(files, names):
    require(all(files[name].read_bytes() == b"0\n" for name in names),
            "literal original physical, proof and whole-inventory zeros required")


def qualify(record, context, nodes, candidate, bindings, source):
    rows = {row["id"]: row for row in nodes}
    require(KEY not in bindings, "verified backup workflow cannot also execute")
    require(rows[KEY]["owner"] == "m478" and rows[KEY]["depends_on"] == [SOURCE]
            and rows[KEY]["entrypoint"] == "production BackupFiles.with_jobs + actual default Main/worker composition"
            and rows["native/backup-lifecycle"]["depends_on"] == [KEY],
            "preserve the approved backup graph and production entrypoint")
    require(isinstance(source, dict) and source["status"] == "retained_source_authority"
            and source["satisfies_dependency"] is True, "actual qualified Original16 authority required")
    require(isinstance(context, dict) and set(context) == {"record_commit", "artifact_root", "files"},
            "exact portable backup input shape")
    root = Path(context["artifact_root"])
    require(root.is_absolute() and root.is_dir() and root.resolve() == root,
            "exact local artifact root required")
    refs = record["evidence_sha256"]
    require(set(context["files"]) == set(refs), "committed private evidence roster differs")
    files = {}
    for name, value in context["files"].items():
        relative = Path(value)
        require(not relative.is_absolute() and ".." not in relative.parts,
                "evidence path must stay below the declared artifact root")
        path = root / relative
        require(digest(path) == refs[name], "reviewed proof or physical custody changed: " + name)
        files[name] = path
    app = candidate["git"]
    image = candidate["image_id"]
    manifest = candidate["build_manifest_sha256"]
    original = json.loads(files["source_authority"].read_bytes())
    require(source == original and record["retained_source"] == {k: source[k] for k in SOURCE_FIELDS}
            and source["historical_scoped_execution_performed"] is True
            and source["execution_performed"] is False and source["scoped_source_exact_cleanup"] is True
            and source["actual_scoped_source_calls"] == 16 and source["actual_scoped_worker_jobs"] == 5,
            "exact retained physical Original scope and cleanup required")
    owner_zero(files, ("pg_owner", "pg_run", "pg_eval", "pg_group_eval", "pg_census",
                       "pg_outer_run", "guard_owner", "guard_run", "guard_eval", "guard_census",
                       "guard_outer_run", "pg_replay_exit", "guard_replay_exit", "joined_exit", "upload_projection_exit"))
    for group in ("pg", "guard"):
        require(files[group + "_census_before"].read_bytes() == files[group + "_census_after"].read_bytes(),
                "original whole engine inventory changed: " + group)
        require(files[group + "_eval_stdout"].read_bytes() == files[group + "_replay"].read_bytes(),
                "independent file-only replay differs: " + group)
    pg = json.loads(files["pg_eval_stdout"].read_bytes())
    guard = json.loads(files["guard_eval_stdout"].read_bytes())
    for scope in (pg, guard):
        require(scope["candidate_revision"] == app and scope["candidate_image_id"] == image
                and scope["exact_cleanup"] is True, "exact current actual package and cleanup")
    require(pg["physical_receipt_sha256"] == digest(files["pg_raw"])
            and guard["candidate_manifest_sha256"] == manifest
            and guard["raw_report_sha256"] == digest(files["guard_payload"]),
            "exact independent package and guarded evidence")
    pg_raw = json.loads(files["pg_raw"].read_bytes())
    guard_raw = json.loads(files["guard_raw"].read_bytes())
    require(all(scope["passed"] is scope["exact_cleanup"] is True and scope["candidate"] == app
                and scope["image_id"] == image for scope in (pg_raw, guard_raw)),
            "both genuine physical owner results and retirement")
    payload = json.loads(files["guard_payload"].read_bytes())
    jobs = payload["jobs"]
    require(len(jobs) == len({job["id"] for job in jobs}) == 4
            and all(job["submit_HTTP"] == 202 and job["valid_signed_HTTP"] == 200
                    and job["signed_after_restart_HTTP"] == 200
                    and valid_hash(job["frame_sha256"])
                    and job["initial_result_sha256"] == job["result_after_restart_sha256"]
                    for job in jobs)
            and all(guard_raw["result"][key] == guard[key] for key in (
                "joint_signed_history_raw_transport_metadata", "actual_distinct_jobs",
                "scoped_guarded_partial_then_explicit_rollback", "fresh_restart_readback")),
            "guarded raw job lineage, restart result bytes and owner projection")
    result = pg_raw["result"]
    require([row["operation"] for row in result["routes"]] ==
            ["create-inventory-signed-download", "restore-refusal", "restore-refusal", "same-schema-restore"]
            and result["default_AIO"] is True and result["worker_publications"] == 5
            and result["independent_template_DB_unchanged"] is True
            and pg["native_original_matching_refusal_messages"] == 2
            and pg["actual_owned_empty_HTTP204_deletions"] == 2,
            "ordinary HTTP archive, positive restore, refusal and cleanup outcomes")
    retention = result["retention"]
    require((retention["initial_run_count"], retention["durable_run_count"], retention["archives_removed"],
             retention["archives_remaining"]) == (0, 1, 3, 2)
            and 0 <= retention["published_epoch"] - retention["due_epoch"] < 30,
            "genuine saved due publication and retention")
    runtime = pg["postgres_runtime"]
    require(set(runtime["actual_versions"]) == {"pg_dump", "pg_restore", "psql"}
            and all(runtime["linked_libraries"][tool] for tool in ("pg_dump", "pg_restore", "psql"))
            and all(type(runtime[key]) is int and runtime[key] > 0 for key in
                    ("files_verified", "owning_notices_verified"))
            and valid_hash(runtime["manifest_sha256"]),
            "exact packaged PostgreSQL binaries, libraries and notices")
    readback = pg["restore_readback"]
    require(readback["fresh_password_HTTP"] == [200, 200, 401]
            and readback["recording_range_HTTP"] == [206, 404, 206]
            and all(readback[key] is True for key in ("same_owned_media_bytes", "same_packaged_schema",
                                                   "same_password_hash", "same_recording_reference", "same_typed_actor")),
            "exact signed SQL/password/schema/recording/media restore readback")
    require(guard["fresh_restart_readback"] is guard["scoped_guarded_partial_then_explicit_rollback"] is True
            and guard["joint_signed_history_raw_transport_metadata"] is True
            and guard["actual_distinct_jobs"] == 4 and guard["actual_signed_archive_deletions"] == 2
            and guard["automatic_restore_rollback_qualified"] is False,
            "actual explicit rollback, signed restart and raw transport joins")
    validate_uploads(files, candidate, pg, pg_raw)
    joined = json.loads(files["joined_scope"].read_bytes())
    require(joined["same_package_separate_disposable_physical_fixtures"] is True
            and joined["exact_independent_cleanup_and_outer_inventory"] is True
            and joined["candidate"] == app and joined["image_id"] == image and joined["manifest_sha256"] == manifest
            and joined["PG_workflow"]["proof_sha256"] == digest(files["pg_eval_stdout"])
            and joined["guarded_restore"]["proof_sha256"] == digest(files["guard_eval_stdout"])
            and joined["new_physical_runs"] == 0 and joined["formal_rows_awarded"] == [],
            "reviewed composite preserves separate existing observations")
    return {"id": KEY, "status": STATUS, "candidate": candidate,
            "satisfies_dependency": True, "cleanup_verified": True,
            "counts_as_new_execution": False, "authority_context": context,
            "receipt_sha256": sha256(canonical_record(record)).hexdigest(), "physical_scopes": [
                {"scope": "ordinary-postgresql-backup-restore-schedule-retention",
                 "proof_sha256": digest(files["pg_eval_stdout"])},
                {"scope": "guarded-partial-effects-explicit-rollback-and-restart",
                 "proof_sha256": digest(files["guard_eval_stdout"])}],
            "automatic_restore_rollback_qualified": False}


def canonical_record(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def valid_hash(value, size=64):
    return isinstance(value, str) and re.fullmatch(f"[0-9a-f]{{{size}}}", value) is not None


def git(repo, *argv):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="file", GIT_NO_REPLACE_OBJECTS="1")
    return subprocess.check_output(["git", "--no-optional-locks", "--no-replace-objects",
                                    "-C", str(repo), *argv], env=env, timeout=25)


def identity(candidate):
    manifest = Path(candidate["build_manifest_file"])
    require(digest(manifest) == candidate["build_manifest_sha256"], "exact admitted current manifest")
    build = json.loads(manifest.read_bytes())
    require(build["passed"] is True and type(build["exit_code"]) is int and build["exit_code"] == 0
            and build["platform"] == "linux/amd64" and build["git_revision"] == candidate["git"]
            and build["image_id"] == candidate["image_id"]
            and build["source_archive_sha256"] == candidate["source_archive_sha256"],
            "committed evidence must name the independently admitted ordinary package")
    return {"git": candidate["git"], "image_id": candidate["image_id"],
            "manifest_sha256": candidate["build_manifest_sha256"],
            "archive_sha256": candidate["source_archive_sha256"],
            "source_tree_sha256": build["source_tree_sha256"]}


def received_record(repo, policy, context, candidate):
    require(policy is not None and policy[1].get("profile") == PROFILE,
            "verified selected datastore release authority required")
    meta = policy[1]
    app = candidate["git"]
    commit = context.get("record_commit")
    require(valid_hash(commit, 40) and valid_hash(app, 40), "exact committed evidence object required")
    # Trusted input is the coordinator's own independently verified receiving
    # checkout. The fixture binding supplies neither the repository nor its ref.
    remote = git(repo, "rev-parse", "--verify", BRANCH).decode().strip()
    head = git(repo, "rev-parse", "HEAD").decode().strip()
    require(valid_hash(remote, 40) and head == remote,
            "run admission from the independently received release tip")
    lineage = git(repo, "rev-list", "--first-parent", remote).decode().splitlines()
    ancestry = (commit, meta["successor_evidence_commit"], meta["application_publication"],
                meta["decision_commit"])
    require(all(item in lineage for item in ancestry), "committed receipt is absent from received release lineage")
    positions = [lineage.index(item) for item in ancestry]
    require(positions == sorted(set(positions)), "committed backup record must follow approved current datastore evidence")
    leaf = "docs/rust-native-backup-evidence-" + app + ".json"
    raw = git(repo, "show", f"{commit}:{leaf}")
    try:
        tip_blob = git(repo, "show", f"{remote}:{leaf}")
    except subprocess.CalledProcessError as error:
        raise ValueError("backup evidence is absent from the received release tip") from error
    require(tip_blob == raw, "backup evidence changed at the received release tip")
    require((repo / leaf).resolve() == repo / leaf and (repo / leaf).read_bytes() == raw,
            "current evidence bytes must equal the received committed record")
    record = json.loads(raw)
    require(raw == canonical_record(record) and record["schema"] == SCHEMA
            and set(record) == {"schema", "identity", "datastore", "retained_source", "evidence_sha256"},
            "exact reviewed sanitized evidence index required")
    require(record["identity"] == identity(candidate)
            and record["datastore"] == {"decision_commit": meta["decision_commit"],
                  "evidence_commit": meta["successor_evidence_commit"],
                  "application_publication": meta["application_publication"],
                  "approved_graph_sha256": meta["projected_graph_sha256"]}
            and all(valid_hash(value) for value in record["evidence_sha256"].values()),
            "committed sanitized evidence differs from the selected exact package, graph or datastore")
    earlier = "docs/rust-datastore-release-evidence-" + app + ".json"
    datastore = json.loads(git(repo, "show", f'{meta["successor_evidence_commit"]}:{earlier}'))
    pg_proof = datastore["pg_proof"]
    earlier_pg_sha = sha256((json.dumps(pg_proof, sort_keys=True) + "\n").encode()).hexdigest()
    require(earlier_pg_sha == record["evidence_sha256"]["pg_eval_stdout"]
            and pg_proof["physical_receipt_sha256"] == record["evidence_sha256"]["pg_raw"],
            "existing approved PostgreSQL proof and raw receipt must match")
    return record


def resolve(context, nodes, candidate, bindings, source, repo, policy):
    return qualify(received_record(repo, policy, context, candidate), context, nodes, candidate, bindings, source)


def validate_uploads(files, candidate, pg, raw):
    projection = json.loads(files["upload_projection"].read_bytes())
    require(projection["application"] == candidate["git"] and projection["image"] == candidate["image_id"]
            and projection["manifest_sha256"] == candidate["build_manifest_sha256"]
            and projection["original_physical_receipt_sha256"] == pg["physical_receipt_sha256"]
            and projection["independent_PG_proof_sha256"] == digest(files["pg_eval_stdout"])
            and projection["effective_workload_sha256"] == digest(files["upload_effective_workload"])
            and projection["effective_fixture_driver_sha256"] == digest(files["upload_effective_driver"])
            and projection["whole_inventory_and_teardown_exact"] is True
            and projection["archive_bytes_match_actual_uploaded_inert_recipes"] is True
            and projection["scheduled_due_jobs"] == 1
            and projection["periodic_schedule_PUT_and_GET_200_required_by_sealed_owner"] is True
            and projection["new_physical_runs"] == 0 and projection["direct_upload_http_transcript_retained"] is False,
            "exact upload/periodic projection must come from the genuine current physical run")
    routes = raw["result"]["routes"]
    uploads = projection["bearer_authenticated_multipart_uploads"]
    require(len(uploads) == 2, "two exact authenticated multipart upload chains")
    for arm, (route, upload) in enumerate(zip(routes[1:3], uploads, strict=True), 1):
        name = route["archive"]
        require(upload["recipe"] == name and upload["signed_restore_status_HTTP"] == route["signed_token_HTTP"] == 200
                and upload["owned_delete_HTTP"] == 204
                and upload["upload_HTTP_201_required_by_sealed_owner_before_actual_refusal_row"] is True,
                "upload success is required before actual signed restore-status row")
        snapshots = []
        for role, reference in zip(("uploaded", "before", "after"), upload["actual_direct_guest_inventory_sha256"],
                                   strict=True):
            receipt = files[f"upload_{arm}_{role}_receipt"]
            stdout = files[f"upload_{arm}_{role}_stdout"]
            command = json.loads(receipt.read_bytes())
            require(digest(receipt) == reference["command_receipt_sha256"]
                    and digest(stdout) == reference["settled_stdout_sha256"] == command["stdout_sha256"]
                    and Path(command["stdout"]).parts[-2:] == stdout.parts[-2:]
                    and type(command["actual_exit"]) is int
                    and command["actual_exit"] == 0 and command["failure"] is None
                    and command["remaining_group_members"] == [] and command["output_bytes_discarded"] == 0
                    and all(command[key] is True for key in (
                        "process_group_settled", "stdout_EOF", "stderr_EOF", "secret_output_excluded")),
                    "actual settled guest inspection and stdout custody")
            snapshots.append(json.loads(stdout.read_bytes()))
        posted, before, after = snapshots
        require(posted == before and len(after) == 1 and name not in after
                and posted == after | {name: upload["uploaded_archive_sha256"]}
                and list(after.values()) == [routes[0]["archive_sha256"]],
                "owned uploaded archive bytes and exact deletion preserved")
