"""Qualify the second local XMLTV import from fresh coordinator-owned package executions."""
from argparse import ArgumentParser
from datetime import datetime
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import re
import subprocess
from uuid import UUID

NODE = "m6/second-import-replaces-title"
GUIDE = "m6/guide-current-output"
CELLS = (GUIDE, NODE)
SOURCE = "package/complete-refresh-original"
NATIVE = "package/complete-refresh"
ENTRYPOINT = "production-Main; owner full workflow evaluator required"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
TASKS = (
    "apps.epg.tasks.refresh_epg_data",
    "apps.epg.tasks.build_programme_index_task",
    "apps.channels.tasks.evaluate_series_rules",
)
TITLES = ("Owned first import", "Owned second import")
PYTHON = "/usr/bin/python3"
RECEIPT_LIMIT = 4 * 1024 * 1024
HOST_RUNNER = "scripts/rust-job-contracts/run_refresh_workflow.py"
WORKFLOW_INPUTS = (HOST_RUNNER, "scripts/rust-job-contracts/ordinary_refresh_launch.py",
                   "scripts/rust-job-contracts/source_refresh_workflow.py",
                   "scripts/rust-job-contracts/refresh_workflow_projection.py")


def need(value, label):
    if not value:
        raise ValueError(label)


def raw(path):
    need(path.is_absolute() and path.resolve() == path and path.is_file()
         and path.stat().st_size <= RECEIPT_LIMIT, "exact bounded regular receipt: " + str(path))
    return path.read_bytes()


def read(path):
    return json.loads(raw(path))


def stamp(path):
    return sha256(raw(path)).hexdigest()


def wire(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def package_folder(owner_root, node):
    return owner_root / node


def receipt_folder(controller_root, node):
    return controller_root / (node.replace("/", "__") + "-" + sha256(node.encode()).hexdigest()[:12])


def require_fresh_roots(owner_root, controller_root):
    need(controller_root is not None, "exact fresh coordinator output required")
    need(not any(os.path.lexists(root) for root in (owner_root, controller_root)),
         "fresh package and coordinator output required; no retained results")


def package_dispatch_spec(node, candidate, owner_binding, controller_root, repository, resolved):
    """Extend the existing ordered package resolver with exact tracked proof methods."""
    need(node in (SOURCE, NATIVE) and resolved["ready"] is True, "existing complete-refresh resolver")
    owner_root = Path(owner_binding["output_root"])
    support = Path(owner_binding["support"]["root"])
    repository = Path(repository)
    need(support.is_absolute() and support.resolve() == support
         and support.is_relative_to(repository / "artifacts/rust"), "exact materialized fixture tools")
    checker = dispatch_spec(candidate, owner_root, controller_root, repository)
    expected = ["python3", str(support / HOST_RUNNER), "--output", str(package_folder(owner_root, node))]
    if node == NATIVE:
        expected += ["--manifest", candidate["build_manifest_file"], "--reference", str(package_folder(owner_root, SOURCE))]
    need(resolved["argv"] == expected, "exact canonical package command")
    inputs = dict(checker["input_sha256"])
    for leaf in WORKFLOW_INPUTS:
        canonical = repository / leaf
        staged = support / leaf
        need(canonical.is_file() and stamp(canonical) == stamp(staged), "committed ordinary workflow input: " + leaf)
        inputs[str(canonical)] = inputs[str(staged)] = stamp(canonical)
    prefix = checker["argv"][:-1]
    return {"argv": expected, "cwd": resolved["cwd"], "environment": resolved["environment_overlay"],
            "strip_inherited_prefixes": resolved["strip_inherited_prefixes"], "input_sha256": inputs,
            "seconds": 900, "cleanup_seconds": 45, "output_bytes": 1048576,
            "cleanup_argv": [*prefix, "package-cleanup", "--node", node],
            "evaluator_argv": [*prefix, "package-evaluate", "--node", node, "--execution", "@EXECUTION@"]}


def dispatch_spec(candidate, owner_root, controller_root, repository, *, node=NODE):
    """Resolve only the fixed local verifier, with paths derived from the existing owners."""
    need(node in CELLS, "declared complete-refresh scope")
    need(controller_root is not None and Path(controller_root).is_absolute(),
         "exact fresh M6 global coordinator output required")
    owner_root, controller_root, repository = map(Path, (owner_root, controller_root, repository))
    need(all(path.is_absolute() and path.resolve() == path for path in (owner_root, controller_root, repository))
         and owner_root != controller_root and not owner_root.is_relative_to(controller_root)
         and not controller_root.is_relative_to(owner_root), "distinct exact coordinator and package roots")
    need(owner_root.is_relative_to(repository / "artifacts/rust")
         and controller_root.is_relative_to(repository / "artifacts/rust"), "workspace-owned fixture roots")
    script = Path(__file__).resolve()
    manifest = Path(candidate["build_manifest_file"])
    need(stamp(manifest) == candidate["build_manifest_sha256"], "exact common package manifest")
    prefix = [PYTHON, "-I", "-B", str(script), "--owner-root", str(owner_root),
              "--coordinator-output", str(controller_root), "--candidate", wire(candidate)]
    if node == GUIDE:
        prefix += ["--workflow-node", GUIDE]
    return {"argv": [*prefix, "check"], "cleanup_argv": [*prefix, "cleanup"],
            "evaluator_argv": [*prefix, "evaluate", "--execution", "@EXECUTION@"],
            "cwd": str(repository), "environment": {},
            "input_sha256": {str(script): stamp(script), str(manifest): candidate["build_manifest_sha256"]},
            "seconds": 25, "cleanup_seconds": 25, "output_bytes": 65536}


def phase(phase_role, step, directory):
    need(type(step["actual_exit"]) is int and step["actual_exit"] == 0 and step["failure"] is None,
         "literal successful " + phase_role)
    need(all(step[field] is True for field in ("process_group_settled", "stdout_EOF", "stderr_EOF"))
         and step["remaining_group_members"] == [] and step["output_bytes_discarded"] == 0,
         "settled bounded " + phase_role)
    need(Path(step["stdout"]) == directory / (phase_role + ".stdout")
         and Path(step["stderr"]) == directory / (phase_role + ".stderr"),
         "exact current " + phase_role + " receipt paths")


def phase_closure(owner_root, controller_root, node, candidate, check_pass):
    directory = receipt_folder(controller_root, node)
    result = read(directory / "result.json")
    attempt = read(directory / "attempt.json")
    expected_entrypoint = "production-Main" if node == NATIVE else "pinned-Source workflow reference"
    need(attempt["node"]["entrypoint"] == expected_entrypoint and attempt["node"]["id"] == result["id"] == node
         and attempt["candidate"] == candidate
         and attempt["attempt"] == 1, "fresh exact candidate package attempt")
    args = attempt["binding"]["argv"]
    case = package_folder(owner_root, node)
    need(Path(args[args.index("--output") + 1]) == case, "package evidence bound to exact output")
    if node == NATIVE:
        need(Path(args[args.index("--reference") + 1]) == package_folder(owner_root, SOURCE)
             and Path(args[args.index("--manifest") + 1]) == Path(candidate["build_manifest_file"]),
             "Native exact fresh Original reference and manifest binding")
    phase("cleanup", result["cleanup"], directory)
    cleanup = read(directory / "cleanup.stdout")
    integration = case / "integration.json"
    need(result["cleanup_verified"] is True and cleanup["exact_owned_absence"] is True
         and cleanup["owned"] == {"container": [], "network": [], "volume": []}
         and cleanup["integration_sha256"] == stamp(integration), "actual dependent package closure")
    if check_pass:
        need(result["status"] == "passed" and result["qualification"]["candidate"] == candidate
             and result["qualification"]["entrypoint"] == attempt["node"]["entrypoint"]
             and result["qualification"]["full_required_workflow"] is True
             and result["qualification"]["semantic_assertions_passed"] is True
             and result["qualification"]["integration_sha256"] == stamp(integration),
             "scoped semantic and candidate authority from actual prerequisite")
        for label in ("execution", "evaluation"):
            phase(label, result[label], directory)
        need(read(directory / "evaluation.stdout") == result["qualification"],
             "retained exact evaluator receipt")
    return {"package_result_sha256": stamp(directory / "result.json"),
            "cleanup_sha256": stamp(directory / "cleanup.stdout"), "integration_sha256": stamp(integration)}


def timestamp(value):
    parsed = datetime.fromisoformat(value)
    need(parsed.utcoffset() is not None, "qualified offset-aware fixture timestamp")
    return parsed


def child_ids(metadata):
    return [row[0][0] for row in metadata["children"] if len(row) == 2 and len(row[0]) == 2]


def typed_branch(raw_report, native):
    need(raw_report["passed"] is True and raw_report["native_candidate"] is native, "actual expected arm")
    checks = raw_report["checks"]
    need(len(checks) == len({row["name"] for row in checks}) == 40
         and all(row["passed"] is True for row in checks), "full exact complete-refresh controls")
    required = {"recordings-unchanged", "event-reader-no-errors", "only-three-owned-families-executed"}
    for number in (1, 2):
        required |= {f"import{number}-{check}" for check in ("http202", "current-and-guide", "programme",
                     "stored-index", "source-success", "queue-and-locks-drained", "realtime-refresh")}
    need(required <= {row["name"] for row in checks}, "required second-import control roster")
    need(raw_report["queue_and_results_after_cleanup"] == [0, 0] and raw_report["event_reader_errors"] == [],
         "queue/result and realtime cleanup")
    imports = raw_report["imports"]
    need([row["iteration"] for row in imports] == [1, 2], "two real ordered imports")
    programme_ids, unique_tasks, file_digests, schedule = [], set(), [], []
    for row, title in zip(imports, TITLES, strict=True):
        need(row["http"] == {"status": 202, "body": {"success": True, "message": "EPG data refresh initiated."}},
             "ordinary signed accepted HTTP 202")
        current, = row["current"]
        programme, = row["programmes"]
        need(current["title"] == programme["title"] == title and current["id"] == programme["id"]
             and type(programme["id"]) is int and programme["id"] > 0
             and current["tvg_id"] == programme["tvg_id"] == "workflow-owned",
             "exact single changed title in Current and programme readback")
        start = timestamp(programme["start_time"])
        end = timestamp(programme["end_time"])
        need(start < end and timestamp(current["start_time"]) == start and timestamp(current["end_time"]) == end,
             "same exact Current and programme times")
        schedule.append((start.isoformat(), end.isoformat(), str(UUID(current["channel_uuid"]))))
        programme_ids.append(programme["id"])
        need(len(row["events"]) == 11 and len(row["index"]["channels"]) == 1,
             "all ordered realtime events and channel index retained")
        need(re.fullmatch(r"[0-9a-f]{64}", row["file_sha256"]) is not None
             and row["source_status"] == "success", "actual source file and successful status")
        file_digests.append(row["file_sha256"])
        jobs = {task["task"]: task for task in row["completed"]}
        need(len(jobs) == len(row["completed"]) == 3 and set(jobs) == set(TASKS)
             and all(task["state"] == "SUCCESS" for task in jobs.values()), "three actual successful task terminals")
        need([jobs[name]["result"] for name in TASKS] == [None, None, {"scheduled": 0, "details": []}],
             "exact typed Refresh, Index and zero-rule Series outcomes")
        ordered_ids = [str(UUID(jobs[name]["id"])) for name in TASKS]
        need(len(set(ordered_ids)) == 3 and not unique_tasks.intersection(ordered_ids), "fresh distinct task IDs")
        unique_tasks.update(ordered_ids)
        meta = row["metadata"]
        need(meta["task_id"] == ordered_ids[0] and meta["status"] == "SUCCESS"
             and meta["result"] is None and meta["traceback"] is None and child_ids(meta) == ordered_ids[1:],
             "exact actual child lineage")
    need(programme_ids[0] != programme_ids[1] and file_digests[0] != file_digests[1]
         and schedule[0] == schedule[1], "same retained channel/window with the physically replaced programme")
    return {"titles": list(TITLES), "file_sha256": file_digests, "schedule": list(schedule[0]),
            "http_202_count": 2, "terminal_count": len(unique_tasks), "typed_check_count": len(checks)}


def compare(owner_root, controller_root, candidate, *, node=NODE):
    need(node in CELLS, "declared complete-refresh scope")
    manifest_path = Path(candidate["build_manifest_file"])
    manifest = read(manifest_path)
    need(stamp(manifest_path) == candidate["build_manifest_sha256"]
         and manifest["git_revision"] == candidate["git"] and manifest["image_id"] == candidate["image_id"]
         and manifest["passed"] is True and manifest["exit_code"] == 0
         and manifest["source_archive_sha256"] == candidate["source_archive_sha256"],
         "exact current complete-refresh production package")
    metrics, authority, guide = {}, {}, {}
    for arm in (SOURCE, NATIVE):
        authority[arm] = phase_closure(owner_root, controller_root, arm, candidate, True)
        integration, observed, work = workflow_receipt(owner_root, arm, candidate, manifest)
        validate_tool_record(integration, Path(__file__).resolve().parents[3])
        metrics[arm] = typed_branch(observed, arm == NATIVE) | {
            "fixture_clock": timestamp(observed["fixture_clock"]).isoformat()
        }
        authority[arm]["raw_workflow_sha256"] = stamp(work)
        if node == GUIDE:
            guide[arm] = guide_output(observed, integration)
    need(metrics[SOURCE] == metrics[NATIVE], "equal exact two-import typed workloads and reference clock")
    proof = {"node": node, "authority": authority, "new_physical_resources": False}
    if node == GUIDE:
        need(guide[SOURCE] == guide[NATIVE], "equal signed Current and programme JSON")
        proof.update(guide_current=guide[NATIVE], same_package_reservation=paired_guide_lease(controller_root, candidate, authority))
    else:
        proof["two_imports"] = metrics[NATIVE]
    return proof


def guide_output(observed, integration):
    first = observed["imports"][0]
    check, = [item for item in observed["checks"] if item["name"] == "import1-current-and-guide"]
    need(check["passed"] is True, "observed HTTP 200 for both signed guide reads")
    programme, = first["programmes"]
    current, = first["current"]
    channel = str(UUID(current["channel_uuid"]))
    start, end = (timestamp(programme[key]) for key in ("start_time", "end_time"))
    clock = timestamp(observed["fixture_clock"])
    need(start < clock < end and type(programme["id"]) is int and programme["id"] > 0,
         "real programme active during the common reference clock")
    expected = {"id": programme["id"], "start_time": programme["start_time"], "end_time": programme["end_time"],
                "title": TITLES[0], "sub_title": None, "description": None, "tvg_id": "workflow-owned",
                "season": None, "episode": None, "is_new": False, "is_live": False, "is_premiere": False, "is_finale": False}
    flags = ("is_new", "is_live", "is_premiere", "is_finale")
    need(programme == expected and current == expected | {"channel_uuid": channel}
         and all(programme[key] is current[key] is False for key in flags)
         and type(current["id"]) is int, "exact signed programme and Current fields")
    if observed["native_candidate"]:
        native, source = integration["paired_projection"][0], integration["original_projection"][0]
        need(all(projection["current"] == first["current"] and projection["programmes"] == first["programmes"]
                 for projection in (native, source)), "captured full JSON matches the qualified pair projection")
    return {"role": "admin", "observed_http": [
                {"method": "POST", "path": "/api/epg/current-programs/", "json": {"channel_uuids": [channel]}, "status": 200},
                {"method": "GET", "path": "/api/epg/programs/", "query": {"title": TITLES[0]}, "status": 200}],
            "fixture_clock": clock.isoformat(), "current": first["current"], "programmes": first["programmes"]}


def paired_guide_lease(controller_root, candidate, authority):
    lease = read(receipt_folder(controller_root, NATIVE) / "pair-lease.json")
    age = lease["reference_age_at_dispatch_seconds"]
    need(lease["candidate"] == candidate and lease["paired_lease"] == {"source": SOURCE, "native": NATIVE, "one_global_slot": True}
         and type(age) in (int, float) and 0 <= age <= 900
         and lease["result_sha256"] == {arm: item["package_result_sha256"] for arm, item in authority.items()},
         "current qualified pair lease and reference age")
    return {"same_global_slot": True, "Original_reference_age_seconds": age, "maximum_reference_age_seconds": 900}



def verify_ordinary(integration, observed):
    ordinary = integration["ordinary_aio_verified"]
    need(all(ordinary[key] is True for key in ("ordinary_aio", "no_selected_or_eager_overrides",
                                               "prepared_before_candidate_intake"))
         and type(ordinary["source_id"]) is int and ordinary["source_id"] > 0
         and ordinary["input_file_sha256"] == observed["imports"][0]["file_sha256"], "actual ordinary AIO handoff")


def validate_tool_record(integration, repository):
    for leaf in WORKFLOW_INPUTS:
        need(integration["tool_source_sha256"][leaf] == stamp(repository / leaf), "actual tracked fixture tool: " + leaf)


def package_cleanup(owner_root, node):
    integration = package_folder(owner_root, node) / "integration.json"
    report = read(integration)
    project = report["project"]
    need(re.fullmatch(r"dispatcharr-refresh-workflow-original-[a-f0-9]{10}", project), "exact owned workflow project")
    label = "label=com.docker.compose.project=" + project
    owned = {kind: subprocess.check_output(["docker", kind, *flags, "--filter", label], timeout=10, text=True).splitlines()
             for kind, flags in (("container", ["ls", "--all", "--quiet", "--no-trunc"]),
                                 ("network", ["ls", "--quiet", "--no-trunc"]), ("volume", ["ls", "--quiet"]))}
    absent = report["cleanup"] == "project removed" and report["resources_retained"] is False and not any(owned.values())
    return {"scope": node.split("/", 1)[1], "exact_owned_absence": absent, "owned": owned,
            "integration_sha256": stamp(integration)}


def workflow_receipt(owner_root, node, candidate, manifest):
    folder = package_folder(owner_root, node)
    integration = read(folder / "integration.json")
    work = folder / "source-refresh-workflow-private.json"
    observed = read(work)
    need(integration["passed"] is integration["default_database_unchanged"] is True
         and integration["source_report_sha256"] == stamp(work)
         and integration["cleanup"] == "project removed" and integration["resources_retained"] is False
         and integration["original_revision"] == ORIGINAL and integration["original_archive_sha256"] == ARCHIVE
         and integration["checks"] == 40, "exact copied-schema workflow and cleanup witness")
    if node == NATIVE:
        verify_ordinary(integration, observed)
        need(integration["candidate_image"] == candidate["image_id"]
             and integration["candidate_manifest_sha256"] == candidate["build_manifest_sha256"]
             and integration["candidate_tree"] == manifest["source_tree_sha256"]
             and integration["reference_integration_sha256"] == stamp(package_folder(owner_root, SOURCE) / "integration.json")
             and integration["paired_complete_workflow_verified"] is integration["native_worker_execution_verified"] is True
             and integration["paired_projection"] == integration["original_projection"]
             and integration["candidate_shutdown"] == {"exit_code": 0, "oom_killed": False, "running": False},
             "ordinary Native paired against same fresh Original and actual shutdown")
    else:
        need(integration["original_complete_refresh_workflow_verified"] is True, "qualified actual Original scope")
    need(observed["precleanup_queue"] == [] and observed["precleanup_unacked_count"] == 0
         and all(value is None for value in observed["precleanup_owned_locks"].values()),
         "no pending heads or locks before physical cleanup")
    return integration, observed, work


def package_evaluate(owner_root, controller_root, node, candidate, execution):
    directory = receipt_folder(controller_root, node)
    need(execution == directory / "execution.json", "exact package evaluator input")
    result = read(execution)
    phase("execution", result["execution"], directory)
    phase("cleanup", result["cleanup"], directory)
    need(result["cleanup_verified"] is True, "actual settled package cleanup")
    cleanup = read(directory / "cleanup.stdout")
    integration_file = package_folder(owner_root, node) / "integration.json"
    need(cleanup["exact_owned_absence"] is True and cleanup["owned"] == {"container": [], "network": [], "volume": []}
         and cleanup["integration_sha256"] == stamp(integration_file), "live cleanup receipt belongs to workflow")
    manifest = read(Path(candidate["build_manifest_file"]))
    integration, observed, _ = workflow_receipt(owner_root, node, candidate, manifest)
    validate_tool_record(integration, Path(__file__).resolve().parents[3])
    native = node == NATIVE
    metrics = typed_branch(observed, native)
    proof = {"scope": node, "candidate": candidate,
             "entrypoint": "production-Main" if native else "pinned-Source workflow reference",
             "semantic_assertions_passed": True, "full_required_workflow": True, "imports": metrics["http_202_count"],
             "actual_checks": metrics["typed_check_count"], "actual_task_terminals": metrics["terminal_count"],
             "exact_default_database_preserved": True, "integration_sha256": stamp(integration_file)}
    if native:
        proof["ordinary_aio_verified"] = True
    return proof


def bind(document, owner_root, controller_root, repository):
    candidate = document["candidate"]
    owner_binding = document["owner_bindings"]["m235"]
    need(Path(owner_binding["output_root"]) == owner_root, "same package owner root as coordinator document")
    require_fresh_roots(owner_root, controller_root)
    batch_path = repository / "scripts/rust-milestone-batches/m235/batch.py"
    spec = spec_from_file_location("m235_complete_refresh_binding", batch_path)
    batch = module_from_spec(spec)
    spec.loader.exec_module(batch)
    nodes = dict(document["nodes"])
    for node in (SOURCE, NATIVE):
        resolved = batch.resolve_node(node, owner_binding, repository)
        nodes[node] = package_dispatch_spec(node, candidate, owner_binding, controller_root, repository, resolved)
    for node in CELLS:
        nodes[node] = dispatch_spec(candidate, owner_root, controller_root, repository, node=node)
    return document | {"nodes": nodes}


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--owner-root", required=True, type=Path)
    parser.add_argument("--coordinator-output", required=True, type=Path)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--workflow-node", choices=CELLS, default=NODE)
    parser.add_argument("mode", choices=("plan", "bind", "check", "cleanup", "evaluate", "package-cleanup", "package-evaluate"))
    parser.add_argument("--execution", type=Path)
    parser.add_argument("--node", choices=(SOURCE, NATIVE))
    parser.add_argument("--binding-document", type=Path)
    args = parser.parse_args()
    candidate = json.loads(args.candidate)
    repository = Path(__file__).resolve().parents[3]
    if args.mode == "bind":
        need(args.binding_document is not None and args.execution is None and args.node is None,
             "fixed package binding arguments")
        document = read(args.binding_document)
        need(document["candidate"] == candidate, "same exact common candidate")
        print(wire(bind(document, args.owner_root, args.coordinator_output, repository)))
        return 0
    need(args.binding_document is None, "fixed workflow modes")
    if args.mode == "package-cleanup":
        need(args.execution is None and args.node is not None, "fixed package cleanup arguments")
        proof = package_cleanup(args.owner_root, args.node)
        print(wire(proof))
        return int(not proof["exact_owned_absence"])
    if args.mode == "package-evaluate":
        need(args.node is not None, "fixed package evaluation arguments")
        print(wire(package_evaluate(args.owner_root, args.coordinator_output, args.node, candidate, args.execution)))
        return 0
    need(args.node is None, "fixed formal checker mode")
    if args.mode == "plan":
        need(args.execution is None, "fixed plan arguments")
        print(wire(dispatch_spec(candidate, args.owner_root, args.coordinator_output,
                                 repository, node=args.workflow_node)))
        return 0
    if args.mode == "cleanup":
        need(args.execution is None, "fixed cleanup arguments")
        closures = {node: phase_closure(args.owner_root, args.coordinator_output, node, candidate, False)
                    for node in (SOURCE, NATIVE)}
        print(wire({"exact_owned_absence": True, "consumed_current_package_cleanup": closures,
                    "new_physical_resources": False}))
        return 0
    proof = compare(args.owner_root, args.coordinator_output, candidate, node=args.workflow_node)
    if args.mode == "evaluate":
        directory = receipt_folder(args.coordinator_output, args.workflow_node)
        need(args.execution == directory / "execution.json", "exact coordinator evaluator input")
        execution = read(args.execution)
        phase("execution", execution["execution"], directory)
        need(execution["cleanup_verified"] is True, "actual formal adapter cleanup")
        need(read(directory / "execution.stdout") == proof, "unchanged independently checked typed observations")
        proof.update(candidate=candidate, entrypoint=ENTRYPOINT, semantic_assertions_passed=True,
                     full_required_workflow=True)
    else:
        need(args.execution is None, "fixed checker arguments")
    print(wire(proof))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
