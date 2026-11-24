"""Cold-bind and independently evaluate the ordinary first-scanner scope."""
import argparse
import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    import recording61_owner as common
    import scanner_first_scenario as scenario
    from prepare_recording61 import rebind_package
finally:
    sys.path.pop(0)

NODE = "m6/scanner-first"
SCRIPT = Path(__file__).resolve()
TEMPLATE = "paseo-async-f960875-1/release-matrix/m6-48181ab0-missing-xmltv-index-pair-1"
TEMPLATE_SHA = "e9c4a65770fb9b062861e067989e62b041dc3950295c86d340f2164069e6c330"
SUPPORT = ("run.py", "fixture_adapter.py", "scenario.py", "origin61.py", "external-pins.json")
SOURCE_ROOT = "scripts/rust-job-contracts/scanner-logo"
SOURCE = {SOURCE_ROOT + "/source-logo-scanner-2.json": "f5ba43d632af3ed6803cff6296c41add7c6573f079358f9d8a5cc0dfa534f648",
          SOURCE_ROOT + "/verified-scanner-logo-original-2.json": "07052efefe8b8da0c899cdc539cd9f0849ec1b5bc752312320513a14de7ddcf2"}
require, read, digest = common.require, common.read, common.digest


def original(repository):
    paths = {repository / leaf: stamp for leaf, stamp in SOURCE.items()}
    require(all(digest(path) == stamp for path, stamp in paths.items()), "exact Original scanner-logo authority required")
    dataset, summary = (read(path) for path in paths)
    require(summary["passed"] is True and summary["original_revision"] == common.ORIGINAL
            and summary["actual_original_worker_verified"] is True and summary["resources_retained"] is False
            and summary["dataset_sha256"] == next(iter(SOURCE.values())), "pinned Original worker and cleanup required")
    first, = [row for row in dataset["cases"] if row["name"] == "first-scan-index-lineage"]
    require(first["metadata"]["status"] == "SUCCESS" and first["metadata"]["result"] is None
            and first["events"] == [] and len(first["children"]) == 2,
            "Original first scanner null result and empty report differ")
    checks = {row["name"] for row in dataset["checks"] if row["passed"] is True}
    require({"first-index-only-missing-and-sqlnull", "first-index-real-routing-root-parent",
             "first-scan-index-lineage-exact-summary", "first-scan-index-lineage-unchanged-owned-files"} <= checks,
            "retained Original first-scan eligibility and report authority missing")
    query, = first["sql"]["queries"]
    require("LEFT OUTER JOIN" in query["sql"] and '"epg_epgsourceindex"."data" IS NULL' in query["sql"],
            "retained Original scanner must use the actual SQL missing-index condition")
    ids = []
    for job in first["children"]:
        header = job["headers"]
        require(job["task"] == scenario.INDEX and job["routing_key"] == "celery"
                and header["root_id"] == header["parent_id"] == first["parent_id"]
                and len(job["body"][0]) == 1 and job["body"][1] == {},
                "Original scanner child identity or routing differs")
        ids.append(header["id"])
    require(len(set(ids)) == 2 and first["metadata"]["children"] == [[[value, None], None] for value in ids],
            "Original scanner child result order differs")
    for label, children, queries in (("second-scan-retains-children", 0, 0),
                                      ("worker-restart-republishes-missing-index", 2, 1)):
        row, = [case for case in dataset["cases"] if case["name"] == label]
        require(len(row["children"]) == children and len(row["sql"]["queries"]) == queries
                and row["events"] == [] and row["metadata"]["status"] == "SUCCESS"
                and row["metadata"]["result"] is None, "exact Original scanner continuation evidence required")
    require({"second-no-new-child-or-deletion", "restart-actual-duplicate-source-publications",
             "restart-preserves-all-four-descendants"} <= checks, "retained Original child custody required")
    return {str(path): value for path, value in paths.items()}


def replace(text, old, new):
    require(text.count(old) == 1, "pinned scanner fixture adaptation boundary changed")
    return text.replace(old, new)


def prepare(repository, package_template, candidate, stage):
    template = repository / "artifacts/rust" / TEMPLATE
    _, files = common.sealed_inputs(template, TEMPLATE_SHA)
    require(set(SUPPORT) | {"missing_scenario.py"} <= files, "exact stock scanner fixture template required")
    original(repository)
    old = read(package_template / "package-binding.json")
    anchor = next(path for path in old["pins"] if path.endswith("/package/build.json"))
    prior = {"git": old["candidate"], "image_id": old["image"], "build_manifest_file": anchor,
             "build_manifest_sha256": old["manifest"], "source_archive_sha256": old["archive"]}
    from_seal = "3a8e6bde76d862406ca7f8ccd30e86cdeb987f48d077248a888e9b7350e49919"
    common.stage_inputs(package_template, from_seal, prior)
    manifest, package = rebind_package(old, candidate)
    data = {name: (template / name).read_bytes() for name in SUPPORT}
    data["scanner_wire_support.py"] = (template / "missing_scenario.py").read_bytes()
    checker = (package_template / "candidate.py").read_text()
    for key in ("candidate", "image", "manifest"):
        checker = replace(checker, old[key], package[key])
    checker = replace(checker, f"=={old['source_files']}==", f"=={package['source_files']}==")
    data["candidate.py"] = checker.encode()
    adapter = data["fixture_adapter.py"].decode()
    data["fixture_adapter.py"] = replace(adapter, "ART = HERE.parents[2]",
                                        f"ART = Path({str(repository / 'artifacts/rust')!r})").encode()
    text = data["run.py"].decode()
    text = replace(text, 'SIDES = ("Original", "Native")', 'SIDES = ("Native",)')
    text = replace(text, 'BINDING = HERE / "bound-candidate.json"', 'BINDING = HERE / "package-binding.json"')
    prior_original = template.parent / "m6-009cc8dd-interleaved-programme-index-pair-retry-2/actual-1/Original/Original-source"
    text = replace(text, 'candidate.PREVIOUS / "actual-1/Original/Original-source"', f"Path({str(prior_original)!r})")
    text = replace(text, '    need(len(candidate.application_map()) == 2178, "verified exact ordinary 48181 map")\n', "")
    start, end = text.index("def load_binding():"), text.index("def authority():")
    text = text[:start] + f"""def load_binding():
    global PACKAGE, REVISION, IMAGE, MANIFEST
    PACKAGE = Path({str(manifest)!r})
    binding = read(BINDING)
    MANIFEST = binding["manifest"]
    package = candidate.verify_package(PACKAGE, MANIFEST)
    REVISION, IMAGE = package["git_revision"], package["image_id"]
    return package


""" + text[end:]
    text = replace(text, 'missing_scenario.snapshot(fixture, business["targets"])',
                   'missing_scenario.snapshot(fixture, business["seed"])')
    text = replace(text, 'missing_scenario.unchanged(business["targets"], business["before"], retired)',
                   'need(retired == business["after"], "same copied scanner state after actual Main exit")')
    text = replace(text, 'need(arms and arms[0] == "Original", "at least the owned Original arm admitted")',
                   'need(arms == ["Native"], "one exact owned Native scanner arm required")')
    start, end = text.index("def verify_business(side):"), text.index("def run(base, package):")
    text = text[:start] + '''def verify_business(side):
    owned = Path(read(OUT / side / "fixture.json")["output"])
    business = OUT / side / "business"
    report = read(business / "scenario.json")
    journal = owned / report["journal_path"]
    need(journal.resolve() == journal and journal.is_relative_to(owned) and stamp(journal) == report["journal_sha256"],
         "exact private scanner journal required")
    final = read(journal.with_name("final.json"))
    need(journal.stat().st_size <= 16777216
         and all(final[key] is True for key in ("terminal_barrier_observed", "terminal_barrier_fsynced",
                                                "thread_settled", "writer_closed"))
         and final["observer_error"] is None and not final["terminal_reader_error"],
         "exact first-scanner observer wire closure required")
    stop = report["observer_stopped"]
    need(type(stop["actual_exit"]) is int and stop["actual_exit"] == 0 and stop["forced"] is False
         and all(stop[key] is True for key in ("actual_wait_completed", "stdout_EOF", "stderr_EOF",
                                              "all_original_process_generations_absent")),
         "exact first-scanner observer process must retire")
    import scanner_wire_support
    validated = missing_scenario.validate_lifecycle(report, scanner_wire_support.from_journal(journal))
    need(read(business / "after-application-exit.json") == report["after"],
         "same affected SQL projection after literal application stop")
    verify_shutdown(side, read(OUT / side / "application-shutdown.json"))
    return validated


''' + text[end:]
    text = replace(text, 'def run(base, package):', 'def run(base, package, coordinator_lease):')
    text = replace(text, '    lease = load(LEASE, "ordinary_recording61_global_lease")\n', "")
    text = replace(text, 'with lease.fixture_lease(SHARED / ".dispatcher.lock"):', 'with coordinator_lease(SHARED / ".dispatcher.lock"):')
    text = replace(text, "def main():", "def main(argv=None, coordinator_lease=None):")
    text = replace(text, "args = parser.parse_args()", "args = parser.parse_args(argv)")
    start, end = text.index('    if args.mode == "offline":'), text.index('    if args.mode == "lane":')
    text = text[:start] + text[end:]
    start = text.index('    elif args.mode == "proof":')
    end = text.index('    else:\n        binding, package = authority()', start)
    text = text[:start] + '''    elif args.mode == "proof":
        proof = {"candidate": REVISION, "manifest_sha256": MANIFEST, "native": verify_business("Native"),
                 "physical_cleanup": cleanup()}
''' + text[end:]
    text = replace(text, 'return run(binding, package)', 'need(coordinator_lease is not None, "reviewed scanner coordinator lease required")\n            return run(binding, package, coordinator_lease)')
    start = text.index('        proof = {"engine_calls": 0')
    end = text.index('    print(json.dumps(proof', start)
    text = text[:start] + """        proof = {"engine_calls": 0, "candidate": REVISION, "image_id": IMAGE,
                 "mounted_tools": donor.verify_tool_modes(), "first_scanner_fixture_prepared": True}
""" + text[end:]
    data["run.py"] = text.encode()
    data["missing_scenario.py"] = Path(scenario.__file__).read_bytes()
    data["package-binding.json"] = (json.dumps(package, sort_keys=True) + "\n").encode()
    require(stage.resolve() == stage and not stage.exists(), "fresh absolute scanner stage required")
    stage.mkdir(mode=0o700)
    for name, body in data.items():
        if name.endswith(".py"):
            compile(body, name, "exec")
        (stage / name).write_bytes(body)
    sums = stage / "SHA256SUMS"
    sums.write_text("".join(f"{digest(stage / name)}  {name}\n" for name in sorted(data)))
    cold = subprocess.run(["/usr/bin/python3", "-B", str(stage / "run.py"), "preflight"],
                          capture_output=True, check=True, timeout=60)
    proof = json.loads(cold.stdout)
    require(proof["engine_calls"] == 0 and proof["candidate"] == candidate["git"],
            "exact cold ordinary scanner package admission required")
    return {"stage": str(stage), "seal": digest(sums), "cold_preflight": proof}


def stage_binding(repository, stage, seal, candidate):
    inputs, files = common.sealed_inputs(stage, seal)
    require(set(SUPPORT) | {"missing_scenario.py", "scanner_wire_support.py", "package-binding.json"} <= files
            and digest(stage / "missing_scenario.py") == digest(Path(scenario.__file__).resolve()),
            "exact fresh scanner evaluator and runtime inputs required")
    value = read(stage / "package-binding.json")
    manifest = Path(candidate["build_manifest_file"])
    require((value["candidate"], value["image"], value["manifest"], value["archive"])
            == (candidate["git"], candidate["image_id"], digest(manifest), candidate["source_archive_sha256"])
            and value["manifest"] == candidate["build_manifest_sha256"],
            "scanner stage must match the frozen ordinary package")
    return inputs | original(repository) | {str(manifest): value["manifest"]}


def dispatch_spec(repository, stage, seal, candidate, entrypoint, output):
    inputs = stage_binding(repository, stage, seal, candidate)
    require(output.resolve() == output and not stage.is_relative_to(output) and not output.is_relative_to(stage),
            "separate coordinator output and scanner stage required")
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    coordinator_sha = digest(coordinator)
    prefix = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(repository), str(stage), seal,
              json.dumps(candidate, sort_keys=True)]
    tooling = repository / "artifacts/rust/tooling"
    return {"argv": [*prefix, "run", entrypoint, str(output), coordinator_sha],
            "cleanup_argv": [*prefix, "cleanup"],
            "evaluator_argv": [*prefix, "evaluate", entrypoint, str(output), "@EXECUTION@"],
            "input_sha256": inputs | {str(path): digest(path) for path in
                                      (SCRIPT, common.SCRIPT, Path(scenario.__file__).resolve(), coordinator)},
            "cwd": str(repository), "environment": {"DOCKER_HOST": common.DAEMON,
                "DOCKER_CONFIG": str(tooling / "docker-config"), "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes": common.STRIP_ENV, "seconds": 660, "cleanup_seconds": 60,
            "output_bytes": 65536, "exclusive_heavy": True}


def evaluate(stage, candidate, driver):
    ledger = stage / "owner-1"
    require(all((ledger / (phase + ".actual-exit")).read_bytes() == b"0\n" for phase in common.PHASES),
            "literal scanner owner phase exits required")
    root = stage / "actual-1"
    require(read(root / "admission.json")["native"] == candidate["image_id"]
            and common.settled(read(root / "Native-execution.json")), "actual scanner Native admission and lane exit required")
    retirement = common.cleanup_receipt(stage, driver)
    body = read(ledger / "proof.stdout")
    require(body["candidate"] == candidate["git"] and body["manifest_sha256"] == candidate["build_manifest_sha256"]
            and body["physical_cleanup"] == retirement["physical_cleanup"]
            and driver.verify_business("Native") == body["native"],
            "scanner semantic, package and scoped teardown receipts differ")
    lane = root / "Native"
    fixture = Path(read(lane / "fixture.json")["output"])
    report = read(lane / "business/scenario.json")
    require(read(fixture / "ordinary-aio-ready.json")
            == {"ordinary_aio": True, "core_saved_scheduler_observed": True, "both_lanes_accepting": True},
            "real default-selected scanner Main readiness required")
    restart = report["restart"]
    project = read(lane / "fixture.json")["project"]
    generations = []
    networks = set()
    for name in (restart["old"], restart["new"]):
        require(re.fullmatch(re.escape(project) + r"-native-[0-9a-f]{5}", name), "exact owned Main name")
        info = read(fixture / (name + "-inspection.json"))
        require(info["Name"] == "/" + name and info["Image"] == candidate["image_id"]
                and info["State"]["Running"] is True and type(info["State"]["Pid"]) is int
                and info["State"]["Pid"] > 0 and info["HostConfig"]["NetworkMode"].startswith("container:")
                and info["Config"]["Labels"]["com.docker.compose.project"] == project
                and len(info["Mounts"]) == 1 and info["Mounts"][0]["Destination"] == "/data"
                and info["Mounts"][0]["Source"] == str(fixture / "data" / scenario.CONTEXT),
                "exact owned candidate and retained Main mounts/network")
        generations.append(info["Id"])
        networks.add(info["HostConfig"]["NetworkMode"])
    require(len(set(generations)) == 2 and len(networks) == 1
            and read(fixture / (restart["old"] + "-exit.json")) == restart["exit"]
            and read(fixture / (restart["new"] + "-exit.json"))
            == read(lane / "application-shutdown.json")["native_application"],
            "both distinct Main container IDs and real retirement receipts required")

    def queue_proof(part):
        command_name = part["queue_command"]
        require(re.fullmatch(r"command-[0-9]{4}\.json", command_name), "exact queue command receipt required")
        queue = read(fixture / command_name)
        stdout = Path(queue["stdout"])
        raw_queue = read(stdout)
        require(stdout.is_relative_to(fixture) and common.settled(queue)
                and digest(stdout) == queue["stdout_sha256"] and type(raw_queue) is int and raw_queue == 0,
                "actual ready/reserved queue must drain")

    wanted = [report["before"]]
    for number, part in enumerate(report["passes"], 1):
        queue_proof(part)
        if number == 3:
            wanted.extend((restart["after_stop"], restart["after_start"]))
            for prior in report["passes"][:2]:
                jobs = prior["jobs"]
                wanted.extend(report["retained_results"][ident] for ident in
                              [jobs["root"]["id"], *(child["id"] for child in jobs["children"])])
        if number in (2, 3):
            wanted.append(part["lock_before_arm"])
        wanted.append(part["arm"])
        if number == 3:
            wanted.extend((part["blocked_first_helper"], part["blocked_root_result"]))
        ids = [part["jobs"]["root"]["id"], *(child["id"] for child in part["jobs"]["children"])]
        wanted.extend(part["terminals"][ident] for ident in ids)
        if number == 2:
            wanted.append(part["lock_after_root"])
        wanted.extend((part["after"], part["events"]))
    wanted.append(read(lane / "business/after-application-exit.json"))
    observed = 0
    for path in sorted(fixture.glob("command-*.json")):
        receipt = read(path)
        stdout = Path(receipt["stdout"])
        if (not common.settled(receipt) or not stdout.is_file() or not stdout.is_relative_to(fixture)
                or not 0 < stdout.stat().st_size <= 131072):
            continue
        try:
            raw = read(stdout)
            value = json.loads(raw) if isinstance(raw, str) else raw
        except ValueError:
            continue
        if value == wanted[observed]:
            require(digest(stdout) == receipt["stdout_sha256"], "exact scanner SQL/DB2/Channels stdout custody")
            observed += 1
            if observed == len(wanted):
                break
    require(observed == len(wanted), "ordered actual scanner SQL, DB2 and Channels command receipts required")
    return {"scope": NODE, "candidate": candidate, "scanner": body["native"],
            "private_journal_sha256": report["journal_sha256"], "exact_owned_absence": True}


def main():
    if sys.argv[1:2] == ["prepare"]:
        cold = argparse.ArgumentParser(description="Prepare the first-scanner stage without an engine.")
        for name in ("repository", "package_template", "candidate", "stage"):
            cold.add_argument(name, type=Path)
        args = cold.parse_args(sys.argv[2:])
        print(json.dumps(prepare(args.repository, args.package_template, read(args.candidate), args.stage), sort_keys=True))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", type=Path)
    parser.add_argument("stage", type=Path)
    parser.add_argument("seal")
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("mode", choices=("run", "cleanup", "evaluate"))
    parser.add_argument("entrypoint", nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("witness", nargs="?")
    args = parser.parse_args()
    repository, stage, seal, candidate = args.repository, args.stage, args.seal, args.candidate
    stage_binding(repository, stage, seal, candidate)
    if args.mode == "run":
        binding = dispatch_spec(repository, stage, seal, candidate, args.entrypoint, args.output)
        coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
        require(binding["argv"] == sys.orig_argv and Path.cwd() == repository, "actual exact scanner coordinator argv required")
        lock = common.coordinator_parent(coordinator, args.witness, args.output, binding, candidate, node=NODE)
        require(not any(os.path.lexists(stage / name) for name in ("owner-1", "actual-1"))
                and all(os.environ.get(key) == value for key, value in binding["environment"].items())
                and not any(key.startswith(("DOCKER_CONTEXT", "RUST_", "USE_NGINX_ACCEL")) for key in os.environ)
                and Path(shutil.which("docker") or "").resolve() == repository / "artifacts/rust/tooling/docker/docker",
                "fresh owned scanner fixture and explicit isolated engine required")
        os.umask(0o077)
    sys.path.insert(0, str(stage))
    driver = common.load_driver(stage)
    driver.load_binding()
    if args.mode == "run":
        require(driver.SHARED / ".dispatcher.lock" == lock, "scanner fixture requires same coordinator lock")
        status = common.run_campaign(stage, driver, lock)
        if status:
            return status
    if args.mode == "cleanup":
        result = common.cleanup_receipt(stage, driver)
    else:
        result = evaluate(stage, candidate, driver)
        if args.mode == "evaluate":
            directory = common.attempt_directory(args.output, NODE)
            require(Path(args.witness) == directory / "execution.json", "exact scanner coordinator execution required")
            row = read(directory / "execution.json")
            require(row["id"] == NODE and common.settled(row["execution"]) and common.settled(row["cleanup"])
                    and row["cleanup_verified"] is True and read(directory / "execution.stdout") == result,
                    "actual scanner coordinator attempt did not settle")
            result.update(entrypoint=args.entrypoint, semantic_assertions_passed=True, full_required_workflow=True)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
