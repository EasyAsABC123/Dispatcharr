"""Coordinator-owned ordinary Schedules Direct replay of nine retained cases."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

SCRIPT = Path(__file__).resolve()
sys.path.insert(0, str(SCRIPT.parent))
try:
    import programme_index_owner as donor
    import sd_guide_evidence as evidence
finally:
    sys.path.pop(0)

common = donor.common
require, read, digest, edit = donor.require, donor.read, donor.digest, donor.edit
NODE = "m6/schedules-direct-guide"
COHORT = ("artifacts/rust/paseo-async-f960875-1/release-matrix/"
          "m4-27531aee-scheduled-stop-drain-successor-1/cohort")
COHORT_PINS = {
    "fixture.py": "3da82f041e0e91968a3881df1a7576b827600942bd4bd77112163b1e57dedd4f",
    "sd_guide_cases.py": "4db397de99f7ea80ff610108ccaebd02ad629a1a6acf00c0abb8325bf4757c3c",
    "sd_guide_origin.py": "0688803ce720da56d84c126faab04ccc822ef77d8c09416895f58231a74b171c",
    "broker_monitor.py": "9634de9f4ce7dfaffa269c5f940e8d37bcce111e9e9a3e5e0d1dd7c32612e4e0",
}
MEMBERS = {"run.py", "scenario.py", "missing_scenario.py", "origin61.py", "fixture_adapter.py",
           "external-pins.json", "candidate.py", "package-binding.json", "sd_guide_evidence.py"}


def authority(repository):
    pins = {repository / COHORT / name: stamp for name, stamp in COHORT_PINS.items()}
    pins[repository / evidence.SOURCE] = evidence.SOURCE_SHA
    require(all(digest(path) == stamp for path, stamp in pins.items()), "exact SD fixture and Original inputs required")
    return {str(path): stamp for path, stamp in pins.items()}


def stage_files(repository, template, candidate):
    authority(repository)
    files = donor.stage_files(repository, template, candidate, "missing")
    del files["index_http_guest.py"]
    adapter = files["fixture_adapter.py"]
    files["fixture_adapter.py"] = edit(adapter, "        import missing_scenario\n        return missing_scenario",
                                        "        return None")
    files["missing_scenario.py"] = "import sd_guide_evidence as evidence\n"
    files["sd_guide_evidence.py"] = Path(evidence.__file__).read_text()
    run = edit(files["run.py"], 'SIDES = ("Original", "Native")', 'SIDES = ("Native",)')
    run = edit(run, 'need(arms and arms[0] == "Original", "at least the owned Original arm admitted")',
               'need(arms == ["Native"], "one exact owned Native SD arm required")')
    run = edit(run, "fresh ordinary M6 two missing XMLTV programme-index states", "fresh ordinary M6 Schedules Direct")
    run = edit(run,
               '        need(len(close["arms"]) == 2 and read(OUT / "admission.json")["native"] == IMAGE, '
               '"fresh matched Original and exact Native arms")',
               '        need([arm["side"] for arm in close["arms"]] == ["Native"] '
               'and read(OUT / "admission.json")["native"] == IMAGE, "fresh exact Native SD arm")')
    run = edit(run, '        need(outcomes["Original"]["sql_states"] == outcomes["Native"]["sql_states"],\n'
                    '             "ordinary two unchanged index-row states match")\n', "")
    run = edit(run, "ordinary-scanner-two-missing-XMLTV-index-states", "ordinary-schedules-direct-nine-cases")
    start = run.index('        with fixture.independent_context("ordinary-missing-programme-index"):')
    end = run.index("    except BaseException as error:", start)
    original = repository / evidence.SOURCE
    run = run[:start] + f'''        with fixture.independent_context("schedules-direct"):
            import sd_guide_cases
            from missing_scenario import evidence
            sd_guide_cases.run(fixture)
            source = Path({str(original)!r})
            identity = evidence.cases(evidence.read(fixture.output / "schedules-direct/report.json"), evidence.read(source))
            delivery = evidence.retry(fixture.output, identity)
            evidence.capture_retirement(fixture, folder, identity, delivery)
            shutdown = fixture.capture_application_shutdown()
            put(folder / "application-shutdown.json", shutdown)
            verify_shutdown(side, shutdown)
            evidence.validate(fixture.output, folder, source)
        code = 0
''' + run[end:]
    start = run.index("def verify_business(side):")
    end = run.index("def run(base, package,", start)
    files["run.py"] = run[:start] + f'''def verify_business(side):
    from missing_scenario import evidence
    folder = OUT / side
    fixture = Path(read(folder / "fixture.json")["output"])
    verify_shutdown(side, read(folder / "application-shutdown.json"))
    need(read(fixture / "ordinary-aio-ready.json")
         == {{"ordinary_aio": True, "core_saved_scheduler_observed": True, "both_lanes_accepting": True}},
         "actual ordinary Main worker readiness")
    need(read(OUT / (side + "-execution.json"))["actual_exit"] == 0, "literal SD lane exit")
    return evidence.validate(fixture, folder, Path({str(original)!r}))


''' + run[end:]
    require(files.keys() == MEMBERS, "exact SD stage members")
    return files


def prepare(repository, template, candidate, stage):
    files = stage_files(repository, template, candidate)
    require(stage.resolve() == stage and not os.path.lexists(stage), "fresh absolute SD stage required")
    stage.mkdir(mode=0o700)
    for name, body in files.items():
        if name.endswith(".py"):
            compile(body, name, "exec")
        (stage / name).write_text(body)
    sums = stage / "SHA256SUMS"
    sums.write_text("".join(f"{digest(stage / name)}  {name}\n" for name in sorted(files)))
    cold = subprocess.run(["/usr/bin/python3", "-B", str(stage / "run.py"), "preflight"],
                          capture_output=True, check=True, timeout=60)
    proof = json.loads(cold.stdout)
    require(proof["engine_calls"] == 0 and proof["candidate"] == candidate["git"]
            and proof["image_id"] == candidate["image_id"], "cold exact common SD package required")
    return {"stage": str(stage), "seal": digest(sums), "cold_preflight": proof}


def dispatch_spec(repository, stage, seal, candidate, entrypoint, output):
    files, names = common.sealed_inputs(stage, seal)
    require(names == MEMBERS and digest(stage / "sd_guide_evidence.py") == digest(Path(evidence.__file__))
            and stage.resolve() == stage and output.resolve() == output
            and not stage.is_relative_to(output) and not output.is_relative_to(stage), "exact SD stage and output required")
    package = read(stage / "package-binding.json")
    manifest = Path(candidate["build_manifest_file"])
    build = read(manifest)
    require((package["candidate"], package["image"], package["manifest"], package["archive"], package["Original"])
            == (candidate["git"], candidate["image_id"], digest(manifest), candidate["source_archive_sha256"],
                common.ORIGINAL)
            and package["manifest"] == candidate["build_manifest_sha256"]
            and build["platform"] == "linux/amd64", "exact common final AMD64 SD package required")
    for path, stamp in package["pins"].items():
        require(digest(Path(path)) == stamp, "frozen SD package input differs")
        files[path] = stamp
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    tooling = repository / "artifacts/rust/tooling"
    prefix = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(repository), str(stage), seal,
              json.dumps(candidate, sort_keys=True), entrypoint, str(output)]
    return {"argv": [*prefix, "run", digest(coordinator)], "cleanup_argv": [*prefix, "cleanup"],
            "evaluator_argv": [*prefix, "evaluate", "@EXECUTION@"], "cwd": str(repository),
            "environment": {"DOCKER_HOST": common.DAEMON, "DOCKER_CONFIG": str(tooling / "docker-config"),
                            "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes": common.STRIP_ENV,
            "input_sha256": files | authority(repository) | {str(path): digest(path) for path in
                (SCRIPT, donor.SCRIPT, coordinator, Path(evidence.__file__), manifest)},
            "seconds": 660, "cleanup_seconds": 60, "output_bytes": 65536, "exclusive_heavy": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    if sys.argv[1:2] == ["prepare"]:
        for name in ("repository", "template", "candidate", "stage"):
            parser.add_argument(name, type=Path)
        args = parser.parse_args(sys.argv[2:])
        print(json.dumps(prepare(args.repository, args.template, read(args.candidate), args.stage)))
        return 0
    parser.add_argument("repository", type=Path)
    parser.add_argument("stage", type=Path)
    parser.add_argument("seal")
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("entrypoint")
    parser.add_argument("output", type=Path)
    parser.add_argument("mode", choices=("run", "cleanup", "evaluate"))
    parser.add_argument("witness", nargs="?")
    a = parser.parse_args()
    binding = dispatch_spec(a.repository, a.stage, a.seal, a.candidate, a.entrypoint, a.output)
    with donor.load_driver(a.stage) as driver:
        if a.mode == "run":
            require(sys.orig_argv == binding["argv"] and Path.cwd() == a.repository
                    and all(os.environ.get(key) == value for key, value in binding["environment"].items())
                    and not any(key.startswith(("DOCKER_CONTEXT", "RUST_", "USE_NGINX_ACCEL")) for key in os.environ)
                    and not any(os.path.lexists(a.stage / name) for name in ("owner-1", "actual-1")),
                    "exact fresh coordinator SD invocation required")
            lock = common.coordinator_parent(a.repository / "scripts/rust-milestone-batches/coordinator.py",
                                             a.witness, a.output, binding, a.candidate, node=NODE)
            require(driver.SHARED / ".dispatcher.lock" == lock, "same dispatcher lease required")
            os.umask(0o077)
            status = common.run_campaign(a.stage, driver, lock)
            if status:
                return status
        cleanup = donor.retirement(a.stage, driver)
        proof = {"exact_owned_absence": True, "physical_cleanup": cleanup}
        if a.mode != "cleanup":
            require(all((a.stage / "owner-1" / (phase + ".actual-exit")).read_bytes() == b"0\n"
                        for phase in common.PHASES), "literal owner phase exits required")
            root = a.stage / "actual-1"
            require(read(root / "admission.json")["native"] == a.candidate["image_id"]
                    and common.settled(read(root / "Native-execution.json")), "exact actual Native lane required")
            proof.update(scope=NODE, candidate=a.candidate, native=native)
        if a.mode == "evaluate":
            directory = common.attempt_directory(a.output, NODE)
            require(Path(a.witness) == directory / "execution.json", "exact SD coordinator witness required")
            row = read(directory / "execution.json")
            require(row["id"] == NODE and common.settled(row["execution"]) and common.settled(row["cleanup"])
                    and row["cleanup_verified"] is True and proof == read(directory / "execution.stdout"),
                    "actual scoped SD attempt did not settle")
            proof.update(entrypoint=a.entrypoint, semantic_assertions_passed=True, full_required_workflow=False)
    print(json.dumps(proof, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
