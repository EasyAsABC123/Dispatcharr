"""Bind the ordinary paired M6 Programme Index release campaign."""
import argparse
from contextlib import contextmanager
from hashlib import sha256
import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile

SCRIPT = Path(__file__).resolve()
sys.path.insert(0, str(SCRIPT.parent))
try:
    import recording61_owner as common
    import scanner_first_owner as scanner
    import programme_index_held_ready as held_ready
    from prepare_recording61 import rebind_package
finally:
    sys.path.pop(0)

NODE = "m6/programme-index"
PAIR = "paseo-async-f960875-1/release-matrix"
POSITIVE = "m6-009cc8dd-interleaved-programme-index-pair-retry-2"
MISSING = "m6-48181ab0-missing-xmltv-index-pair-1"
SPECS = {
    "interleaved": (POSITIVE, "320d6c4c317fd77af576a632c50af26b794a09233248361bfbe284c4162f0a70",
                    "index_scenario.py"),
    "missing": (MISSING, scanner.TEMPLATE_SHA, "missing_scenario.py"),
    "typed-failure": (MISSING, scanner.TEMPLATE_SHA, "missing_scenario.py"),
}
PACKAGE_TEMPLATE_SEAL = "3a8e6bde76d862406ca7f8ccd30e86cdeb987f48d077248a888e9b7350e49919"
PRODUCTION_BASE = "28cd138381c80e88ed0f37c062ae203cc2be8fec"
PRODUCTION_LEAVES = {
    "rust/crates/ingest-core/src/lib.rs": "447d33f4815add1a8f0b9425004b467ad44c93527ac1b1159187050836caadb2",
    "rust/crates/ingest-core/src/xmltv.rs": "3eab2e16626ccf65070d98897f9394c7f6f28124985b624c96a8fa3ff4d751e7",
    "rust/crates/ingest-epg/src/file_job.rs": "7968dcb1454b18ac31a18890ae6991b5f42785d81749430ee37d364aa0ac6a06",
    "rust/crates/ingest-epg/src/index_service.rs": "6937fef3311ae500ad082738ddc864e20eeb4a613291707e2cc14df0df3831f7",
    "rust/crates/job-transport/src/index_classifier.rs": "5e54508247fd564f2f83b6b0912a2e18bd168cf31c0c8706b7cabcdd22dd8842",
    "rust/crates/job-transport/src/refresh_worker.rs": "a3406ded7ac67757fd516693d39a4e2a2ebd70bb8182ff071c09ec48ccb804c8",
}
TYPED = (
    {"exc_type": "IsADirectoryError", "exc_module": "builtins",
     "exc_message": [21, "Is a directory"]},
    {"exc_type": "ProgrammingError", "exc_module": "django.db.utils",
     "exc_message": ["owned-index-sql-write-failure\nCONTEXT:  PL/pgSQL function "
                     "owned_index_failure() line 1 at RAISE"]},
)
require, read, digest = common.require, common.read, common.digest


def production_binding(repository, candidate):
    revision = candidate["git"]
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "full common candidate revision required")
    git = ["/usr/bin/git", "-C", str(repository)]
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    ancestry = subprocess.run([*git, "merge-base", "--is-ancestor", PRODUCTION_BASE, revision],
                              env=env, capture_output=True, check=False, timeout=20)
    require(ancestry.returncode == 0, "common candidate must include the reviewed production revision")
    manifest = Path(candidate["build_manifest_file"])
    build = read(manifest)
    require(digest(manifest) == candidate["build_manifest_sha256"]
            and (build["git_revision"], build["platform"], build["image_id"], build["source_archive_sha256"])
            == (revision, "linux/amd64", candidate["image_id"], candidate["source_archive_sha256"]),
            "exact common final AMD64 build required")
    archive = manifest.with_name("source.tar")
    require(digest(archive) == candidate["source_archive_sha256"], "exact common final source archive required")
    for name, expected in PRODUCTION_LEAVES.items():
        blob = subprocess.run([*git, "show", f"{revision}:{name}"], env=env,
                              capture_output=True, check=False, timeout=20)
        require(blob.returncode == 0 and sha256(blob.stdout).hexdigest() == expected
                and build["source_sha256"].get(name) == expected,
                f"reviewed production leaf drifted: {name}")
    verify_archive_leaves(archive)
    return {str(manifest): candidate["build_manifest_sha256"],
            str(archive): candidate["source_archive_sha256"]}


def verify_archive_leaves(archive):
    seen = set()
    with tarfile.open(archive, "r|") as stream:
        for member in stream:
            expected = PRODUCTION_LEAVES.get(member.name)
            if expected is None:
                continue
            require(member.name not in seen and member.isfile() and 0 < member.size <= 1024 * 1024,
                    f"unique regular production leaf required: {member.name}")
            with stream.extractfile(member) as blob:
                require(sha256(blob.read()).hexdigest() == expected,
                        f"archived production leaf drifted: {member.name}")
            seen.add(member.name)
    require(seen == PRODUCTION_LEAVES.keys(), "complete archived production leaf set required")


def edit(text, old, new):
    require(text.count(old) == 1, f"reviewed fixture boundary changed: {old!r}")
    return text.replace(old, new, 1)


def instrument(text, positive):
    text = "from time import monotonic_ns, time_ns\n" + text
    start = ("        put(out / \"index-terminal.json\", meta)\n" if positive else
             "            verify_terminal(meta, child[\"task_id\"], root)\n")
    indent = "        " if positive else "            "
    text = edit(text, start, f"{indent}observed = [time_ns(), monotonic_ns()]\n" + start)
    needle = ("        terminal = read_projection(fixture, source_id)\n" if positive else
              "            current = snapshot(fixture, targets)\n")
    name = '"terminal-timing.json"' if positive else 'f"terminal-{i}-timing.json"'
    snapshot = "terminal" if positive else "current"
    return edit(text, needle,
                f"{indent}sql_start = monotonic_ns()\n" + needle
                + f"{indent}sql_end = monotonic_ns()\n"
                + f"{indent}put(out / {name}, {{'task_id': meta['task_id'], "
                + f"'date_done': meta['date_done'], 'observed': observed, "
                + f"'sql_start': sql_start, 'sql_end': sql_end, "
                + f"'read_only': {snapshot}['read_only']}})\n")


def typed_scenario(text):
    text = edit(text,
        "    targets.append({{'case':name,'source_id':source.pk,'parser_id':epg.pk,'file':str(p)}})",
        """    if number==0:
        p.mkdir(mode=0o755)
    else:
        p.write_bytes(b'<tv></tv>')
        p.chmod(0o644)
        from django.db import connection
        assert type(source.pk) is int and source.pk>0
        with connection.cursor() as cursor:
            assert connection.settings_dict['NAME'].startswith('normal_dvr_')
            cursor.execute("CREATE FUNCTION owned_index_failure() RETURNS trigger LANGUAGE plpgsql "
                           "AS $$ BEGIN RAISE EXCEPTION 'owned-index-sql-write-failure'; END $$")
            cursor.execute("CREATE TRIGGER owned_index_failure BEFORE INSERT OR UPDATE ON epg_epgsourceindex "
                           "FOR EACH ROW WHEN (NEW.source_id = " + str(source.pk)
                           + ") EXECUTE FUNCTION owned_index_failure()")
    targets.append({{'case':name,'source_id':source.pk,'parser_id':epg.pk,'file':str(p)}})""")
    text = edit(text, """        assert not path.exists() and not path.is_symlink()
""", """        assert not path.is_symlink()
        assert path.is_dir() if target['case']==targets[0]['case'] else path.is_file()
""")
    text = edit(text, "'file_absent':True", "'file_absent':False")
    text = edit(text, 'observed["file_absent"] is True', 'observed["file_absent"] is False')
    text += f"""

TYPED = {TYPED!r}
_original_verify_terminal = verify_terminal

def verify_terminal(meta, task, parent):
    if parent is None:
        return _original_verify_terminal(meta, task, parent)
    need(meta['task_id'] == task and meta.get('parent_id') == parent
         and meta['status'] == 'FAILURE'
         and json.dumps(meta['result'], sort_keys=True)
             in [json.dumps(payload, sort_keys=True) for payload in TYPED]
         and type(meta['traceback']) is str and bool(meta['traceback']),
         'actual typed failure on the ordinary Index worker')
"""
    return text


def stage_files(repository, template, candidate, scope):
    art = repository / "artifacts/rust"
    leaf, seal, scenario = SPECS[scope]
    donor = art / PAIR / leaf
    _, members = common.sealed_inputs(donor, seal)
    names = {"run.py", "scenario.py", "fixture_adapter.py", "origin61.py",
             "external-pins.json", "index_http_guest.py", scenario}
    require(names <= members, "missing reviewed programme-index fixture input")
    old = read(template / "package-binding.json")
    anchor = next(path for path in old["pins"] if path.endswith("/package/build.json"))
    prior = {"git": old["candidate"], "image_id": old["image"], "build_manifest_file": anchor,
             "build_manifest_sha256": old["manifest"], "source_archive_sha256": old["archive"]}
    common.stage_inputs(template, PACKAGE_TEMPLATE_SEAL, prior)
    manifest, package = rebind_package(old, candidate)

    files = {name: (donor / name).read_text() for name in names}
    checker = (template / "candidate.py").read_text()
    for key in ("candidate", "image", "manifest"):
        checker = edit(checker, old[key], package[key])
    files["candidate.py"] = edit(checker, f"=={old['source_files']}==", f"=={package['source_files']}==")
    files["package-binding.json"] = json.dumps(package, sort_keys=True)
    files["fixture_adapter.py"] = edit(files["fixture_adapter.py"], "ART = HERE.parents[2]",
                                        f"ART = Path({str(art)!r})")

    text = edit(files["run.py"], 'BINDING = HERE / "bound-candidate.json"',
                'BINDING = HERE / "package-binding.json"')
    lines = text.splitlines(keepends=True)
    stale = [line for line in lines if "need(len(candidate.application_map())" in line]
    require(len(stale) == 1, "reviewed old package map predicate")
    text = edit(text, stale[0], "")
    if scope != "interleaved":
        previous = art / PAIR / POSITIVE / "actual-1/Original/Original-source"
        text = edit(text, 'candidate.PREVIOUS / "actual-1/Original/Original-source"', f"Path({str(previous)!r})")
    start, end = text.index("def load_binding():"), text.index("def authority():")
    text = text[:start] + f'''def load_binding():
    global PACKAGE, REVISION, IMAGE, MANIFEST
    PACKAGE = Path({str(manifest)!r})
    MANIFEST = read(BINDING)["manifest"]
    package = candidate.verify_package(PACKAGE, MANIFEST)
    REVISION, IMAGE = package["git_revision"], package["image_id"]
    return package


''' + text[end:]
    for before, after in (
        ("def run(base, package):", "def run(base, package, coordinator_lease):"),
        ('    lease = load(LEASE, "ordinary_recording61_global_lease")\n', ""),
        ('with lease.fixture_lease(SHARED / ".dispatcher.lock"):',
         'with coordinator_lease(SHARED / ".dispatcher.lock"):'),
        ("def main():", "def main(argv=None, coordinator_lease=None):"),
        ("args = parser.parse_args()", "args = parser.parse_args(argv)"),
        ("return run(binding, package)",
         'need(coordinator_lease is not None, "exact Programme Index coordinator lease required")\n'
         '            return run(binding, package, coordinator_lease)'),
    ):
        text = edit(text, before, after)
    start, end = text.index('    if args.mode == "offline":'), text.index('    if args.mode == "lane":')
    text = text[:start] + text[end:]
    start, end = text.index('        proof = {"engine_calls": 0'), text.index('    print(json.dumps(proof', text.index('        proof = {"engine_calls": 0'))
    files["run.py"] = text[:start] + '''        proof = {"engine_calls": 0, "candidate": REVISION, "image_id": IMAGE,
                 "mounted_tools": donor.verify_tool_modes()}
''' + text[end:]
    files[scenario] = instrument(files[scenario], scope == "interleaved")
    if scope == "typed-failure":
        files[scenario] = typed_scenario(files[scenario])
        files[scenario] = "import programme_index_held_ready as held_ready\n" + edit(
            files[scenario],
            '            meta = fixture.wait(lambda: receive(fixture, child["task_id"]), 35, "first real Index result")\n',
            '            if i == 2:\n'
            '                held_ready.observe(fixture, out, child, targets[1], before, lambda f: snapshot(f, targets))\n'
            '            meta = fixture.wait(lambda: receive(fixture, child["task_id"]), 35, "first real Index result")\n')
        files[scenario] = edit(files[scenario], "            results.append(meta)\n",
                               "            if i == 2:\n"
                               "                held_ready.observe_after(fixture, out, child)\n"
                               "            results.append(meta)\n")
        files[scenario] = edit(files[scenario],
            "    need(len(reads) == 4 and set(result_ordinals) == set(expected_results),",
            '    held = (owned / "index-pg-gate-exit.json").is_file()\n'
            "    need(len(reads) == (5 if held else 4) and set(result_ordinals) == set(expected_results),")
        files[scenario] = edit(files[scenario],
            "    need(reads[0] < first < reads[1] < second < reads[2] < parent < reads[3],",
            "    if held:\n"
            "        need(reads[1] < reads[2] < second, 'held read-only SQL before Index terminal')\n"
            "        del reads[2]\n"
            "    need(reads[0] < first < reads[1] < second < reads[2] < parent < reads[3],")
        files["programme_index_held_ready.py"] = Path(held_ready.__file__).read_text()
        files["index_pg_gate.py"] = held_ready.guest_source(repository)
        adapter = files["fixture_adapter.py"]
        adapter = edit(adapter, "class Fixture(donor.Fixture):\n",
                       "import programme_index_held_ready as held_ready\n\n"
                       "class Fixture(donor.Fixture):\n"
                       "    def start_native(self):\n"
                       "        held_ready.start(self)\n"
                       "        super().start_native()\n\n")
        files["fixture_adapter.py"] = edit(adapter, '        return {"native_application": result}',
                                          "        held_ready.stop(self)\n"
                                          '        return {"native_application": result}')
    return files


def prepare(repository, template, candidate, stage):
    require(stage.resolve() == stage and not os.path.lexists(stage), "fresh absolute owner stage required")
    production_binding(repository, candidate)
    inputs = {scope: stage_files(repository, template, candidate, scope) for scope in SPECS}
    for files in inputs.values():
        for name, content in files.items():
            if name.endswith(".py"):
                compile(content, name, "exec")
    stage.mkdir(mode=0o700)
    names = []
    for scope, files in inputs.items():
        (stage / scope).mkdir(mode=0o700)
        for name, content in files.items():
            relative = f"{scope}/{name}"
            (stage / relative).write_text(content + ("" if content.endswith("\n") else "\n"))
            names.append(relative)
        cold = subprocess.run(
            ["/usr/bin/python3", "-B", str(stage / scope / "run.py"), "preflight"],
            check=True, capture_output=True, timeout=60,
        )
        proof = json.loads(cold.stdout)
        require(proof["engine_calls"] == 0 and proof["candidate"] == candidate["git"]
                and proof["image_id"] == candidate["image_id"], "exact cold package admission")
    sums = stage / "SHA256SUMS"
    sums.write_text("".join(f"{digest(stage / name)}  {name}\n" for name in sorted(names)))
    return {"stage": str(stage), "seal": digest(sums)}


def dispatch_spec(repository, stage, seal, candidate, entrypoint, output):
    require(output.resolve() == output and not stage.is_relative_to(output)
            and not output.is_relative_to(stage), "separate canonical coordinator output required")
    inputs, names = common.sealed_inputs(stage, seal)
    inputs.update(production_binding(repository, candidate))
    manifest = Path(candidate["build_manifest_file"])
    for scope, (_, _, scenario) in SPECS.items():
        required = {"run.py", "scenario.py", scenario, "fixture_adapter.py", "origin61.py",
                    "external-pins.json", "index_http_guest.py", "candidate.py", "package-binding.json"}
        if scope == "typed-failure":
            required |= {"programme_index_held_ready.py", "index_pg_gate.py"}
            require(digest(stage / scope / "programme_index_held_ready.py") == digest(Path(held_ready.__file__))
                    and (stage / scope / "index_pg_gate.py").read_text() == held_ready.guest_source(repository),
                    "exact scoped PG reply gate required")
        require({f"{scope}/{name}" for name in required} <= names, "sealed campaign files required")
        package = read(stage / scope / "package-binding.json")
        require((package["candidate"], package["image"], package["manifest"], package["archive"])
                == (candidate["git"], candidate["image_id"], digest(manifest), candidate["source_archive_sha256"])
                and package["manifest"] == candidate["build_manifest_sha256"]
                and package["Original"] == common.ORIGINAL, "campaign package or Original binding differs")
        for path, stamp in package["pins"].items():
            require(digest(Path(path)) == stamp, "frozen package input differs")
            inputs[path] = stamp
    coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
    tooling = repository / "artifacts/rust/tooling"
    prefix = ["/usr/bin/python3", "-I", "-B", str(SCRIPT), str(repository), str(stage),
              seal, json.dumps(candidate, sort_keys=True), entrypoint, str(output)]
    return {
        "argv": [*prefix, "run", digest(coordinator)],
        "cleanup_argv": [*prefix, "cleanup"],
        "evaluator_argv": [*prefix, "evaluate", "@EXECUTION@"],
        "cwd": str(repository),
        "environment": {"DOCKER_HOST": common.DAEMON,
                        "DOCKER_CONFIG": str(tooling / "docker-config"),
                        "PATH": f"{tooling / 'docker'}:/usr/bin:/bin"},
        "strip_inherited_prefixes": common.STRIP_ENV,
        "input_sha256": inputs | {str(path): digest(path) for path in (SCRIPT, common.SCRIPT, coordinator, Path(held_ready.__file__), repository / held_ready.PROXY)},
        "seconds": 3300,
        "cleanup_seconds": 60,
        "output_bytes": 65536,
        "exclusive_heavy": True,
    }


@contextmanager
def load_driver(folder):
    local = ("candidate", "scenario", "fixture_adapter", "index_scenario", "missing_scenario", "programme_index_held_ready")
    old = {name: sys.modules.pop(name) for name in local if name in sys.modules}
    paths = sys.path[:]
    sys.path.insert(0, str(folder))
    try:
        driver = common.load_driver(folder)
        driver.load_binding()
        yield driver
    finally:
        for name in local:
            sys.modules.pop(name, None)
        sys.modules.update(old)
        sys.path[:] = paths


def retirement(folder, driver):
    close = driver.cleanup()
    require(close == read(folder / "actual-1/independent-cleanup.json")
            and close["exact_cleanup"] is True and not os.path.lexists(driver.MARKER),
            "exact settled scoped cleanup required")
    return close


def evaluate(folder, driver, scope, candidate):
    for phase in common.PHASES:
        require((folder / "owner-1" / f"{phase}.actual-exit").read_bytes() == b"0\n",
                "literal campaign phase exit required")
    close = retirement(folder, driver)
    require([row["side"] for row in close["arms"]] == ["Original", "Native"],
            "both application arms required")
    root = folder / "actual-1"
    require(read(root / "admission.json")["native"] == candidate["image_id"], "wrong admitted image")
    actual = {}
    for side in ("Original", "Native"):
        require(common.settled(read(root / f"{side}-execution.json")), "application lane did not settle")
        actual[side] = driver.verify_business(side)
        owned = Path(read(root / side / "fixture.json")["output"])
        if side == "Native":
            require(read(owned / "ordinary-aio-ready.json") ==
                    {"ordinary_aio": True, "core_saved_scheduler_observed": True, "both_lanes_accepting": True},
                    "ordinary Native package readiness required")
        business = root / side / "business"
        cases = (("terminal-timing.json", read(business / "index-terminal.json")),) if scope == "interleaved" else tuple(
            (f"terminal-{i}-timing.json", read(business / f"terminal-{i}.json")["terminal"])
            for i in (1, 2)
        )
        timings = []
        for name, terminal in cases:
            timing = read(business / name)
            wall, monotonic = timing["observed"]
            require(timing["task_id"] == terminal["task_id"] and timing["date_done"] == terminal["date_done"]
                    and all(type(value) is int and value > 0 for value in
                            (wall, monotonic, timing["sql_start"], timing["sql_end"]))
                    and monotonic <= timing["sql_start"] <= timing["sql_end"]
                    and timing["read_only"] is True, "first observed DB2 terminal must precede the read-only SQL probe")
            timings.append({"timing_sha256": digest(business / name), "date_done": terminal["date_done"]})
        if scope == "typed-failure":
            typed = tuple(terminal["result"] for _, terminal in cases)
            require(all(terminal["status"] == "FAILURE" for _, terminal in cases)
                    and json.dumps(typed, sort_keys=True) == json.dumps(TYPED, sort_keys=True),
                    "typed errors must match their exact owned input in both arms")
            actual[side] = {"typed_results": typed,
                            "raw_business_sha256": actual[side]["private_business_receipt_sha256"]}
            if side == "Native":
                actual[side]["index_pg_reply"] = held_ready.verify(owned, business, cases[1][1], TYPED[1])
        actual[side]["first_terminal_observations"] = timings
    if scope == "interleaved":
        for field in ("index", "guide_sha256", "targeted_parser_rows", "targeted_programme_rows"):
            require(actual["Original"][field] == actual["Native"][field], "paired interleaving differs")
    return {"actual": actual, "physical_cleanup": close}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    if sys.argv[1:2] == ["prepare"]:
        for name in ("repository", "package_template", "candidate_file", "stage"):
            parser.add_argument(name, type=Path)
        args = parser.parse_args(sys.argv[2:])
        print(json.dumps(prepare(args.repository, args.package_template, read(args.candidate_file), args.stage)))
        return 0
    parser.add_argument("repository", type=Path)
    parser.add_argument("stage", type=Path)
    parser.add_argument("seal")
    parser.add_argument("candidate", type=json.loads)
    parser.add_argument("entrypoint")
    parser.add_argument("output", type=Path)
    parser.add_argument("mode", choices=("run", "cleanup", "evaluate"))
    parser.add_argument("witness", nargs="?")
    args = parser.parse_args()
    repository, stage, candidate = args.repository, args.stage, args.candidate
    binding = dispatch_spec(repository, stage, args.seal, candidate, args.entrypoint, args.output)
    lock = None
    if args.mode == "run":
        require(sys.orig_argv == binding["argv"] and Path.cwd() == repository, "exact coordinator invocation required")
        coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
        lock = common.coordinator_parent(coordinator, args.witness, args.output, binding, candidate, node=NODE)
        require(all(os.environ.get(key) == value for key, value in binding["environment"].items())
                and not any(key.startswith(("DOCKER_CONTEXT", "RUST_", "USE_NGINX_ACCEL")) for key in os.environ)
                and Path(shutil.which("docker") or "").resolve() == repository / "artifacts/rust/tooling/docker/docker",
                "exact isolated fixture environment required")
        require(not any(os.path.lexists(stage / scope / name) for scope in SPECS
                        for name in ("owner-1", "actual-1")), "fresh scope output required")
        os.umask(0o077)
    result = {}
    for scope in SPECS:
        folder = stage / scope
        with load_driver(folder) as driver:
            if args.mode == "run":
                require(driver.SHARED / ".dispatcher.lock" == lock, "same coordinator lease required")
                status = common.run_campaign(folder, driver, lock)
                if status:
                    return status
            result[scope] = (retirement(folder, driver) if args.mode == "cleanup"
                             else evaluate(folder, driver, scope, candidate))
    if args.mode == "cleanup":
        print(json.dumps({"exact_owned_absence": True, "scopes": result}, sort_keys=True))
        return 0
    proof = {"scope": NODE, "candidate": candidate, "scopes": result, "exact_owned_absence": True}
    if args.mode == "evaluate":
        directory = common.attempt_directory(args.output, NODE)
        require(Path(args.witness) == directory / "execution.json", "exact coordinator execution receipt required")
        row = read(directory / "execution.json")
        require(row["id"] == NODE and common.settled(row["execution"]) and common.settled(row["cleanup"])
                and row["cleanup_verified"] is True
                and json.dumps(read(directory / "execution.stdout"), sort_keys=True) == json.dumps(proof, sort_keys=True),
                "actual grouped coordinator attempt did not settle")
        proof.update(entrypoint=args.entrypoint, semantic_assertions_passed=True,
                     full_required_workflow=False)
    print(json.dumps(proof, sort_keys=True))
    return 0


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
