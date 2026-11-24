"""Verify the two retained Original TVG and Version200 acceptance epochs."""
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import sys

NODE = "m6/Source-missing-domains"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ORIGINAL_ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
BASE = Path("scripts/rust-job-contracts/m6-missing-source")
MANIFEST_SHA256 = "c56be2a0bb088efd4a81e39a971af638bbba21ff4b15641d5dbef5a47e853307"
PREFIX_REVIEW = "f827df0c41d5e7faa354f788c7c83ea384a828dc4863d16b9fa83432e8885cc7"
TOOLS = Path("scripts/rust-milestone-batches/m6")
RUNTIME_PINS = {
    "source_accept.py": "2089ee694d60b61f6a092285bbd6324adf2b46c06bbee8135990abdbb015843d",
    "source_settlement.py": "15279c7ad139d319d6d44aa4e86bcbef9b1a230e55041b30805d42335f370c94",
    "Source-input-template.json": "a34c41f87f3dce27907804b837aaffe8e68aeeacf7333ddf475836d4a2594884",
}
COMMAND_FAILURES = {1: (16,), 2: ()}
FILE_COUNTS = {1: 113, 2: 108}
CASE_PATH = "m6-missing-domains/Source-extension-complete-or-negative.json"
GUEST_WRAPPER = "m6-missing-domains/Source-extension.json"
CLEANUP = [
    "discover_partial_owned_baseline", "default_after_full_audit", "owned_backend_absence_Unix",
    "normal_management_PGRedis_stop", "owned_compose_down", "fresh_exact_project_and_CID_absence",
    "physical_CLI_container_settlement", "final_frozen_input_recheck",
]
EMPTY = {"container": [], "network": [], "volume": []}


def require(value, message):
    if not value:
        raise ValueError(message)


def exact_file(repo, path, sha, count=None):
    require(path.is_absolute() and path.is_relative_to(repo) and path.resolve() == path and path.is_file(),
            "exact bound Original evidence file required")
    size = path.stat().st_size
    require(size <= 5 * 1024 * 1024 and (count is None or count == size), "bounded Original evidence bytes")
    body = path.read_bytes()
    require(sha256(body).hexdigest() == sha, "retained Original evidence changed")
    return body


def load(path, name):
    spec = spec_from_file_location(name, path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def reducer(repo):
    tool_dir = repo / TOOLS
    for name, digest in RUNTIME_PINS.items():
        exact_file(repo, tool_dir / name, digest)
    # source_accept uses this ordinary helper under its existing import name.
    # Install only the exact reviewed module for that load and restore the
    # caller's module table afterward.
    name = "source_settlement"
    previous = sys.modules.get(name)
    sys.modules[name] = load(tool_dir / "source_settlement.py", "retained_m6_source_settlement")
    try:
        return load(tool_dir / "source_accept.py", "retained_m6_source_acceptor")
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def load_epoch(repo, epoch):
    number = epoch["id"]
    root = repo / BASE / f"epoch{number}"
    require(len(epoch["files"]) == FILE_COUNTS[number], "complete exact declared Original epoch packet")
    raw = {}
    for entry in epoch["files"]:
        name = Path(entry["path"])
        require(not name.is_absolute() and ".." not in name.parts and str(name) == entry["path"]
                and str(name) not in raw, "unique exact confined Original epoch path")
        raw[str(name)] = exact_file(repo, root / name, entry["sha256"], entry["bytes"])
    return raw


def check_host(epoch, raw):
    number = epoch["id"]
    host = json.loads(raw["execution-private.json"])
    accepted = number == 2
    require(host["passed"] is epoch["passed"] is accepted
            and type(host["controller_exit"]) is int and host["controller_exit"] == epoch["host_actual_exit"] == int(not accepted)
            and host["capture_only"] is True and host["actual_Native_requests"] == 0
            and host["Source32_replay"] is host["Source51_replay"] is False
            and epoch["Source_replay"] is epoch["Native_executed"] is False,
            "actual split Original exit and no Native or replay credit")
    commands = host["commands"]
    require(len(commands) == 27, "all actual Original CLI command receipts")
    failed = []
    for index, command in enumerate(commands, 1):
        stem = f"command-{index:03d}"
        require(command == json.loads(raw[f"{stem}.json"])
                and all(sha256(raw[f"{stem}.{pipe}"]).hexdigest() == command[f"{pipe}_sha256"]
                        for pipe in ("stdout", "stderr"))
                and type(command["actual_exit"]) is int and command["invalid"] is None
                and command["physically_settled"] is command["pipe_threads_settled"] is True,
                "literal Original CLI identity, process and pipe settlement")
        if command["actual_exit"]:
            require(command["actual_exit"] == 1, "retained Original literal failure exit")
            failed.append(index)
    require(tuple(failed) == COMMAND_FAILURES[number], "retained unexpected or rewritten Original exit")
    steps = host["cleanup_steps"]
    require(epoch["cleanup_all8"] is True and [row["name"] for row in steps] == CLEANUP
            and all(row["passed"] is True for row in steps), "all recorded Original cleanup phases")
    values = {row["name"]: row["value"] for row in steps}
    database = values["normal_management_PGRedis_stop"]
    require(values["default_after_full_audit"] == host["default_SQL_and_cache_isolation"]
            == epoch["default_SQL_cache_audits"] and all(v is True for v in epoch["default_SQL_cache_audits"].values())
            and values["owned_backend_absence_Unix"] is values["final_frozen_input_recheck"] is True
            and type(database["ExitCode"]) is int and database["ExitCode"] == 0
            and database["Running"] is database["OOMKilled"] is False and raw["bootstrap-stop-status"] == b"0\n"
            and values["fresh_exact_project_and_CID_absence"] == EMPTY
            and values["physical_CLI_container_settlement"] == {"CLI_identities": 27, "container_identities": 1},
            "Original database, Redis, exact owner process and project retirement")
    return host["controller_exit"]


def inspect_epoch(repo, epoch, acceptance, template):
    raw = load_epoch(repo, epoch)
    number = epoch["id"]
    exit_code = check_host(epoch, raw)
    inputs = json.loads(raw["m6-inputs.json"])
    require(inputs["Original_revision"] == ORIGINAL and inputs["Original_archive_sha256"] == ORIGINAL_ARCHIVE
            and inputs["Original_module_sha256"] == template["Original_module_sha256"]
            and len(inputs["Original_module_sha256"]) == 11 and inputs["Source_DVR_replay"] is False,
            "exact pinned Original and eleven deployment module hashes")
    report = json.loads(raw[CASE_PATH])
    wrapper = json.loads(raw[GUEST_WRAPPER])
    require(wrapper["passed"] is (number == 2) and wrapper["fullM6"] is wrapper["performance_verified"] is False
            and wrapper["all_actual_worker_settlements"] == [report["worker_stop_actual"]]
            and wrapper["origin_close"]["thread_settled"] is True
            and len(wrapper["origin_close"]["all_request_records"]) == (0 if number == 1 else 4),
            "actual Original guest worker and dependency service retirement")
    if number == 1:
        typed = acceptance.validate_tvg_prefix(report)
        prefix = json.loads(exact_file(repo, repo / BASE / "source-tvg-prefix-review-1.json", PREFIX_REVIEW))
        require(prefix == typed | {"report_sha256": sha256(raw[CASE_PATH]).hexdigest()},
                "exact independently qualified Original TVG prefix")
    else:
        typed = acceptance.validate_versions(report)
        require(wrapper["report"].get("acceptance") == typed
                and {k: v for k, v in wrapper["report"].items() if k not in {"acceptance", "passed"}} ==
                    {k: v for k, v in report.items() if k != "passed"},
                "separately passing four-case Original Version epoch")
    return {"id": number, "host_actual_exit": exit_code, "guest_actual_exit": int(bool(COMMAND_FAILURES[number])), "required_case_ids": [item["name"] for item in report["cases"]],
            "report_sha256": sha256(raw[CASE_PATH]).hexdigest(), "exact_cleanup": True}


def resolve(context, repository, candidate):
    require(type(context) is dict and set(context) in ({"manifest_sha256"}, {"manifest_sha256", "repository"})
            and context["manifest_sha256"] == MANIFEST_SHA256, "exact committed Original Source evidence manifest binding")
    repo = Path(context.get("repository", repository))
    require(repo.is_absolute() and repo == repo.resolve(strict=True) and repo.is_dir(),
            "canonical Original evidence repository")
    if repo != repository:
        require(Path(candidate["build_manifest_file"]).is_relative_to(repo / "artifacts/rust"),
                "copied coordinator Source repository must own the common build manifest")
    packet = json.loads(exact_file(repo, repo / BASE / "manifest.json", MANIFEST_SHA256))
    require([group["id"] for group in packet["epochs"]] == [1, 2]
            and "not frozen at launch" in packet["source_tool_capture_limitation"],
            "preserve independently scoped Original epochs and launch tooling limit")
    acceptance = reducer(repo)
    template = json.loads(exact_file(repo, repo / TOOLS / "Source-input-template.json",
                                     RUNTIME_PINS["Source-input-template.json"]))
    epochs = [inspect_epoch(repo, epoch, acceptance, template) for epoch in packet["epochs"]]
    cases = [case for epoch in epochs for case in epoch["required_case_ids"]]
    require(cases == acceptance.EXPECTED and len(set(cases)) == 10, "all exact ten declared Original observations")
    return {"status": "retained_source_authority", "scope": NODE, "Original_revision": ORIGINAL,
            "source_manifest_sha256": MANIFEST_SHA256, "required_case_ids": cases, "epochs": epochs,
            "observer_tools_frozen_at_launch": False, "Native_executed": False, "Source_replay": False,
            "current_package_pass": False, "full_M6": False}
