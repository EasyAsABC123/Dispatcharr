"""Bind exact Original Celery registration observations without executing tasks."""
import hashlib
import json
from pathlib import PurePosixPath

import source_contracts

KIND = "retained_original_celery_registration_non_HTTP"
FOLDER = "scripts/rust-contracts/retained-task-registry/"
INPUT = FOLDER + "bindings.json"
DECLARATIONS = FOLDER + "declarations.json"
OBSERVATIONS = FOLDER + "observations.json"
EXECUTION = FOLDER + "execution.json"
PROBE = FOLDER + "probe.py"
PROVENANCE = FOLDER + "provenance.json"
FIXTURE = "Original-1241b3-Celery-registration"
STATE = "partial_executable_Original_Celery_registration_metadata"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
IMAGE = "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
EXPECTED_SHA256 = {
    DECLARATIONS: "afdaa580ff8d4024a7e23ae18132a1e8bfe722e0acbed2fc74ef5182bff7da23",
    OBSERVATIONS: "c18dd0cd5768abd04531351b5c2640e119d608911ad6690da43967247b179747",
    EXECUTION: "b8bbeb66de13f3875eb23171a52550879d1c3e15b16d6d6387c8164e23dd7877",
    PROBE: "d29b149b31c800a455be42c59ecc57b765324399c1afc149822c185a33918698",
    PROVENANCE: "5a3dcc4249f4a4a7b67e25f6b7250acd2d04e398768e9944dd7d53d98fcb1f42",
}
FALLBACK = [{
    "api":"socket.bind", "function":"_has_ipv6", "line":127, "module":"urllib3.util.connection",
    "result":"EAFNOSUPPORT", "source_sha256":"2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74",
}]
DIAGNOSTIC = {"api_counts":{}, "reports":[]}


def dependencies():
    return {INPUT, *EXPECTED_SHA256}


def exact_bytes(root, path):
    raw = (root / path).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == EXPECTED_SHA256[path], path
    return raw


def materialize(declarations, observation, execution):
    assert execution == {
        "actual_exit":0, "candidate_count":20, "celery":"5.6.3", "cleanup":True,
        "denied_effect_attempts":{}, "django":"6.0.9", "full_M0_accepted":False,
        "image_id":IMAGE, "ipv6_fallbacks":FALLBACK, "ipv6_unavailable":True, "network_diagnostic":DIAGNOSTIC,
        "observed_registration_metadata":20, "probe_output_sha256":EXPECTED_SHA256[OBSERVATIONS],
        "registration_metadata_replay_qualified":True, "task_execution_accepted":False, "unobserved":0,
    }
    rows = observation["observations"]
    assert observation == {
        "celery":execution["celery"], "denied_attempts":{}, "django":execution["django"],
        "external_effects_executed":False, "ipv6_fallbacks":FALLBACK, "ipv6_unavailable":True,
        "management_commands_started":False, "network_diagnostic":DIAGNOSTIC, "observations":rows,
    }
    assert len(declarations)==len(rows)==20
    fixture = {
        "id":FIXTURE, "baseline_revision":ORIGINAL, "original_image_id":IMAGE,
        "Celery":execution["celery"], "Django":execution["django"], "actual_exit":0,
        "exact_owned_cleanup":True, "network_mode":"none", "external_effects_executed":False,
        "guarded_import_observation":"One urllib3 IPv6 availability check received EAFNOSUPPORT before a kernel bind.",
        "files_sha256":EXPECTED_SHA256,
    }
    contracts = []
    ids = set()
    paths = set()
    for declared, row in zip(declarations,rows,strict=True):
        coordinate = {key:declared[key] for key in ("kind","path","line","name","callable")}
        identity = "candidate-" + source_contracts.digest(coordinate)[:20]
        assert identity==declared["candidate_id"] and identity not in ids
        ids.add(identity)
        path = PurePosixPath(coordinate["path"])
        assert not path.is_absolute() and ".." not in path.parts and path.suffix==".py"
        paths.add(path)
        assert coordinate["kind"]=="task_candidate" and coordinate["callable"]=="celery.shared_task"
        assert coordinate["name"].isidentifier() and type(coordinate["line"]) is int and coordinate["line"]>0
        assert coordinate["line"]==declared["decorator_line"]<declared["definition_line"]
        assert declared["module"]==".".join(path.with_suffix("").parts)
        expected_name = declared["module"]+"."+coordinate["name"]
        if (coordinate["path"], coordinate["name"])==("apps/epg/sd_tasks.py","fetch_sd_guide_for_epg"):
            expected_name = "apps.epg.tasks.fetch_sd_guide_for_epg"
        assert declared["task_name"]==expected_name
        stamp=declared["source_sha256"]
        assert isinstance(stamp,str) and len(stamp)==64 and all(char in "0123456789abcdef" for char in stamp)
        assert all(set(param)=={"name","kind","has_default"} and param["name"].isidentifier()
                   and isinstance(param["kind"],str) and type(param["has_default"]) is bool for param in declared["parameters"])
        assert row == {
            "candidate_id":identity, "state":"registered_metadata_observed", "run_source_path":coordinate["path"],
            "run_decorator_line":coordinate["line"], "run_definition_line":declared["definition_line"],
            **{key:declared[key] for key in ("task_name","bound","parameters","time_limit","soft_time_limit")},
        }
        contracts.append({
            "candidate_id":identity, "source":coordinate, "original_file_sha256":stamp, "fixture_ids":[FIXTURE],
            "observation":{"task_name":row["task_name"],"bound":row["bound"],"parameter_count":len(row["parameters"]),
                           "time_limit":row["time_limit"],"soft_time_limit":row["soft_time_limit"]},
            "observation_sha256":source_contracts.digest(row),
            "assertions":[
                "Pinned Original Celery registry contains the exact callable, Source coordinate and signature metadata.",
                "No task body, management command, or external effect was executed.",
            ],
            "result":{"state":STATE,"fully_verified":False,"Native_coverage_increased":False,"task_execution_verified":False},
            "fully_verified":False,
        })
    assert len(paths)==6
    return {"format_version":1,"kind":KIND,"credit":"Original Dispatcharr, Celery and Django contributors retain full credit.",
            "contracts":[],"fixtures":{FIXTURE:fixture},"non_HTTP_contracts":contracts,
            "fully_verified_added":0,"performance_verified":False}


def derive(root):
    exact_bytes(root, PROBE)
    provenance = json.loads(exact_bytes(root, PROVENANCE))
    terminal = provenance["metadata_replay"]
    assert provenance["source"]["revision"]==ORIGINAL and provenance["source"]["image"].endswith(IMAGE)
    assert provenance["source"]["sealed_roster_sha256"]==EXPECTED_SHA256[DECLARATIONS]
    assert terminal["owner_receipt_sha256"]==EXPECTED_SHA256[EXECUTION]
    assert terminal["guest_report_sha256"]==EXPECTED_SHA256[OBSERVATIONS]
    assert terminal["owner_terminal_exit"]==terminal["guest_exit"]==0 and terminal["observed"]==20
    assert terminal["network_mode"]=="none" and terminal["exact_cleanup"] and terminal["read_only_root_and_source_mounts"]
    assert not terminal["external_effects_executed"] and not terminal["management_commands_started"]
    assert provenance["first_attempt"]["unobserved"]==provenance["diagnostic_attempt"]["unobserved"]==20
    return materialize(*(json.loads(exact_bytes(root,path)) for path in (DECLARATIONS,OBSERVATIONS,EXECUTION)))


def validate(root, value):
    assert source_contracts.digest(value)==source_contracts.digest(derive(root))


def restore_before(root, entries):
    binding = derive(root)
    by_id = {row["candidate_id"]:row for row in binding["non_HTTP_contracts"]}
    for entry in entries:
        if entry["candidate_id"] not in by_id:
            continue
        contract = by_id.pop(entry["candidate_id"])
        assert entry.pop("executable_Source_contract")==contract
        assert entry.pop("setup_fixture")==binding["fixtures"]
        assert entry.pop("assertions")==contract["assertions"]
        assert entry["state"]==STATE and entry["result"]==contract["result"]
        entry.update(state="static_candidate_requires_runtime_contract",result=None)
    assert not by_id
