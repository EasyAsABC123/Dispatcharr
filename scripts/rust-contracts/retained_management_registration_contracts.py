"""Bind pinned Original Django command discovery without importing command modules."""
import hashlib
import json
from pathlib import PurePosixPath

import retained_task_registry_contracts as tasks
import source_contracts

KIND = "retained_original_django_management_registration_non_HTTP"
FOLDER = "scripts/rust-contracts/retained-management-registration/"
INPUT = FOLDER + "bindings.json"
DECLARATIONS = FOLDER + "declarations.json"
OBSERVATIONS = FOLDER + "observations.json"
EXECUTION = FOLDER + "execution.json"
PROBE = FOLDER + "probe.py"
PROVENANCE = FOLDER + "provenance.json"
FIXTURE = "Original-1241b3-Django-management-registration"
STATE = "partial_executable_Original_Django_management_registration_metadata"
EXPECTED_SHA256 = {
    DECLARATIONS: "9150720002dbe7370da06eb3cd9698f8645f465625c77a47c40b88e8a782da53",
    OBSERVATIONS: "6f43a2c088b6655d68fcc9461410bb513b732fcdd5a36857f4051d213098cee8",
    EXECUTION: "b13f84cb8b04f6f81023bfffaf95a6cc43b9483bbcb4bd4039373f3b2068cd68",
    PROBE: "2a449567ff162ea1d718af1567c5e3ddd39f16859cd8cd2250753c6f4c20a389",
    PROVENANCE: "4ab46a9ff6d5e21d5e60c2d05b54911ed1435bae9d7efebca416481ae4ae5f79",
}


def dependencies():
    return {INPUT, *EXPECTED_SHA256}


def exact_bytes(root, path):
    raw = (root/path).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==EXPECTED_SHA256[path], path
    return raw


def materialize(declarations, observed, execution):
    assert execution == {
        "actual_exit":0, "candidate_count":3, "celery":"5.6.3", "cleanup":True,
        "command_modules_loaded":False, "denied_effect_attempts":{}, "django":"6.0.9",
        "full_M0_accepted":False, "image_id":tasks.IMAGE, "ipv6_fallbacks":tasks.FALLBACK,
        "management_execution_accepted":False, "observed_management_registration":3,
        "probe_output_sha256":EXPECTED_SHA256[OBSERVATIONS], "registration_qualified":True, "unobserved":0,
    }
    rows = observed["observations"]
    assert observed == {
        "celery":execution["celery"], "command_modules_loaded":False, "denied_attempts":{},
        "django":execution["django"], "external_effects_executed":False, "ipv6_fallbacks":tasks.FALLBACK,
        "ipv6_unavailable":True, "management_commands_started":False, "observations":rows,
    }
    assert len(rows)==len(declarations)==3
    fixture = {
        "id":FIXTURE, "baseline_revision":tasks.ORIGINAL, "original_image_id":tasks.IMAGE,
        "Django":execution["django"], "network_mode":"none", "exact_owned_cleanup":True,
        "runtime_compose_sha256":"f6b118f62c34096073ce0564e8e6d6931b2e3da0870f336dd75c93332817ab52",
        "command_modules_loaded":False, "management_commands_started":False, "external_effects_executed":False,
        "files_sha256":EXPECTED_SHA256,
    }
    contracts=[]
    names=set()
    for declaration, row in zip(declarations,rows,strict=True):
        coordinate={key:declaration[key] for key in ("kind","path","line","name")}
        identity="candidate-"+source_contracts.digest(coordinate)[:20]
        assert identity==declaration["candidate_id"]
        path=PurePosixPath(coordinate["path"])
        assert not path.is_absolute() and ".." not in path.parts and path.suffix==".py"
        assert coordinate["kind"]=="management_command" and coordinate["name"]=="Command"
        assert type(coordinate["line"]) is int and coordinate["line"]>0
        assert tuple(path.parts[-3:-1])==("management","commands")
        assert path.stem==declaration["command"] and path.stem not in names
        names.add(path.stem)
        assert declaration["owner"]==".".join(path.parts[:-3])
        assert row=={key:value for key,value in declaration.items() if key!="kind"} | {
            "state":"management_registration_observed",
        }
        file_hash=declaration["source_sha256"]
        assert isinstance(file_hash,str) and len(file_hash)==64 and all(ch in "0123456789abcdef" for ch in file_hash)
        contracts.append({
            "candidate_id":identity, "source":coordinate, "original_file_sha256":file_hash, "fixture_ids":[FIXTURE],
            "observation":{"command":row["command"],"owner":row["owner"]},
            "observation_sha256":source_contracts.digest(row),
            "assertions":[
                "Pinned Original Django command discovery reports this exact command and installed app owner.",
                "The observed registry entry matches the pinned source coordinate without an import or command invocation.",
            ],
            "result":{"state":STATE,"fully_verified":False,"Native_coverage_increased":False,"management_execution_verified":False},
            "fully_verified":False,
        })
    assert names=={"proxy","dropdb","kill_processes"}
    return {
        "format_version":1, "kind":KIND, "credit":"Original Dispatcharr and Django contributors retain full credit.",
        "contracts":[], "fixtures":{FIXTURE:fixture}, "non_HTTP_contracts":contracts,
        "fully_verified_added":0, "performance_verified":False,
    }


def derive(root):
    exact_bytes(root,PROBE)
    provenance=json.loads(exact_bytes(root,PROVENANCE))
    terminal=provenance["physical_registration"]
    assert provenance["source"]["revision"]==tasks.ORIGINAL and provenance["source"]["image"].endswith(tasks.IMAGE)
    assert provenance["source"]["sealed_roster_sha256"]==EXPECTED_SHA256[DECLARATIONS]
    assert terminal["owner_receipt_sha256"]==EXPECTED_SHA256[EXECUTION]
    assert terminal["guest_report_sha256"]==EXPECTED_SHA256[OBSERVATIONS]
    assert terminal["owner_terminal_exit"]==terminal["guest_exit"]==0 and terminal["observed"]==3
    assert terminal["lookup"]=="django.core.management.get_commands"
    assert terminal["network_mode"]=="none" and terminal["exact_cleanup"] and terminal["read_only_root_and_source_mounts"]
    assert terminal["command_import_construction_parser_and_dispatch_guarded"]
    assert not terminal["external_effects_executed"] and not terminal["management_commands_started"]
    assert not terminal["command_modules_loaded"]
    return materialize(*(json.loads(exact_bytes(root,path)) for path in (DECLARATIONS,OBSERVATIONS,EXECUTION)))


def validate(root, value):
    assert source_contracts.digest(value)==source_contracts.digest(derive(root))


def restore_before(root, entries):
    binding=derive(root)
    by_identity={row["candidate_id"]:row for row in binding["non_HTTP_contracts"]}
    for entry in entries:
        if entry["candidate_id"] not in by_identity:
            continue
        contract=by_identity.pop(entry["candidate_id"])
        assert entry.pop("executable_Source_contract")==contract
        assert entry.pop("setup_fixture")==binding["fixtures"]
        assert entry.pop("assertions")==contract["assertions"]
        assert entry["state"]==STATE and entry["result"]==contract["result"]
        entry.update(state="static_candidate_requires_runtime_contract",result=None)
    assert not by_identity
