"""Offline admission of the one physically qualified Original cache/profile capture."""

import hashlib
import json
from pathlib import Path

NODE = "source/cache-and-profile-contexts"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
IMAGE = "ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
STAGE = Path("artifacts/rust/m235-new-source-cohort-2")
OWNER = Path("scripts/rust-milestone-batches/m235")
QUALIFICATION = "Source-domain-qualification.json"
PINS = {
    "context.json": "69e45fea51996892cabd3fd1fa7d6fe68b4705bff186a97dbf03e9c83acb3280",
    "application/execution-private.json": "3e803881d3ec4ca8c6f5017aef85e0cb9c848d177d7b22653ff8ad89992c2aed",
    "application/ts-cache-source-private.json": "3c49009525f44e609f0b7c9844806169cac155f17de6448226e6820b2c1eede3",
    "application/profile-arguments-owner-private.json": "1ec78150899f1baba04c43de2fd33ef97add20c8ec93e36c8853612595ea315f",
    "application/" + QUALIFICATION: "fe103ce69c3cb36302314024d291bf433bdcd499b0636e2109bd4489e7c8f308",
    "application/owned-role.json": "f7a2554df37b08a760aa4879ea96942acf37d501e309db6043fae4d0a52b6639",
    "application/compose.json": "ec8b27ca01fae4ffcac0e5828311a4d1ee70355b450ba8e7d470463303b74cb5",
}
OWNER_PINS = {
    "matrix.json": "33b7afc473a4469b9085b42413b285a1e43effdfd21be2e3616dfb31c88a306d",
    "qualify_source.py": "ab21f9c9f028a03fe46759477825efab34667d0c65d9efe8bc822be91d7cc25f",
    "source_workflow.py": "bf4d35d3ff56c75e4707434cafb9a801eaeff598c83525c70f169e684093d56b",
    "inputs.json": "adf02dddd201a31e7e1ce201b890d477d38d22b306e45f0ea6f94d7b76bc2c4f",
}
CLEANUP = (
    "discover_partial_owned_baseline", "default_after_full_audit",
    "owned_backend_absence_Unix", "normal_management_PGRedis_stop",
    "owned_compose_down", "fresh_exact_project_and_CID_absence",
    "physical_CLI_container_settlement", "final_frozen_input_recheck",
)


def require(value, label):
    if not value:
        raise ValueError(label)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def checked(repo, relative, digest):
    path = repo / relative
    require(path.resolve() == path and path.is_file(), f"RetainedSourceRegularInput:{relative}")
    data = path.read_bytes()
    require(sha256(data) == digest, f"RetainedSourceExactInput:{relative}")
    return data


def reference():
    key = "application/" + QUALIFICATION
    return {
        "Original_git": ORIGINAL,
        "qualification": {"path": str(STAGE / key), "sha256": PINS[key]},
    }


def resolve(ref, repo):
    require(ref == reference(), "ExactRetainedCacheProfileReference")
    repo = Path(repo).resolve()
    verified = {name: checked(repo, STAGE / name, digest) for name, digest in PINS.items()}
    owner = {name: checked(repo, OWNER / name, digest) for name, digest in OWNER_PINS.items()}
    archive = Path("artifacts/rust/m5-core-settings-source-executor-2/inputs/original-source.tar")
    checked(repo, archive, "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff")

    context = json.loads(verified["context.json"])
    require(context["node_id"] == NODE and context["batch"] == "M2-M3-M5-wide-1",
            "ExactOriginalNodeContext")
    matrix = json.loads(owner["matrix.json"])
    cells = [cell for cell in matrix["nodes"] if cell["id"] == NODE]
    require(len(cells) == 1 and cells[0]["implementation"] == "available-source-capture-adapter"
            and cells[0]["depends_on"] == [] and cells[0]["input_requires"] == ["source-workflow-binding"],
            "ExactCanonicalSourceCell")
    services = json.loads(verified["application/compose.json"])["services"]
    require(set(services) == {"baseline"} and services["baseline"]["image"] == IMAGE
            and services["baseline"]["network_mode"] == "none"
            and set(json.loads(verified["application/owned-role.json"])) == {"baseline"},
            "ExactOwnedOriginalImageAndRole")
    execution = json.loads(verified["application/execution-private.json"])
    require(execution["passed"] is True and type(execution["controller_exit"]) is int
            and execution["controller_exit"] == 0 and execution["capture_complete"] is True
            and execution["capture_only"] is True and execution["actual_Source_requests"] == 1
            and execution["actual_Native_requests"] == 0 and execution["Source32_replay"] is False
            and execution["Source51_replay"] is False and execution["workers_or_beat_started"] is False,
            "OriginalPhysicalCaptureOnly")
    cleanup = execution["cleanup_steps"]
    require(tuple(step["name"] for step in cleanup) == CLEANUP
            and all(step["passed"] is True for step in cleanup), "ExactEightOriginalCleanupSteps")
    isolation = execution["default_SQL_and_cache_isolation"]
    require(isolation["default_SQL_full6259_and11_equal"] is True
            and isolation["default_cache_DB7_keys_type_bytes_equal"] is True,
            "OriginalDefaultSQLCachePreserved")

    namespace = {}
    exec(compile(owner["qualify_source.py"], str(OWNER / "qualify_source.py"), "exec"), namespace)
    qualified = namespace["qualify"](repo / STAGE / "application")
    recorded = json.loads(verified["application/" + QUALIFICATION])
    require(qualified == recorded and qualified["capture_and_domain_valid"] is True
            and qualified["failures"] == [] and qualified["Source_model_and_cache_authority_only"] is True
            and qualified["Main_or_profile_delivery_or_media_or_performance"] is False
            and qualified["Source_original_recorder_false_preserved"] is True,
            "ExactQualifiedOriginalCacheProfileCapture")
    for name, digest in PINS.items():
        checked(repo, STAGE / name, digest)

    return {
        "ready": False, "status": "retained_source_authority", "satisfies_dependency": True,
        "reasons": [], "retained_source": ref,
        "retained_counts": {"Original_API": 1, "direct_helpers": 5, "accessors": 6,
                            "profile_arguments": 18, "profile_owners": 6, "cleanup_steps": 8},
    }
