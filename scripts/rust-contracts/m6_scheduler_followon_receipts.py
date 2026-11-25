"""Admit reviewed scheduler follow-ons for the exact application that ran them."""
from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import subprocess


APPLICATION = "f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1"
INSTALL = "m6/scheduler-install-known-defaults"
TICK = "m6/scheduler-tick-publication"
RECURRING = "m6/recurring-positive-repeat"
BACKEND = "m6/backend-cleanup-expiry"
ORDER = (INSTALL, TICK, RECURRING, BACKEND)
STATUS = "verified_current_receipt"
CAPSULE = "m6-f38f6cdd-scheduler-followons-1"
CAPSULE_SEAL = "952184d2a7586ef52af1a3c69802ecbffa07bdcb7559805c0eb38c575d1fa025"
REVIEWED_PACKAGE = Path(
    "paseo-resume-698ce81e/wave-m9-series-task-lease-idle-product-1/output-amd64-series-task-lease-1"
    "/candidate-1/actual-package-1/package/build.json"
)
MANIFEST = "dc526bb02476e8610b8f79ba018242fcd34bbaa1c19988ee4b7f9e440a761387"
GROUP_PROOF_SHA = "777ec7d760bd4f7500db7779821f0468ae0a297e43196d1d207867a2712bd182"
# F38 pins above remain usable for exact historical replay.
B353_APPLICATION = "b3530c8eeab4741256f3225d479785b5a622dda9"
B353_MANIFEST = "e1ecab4ca4e53b8d2bc8a1a61094c059c10809261c7661218fe7a7e670eb9660"
B353_CAPSULE = "paseo-resume-698ce81e/wave-m6-b353-scheduler-dependency-cohort-1/output"
B353_CAPSULE_SEAL = "6ae4c36df462c7432804301d511be42485eef272db41410e8b315bd9485d1b6e"
B353_PACKAGE = Path(
    "paseo-resume-698ce81e/wave-m0-f38-cumulative-product-1/output-amd64-catalogue-b3530c8e-1"
    "/candidate-1/actual-package-1/package/build.json"
)
B353_GROUP_PROOF_SHA = "aff90f861b7956b515a7cac9fbdabc261d92ab9d159cc63507b4aaf57e8b31b7"
B353_APPROVED_GRAPH_SHA = "d02105f4ac2f8bdb834562528d89417ee94f8789ee7e13da9b4df2ecd00aea67"
HASH = re.compile("[0-9a-f]{64}")


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def digest(path):
    require(path.resolve() == path and path.is_file(), "exact regular scheduler evidence required: " + path.name)
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def checked_read(path):
    digest(path)
    return path.read_bytes()


def verify_graph(nodes):
    indexed = {node["id"]: node for node in nodes}
    for index, key in enumerate(ORDER):
        node = indexed.get(key, {})
        require(node.get("owner") == "m6"
                and node.get("resources") == ["m6/scheduler-default"]
                and node.get("depends_on") == ([ORDER[index - 1]] if index else []),
                "current scheduler receipt requires the exact canonical dependency graph: " + key)


def receipt_pins(candidate):
    application = candidate.get("git")
    if application == B353_APPLICATION:
        return B353_MANIFEST, B353_PACKAGE, B353_CAPSULE, B353_CAPSULE_SEAL, B353_GROUP_PROOF_SHA
    require(application == APPLICATION, "exact reviewed scheduler application required")
    return MANIFEST, REVIEWED_PACKAGE, CAPSULE, CAPSULE_SEAL, GROUP_PROOF_SHA


def validate_chain(nodes, candidate, authorities):
    """Check imported results before the scheduler treats them as dependencies."""
    verify_graph(nodes)
    manifest, *_ = receipt_pins(candidate)
    require(candidate.get("build_manifest_sha256") == manifest
            and set(authorities) == set(ORDER), "exact four current scheduler receipts required")
    predecessor = {}
    for key in ORDER:
        row = authorities[key]
        require(row.get("id") == key and row.get("status") == STATUS and row.get("candidate") == candidate
                and row.get("satisfies_dependency") is row.get("cleanup_verified") is True
                and row.get("counts_as_new_execution") is False
                and row.get("dependencies") == predecessor
                and HASH.fullmatch(row.get("receipt_sha256", "")) is not None,
                "unverified current scheduler receipt chain: " + key)
        predecessor = {key: row["receipt_sha256"]}


def expected_capsule(context, candidate):
    manifest, package_path, capsule, _, group_sha = receipt_pins(candidate)
    require(type(context) is dict and set(context) == {"artifact_root", "group_proof_sha256"}
            and isinstance(context["artifact_root"], str) and isinstance(context["group_proof_sha256"], str)
            and context["group_proof_sha256"] == group_sha,
            "exact current scheduler receipt context required")
    art = Path(context["artifact_root"])
    package = art / package_path
    require(art.is_absolute() and art.resolve() == art and art.name == "rust" and art.parent.name == "artifacts"
            and candidate.get("build_manifest_file") == str(package)
            and candidate.get("build_manifest_sha256") == manifest and digest(package) == manifest,
            "current scheduler evidence differs from its exact ordinary package")
    return art / capsule


def check_capsule(capsule, seal):
    manifest = capsule / "SHA256SUMS"
    require(digest(manifest) == seal, "exact reviewed scheduler capsule seal required")
    for line in manifest.read_text().splitlines():
        checksum, rel = line.split("  ", 1)
        child = Path(os.path.normpath(capsule / rel))
        require(child.is_relative_to(capsule.parent) and digest(child) == checksum,
                "reviewed scheduler capsule changed: " + rel)


def replay(capsule, proof):
    """Run only the immutable, independent offline group verifier."""
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "DOCKER_", "COMPOSE_", "PYTHON"))}
    environment.update(PATH="/usr/bin:/bin", GIT_OPTIONAL_LOCKS="0")
    command = ["/usr/bin/python3", "-B", str(capsule / "receipts.py"), "group"]
    run = subprocess.run(command, cwd=capsule, env=environment, capture_output=True,
                         timeout=300, check=False)
    require(run.returncode == 0 and not run.stderr and run.stdout == proof,
            "independent scheduler group replay did not reproduce the settled receipt")
    return json.loads(run.stdout)


def resolve(context, nodes, candidate, bindings):
    verify_graph(nodes)
    require(not bindings.keys() & set(ORDER), "settled current scheduler evidence cannot also be dispatched")
    capsule = expected_capsule(context, candidate)
    _, _, _, seal, _ = receipt_pins(candidate)
    check_capsule(capsule, seal)
    ledger = capsule / "owner-1"
    for phase in ("actual-exit", "scheduler-prerequisite.actual-exit", "recurring.actual-exit",
                  "recurring-proof.actual-exit", "recurring-acceptance.actual-exit",
                  "backend.actual-exit", "backend-proof.actual-exit", "group-proof.actual-exit"):
        require(checked_read(ledger / phase) == b"0\n", "fresh literal success required: " + phase)
    need_candidate = json.loads(checked_read(capsule / "scheduler/common.json"))["candidate"]
    require(candidate == need_candidate, "all current scheduler candidate bindings must match")
    outer = capsule / "root-owner-1"
    for phase in ("actual-exit", "group.actual-exit", "followons.actual-exit", "tick.actual-exit"):
        require(checked_read(outer / phase) == b"0\n", "settled scheduler root phase required: " + phase)
    require(checked_read(outer / "scheduler.actual-exit") == b"2\n"
            and not checked_read(outer / "engine-before.stdout")
            and not checked_read(outer / "engine-after.stdout"),
            "exact scoped matrix exit and empty engine censuses required")
    application = candidate["git"]
    if application == B353_APPLICATION:
        for phase in ("engine-before", "engine-after", "approved-reparse"):
            require(checked_read(outer / (phase + ".actual-exit")) == b"0\n",
                    "exact B353 scheduler root control required: " + phase)
        require(not checked_read(outer / "engine-before.stderr")
                and not checked_read(outer / "engine-after.stderr")
                and digest(outer / "approved-reparse.stdout") == B353_APPROVED_GRAPH_SHA,
                "exact B353 daemon diagnostics and approved graph replay required")
    proof_path = ledger / "group-proof.stdout"
    require(digest(proof_path) == context["group_proof_sha256"], "exact current scheduler group proof required")
    proof = checked_read(proof_path)
    require(checked_read(outer / "group.stdout") == proof, "outer proof differs from independent owner")
    group = replay(capsule, proof)
    require(set(group) == {"candidate", "prerequisite_proof_sha256", "recurring", "backend_cleanup"}
            and group["candidate"] == application, "current scheduler group candidate differs")
    prerequisite = group["prerequisite_proof_sha256"]
    require(set(prerequisite) == {INSTALL, TICK}, "canonical install and tick receipts required")
    ordered = {"candidate": application, "prerequisite_proof_sha256": prerequisite}
    require(json.loads(checked_read(ledger / "scheduler-prerequisite.stdout")) == ordered
            and json.loads(checked_read(ledger / "recurring-acceptance.stdout"))
            == ordered | {"recurring": group["recurring"]},
            "exact scheduler tick and recurring receipts must precede the backend result")

    stamps = dict(prerequisite)
    for key, projection, phase in ((RECURRING, "recurring", "recurring-proof"),
                                   (BACKEND, "backend_cleanup", "backend-proof")):
        row = group[projection]
        require(row["row"] == key and row["paired_physical_scoped_pass"] is True
                and row["formal_credit"] is False
                and row["proof_sha256"] == digest(ledger / (phase + ".stdout"))
                and HASH.fullmatch(row["engine_receipt_sha256"]) is not None,
                "exact fresh paired scheduler receipt required: " + key)
        stamps[key] = row["proof_sha256"]

    accepted = {}
    dependency = {}
    for key in ORDER:
        accepted[key] = {
            "id": key, "candidate": candidate, "status": STATUS, "receipt_sha256": stamps[key],
            "dependencies": dependency, "cleanup_verified": True, "satisfies_dependency": True,
            "counts_as_new_execution": False,
        }
        dependency = {key: stamps[key]}
    validate_chain(nodes, candidate, accepted)
    return accepted
