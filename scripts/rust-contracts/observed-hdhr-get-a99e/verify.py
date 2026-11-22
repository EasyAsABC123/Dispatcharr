#!/usr/bin/env python3
"""Verify the published fixed HDHomeRun GET evidence using only local files."""
from hashlib import sha256
from itertools import product
import json
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
PREFIX = "scripts/rust-contracts/observed-hdhr-get-a99e/"
PINS = {
    "review.json": "bfd11f7029477b69784f7ab4b879eb3ec0e8cd25031e4234eb6fcccbb4f5f2b3",
    "publication.json": "0962bbffc1365b50f090aa99d4d819313e287dbd85472336a0dd19a7f8b7a45c",
    "authority/fixture-SHA256SUMS": "39b9c598dce53822c634ad057c782de9e401f3b1f41a7addb64ae8d42b715baa",
    "authority/forced-browser-proof.json": "7d463ea0a09d98103108069742d78630d626c46f0cd7b880326a03f936c1f924",
    "authority/ordinary-browser-proof.json": "801f7557144aedd9aea011a5abe0dca8b437636a62b59b9d4afd73908c029f8f",
    "actual-pair-1/actual-exit": "9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa",
    "actual-pair-1/generation-custody.json": "87370c0890fa0789ef3d89e112b6051a486615e6228dc483067541808ba056da",
    "actual-pair-1/receipt.json": "af43b0f7b2bcf60340051c6a8ef9a4e2d37851a430307a9ecc5b8cf74d759490",
}
PATH = "/hdhr/lineup_status.json"
WORK_ID = "route-9198a3509e35fbb64b8e:METHOD_UNRESOLVED"
METHODS_HELD = ["HEAD", "OPTIONS"]
ORIGINAL = {
    "revision": "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430",
    "image": "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f",
    "archive_sha256": "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff",
}
BROWSER = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
NEGOTIATION = {
    "implicit": ("", None), "json": ("", "application/json"), "browser": ("", BROWSER),
    "format-json": ("format=json", "application/json"), "format-api-browser": ("format=api", BROWSER),
    "format-api-json": ("format=api", "application/json"), "unavailable-media": ("", "image/png"),
    "unknown-format": ("format=unavailable", "application/json"),
}
CREDENTIALS = ("bearer", "api-key-header", "api-key-authorization")
IDENTITIES = [("anonymous", "none"), *product(("basic", "standard", "admin", "inactive"), CREDENTIALS),
              ("standard", "expired-bearer")]
POLICIES = ("allowed", "policy-denied")
HEADERS = {"allow", "content-length", "content-type", "location", "vary", "www-authenticate"}
COORDINATE = ("role", "credential", "policy", "negotiation")
SHELLS = {
    "format-api-browser": ("authority/forced-browser-proof.json", "0c11514b874deb7966a632de929e2860160e4ec8f915a943edafa956d615b6ec"),
    "browser": ("authority/ordinary-browser-proof.json", "3a6b189a013dcd14ad1b152b0b19a63e1be2820d6903aa8a77f59d9bc78f5811"),
}
STATIC_ENTITY = {
    200: (84, "f06249539f3780a26c05931b08f9f0c921d5b6c5b5cb0c15b576412a32d81b37"),
    403: (22, sha256(b'{"error": "Forbidden"}').hexdigest()),
}
GENERATION_ROLES = {"baseline", "origin", "data-init", "candidate"}
SENSITIVE_TEXT = re.compile(
    r"(?:file://|/(?:home|tmp|var/run|proc|mnt|root)/|owned-hdhr-(?:status|browsable-auth)-|"
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|"
    r"\b(?:Bearer|ApiKey)\s+[A-Za-z0-9_-]{20,}|\bcsrftoken\s*=\s*[A-Za-z0-9]{20,})", re.I
)
SENSITIVE_KEYS = {"authorization", "x-api-key", "password", "token", "api_key", "cookie", "set-cookie",
                  "stderr", "stdout", "command", "raw_body", "seed", "client_secret"}


def require(passed, message):
    if not passed:
        raise ValueError(message)


def stamp(data):
    return sha256(data).hexdigest()


def canonical(value):
    return stamp(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode())


def hex_digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def privacy(value):
    if isinstance(value, dict):
        require(not SENSITIVE_KEYS.intersection(key.lower() for key in value), "unexpected sensitive field")
        for key, item in value.items():
            privacy(key)
            privacy(item)
    elif isinstance(value, list):
        for item in value:
            privacy(item)
    elif isinstance(value, str):
        require(not SENSITIVE_TEXT.search(value), "unexpected credential, absolute file path or private identifier")


def read(root, leaf):
    path = root / leaf
    require(path.is_file() and path.resolve() == path, "missing or aliased evidence file")
    raw = path.read_bytes()
    require(stamp(raw) == PINS[leaf], "evidence file hash mismatch")
    if leaf.endswith(".json"):
        value = json.loads(raw)
        privacy(value)
        return value
    return raw


def reference(leaf):
    return {"path": PREFIX + leaf, "sha256": PINS[leaf]}


def status(key):
    role, credential, policy, negotiation = key
    if negotiation == "unknown-format":
        return 404
    if negotiation in ("format-api-json", "unavailable-media"):
        return 406
    if role == "inactive" or credential == "expired-bearer":
        return 401
    return 403 if policy == "policy-denied" else 200


def coordinates():
    return {(role, credential, policy, negotiation): status((role, credential, policy, negotiation))
            for (role, credential), policy, negotiation in product(IDENTITIES, POLICIES, NEGOTIATION)}


def stock(root, review):
    proofs = {}
    for negotiation, (leaf, shell_digest) in SHELLS.items():
        require(review["Original_HTML"][negotiation] == reference(leaf), "independent Original proof reference")
        proof = read(root, leaf)
        require(proof["Original"] == ORIGINAL and proof["qualified_cases"] == 8
                and proof["exact_cleanup"] is True and proof["Native_measured"] is False, "Original HTML custody")
        shell = proof["shell_authority"]["redacted_shell"]
        require(stamp(shell.encode()) == shell_digest and shell.count("@@HDHR_ERROR_CONTENT@@") == 1
                and shell.count("@@HDHR_CSRF_SCRIPT@@") == 1 and "@@HDHR_CSRF_INPUT@@" not in shell,
                "redacted HTML shell and exact placeholder scope")
        require(set(proof["stable_wire"]) == set(proof["typed_cases"]) and len(proof["typed_cases"]) == 8,
                "eight independently observed HTML coordinates per shell")
        proofs[negotiation] = proof
    return proofs


def index(arm, required, dynamic, proofs):
    require(arm["case_count"] == len(arm["calls"]) == 224 and arm["passed"] is True
            and arm["network_policy_restored"] is arm["read_SQL_unchanged"] is True
            and arm["original_archive_sha256"] == ORIGINAL["archive_sha256"], "real arm completion and effects")
    require([(row["policy"], row["count"], row["natural_quiet_seconds"]) for row in arm["inactive_key_windows"]] ==
            [("allowed", 5, 0), ("allowed", 5, 61), ("policy-denied", 5, 61), ("policy-denied", 5, 61)],
            "actual throttle windows")
    output = {}
    for row in arm["calls"]:
        key = tuple(row[name] for name in COORDINATE)
        require(key in required and key not in output and row["method"] == "GET" and row["path"] == PATH
                and (row["query"], row["accept"]) == NEGOTIATION[key[-1]] and row["status"] == required[key]
                and row["assertions_passed"] is True, "exact reviewed HTTP coordinate")
        require(set(row["headers"]) == HEADERS and hex_digest(row["body_sha256"])
                and type(row["body_bytes"]) is int and 0 <= row["body_bytes"] <= 131072, "bounded wire observation")
        if row["status"] in STATIC_ENTITY:
            require((row["body_bytes"], row["body_sha256"]) == STATIC_ENTITY[row["status"]]
                    and row["headers"]["content-type"] == "application/json", "exact Original JSON body")
        body = row["body_authority"]
        if key in dynamic:
            role, credential, policy, negotiation = key
            name = f"{policy}-{role}-{credential}"
            proof = proofs[negotiation]
            witness = proof["stable_wire"][name]
            require(row["headers"] == witness["headers"] and row["body_bytes"] == witness["body_bytes"]
                    and body == proof["typed_cases"][name] | {"cookie_sent": False},
                    "exact independent HTML headers, size, shell, typed error and cookie validation")
        else:
            require(body is None, "raw wire is required outside the sixteen HTML coordinates")
        output[key] = row
    require(output.keys() == required.keys(), "complete physical arm")
    return output


def verify(root):
    review, publication = (read(root, leaf) for leaf in ("review.json", "publication.json"))
    route = review["route"]
    require(review["kind"] == "complete_observed_hdhr_get_v2" and review["Original"] == ORIGINAL
            and review["held_other_methods"] == METHODS_HELD and route == {
                "stable_work_id": WORK_ID, "resolver_ordinal": 376, "pattern_exact": PATH[1:],
                "source_handler": "apps.hdhr.api_views.LineupStatusAPIView", "observed_method": "GET",
            }, "route-local scope")
    candidate = review["candidate"]
    require(candidate == {"revision": publication["application_revision"], "image": publication["candidate_image"],
                          "manifest_sha256": publication["build_manifest_sha256"],
                          "runtime_binding_sha256": publication["runtime_binding_sha256"],
                          "verified_publication_sha256": PINS["publication.json"]}
            and publication["repository"] == "EasyAsABC123/Dispatcharr"
            and publication["ref"] == "refs/heads/rust-backend-migration"
            and publication["published_tip"] == publication["receiving_tip"]
            and publication["exact_app_inputs"] == 2156 and publication["protected_inputs"] == 497
            and publication["literal_publication_exit"] == 0
            and all(hex_digest(val) for val in publication["recorded_publication_artifact_sha256"].values()),
            "recorded published image and receiving revision")
    fixture = "authority/fixture-SHA256SUMS"
    require(review["fixture"] == reference(fixture)
            and len(read(root, fixture).splitlines()) == 3, "exact capture fixture seal")
    leaves = {name: f"actual-pair-1/{leaf}" for name, leaf in
              (("actual_exit", "actual-exit"), ("receipt", "receipt.json"), ("generation_custody", "generation-custody.json"))}
    require(review["execution"] == {name: reference(leaf) for name, leaf in leaves.items()}, "paired receipt references")
    require(read(root, leaves["actual_exit"]) == b"0\n", "literal paired exit zero")
    receipt, custody = (read(root, leaves[key]) for key in ("receipt", "generation_custody"))
    require(receipt["actual_exit"] == 0 and receipt["phase"] == "capture-native-routes"
            and receipt["candidate_revision"] == candidate["revision"] and receipt["candidate_image"] == candidate["image"]
            and receipt["build_manifest_sha256"] == candidate["manifest_sha256"]
            and receipt["package_binding_sha256"] == candidate["runtime_binding_sha256"]
            and receipt["original_revision"] == ORIGINAL["revision"] and receipt["actual_original_image_id"] == ORIGINAL["image"]
            and receipt["full_M7_accepted"] is receipt["performance_verified"] is False, "actual terminal package binding")
    roles = custody["owned_generation_sha256"]
    cleanup = {"compose_down_passed": True, "remaining_owned": {"containers": 0, "networks": 0, "volumes": 0},
               "exact_generations_absent": dict.fromkeys(GENERATION_ROLES, True)}
    require(receipt["exact_cleanup"] is custody["cleanup_returned_success"] is True
            and receipt["cleanup"] == cleanup and all(custody[key] == value for key, value in cleanup.items())
            and custody["capture_id"] == review["capture_id"] and hex_digest(custody["capture_id"])
            and custody["original_archive_sha256"] == ORIGINAL["archive_sha256"]
            and set(roles) == GENERATION_ROLES and len(set(roles.values())) == 4 and all(map(hex_digest, roles.values()))
            and custody["arm_completion"] == dict.fromkeys(("original", "native"), {"passed": True, "case_count": 224}),
            "same-capture exact resource teardown")
    required = coordinates()
    reviewed = {tuple(row[name] for name in COORDINATE): row["expected_status"] for row in review["required_cases"]}
    require(len(review["required_cases"]) == len(required) == 224 and reviewed == required, "exact reviewed roster")
    dynamic = {key for key, expected in required.items() if expected == 401 and key[-1] in SHELLS}
    proofs = stock(root, review)
    left, right = (index(receipt["business"][arm], required, dynamic, proofs) for arm in ("original", "native"))
    ordinary, structural = [], []
    differences = 0
    for key in sorted(required):
        old, new = left[key].copy(), right[key].copy()
        old_body, new_body = old.pop("body_authority"), new.pop("body_authority")
        if key not in dynamic:
            require(old == new, "literal raw wire mismatch")
            ordinary.append({"coordinate": key, "wire": new})
            continue
        old_hash, new_hash = old.pop("body_sha256"), new.pop("body_sha256")
        require(old == new and old_body == new_body, "independently bound structural HTML mismatch")
        differences += old_hash != new_hash
        structural.append({"coordinate": key, "wire": new, "body_authority": new_body,
                           "Original_body_sha256": old_hash, "Native_body_sha256": new_hash})
    require(len(ordinary) == 208 and len(structural) == 16 and differences == 16, "fixed comparison totals")
    require(receipt["business"]["native"]["comparison"] == {
        "all_contracts_match": True, "differences": [], "measured_pairs": 224,
        "raw_body_variations_in_matching_structural_pairs": 16, "structural_pairs_required": 16,
    }, "recomputed comparison agrees with recorded result")
    return {"candidate": candidate, "observed_method": "GET", "paired": 224, "raw_wire_pairs": 208,
            "structural_HTML_pairs": 16, "exact_cleanup": True, "raw_wire_projection_sha256": canonical(ordinary),
            "structural_projection_sha256": canonical(structural), "held_other_methods": METHODS_HELD,
            "global_register_changed": False}


if __name__ == "__main__":
    print(json.dumps(verify(HERE), sort_keys=True))
