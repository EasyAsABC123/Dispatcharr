"""Admit exact Original scanner evidence for a later Native scheduler owner."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess

NODE = "m6/scanner-empty-and-content"
PREDECESSOR = "m6/stats-live-ghost"
STAGE = Path("artifacts/rust/m6-original-canonical-scanner-empty-content-1")
STAGE_SEAL = "95a86acaa0bd2960167e8e9efea17b1e509167e6e833c17eaefe844b191e6d8e"
FILES = {
    "run.py": "8bc379245226841e69ebfb38fc874c38ef2c91eb835f5ddf1a9da0f9d91c2e36",
    "campaign.sh": "07f5e459a2ed4b10ea9cce9c2a655acc4f7003e2aa4adf3f3f9b15e14cd9f3d8",
    "check.py": "d0876be4ea45a36999669c8fb206c6973cbffbe7ce923a0f3d6638108c9cf297",
    "HANDOFF.md": "ff5fe414bc3983f7e63b2763bde2a50d3a072ea74a2980fbcac3bd9518561911",
}
SOURCE_REVISION = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
IMAGE = "sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f"
ARCHIVE = "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"
CONTENT = "04835264a8fa4a43e5317b842581b7b080620e77e555487574b0307579139db9"
SCOPE = "Original naturally scheduled scanner empty and content"
PHASES = ("preflight.actual-exit", "run.actual-exit", "proof.actual-exit", "actual-exit")


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(body):
    return sha256(body).hexdigest()


def regular(path, ceiling):
    require(path.resolve() == path and path.is_file(), "exact sealed Source scanner file required")
    require(path.stat().st_size <= ceiling, "bounded Source scanner file required")
    return path.read_bytes()


def is_sha(value):
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{64}", value))


def repo_for(context, default, candidate):
    provided = Path(context.get("repository", default))
    require(provided.is_absolute() and provided.resolve() == provided and provided.is_dir(),
            "exact canonical Original scanner repository required")
    if provided != default:
        manifest = Path(candidate["build_manifest_file"])
        require(manifest.resolve() == manifest and manifest.is_file()
                and manifest.is_relative_to(provided / "artifacts/rust")
                and digest(regular(manifest, 8 * 1024 * 1024)) == candidate["build_manifest_sha256"],
                "alternate Original scanner repository must own the common verified package manifest")
    return provided


def checked_reduction(value):
    require(type(value) is dict and value.keys() == {"scope", "immutable_Original_image", "Original_archive_sha256",
                "actual_source_arms", "actual_Native_arms", "exact_cleanup",
                "retained_content_predecessor_sha256", "actual", "formal_m6_credit"}
            and (value["scope"], value["immutable_Original_image"], value["Original_archive_sha256"])
                == (SCOPE, IMAGE, ARCHIVE)
            and type(value["actual_source_arms"]) is int and value["actual_source_arms"] == 1
            and type(value["actual_Native_arms"]) is int and value["actual_Native_arms"] == 0
            and value["exact_cleanup"] is True and value["formal_m6_credit"] is False
            and value["retained_content_predecessor_sha256"] == CONTENT,
            "exact Source-only scanner scope and retirement required")
    proof = value["actual"]
    require(type(proof) is dict and proof.keys() == {"actual_results_sha256", "empty_journal_sha256",
                                                   "content_journal_sha256", "saved_row_count"}
            and type(proof["saved_row_count"]) is int and proof["saved_row_count"] == 2
            and type(proof["actual_results_sha256"]) is list and len(proof["actual_results_sha256"]) == 3,
            "two exact saved scanner claims and three worker terminals required")
    stamps = proof["actual_results_sha256"] + [proof["empty_journal_sha256"], proof["content_journal_sha256"]]
    require(all(is_sha(stamp) for stamp in stamps) and len(set(stamps)) == len(stamps),
            "distinct literal Source scanner journals and worker results required")
    return proof


def resolve(context, repository, candidate):
    require(type(context) is dict and set(context) in (
                {"stage", "seal", "proof_sha256"}, {"stage", "seal", "proof_sha256", "repository"})
            and context["seal"] == STAGE_SEAL and is_sha(context["proof_sha256"]),
            "explicit sealed Original scanner evidence context required")
    repo = repo_for(context, repository, candidate)
    stage = Path(context["stage"])
    require(stage == repo / STAGE and stage.resolve() == stage and stage.is_dir(),
            "exact reviewed Original scanner stage path required")
    manifest = regular(stage / "SHA256SUMS", 4096)
    declaration = "".join(f"{FILES[name]}  {name}\n" for name in FILES).encode()
    require(manifest == declaration and digest(manifest) == STAGE_SEAL
            and all(digest(regular(stage / name, 65536)) == stamp for name, stamp in FILES.items()),
            "immutable Original scanner reducer and companion inputs required")
    ledger = stage / "owner-original-1"
    require(all(regular(ledger / phase, 16) == b"0\n" for phase in PHASES)
            and regular(stage / "actual-original-1/actual-exit", 16) == b"0\n",
            "literal settled Original scanner launch, proof and cleanup required")
    retained = regular(ledger / "proof.stdout", 65536)
    require(digest(retained) == context["proof_sha256"], "exact retained Original scanner proof hash")
    proof = checked_reduction(json.loads(retained))
    # This is the pinned read-only verifier. The run and hidden lane actions
    # are never reachable from this authority context.
    actual = subprocess.run(["/usr/bin/python3", "-E", "-s", "-B", str(stage / "run.py"), "--proof"],
                            cwd=repo, capture_output=True, timeout=30, check=False)
    require(actual.returncode == 0 and not actual.stderr and actual.stdout == retained,
            "retained Source scanner reduction must match raw actual evidence")
    return {"scope": NODE, "original_revision": SOURCE_REVISION, "image": IMAGE, "archive_sha256": ARCHIVE,
            "evidence_cases": ["empty", "content"], "proof_sha256": digest(retained), "stage_seal": STAGE_SEAL,
            "raw": proof, "exact_cleanup": True, "canonical_row_passed": False, "native_executed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", type=Path)
    parser.add_argument("context", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    resolution = resolve(json.loads(regular(args.context, 65536)), args.repository,
                         json.loads(regular(args.candidate, 65536)))
    print(json.dumps(resolution, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
