"""Read-only, fail-closed admission of fresh physical Original media evidence."""

import hashlib
import json
from pathlib import Path
import re
import subprocess

NODE = "source/media-profile-and-format-workflows"
ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
CACHE = "source/cache-and-profile-contexts"
STAGE = Path("artifacts/rust/m9-original-shared-media-cohort-8")
MATRIX = Path("scripts/rust-milestone-batches/m235/matrix.json")
MATRIX_SHA = "33b7afc473a4469b9085b42413b285a1e43effdfd21be2e3616dfb31c88a306d"
STAGE_SHA = "aaf57c627219761674e62ce80c9935af256f49832704dfb01b606079f7ded78c"
RAW = STAGE / "actual-original-1/report.json"
LEDGER = STAGE / "owner-ledger-1"
QUALIFICATION = LEDGER / "proof.stdout"
SHA = re.compile("[a-f0-9]{64}")


def need(value, name):
    if not value:
        raise ValueError(name)


def checked(repo, relative, maximum, stamp=None):
    path = repo / relative
    need(path.is_file() and path.resolve() == path, "ExactRegularFile:" + str(relative))
    with path.open("rb") as stream:
        data = stream.read(maximum + 1)
    need(len(data) <= maximum, "BoundedEvidenceSize:" + str(relative))
    if stamp is not None:
        need(sha(data) == stamp, "ExactEvidenceDigest:" + str(relative))
    return data


def sha(data):
    return hashlib.sha256(data).hexdigest()


def binding(qualification, raw):
    need(all(isinstance(value, str) and SHA.fullmatch(value) for value in (qualification, raw)), "SHA256Binding")
    return {"Original_git": ORIGINAL, "qualification": {"path": str(QUALIFICATION), "sha256": qualification},
            "raw": {"path": str(RAW), "sha256": raw}}


def typed_origin(row):
    if type(row) is not dict:
        return False
    return (
        row.get("method") == "GET" and type(row.get("status")) is int and row["status"] == 200
        and type(row.get("active_at_headers")) is int and row["active_at_headers"] == 1
        and all(type(row.get(key)) is int and row[key] == 0 for key in ("active_at_entry", "active_at_exit"))
        and {"request_range", "content_range"} <= row.keys()
        and (row["request_range"] is None or (type(row["request_range"]) is str
             and re.fullmatch(r"bytes=(?:\d{1,18}-\d{0,18}|-\d{1,18})", row["request_range"])))
        and (row["content_range"] is None or (type(row["content_range"]) is str
             and re.fullmatch(r"bytes (?:\d{1,18}-\d{1,18}/(?:\d{1,18}|\*)|\*/\d{1,18})", row["content_range"])))
    )


def project(proof, cache, receipt_sha):
    """Map the sealed cohort reducer's exact schema to the canonical assertions."""
    need(set(proof) == {
        "scope", "origin_wire", "original_only", "exact_owned_cleanup", "vlc", "decoded_av_observations",
        "joined_finite", "owned_media_child_loss_and_fresh_request_recovery", "owned_recovery_origin_wire",
        "ordinary_profile_stop_and_reopen", "ordinary_live_range", "physical_exit",
        "formal_source_media_observations_verified", "formal_dispatcher_status_granted", "remaining",
        "retained_cache", "raw_receipt_sha256",
    }, "ExactOriginalMediaProjectionSchema")
    need(proof["original_only"] is proof["exact_owned_cleanup"] is proof["formal_source_media_observations_verified"] is True
         and type(proof["physical_exit"]) is int and proof["physical_exit"] == 0
         and proof["formal_dispatcher_status_granted"] is False and proof["remaining"] == []
         and proof["raw_receipt_sha256"] == receipt_sha, "FreshPhysicalOriginalAndCleanup")
    need(cache["status"] == "retained_source_authority" and cache["satisfies_dependency"] is True
         and cache["retained_source"] == proof["retained_cache"], "ExactRetainedCacheDependency")
    need(proof["scope"] == ["distinct", "fmp4", "hls"]
         and type(proof["decoded_av_observations"]) is int and proof["decoded_av_observations"] == 16,
         "ExactMPEGTSHLSFMP4AndAV")
    vlc = proof["vlc"]
    need(vlc["decoders"] == 2 and type(vlc["decoders"]) is int and vlc["separate_web_generation"] is True
         and vlc["actual_vlc_argv"]["actual_executable"] in ("vlc", "cvlc")
         and all(vlc["actual_vlc_argv"][key] is True for key in
                 ("dummy_interface", "actual_user_agent_argument_present", "owned_media_url", "stdout_mpegts_transcode")),
         "ExactOwnedVLCArgvAndA/V")
    need(proof["joined_finite"] == {"real_primary_eofs": 3, "real_fallback": 1, "origin_maximum_active": 1,
         "decoded_av_windows": 4, "recovered_viewers": 2}, "ExactNaturalSourceSwitch")
    need(proof["owned_media_child_loss_and_fresh_request_recovery"] is proof["ordinary_profile_stop_and_reopen"] is True,
         "ExactOwnedChildAndIndependentReopen")
    need(proof["ordinary_live_range"] == {"method": "GET", "range": "bytes=0-187", "status": 200,
         "content_range": None, "matched_complete_segment": True}, "OriginalLiveRangeWireObservation")
    ledger = proof["origin_wire"]
    need(len(ledger) == 3 and [row["scope"] for row in ledger] == ["hls", "fmp4", "distinct"]
         and all(typed_origin(row) for row in (*ledger, vlc["wire"], proof["owned_recovery_origin_wire"])),
         "ExactActiveOwnedOriginAndRangeLedger")
    return {
        "formats": ["mpegts", "hls", "fmp4"],
        "profile_processes": ["ffmpeg", "vlc"], "decoded_av_observations": 16,
        "sharing": "equivalent viewers share one transform, distinct OutputProfiles have distinct process generations",
        "per_profile_keys": "Original fMP4 shared transform and remux ownership and active state read from Redis",
        "source_switch": proof["joined_finite"],
        "process_recovery": "exact owned FFmpeg child loss, on-demand fresh authenticated recovery with raw survivor",
        "live_range": proof["ordinary_live_range"],
        "origin_request_ledgers": "bounded actual GETs, Range and Content-Range fields, one active origin, retirement",
        "exact_owned_cleanup": True,
    }


def resolve(ref, repo, cache):
    repo = Path(repo).resolve()
    need(type(ref) is dict and all(type(ref.get(key)) is dict for key in ("qualification", "raw")),
         "ExactOriginalMediaReference")
    proof_sha, raw_sha = (ref[key].get("sha256") for key in ("qualification", "raw"))
    need(ref == binding(proof_sha, raw_sha), "ExactOriginalMediaReference")
    matrix = json.loads(checked(repo, MATRIX, 1024 * 1024, MATRIX_SHA))
    [node] = [item for item in matrix["nodes"] if item["id"] == NODE]
    need(node["depends_on"] == [CACHE] and node["runner"] == "Source-media-workflow", "CanonicalSourceMediaIdentity")
    checked(repo, STAGE / "SHA256SUMS", 16384, STAGE_SHA)
    subprocess.run(["sha256sum", "--strict", "--status", "-c", "SHA256SUMS"], cwd=repo / STAGE, check=True, timeout=20)
    for name in ("actual-exit", "preflight.actual-exit", "physical.actual-exit", "proof.actual-exit"):
        need(checked(repo, LEDGER / name, 16) == b"0\n", "ExactFreshWorkflowTerminal:" + name)
    for name in ("actual-exit", "controller-exit"):
        need(checked(repo, RAW.parent / name, 16) == b"0\n", "ExactFreshRawTerminal:" + name)
    checked(repo, RAW, 2 * 1024 * 1024, raw_sha)
    qualified = checked(repo, QUALIFICATION, 64 * 1024, proof_sha)
    actual = subprocess.run(["/usr/bin/python3", "-B", str(repo / STAGE / "run.py"), "proof"], cwd=repo,
                            check=True, capture_output=True, timeout=60)
    need(actual.stdout == qualified and not actual.stderr, "IndependentStrictOriginalMediaReplay")
    assertions = project(json.loads(qualified), cache, raw_sha)
    need(checked(repo, QUALIFICATION, 64 * 1024, proof_sha) == qualified, "StableOriginalQualification")
    checked(repo, RAW, 2 * 1024 * 1024, raw_sha)
    return {"id": NODE, "ready": False, "status": "retained_source_authority", "satisfies_dependency": True,
            "reasons": [], "retained_source": ref, "assertions": assertions, "new_candidate_passes": 0}
