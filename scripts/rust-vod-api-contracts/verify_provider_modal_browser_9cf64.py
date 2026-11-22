"""Verify the portable 9cf64 provider modal projection using only the standard library."""

from argparse import ArgumentParser
from hashlib import sha256
import json
from pathlib import Path
import re

DEFAULT = Path(__file__).with_name("verified-provider-modal-browser-9cf64-1.json")
EVIDENCE = "e0b4ac218c67450d3a1a48fc5102da1966efef5bf64a8d46139104a4538632d2"
EXPECTED_APPLICATION = {
    "revision": "9cf640701dacea07ea5f394ce7b18a6fa7c8c088",
    "published_binding_revision": "49fa7420937664064b3d3b04914b9f366fc3feab",
    "image_id": "sha256:fd1a839e1fce4835039b7b079cd0929cd3c658261374cea2c7bbebf813fae3a9",
    "build_manifest_sha256": "19d618bfe72657a0adfc65fd93094ad42cbaffbe8b609bdc68ba8eddfb7e17a1",
    "source_tree_sha256": "d882ac35a940175173a8293595de8b91a00a0372442c12230f3c279e8c603e6c",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def cleanup(arms):
    return {"owned_containers_absent": 5, "remaining_containers": 0, "remaining_networks": 0,
            "remaining_volumes": 0, "browser_process_groups_and_profiles_absent": arms}


def check_semantics(rows):
    require([row["modal"] for row in rows] == ["movie", "series"], "modal order")
    for row in rows:
        require(row["provider_relation_order"] == [1, 2] and row["source_default_account"] == 2
                and row["initial_UI_relation"] == 1, "default and ordered providers")
        choices = row["selections"]
        require([(choice["relation"], choice["account"], choice["provider_info_status"]) for choice in choices]
                == [(1, 3, 200), (2, 2, 200)], "ordered provider switch results")
        episode_order = [[], []] if row["modal"] == "movie" else [[1, 2], [2, 3]]
        require([choice["episode_order"] for choice in choices] == episode_order, "provider-specific episode order")
        expected_artwork = [{"role": "poster", "decoded_pixels": [2, 3]}]
        if row["modal"] == "series":
            expected_artwork.append({"role": "episode", "decoded_pixels": [2, 3]})
        require(all(choice["artwork"] == expected_artwork for choice in choices), "decoded artwork")


def verify(raw):
    require(not re.search(rb"/home/|/tmp/|https?://|Bearer |csrftoken|[?&]v=", raw), "scope-sensitive data present")
    value = json.loads(raw)
    require(value["schema"] == 1 and value["application"] == EXPECTED_APPLICATION, "exact published application")
    require(value["original"] == {"revision": "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430",
                                  "source_archive_sha256": "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff"},
            "exact Original archive")
    first = value["first_attempt"]
    require(first == {"exits": {"campaign": 1, "browser": 1, "cleanup": 0}, "original_document": "about:blank",
                      "original_API_responses": 0, "original_asset_responses": 0, "cleanup": cleanup(["Original"])},
            "retained failed attempt")
    control = value["loopback_diagnosis"]
    require(control["with_switch"] == {"navigation_error": "net::ERR_ABORTED", "HTTP_requests": 0}
            and control["without_switch"] == {"login_form_reached": True, "exact_browser_cleanup": True},
            "controlled browser diagnosis")
    retry = value["retry"]
    require(retry["exits"] == dict.fromkeys(("campaign", "browser", "cleanup"), 0)
            and retry["cleanup"] == cleanup(["Original", "native"]), "successful retry and cleanup")
    require(retry["frontend_files"] == 112 and re.fullmatch("[a-f0-9]{64}", retry["frontend_inventory_sha256"]),
            "frontend inventory")
    require(list(retry["arms"]) == ["Original", "native"] and retry["arms"]["Original"] == retry["arms"]["native"],
            "paired browser observations")
    arm = retry["arms"]["Original"]
    require({key: arm[key] for key in ("exit", "VOD_HTTP_failures", "forbidden_requests", "uncaught_exceptions")}
            == dict.fromkeys(("exit", "VOD_HTTP_failures", "forbidden_requests", "uncaught_exceptions"), 0),
            "browser terminal values")
    require(arm["normal_logins"] == 1 and arm["successful_navigations"] == 2
            and re.fullmatch("[a-f0-9]{64}", arm["ordered_observation_sha256"]), "actual browser arm")
    rows = retry["ordered_semantics"]
    check_semantics(rows)
    canonical = json.dumps(rows, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    require(sha256(canonical).hexdigest() == retry["ordered_semantics_sha256"], "ordered semantic digest")
    require(retry["local_artwork_requests"] == {"episode-a": 2, "episode-b": 2, "poster-a": 4, "poster-b": 4}
            and retry["exact_restoration"] == {"metadata_models": 7, "normal_user_row_microseconds_preserved": True}
            and retry["provider_contact"] is retry["Accept_override"] is False, "fixture scope and restoration")
    require(value["formal_M9_row_promoted"] is value["application_performance_measured"] is False, "scoped qualification")
    require(len(value["private_receipt_sha256"]) == 12 and all(re.fullmatch("[a-f0-9]{64}", digest)
            for digest in value["private_receipt_sha256"].values()), "private receipt digests")
    require(sha256(raw).hexdigest() == EVIDENCE, "exact public evidence bytes")
    return {"passed": True, "revision": EXPECTED_APPLICATION["revision"], "ordered_modal_types": [row["modal"] for row in rows],
            "retained_first_attempt_exit": first["exits"]["campaign"], "retry_exit": retry["exits"]["campaign"],
            "formal_M9_row_promoted": False, "engine_calls": 0}


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path, nargs="?", default=DEFAULT)
    args = parser.parse_args()
    print(json.dumps(verify(args.evidence.read_bytes()), sort_keys=True))


if __name__ == "__main__":
    main()
