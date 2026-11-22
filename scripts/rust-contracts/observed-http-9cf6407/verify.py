"""Verify the minimized 9cf6407 HTTP attachment packets offline."""
from argparse import ArgumentParser
from hashlib import sha256
from itertools import product
import json
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
DIRECTORIES = {
    "route-6b777e58c3150f35aa9c:METHOD_UNRESOLVED": (143, "/api/channels/", "M8", None),
    "route-6ef4eb2b70aebf6471ee:METHOD_UNRESOLVED": (210, "/api/m3u/", "M8", None),
}
PROGRAMME = {
    "route-55aadc5558ca3b8248b1:GET": (159, "/api/epg/programs/", "M5", "GET"),
    "route-82723694fa5ab6ee5f1e:GET": (163, "/api/epg/programs/{pk}/", "M5", "GET"),
}
JSON = "application/json"
CHROME = ("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
          "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
DIRECTORY_CASES = [("GET", role, JSON, "") for role in ("anonymous", "viewer", "standard", "admin")] + [
    ("GET", "admin", variant, "") for variant in ("text/html", CHROME)
] + [(method, "admin", JSON, "") for method in ("HEAD", "OPTIONS")]
PROGRAMME_CASES = [("GET", role, accept, query) for role, (accept, query) in product(
    ("admin", "standard"), ((CHROME, ""), ("text/*", ""), (CHROME, "?format=api"), ("application/json;q=0.9, text/html;q=0.5", "")))]
CANDIDATE = {
    "revision": "9cf640701dacea07ea5f394ce7b18a6fa7c8c088",
    "image_id": "sha256:fd1a839e1fce4835039b7b079cd0929cd3c658261374cea2c7bbebf813fae3a9",
    "manifest_sha256": "19d618bfe72657a0adfc65fd93094ad42cbaffbe8b609bdc68ba8eddfb7e17a1",
    "package_seal_sha256": "1f0c2f7c6559e76114267d57d633d502f2347f6819190b21ea4ce99e95b3f7bc",
    "review_sha256": "9f36a67170b5c6f3d53ef603c2826474cc15c6590ae0a31711015cfed47e3e2e",
}
BASELINE = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
REGISTER = "49d935200a80ada4a1297cee2786257641d24e8908f388a6fc662abf9c9544cb"
PUBLISHED = "49fa7420937664064b3d3b04914b9f366fc3feab"
FORBIDDEN = re.compile(
    rb'(?i)(?:file://|/(?:home|tmp|var/run|proc|mnt|root)/|"(?:token|password|authorization|raw_body|seed|stderr|stdout)"\s*:|'
    rb'\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}|\bBearer\s+[A-Za-z0-9_\-]{20,})')


def digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)


def verify(name, checksum):
    raw = (HERE / (name + ".json")).read_bytes()
    assert sha256(raw).hexdigest() == checksum and not FORBIDDEN.search(raw)
    value = json.loads(raw)
    assert value["schema"] == 1 and value["cohort"] == name and value["candidate"] == CANDIDATE
    assert value["original_revision"] == BASELINE and value["register_snapshot_sha256"] == REGISTER
    published = value["publication"]
    assert published["ref"] == "refs/heads/rust-backend-migration"
    assert published["remote_and_receiving_commit"] == PUBLISHED
    assert published["exact_application_inputs"] == 2157 and published["protected_inputs"] == 497
    assert set(published["verification_digests"]) == {"archive", "remote", "receiving_checkout", "verification"}
    assert all(digest(h) for h in published["verification_digests"].values())
    assert type(value["physical_actual_exit"]) is int and value["physical_actual_exit"] == 0
    assert value["physical_exact_cleanup"] is True
    assert all(digest(value[key]) for key in ("physical_receipt_sha256", "physical_proof_sha256"))
    operations, cases = (DIRECTORIES, DIRECTORY_CASES) if name == "directories" else (PROGRAMME, PROGRAMME_CASES)
    assert len(value["operations"]) == len(operations)
    assert {r["contract_id"]: tuple(r[f] for f in ("resolver_ordinal", "route", "owning_milestone", "declared_method"))
            for r in value["operations"]} == operations
    rows = value["observations"]
    assert len(rows) == len(operations) * len(cases) == value["verified_physical_case_count"]
    actual = {(r["operation"], (r["method"], r["role"], r["accept"], r.get("format_query", ""))) for r in rows}
    assert actual == set(product(operations, cases))
    witnesses = value["original_receipt_digests"]
    assert set(witnesses) == {r["authority"] for r in rows}
    assert all(digest(h) for h in witnesses.values())
    for row in rows:
        observed = row["Original"]
        assert observed == row["Rust"]
        if name == "directories":
            assert observed["status"] in (200, 401, 403) and observed["body_bytes"] >= 0
            if row["accept"] != JSON:
                assert observed["independent_csrf_masks"] is True and digest(observed["template_sha256"])
            else:
                assert digest(observed["body_sha256"]) and type(observed["auth_challenge_present"]) is bool
                if row["method"] == "HEAD":
                    assert observed["body_bytes"] == 0
                    assert observed["body_sha256"] == sha256(b"").hexdigest()
                elif row["method"] == "OPTIONS":
                    assert observed["ordered_metadata_keys"]
        else:
            wire = observed["observed_HTTP"]
            assert wire["status"] == 200 and wire["content_length"] == wire["body_bytes"] > 0
            assert digest(observed["normalized_body_sha256"]) and observed["stable_repeat"] is True
            assert observed["JSON_control"]["status"] == 200 and digest(observed["JSON_control"]["typed_sha256"])
            if wire["content_type"].startswith("text/html"):
                assert observed["fresh_independent_HTML_masks"] is True and digest(observed["template_sha256"])
    return len(rows)


def main():
    assert __debug__, "run without Python optimization"
    cli = ArgumentParser(description=__doc__)
    cli.add_argument("cohort", nargs="?", choices=("directories", "programme"))
    requested = cli.parse_args().cohort
    checks = {}
    for line in (HERE / "SHA256SUMS").read_text().splitlines():
        checksum, filename = line.split()
        assert filename in ("directories.json", "programme.json") and digest(checksum)
        name = filename.removesuffix(".json")
        assert name not in checks
        checks[name] = checksum
    assert checks and (requested is None or requested in checks)
    for name in (checks if requested is None else [requested]):
        print(f"{name}: {verify(name, checks[name])} verified coordinates")


if __name__ == "__main__":
    main()
