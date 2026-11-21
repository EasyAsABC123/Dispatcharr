"""Pin already-executed ordinary Original React management route observations."""

import hashlib
import json
import re

ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ALIAS = "scripts/rust-integration/m0-channel-profile-json-aliases-paired.json"
MEDIA = "scripts/rust-integration/m5-m6-cf64fbfe-profile-and-xc-expiration.json"
GROUP = "scripts/rust-integration/m5-2025258-group-manager-stock-options.json"
INPUT_SHA256 = {
    ALIAS: "e79c1402cab049c41fe34966c21658fd933cea6ad99cc77f09c59a4c0553e820",
    MEDIA: "3cb62a48a98d83e9bb8a5212ced14ee6fd3e8b59dd6bc29d28dfae70397d7088",
    GROUP: "b85e9f079bcfa739e72e6769305691770381c1eabeb968b029b0a80b3f782c32",
}
PROFILES = {
    ("list", False): (115, "route-f140a95f4ea400cc74f0", "api/channels/^profiles/$"),
    ("list", True): (116, "route-46bf8895f1099c41714b", r"api/channels/^profiles\.(?P<format>[a-z0-9]+)/?$"),
    ("detail", False): (117, "route-bc28dbffca264c415359", r"api/channels/^profiles/(?P<pk>[^/.]+)/$"),
    ("detail", True): (118, "route-4b846379969a5288d58b", r"api/channels/^profiles/(?P<pk>[^/.]+)\.(?P<format>[a-z0-9]+)/?$"),
}
GROUPS = {
    "collection": (59, "route-db3a78dbace6356b715c", "api/channels/^groups/$", "GET, POST, HEAD, OPTIONS"),
    "detail": (63, "route-bcad6fcc7a44673cca5c", r"api/channels/^groups/(?P<pk>[^/.]+)/$", "GET, PUT, PATCH, DELETE, HEAD, OPTIONS"),
    "claim-custom": (65, "route-e0adb9f74703029f9925", r"api/channels/^groups/(?P<pk>[^/.]+)/claim-custom/$", "POST, OPTIONS"),
}
RENDERERS = {
    "wildcard-subtype": ("application/*; indent=2", 2),
    "wildcard-all": ("*/*; indent=3", 3),
    "specific-range-selects-compact": ("application/json, application/*; indent=2", None),
    "media-parameter-space": ("application/json; indent = 2", 2),
    "zero-selects-compact": ("application/json; indent=0", None),
    "negative-selects-compact": ("application/json; indent=-1", None),
    "mixed-ranges-select-specific": ("text/html, application/json; indent=4", 4),
}


def load(root, name):
    raw = (root / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == INPUT_SHA256[name]
    return json.loads(raw)


def reference(path, pointer):
    return {"path": path, "sha256": INPUT_SHA256[path], "pointer": pointer,
            "backend": "Original", "source_revision": ORIGINAL}


def route_for(rows, metadata, handler):
    ordinal, identifier, pattern = metadata
    route = rows[ordinal]
    assert route["resolver_ordinal"] == ordinal and route["route_id"] == identifier
    assert route["pattern_exact"] == pattern and route["source_handler"] == "apps.channels.api_views." + handler
    return route


def evidence_shape(role, kind, observation):
    assert observation["status"] == 200 and observation["mime"] == "application/json"
    assigned = {"name": "owned-m0-profile-roles-assigned", "channels": ["owned-m0-profile-roles-visible"]}
    peer = {"name": "owned-m0-profile-roles-peer", "channels": ["owned-m0-profile-roles-peer"]}
    typed = observation["typed_payload"]
    if kind == "detail":
        assert typed == (assigned if role == "standard" else peer)
        return 1
    assert type(typed) is list and sorted(typed, key=lambda row: row["name"]) == (
        [assigned] if role == "standard" else [assigned, peer])
    return len(typed)


def check_wire(witness, field):
    size = witness[field + "_bytes"]
    stamp = witness[field + "_sha256"]
    assert type(size) is int and 0 < size <= 16384 and re.fullmatch("[0-9a-f]{64}", stamp)
    return {"body_bytes": size, "body_sha256": stamp}


def profiles(root, rows):
    alias = load(root, ALIAS)
    media = load(root, MEDIA)["profile_media"]
    assert alias["original_revision"] == media["original_revision"] == ORIGINAL
    assert alias["literal_pair_exit"] == media["paired_controller_exit"] == 0
    assert alias["owned_SQL_preserved_through_reads"] and media["owned_SQL_preserved"]
    assert alias["each_arm_exact_cleanup"]["Original"]["exact_retired_roles"] == 2
    assert media["independently_retired"]["Original"]["exact_retired_roles"] == 2
    assert media["positive_explicit_json_reads_per_backend"] == 28
    assert len(media["observations"]) == len(media["wire_body_witnesses"]["Original"]) == 28
    for (kind, suffix), target in PROFILES.items():
        route = route_for(rows, target, "ChannelProfileViewSet")
        [operation] = [item for item in route["operations"] if item["method"] == "GET"]
        assert operation["contract_id"] == route["route_id"] + ":GET"
        assert operation["source_action"] == ("list" if kind == "list" else "retrieve")
        assert operation["fully_verified"] is False and operation["executable_Source_contract"]
        observations = []
        for role in ("admin", "standard"):
            base_path = "/api/channels/profiles" + ("/{assigned_or_peer_id}" if kind == "detail" else "")
            for case, request in (RENDERERS.items() if suffix else (("query", (None, None)),)):
                label = f"{role}-{kind}-{case}"
                path = base_path + (".json" if suffix else "/?format=json")
                observed = (media if suffix else alias)["observations"][label]
                projected = {"request_method": "GET", "fixture_path_template": path,
                             "role": "assigned_standard" if role == "standard" else role,
                             "case": label, "status": observed["status"],
                             "mime": observed["mime"], "visible_profile_count": evidence_shape(role, kind, observed),
                             "owned_SQL_unchanged": True, "effective_indent": request[1]}
                if suffix:
                    assert observed["effective_indent"] == request[1] and observed["same_bytes_as_selected_renderer"]
                    projected["accept"] = request[0]
                    projected.update(check_wire(media["wire_body_witnesses"]["Original"][label], "selected_body"))
                    projected["evidence"] = reference(MEDIA, f"/profile_media/observations/{label}")
                else:
                    assert observed["same_bytes_as_canonical"]
                    projected.update(check_wire(alias["body_byte_witnesses"]["Original"][label], "canonical_body"))
                    projected["evidence"] = reference(ALIAS, f"/observations/{label}")
                observations.append(projected)
        operation["scoped_Original_role_method_observations"] = observations


def groups(root, rows, mapping):
    proof = load(root, GROUP)
    assert proof["original_revision"] == ORIGINAL and proof["literal_exit"] == 0 and proof["source_only"] is True
    assert proof["ordinary_authentication"] == "Admin Bearer" and proof["eight_scoped_SQL_tables_unchanged"]
    cleanup = proof["owned_cleanup"]
    assert cleanup["compose_down_passed"] and not any(cleanup["remaining_owned"].values())
    assert cleanup["exact_container_and_process_generations_absent"] == {"baseline": True, "origin": True}
    names = ("collection", "detail", "claim-custom", "unallocated-detail", "unallocated-claim-custom")
    assert [row["case"] for row in proof["observed"]] == list(names)
    pending = mapping.setdefault("unmapped_method_Source_observations", [])
    for position, row in enumerate(proof["observed"]):
        case = row["case"].removeprefix("unallocated-")
        *identity, allow = GROUPS[case]
        route = route_for(rows, identity, "ChannelGroupViewSet")
        assert row["method"] == "OPTIONS" and "OPTIONS" not in {op["method"] for op in route["operations"]}
        assert not any(item["route_id"] == route["route_id"] and item["observed_method"] == "OPTIONS"
                       and item.get("evidence", {}).get("pointer") == f"/observed/{position}" for item in pending)
        wire = row["wire"]
        assert wire["status"] == 200 and wire["mime"] == "application/json" and wire["allow"] == allow
        assert wire["content_length"] == wire["body_bytes"] and wire["vary"] == "Accept, origin"
        assert bool(row["declared_action_fields"]) == (row["case"] != "unallocated-detail")
        assert set(row["declared_action_fields"]) == set(row["ordered_action_schema_sha256"])
        pending.append({
            "route_id": route["route_id"], "contract_id": None, "pattern_exact": route["pattern_exact"],
            "source_handler": route["source_handler"], "observed_method": "OPTIONS",
            "declared_method_set_expanded": False, "fully_verified": False,
            "scope": "canonical Group Manager JSON metadata with authenticated admin",
            "observation": {"case": row["case"], "role": "admin", "wire": wire,
                            "ordered_action_schema_sha256": row["ordered_action_schema_sha256"],
                            "owned_SQL_unchanged": True},
            "evidence": reference(GROUP, f"/observed/{position}"),
        })


def apply(root, rows, mapping):
    profiles(root, rows)
    groups(root, rows, mapping)
