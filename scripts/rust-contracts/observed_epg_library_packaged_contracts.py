"""Attach the eight scoped paired EPG library JSON GETs."""
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json

BASE = "scripts/rust-contracts/observed-epg-library-4d3c"
PACKET = f"{BASE}/epg-library.json"
VALIDATOR = f"{BASE}/verify.py"
INPUT_SHA256 = {
    PACKET: "c77847527f783e55bc179d04c927d0f87c0bf83b8744b79f5deec71791f65b91",
    VALIDATOR: "01c0d10e844a96df80e5a610c5e0e9087a6861bab01034c679296d6f72a6eb51",
}


def apply(root, rows):
    for path, stamp in INPUT_SHA256.items():
        assert sha256((root / path).read_bytes()).hexdigest() == stamp
    spec = spec_from_file_location("published_epg_library_verifier", root / VALIDATOR)
    verify = module_from_spec(spec)
    spec.loader.exec_module(verify)
    payload = (root / PACKET).read_bytes()
    assert not verify.FORBIDDEN.search(payload)
    bindings = verify.project(rows, json.loads(payload), PACKET, INPUT_SHA256[PACKET])
    assert len(bindings) == 4 and sum(len(binding["cases"]) for binding in bindings.values()) == 8
    for route in rows:
        for operation in route["operations"]:
            contract_id = operation["contract_id"]
            if contract_id in bindings:
                assert "case_bindings" not in operation and operation["fully_verified"] is False
                operation["case_bindings"] = bindings[contract_id]
