"""Strict finite-float differential checks. Original authors retain credit.

This validates captured helper output, not HTTP/ORM or performance acceptance.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import struct

FIELDS = {"bits": str, "repr": str, "repr_length": int, "compact": str, "compact_size": int}


def require(value, reason):
    if not value:
        raise ValueError(reason)


def peer_rows(raw):
    require(len(raw) <= 16 * 1024 * 1024, "PeerLogCapacity")
    text = raw.decode("utf-8")
    require(re.search(r"test result: ok\. 1 passed; 0 failed;", text) is not None, "ExactlyOnePeerTestRequired")
    lines = [line.split("FINITE_FLOAT_PEER_JSON=", 1)[1] for line in text.splitlines()
             if "FINITE_FLOAT_PEER_JSON=" in line]
    require(len(lines) == 1, "UniquePeerPayloadRequired")
    return json.loads(lines[0])


def compare(reference, actual):
    require(reference.get("passed") is True, "SourceCaptureFailed")
    expected = reference.get("rows")
    require(type(expected) is list and type(actual) is list, "RowsType")
    require(0 < len(expected) <= 10000 and len(actual) == len(expected), "CaseCount")
    seen = set()
    differences = []
    for index, (source, native) in enumerate(zip(expected, actual)):
        for row in (source, native):
            require(type(row) is dict and set(row) == set(FIELDS), "FieldSet")
            require(all(type(row[key]) is kind for key, kind in FIELDS.items()), "FieldType")
            require(re.fullmatch(r"[0-9a-f]{16}", row["bits"]) is not None, "CanonicalBits")
            value = struct.unpack(">d", bytes.fromhex(row["bits"]))[0]
            require(math.isfinite(value), "FiniteOnly")
            require(row["repr_length"] == len(row["repr"]), "ReprLength")
            require(row["compact_size"] == len(row["compact"].encode("utf-8")), "UTF8Size")
        require(source["bits"] not in seen, "DuplicateSourceBits")
        seen.add(source["bits"])
        require(source["bits"] == native["bits"], "CaseIdentityOrder")
        if source != native:
            differences.append({"index": index, "bits": source["bits"],
                                "different_fields": [key for key in FIELDS if source[key] != native[key]]})
    return {"passed": not differences, "finite_cases": len(expected),
            "strict_typed_field_comparisons": len(expected) * len(FIELDS),
            "different_cases": len(differences), "differences": differences,
            "module_ORM_endpoint_acceptance": False, "performance_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--peer-log", type=Path, required=True)
    args = parser.parse_args()
    reference = args.reference.read_bytes()
    log = args.peer_log.read_bytes()
    result = compare(json.loads(reference), peer_rows(log))
    result.update(reference_sha256=hashlib.sha256(reference).hexdigest(),
                  peer_log_sha256=hashlib.sha256(log).hexdigest())
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
