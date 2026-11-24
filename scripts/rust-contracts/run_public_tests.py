"""Select the exact top-level public contract corpus without SDK duplication."""
import argparse
import json
from pathlib import Path
import sys
import unittest

PASSIVE = "test_passive_binding.py"


def cases(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("archive", type=Path)
    ap.add_argument("--list", action="store_true", help="Import and count without running tests")
    args = ap.parse_args()
    directory = args.archive.resolve()/"scripts/rust-contracts"
    # This exact archive uses only top-level test modules. If a later archive
    # adds a discoverable package, review the selection before changing scope.
    if any(p.is_dir() and (p/"__init__.py").is_file() for p in directory.iterdir()):
        raise ValueError("Review nested unittest discovery before using this selector")
    paths = sorted(directory.glob("test_*.py"))
    if not (directory/PASSIVE).is_file() or not paths:
        raise ValueError("Expected exact top-level suite and passive control")
    sys.path.insert(0, str(directory))
    loader = unittest.TestLoader()
    selected = {}
    occurrences = 0
    for path in paths:
        if path.name == PASSIVE:
            continue
        for item in cases(loader.loadTestsFromName(path.stem)):
            occurrences += 1
            selected.setdefault(item.id(), item)
    if args.list:
        if loader.errors:
            raise ValueError("\n".join(loader.errors))
        print(json.dumps({"files":len(paths), "passive_excluded":[PASSIVE],
                          "loaded_occurrences":occurrences, "unique_tests":len(selected),
                          "duplicate_occurrences":occurrences-len(selected)}, indent=2))
    else:
        success = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(selected.values()))
        return 0 if success.wasSuccessful() else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
