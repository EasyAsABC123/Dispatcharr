"""Emit canonical unsigned25 bindings from one already frozen Root candidate."""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
NODE = "native/plugin-unsigned25"


def bind(document, candidate_file, owner_file, output):
    assert candidate_file.resolve(strict=True) == candidate_file
    assert not output.exists() and not output.is_symlink()
    assert output == output.resolve() and output.is_relative_to(HERE.parents[3] / "artifacts/rust")
    assert NODE not in document["nodes"]
    candidate = json.loads(candidate_file.read_bytes())
    assert candidate == document["candidate"]
    path = HERE.parent / "batch.py"
    spec = importlib.util.spec_from_file_location("unsigned25_m478", path)
    owner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owner)
    assert owner.candidate_ready(candidate)
    command = [sys.executable, "-B", str(HERE / "run.py"), "--emit-binding", "--scope", "unsigned25",
               "--candidate", str(candidate_file), "--output", str(output)]
    raw = subprocess.check_output(command, timeout=30)
    row = json.loads(raw)
    assert row["id"] == NODE and row["covers_existing_cells"] == [NODE]
    matrix, nodes, _ = owner.load()
    assert nodes[NODE]["declared_calls"] == 25 and nodes[NODE]["requires"] == []
    assert row["entrypoint"] == nodes[NODE]["entry_point"]
    assert row["resources"] == nodes[NODE]["exclusive_resources"]
    prior_file = document.get("owner_bindings", {}).get("m478")
    bound = owner.bindings(prior_file) if prior_file else {"candidate": candidate, "nodes": {}}
    assert bound["candidate"] == candidate and NODE not in bound["nodes"]
    bound["nodes"][NODE] = row | {"input_files": row["input_sha256"]}
    spec = owner.dispatch_spec(NODE, matrix, bound)
    assert spec["argv"] == row["argv"] and spec["evaluator_argv"] == row["evaluator_argv"]
    return bound, document | {"nodes": document["nodes"] | {NODE: row},
        "owner_bindings": document.get("owner_bindings", {}) | {"m478": str(owner_file)}}


def main():
    assert __debug__ and sys.dont_write_bytecode
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("document", "candidate", "owner", "output", "bindings"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    assert args.owner.is_absolute() and args.bindings.is_absolute()
    assert args.owner != args.bindings and all(not path.exists() and not path.is_symlink()
        for path in (args.owner, args.bindings))
    owner, combined = bind(json.loads(args.document.read_bytes()), args.candidate, args.owner, args.output)
    for path, data in ((args.owner, owner), (args.bindings, combined)):
        with path.open("x") as stream:
            json.dump(data, stream, indent=2, sort_keys=True)
            stream.write("\n")


if __name__ == "__main__":
    main()
