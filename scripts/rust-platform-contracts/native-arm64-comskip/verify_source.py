"""Verify and stage a commit-pinned Comskip source candidate."""
from hashlib import sha1, sha256
import argparse
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile


HERE = Path(__file__).resolve().parent
BASE = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
REVISION = "a140b6ac8bc8f596729e9052819affc779c3b377"
TREE = "9e05f1ec66b6599ea1977fdb448c6ed57beb0fe5"
PINS = {
    "upstream.tar.gz": "5563d65c0cc5f76883e26b9e460348b4826cc496d10f8d53b25e7648e47f87c2",
    "source-tree.json": "fea3cc7b7aae43162146894f74cad29da56593a7ce4a6849fe9eaf6f6aa921ba",
}


def stamp(data):
    return sha256(data).hexdigest()


def git_object(kind, data):
    head = kind + b" " + str(len(data)).encode() + b"\0"
    return sha1(head + data, usedforsecurity=False).hexdigest()


def verify_tree(rows, folder="."):
    name = PurePosixPath(folder)
    children = [row for path, row in rows.items() if PurePosixPath(path).parent == name]
    children.sort(key=lambda row: PurePosixPath(row["path"]).name + ("/" if row["type"] == "tree" else ""))
    payload = bytearray()
    for row in children:
        kind, mode = row["type"], row["mode"]
        assert (kind, mode) in {("tree", "040000"), ("blob", "100644"), ("blob", "100755")}
        if kind == "tree":
            assert verify_tree(rows, row["path"]) == row["sha"]
        entry = PurePosixPath(row["path"]).name
        payload.extend(f"{int(mode, 8):o} {entry}".encode() + b"\0" + bytes.fromhex(row["sha"]))
    return git_object(b"tree", payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--original-recipe", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    assert stamp(args.archive.read_bytes()) == PINS["upstream.tar.gz"]
    tree_file = HERE / "source-tree.json"
    assert stamp(tree_file.read_bytes()) == PINS["source-tree.json"]
    api = json.loads(tree_file.read_bytes())
    assert api["sha"] == TREE and api["truncated"] is False
    rows = {row["path"]: row for row in api["tree"]}
    assert len(rows) == len(api["tree"]) and verify_tree(rows) == TREE
    root = "Comskip-" + REVISION
    files = {}
    seen = set()
    with tarfile.open(args.archive, "r:gz") as archive:
        for member in archive:
            parts = PurePosixPath(member.name)
            assert not parts.is_absolute() and parts.parts[0] == root and ".." not in parts.parts
            if parts.parts == (root,):
                assert member.isdir()
                continue
            name = PurePosixPath(*parts.parts[1:]).as_posix()
            assert name not in seen
            seen.add(name)
            row = rows[name]
            if member.isdir():
                assert row["type"] == "tree"
                continue
            assert member.isfile() and row["type"] == "blob"
            raw = archive.extractfile(member).read()
            assert len(raw) == row["size"] and git_object(b"blob", raw) == row["sha"]
            assert bool(member.mode & 0o111) == (row["mode"] == "100755")
            files[name] = raw
    assert seen == rows.keys()
    original = args.original_recipe.read_bytes()
    assert stamp(original) == "3195943a1d2dc301c986d55eddc44d14b1fb46b01495251a7b4f006d6270375b"
    assert stamp(files["LICENSE"]) == "db296f2f7f35bca3a174efb0eb392b3b17bd94b341851429a3dff411b1c2fc73"
    sed = re.search(rb"    && (sed -i \\\n(?:[^\n]*\\\n)+        mpeg2dec.c) \\", original)
    assert sed is not None and stamp(sed[1]) == "8c4a74a51cb06820c1bce908602cedd5f978124482960831d693c120657207a5"
    source = {"revision": REVISION, "git_tree": TREE, "files": len(files), "directories": len(rows) - len(files),
              "source_archive_sha256": PINS["upstream.tar.gz"], "upstream_license_sha256": stamp(files["LICENSE"]),
              "original_recipe_revision": BASE, "original_recipe_sha256": stamp(original),
              "compatibility_patch_command_sha256": stamp(sed[1]),
              "credit": "Original Dispatcharr, Comskip and dependency authors retain full credit."}
    target = args.target
    target.mkdir(mode=0o700)
    for name, data in files.items():
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(data)
        path.chmod(int(rows[name]["mode"], 8) & 0o777)
    subprocess.run(["bash", "-euc", sed[1]], cwd=target, check=True, timeout=15)
    patched = (target / "mpeg2dec.c").read_bytes()
    source["patched_mpeg2dec_sha256"] = stamp(patched)
    assert source["patched_mpeg2dec_sha256"] == "84cbe059a233f65a79bc639bc787f8e03dda055a10343773bf775459b11053b5"
    print(json.dumps(source, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
