"""Rebind the reviewed recording61 owner to an exact frozen ordinary AMD64 package."""
import argparse
import json
from pathlib import Path
import re

from recording61_owner import COORDINATOR_PROTOCOL, digest, read, require, stage_inputs

TEMPLATE_SEAL = "3a8e6bde76d862406ca7f8ccd30e86cdeb987f48d077248a888e9b7350e49919"
FILES = ("run.py", "scenario.py", "fixture_adapter.py", "candidate.py",
         "external-pins.json", "inert-input-allowlist.json", "origin61.py", "reduce.py", "retained.py")


def rebind_package(old, candidate):
    anchor = next(path for path in old["pins"] if path.endswith("/package/build.json"))
    manifest = Path(candidate["build_manifest_file"])
    build = read(manifest)
    package = manifest.parents[2]
    require(digest(manifest) == candidate["build_manifest_sha256"]
            and (build["git_revision"], build["image_id"], build["platform"], build["source_archive_sha256"])
            == (candidate["git"], candidate["image_id"], "linux/amd64", candidate["source_archive_sha256"]),
            "recording61 frozen package identity differs")
    require(digest(manifest.with_name("source.tar")) == candidate["source_archive_sha256"],
            "recording61 frozen package archive differs")
    require(re.fullmatch(r"[a-f0-9]{40}", candidate["git"])
            and re.fullmatch(r"sha256:[a-f0-9]{64}", candidate["image_id"]), "invalid build identities")

    # Keep the original admissible receipt set and fixture code. Replace only
    # the build identity, its package path, and the exact source inventory count.
    binding = dict(old, candidate=candidate["git"], image=candidate["image_id"],
                   manifest=candidate["build_manifest_sha256"], archive=candidate["source_archive_sha256"],
                   source_files=len(build["source_sha256"]), source_tree=build["source_tree_sha256"])
    former_package = Path(anchor).parents[2]
    binding["pins"] = {}
    for path in old["pins"]:
        item = package / Path(path).relative_to(former_package)
        binding["pins"][str(item)] = digest(item)
    require(str(manifest) in binding["pins"], "recording61 package uses the ordinary manifest layout")
    return manifest, binding


def prepare(template, output, candidate):
    old = read(template / "package-binding.json")
    anchor = next(path for path in old["pins"] if path.endswith("/package/build.json"))
    original_candidate = {"git": old["candidate"], "image_id": old["image"],
                          "build_manifest_sha256": old["manifest"], "build_manifest_file": anchor,
                          "source_archive_sha256": old["archive"]}
    verified = stage_inputs(template, TEMPLATE_SEAL, original_candidate)
    require(all(str(template / name) in verified for name in FILES), "unsealed recording fixture input")
    require(output.resolve() == output and not output.exists(), "fresh absolute owner directory required")
    manifest, binding = rebind_package(old, candidate)
    files = {name: (template / name).read_bytes() for name in FILES}
    for name in ("candidate.py", "run.py"):
        source = files[name].decode()
        for key in ("candidate", "image", "manifest"):
            require(source.count(old[key]) == 1, "unexpected reviewed build binding shape")
            source = source.replace(old[key], binding[key])
        if name == "run.py":
            source, count = re.subn(r'^PACKAGE = ART / .+$', lambda _: f"PACKAGE = Path({str(manifest)!r})",
                                    source, flags=re.MULTILINE)
            require(count == 1, "unexpected reviewed package path shape")
            for before, after in (
                ("def run(base, package):", "def run(base, package, coordinator_lease):"),
                ('with lease.fixture_lease(SHARED / ".dispatcher.lock"):',
                 'with coordinator_lease(SHARED / ".dispatcher.lock"):'),
                ("def main():", "def main(argv=None, coordinator_lease=None):"),
                ("args = parser.parse_args()", "args = parser.parse_args(argv)"),
                ("return run(binding, package)",
                 'need(coordinator_lease is not None, "reviewed coordinator lease required")\n'
                 '            return run(binding, package, coordinator_lease)'),
            ):
                require(source.count(before) == 1, "unexpected reviewed lease boundary")
                source = source.replace(before, after)
            source = source.replace('    lease = load(LEASE, "ordinary_recording61_global_lease")\n', '')
        else:
            source = source.replace(f"=={old['source_files']}==", f"=={binding['source_files']}==")
        files[name] = source.encode()
    files["package-binding.json"] = (json.dumps(binding, indent=2, sort_keys=True) + "\n").encode()
    files["coordinator-mode.json"] = (json.dumps(COORDINATOR_PROTOCOL) + "\n").encode()
    output.mkdir(mode=0o700)
    for name, content in files.items():
        (output / name).write_bytes(content)
    sums = output / "SHA256SUMS"
    sums.write_text("".join(f"{digest(output / name)}  {name}\n" for name in sorted(files)))
    seal = digest(sums)
    (output / "workload-seal.sha256").write_text(seal + "\n")
    return {"stage": str(output), "seal": seal}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("template", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.template, args.output, read(args.candidate)), sort_keys=True))


if __name__ == "__main__":
    main()
