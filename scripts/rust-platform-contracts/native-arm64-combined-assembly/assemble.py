"""Assemble same-job approved ARM64 rootfs slices without running an engine."""

import argparse
from dataclasses import dataclass
from hashlib import file_digest, sha256
import json
import mmap
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
from tempfile import TemporaryDirectory

from build_manifest import CREDIT, git

HERE = Path(__file__).resolve().parent
HEX = re.compile(r"[0-9a-f]{64}")
MANIFEST_SHA = "760640539b24e9ae88eccc7988610c76636a6b0d321025f21b96b6740a03f5b9"
CREDIT_ROOT = "usr/share/doc/dispatcharr"
HELPER = "opt/dispatcharr-pg-guarded-restore/bin/dispatcharr-pg-guarded-restore"
MAIN = "usr/local/bin/dispatcharr-rs"


@dataclass(frozen=True)
class File:
    name: str
    source: Path
    sha256: str
    mode: int
    size: int
    producer: str


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def inspect_file(path, name, producer):
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode), f"{producer}: irregular file {name}"
    mode = stat.S_IMODE(info.st_mode)
    assert not mode & 0o7002 and mode & 0o004, f"{producer}: invalid packaged mode {name}"
    return File(name, path, digest(path), mode, info.st_size, producer)


def within(name, prefix):
    return name.startswith(prefix + "/")


def named_entries(root, producer, contract):
    assert root.is_dir() and not root.is_symlink()
    allowed_files = set(contract["allowed_files"])
    prefixes = tuple(contract["allowed_prefixes"])
    result = {}
    for current, directories, names in os.walk(root, followlinks=False):
        parent = Path(current)
        for dirname in directories:
            path = parent / dirname
            assert path.is_dir() and not path.is_symlink(), f"{producer}: irregular directory"
        for filename in names:
            path = parent / filename
            relative = path.relative_to(root).as_posix()
            assert relative and all(character.isprintable() for character in relative)
            assert PurePosixPath(relative).as_posix() == relative and ".." not in PurePosixPath(relative).parts
            assert relative in allowed_files or any(within(relative, prefix) for prefix in prefixes), (
                f"{producer}: unexpected export path {relative}")
            if producer == "react":
                assert not within(relative, "opt/dispatcharr-frontend/static/rest_framework")
            result[relative] = inspect_file(path, relative, producer)
    assert set(contract["required_files"]) <= result.keys(), f"{producer}: missing required files"
    for name, expected in contract.get("required_file_sha256",{}).items():
        assert name in result and result[name].sha256 == expected, f"{producer}: pinned export file changed"
    for prefix,count in contract.get("required_prefix_counts",{}).items():
        assert sum(within(name,prefix) for name in result) == count, f"{producer}: protected path count changed"
    return result


def inventory_hash(records):
    rows = [{"path":name,"bytes":row.size,"mode":f"{row.mode:04o}","sha256":row.sha256}
            for name,row in sorted(records.items())]
    return sha256(json.dumps(rows, sort_keys=True, separators=(",",":")).encode()).hexdigest()


def verify_export(path, producer, contract, manifest, build_id):
    assert path.is_dir() and not path.is_symlink()
    assert {child.name for child in path.iterdir()} == {"rootfs","export.json"}
    evidence = path / "export.json"
    assert evidence.is_file() and not evidence.is_symlink()
    proof = json.loads(evidence.read_bytes())
    assert set(proof) == {"component", "validation_job", "source_tree_sha256", "recipe_sha256",
                          "local_source_image_config", "stage", "controller_exit", "inventory_sha256"}
    assert (proof["component"],proof["validation_job"],proof["stage"],proof["controller_exit"]) == (
        producer,build_id,contract["same_job_stage"],0)
    assert proof["source_tree_sha256"] == manifest["application_input_tree_sha256"]
    assert proof["recipe_sha256"] == contract["recipe"]["sha256"]
    assert re.fullmatch("sha256:" + HEX.pattern, proof["local_source_image_config"])
    result = named_entries(path / "rootfs",producer,contract)
    assert proof["inventory_sha256"] == inventory_hash(result)
    return proof, result


def join(all_sections, approved_credits):
    result = {}
    for section, records in sorted(all_sections.items()):
        for name, row in sorted(records.items()):
            if (other := result.get(name)) is None:
                result[name] = row
                continue
            assert name in approved_credits, f"unapproved path collision {name}"
            expected = approved_credits[name]["sha256"]
            assert other.sha256 == row.sha256 == expected and other.size == row.size, (
                f"credit bytes differ at {name}")
            assert other.producer != section
    for name in result:
        components = PurePosixPath(name).parts
        for depth in range(1,len(components)):
            assert "/".join(components[:depth]) not in result, f"path base collides with file {name}"
    return result


def check_arm(path, label):
    with path.open("rb") as stream:
        header = stream.read(20)
    assert header[:7] == b"\x7fELF\x02\x01\x01" and header[18:20] == b"\xb7\x00", label


def check_coupled_runtime(files):
    for name in (
        MAIN, "usr/local/bin/dispatcharr-operator", HELPER, "opt/dispatcharr-media/bin/ffmpeg",
        "opt/dispatcharr-media/bin/ffprobe", "opt/dispatcharr-crypto/bin/gpg",
        "opt/dispatcharr-python/bin/python3.13", "opt/dispatcharr-comskip/bin/comskip",
    ):
        check_arm(files[name].source, name)
    provenance = files[f"{CREDIT_ROOT}/pg-guarded-restore/provenance.json"].source
    assert json.loads(provenance.read_bytes())["helper_sha256"] == files[HELPER].sha256
    with files[MAIN].source.open("rb") as main:
        with mmap.mmap(main.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            assert mapped.find(files[HELPER].sha256.encode()) >= 0, "Main helper digest differs"
    checked_ini = "7614488f8addcd44850f524f5a7d104527aa6d80c25dbdf8d60219d4e658b896"
    assert files["app/docker/comskip.ini"].sha256 == checked_ini


def render_final_dockerfile(manifest):
    original = git("show",f'{manifest["verified_docs_parent"]}:rust/Dockerfile.app').decode()
    final = original.rsplit("\nFROM debian:bookworm-slim@",1)[1].splitlines()
    result = [f'FROM {manifest["final_base"]["repository"]}@{manifest["final_base"]["manifest"]}']
    copies = False
    for line in final[1:]:
        if line.startswith("COPY "):
            if not copies:
                result.append("COPY rootfs/ /")
                copies = True
            continue
        result.append(line)
    assert result.count("COPY rootfs/ /") == 1
    assert result[-1] == 'ENTRYPOINT ["/usr/local/bin/dispatcharr-rs"]'
    assert "USER 65532:65532" in result
    return "\n".join(result) + "\n"


DRF_STATIC_ROOT = "opt/dispatcharr-frontend/static/rest_framework"
DRF_MANIFEST = "rust/runtime/drf-static-source-1.json"
DRF_MANIFEST_SHA256 = "d92ffe32744de2e730df0af9d4c8044e92496d25a6ab7aa532f5d4878f9bb8b9"


def drf_assets(manifest):
    raw=git("show",f'{manifest["verified_docs_parent"]}:{DRF_MANIFEST}')
    assert sha256(raw).hexdigest()==DRF_MANIFEST_SHA256
    result={}
    for item in json.loads(raw)["files"]:
        relative=PurePosixPath(item["path"])
        assert not relative.is_absolute() and ".." not in relative.parts and relative.as_posix()==item["path"]
        name=f"{DRF_STATIC_ROOT}/{relative}"
        assert name not in result and item["mtime_ns"]%1_000_000_000==0
        result[name]={key:item[key] for key in ("bytes","sha256","mtime_ns")}
    assert len(result)==27
    return result


def check_drf_file(row,expected):
    assert row.producer=="drf"
    assert (row.size,row.sha256)==(expected["bytes"],expected["sha256"])
    assert row.source.stat().st_mtime_ns==expected["mtime_ns"], "Original DRF component must retain exact mtime"


def copy_merged(row,target,mode,drf_expected):
    if drf_expected is not None:
        check_drf_file(row,drf_expected)
    shutil.copyfile(row.source,target)
    target.chmod(mode)
    if drf_expected is not None:
        os.utime(target,ns=(target.stat().st_atime_ns,drf_expected["mtime_ns"]))
        merged=inspect_file(target,row.name,row.producer)
        check_drf_file(merged,drf_expected)


def materialize(manifest, exports, output, build_id):
    assert re.fullmatch(r"[A-Za-z0-9_-]{1,90}", build_id)
    assert not output.exists() and output.parent.is_dir()
    required = manifest["components"]
    assert exports.is_dir() and not exports.is_symlink()
    assert {item.name for item in exports.iterdir()} == set(required)
    proofs, sections = {}, {}
    for name, contract in required.items():
        proofs[name], sections[name] = verify_export(exports/name,name,contract,manifest,build_id)
    review = manifest["credits"]
    expected_drf=drf_assets(manifest)
    assert {name for name in sections["drf"] if within(name,DRF_STATIC_ROOT)}==set(expected_drf)
    with TemporaryDirectory(prefix=".arm64-assembly-",dir=output.parent) as temporary:
        stage = Path(temporary) / "complete"
        rootfs = stage / "rootfs"
        rootfs.mkdir(parents=True)
        credits = {}
        for destination, authority in review.items():
            original = git("show",f'{manifest["verified_docs_parent"]}:{authority["source"]}')
            assert sha256(original).hexdigest() == authority["sha256"]
            credit_source = Path(temporary) / "credit-material" / destination
            credit_source.parent.mkdir(parents=True,exist_ok=True)
            credit_source.write_bytes(original)
            credit_source.chmod(0o444)
            credits[destination] = inspect_file(credit_source,destination,"credits")
        sections["credits"] = credits
        files = join(sections, review)
        check_coupled_runtime(files)
        assembled = {}
        for name, row in sorted(files.items()):
            target = rootfs / name
            target.parent.mkdir(parents=True,exist_ok=True)
            for parent in target.parents:
                if parent == rootfs.parent:
                    break
                parent.chmod(0o755)
            mode = 0o444 if name in review else row.mode
            copy_merged(row,target,mode,expected_drf.get(name))
            copied = inspect_file(target,name,row.producer)
            assert copied.sha256 == row.sha256 and copied.size == row.size, f"copied bytes differ: {name}"
            assembled[name] = copied
        (stage/"Dockerfile").write_text(render_final_dockerfile(manifest))
        proof = {"scope":"private combined ARM64 build context assembled","same_validation_job":build_id,
                 "platform":"linux/arm64","source_tree_sha256":manifest["application_input_tree_sha256"],
                 "merged_file_count":len(assembled),"merged_inventory_sha256":inventory_hash(assembled),
                 "exact_drf_asset_metadata":{"files":len(expected_drf),"source_manifest_sha256":DRF_MANIFEST_SHA256},
                 "fresh_component_exports":proofs,"embedded_guarded_helper_sha256":assembled[HELPER].sha256,
                 "dockerfile_sha256":digest(stage/"Dockerfile"),"credit":CREDIT,
                 "runtime_execution_verified":False,"distribution_qualified":False}
        (stage/"assembly-receipt.json").write_text(json.dumps(proof,indent=2,sort_keys=True)+"\n")
        stage.rename(output)
    return proof


def load_manifest(revision=None):
    raw = (HERE/"assembly-manifest.json").read_bytes()
    assert sha256(raw).hexdigest() == MANIFEST_SHA
    data = json.loads(raw)
    versions = [data["verified_docs_parent"]]
    if revision:
        versions.append(revision)
    scope = data["application_input_scope"]
    for ref in versions:
        tree = git("ls-tree","-rz",ref,"--",*scope)
        assert tree.count(b"\0") == data["application_input_count"]
        assert sha256(tree).hexdigest() == data["application_input_tree_sha256"]
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exports",type=Path)
    parser.add_argument("--output",type=Path)
    parser.add_argument("--validation-job")
    parser.add_argument("--source-revision")
    args = parser.parse_args()
    manifest = load_manifest(args.source_revision)
    if args.exports is None:
        assert args.output is None and args.validation_job is None
        print(json.dumps({"source_tree_verified":True,"component_recipes_verified":len(manifest["components"]),
                          "pending_standalone":sorted(name for name,value in manifest["components"].items()
                                                      if value["prior_standalone_terminal"]=="pending"),
                          "engine_calls":0,"runtime_execution_verified":False}, sort_keys=True))
        return
    assert args.output is not None and args.validation_job and args.source_revision
    result = materialize(manifest,args.exports,args.output,args.validation_job)
    print(json.dumps(result,indent=2,sort_keys=True))


if __name__ == "__main__":
    main()
