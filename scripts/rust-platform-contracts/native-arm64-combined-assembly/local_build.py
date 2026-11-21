"""Build only the pinned local ARM64 candidate with the verified native assembler."""
from argparse import ArgumentParser
from hashlib import sha256
import json
import os
from pathlib import Path, PurePosixPath
import re
from secrets import randbelow
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory

HERE=Path(__file__).resolve().parent
REPO_PATH=Path("scripts/rust-platform-contracts/native-arm64-combined-assembly")
APP="cf64fbfe776c9470cc62bc90c2e21216f9df204a"
PARENT="5b1827c5fecd581de9bdbee006b0778104e89b5e"
SOURCE_TREE="1acc1ca78eef4f050808caf07a5ec18c99fa823c4356412471434b7a1bd3f560"


def child_env():
    return {key:value for key,value in os.environ.items() if not key.startswith("GIT_") and key!="PYTHONPATH"}


def git(*args):
    return subprocess.check_output(["git","-C",str(HERE.parents[2]),*args],env=child_env(),timeout=30)


def bundle(directory, seal):
    entries={}
    for line in seal.decode().splitlines():
        digest,relative=line.split("  ",1)
        name=PurePosixPath(relative)
        assert re.fullmatch(r"[a-f0-9]{64}",digest)
        assert not name.is_absolute() and ".." not in name.parts and name.as_posix()==relative
        assert relative not in entries and relative!="LOCAL-SHA256SUMS"
        entries[relative]=digest
    assert entries and {"orchestrate.py","assembly-manifest.json","Dockerfile.app.arm64","local_image.py"}<=entries.keys()
    for relative,digest in entries.items():
        path=directory/relative
        assert path.is_file() and not path.is_symlink()
        assert all(not parent.is_symlink() for parent in path.parents if parent.is_relative_to(directory))
        assert sha256(path.read_bytes()).hexdigest()==digest
    return entries


def preflight():
    assert sys.version_info >= (3,11)
    root=Path(git("rev-parse","--show-toplevel").decode().strip())
    assert root/REPO_PATH==HERE
    manifest=json.loads((HERE/"assembly-manifest.json").read_bytes())
    assert (manifest["application_revision"],manifest["verified_docs_parent"])==(APP,PARENT)
    assert manifest["application_input_tree_sha256"]==SOURCE_TREE
    seal=(HERE/"LOCAL-SHA256SUMS").read_bytes()
    assert git("show",f"HEAD:{REPO_PATH}/LOCAL-SHA256SUMS")==seal
    entries=bundle(HERE,seal)
    assert len(entries)==90
    tree=[]
    for ref in (APP,PARENT):
        tree.append(git("ls-tree","-rz",ref,"--",*manifest["application_input_scope"]))
    assert tree[0]==tree[1] and tree[0].count(b"\0")==2022 and sha256(tree[0]).hexdigest()==SOURCE_TREE
    credits=manifest["credits"]
    assert len(credits)==13
    for item in credits.values():
        assert sha256(git("show",f"{APP}:{item['source']}")).hexdigest()==item["sha256"]
    return {"application_revision":APP,"application_tree_sha256":SOURCE_TREE,"engine_architecture_required":"linux/arm64",
            "sealed_text_files":len(entries),"credits_checked":len(credits),"engine_invoked":False,
            "official_source_image": "ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace"}


def command(work,evidence,job):
    return [sys.executable,"-B",str(HERE/"orchestrate.py"),"--recipe-bundle",str(HERE/"recipes"),
            "--scratch",str(work/"stages"),"--out",str(work/"candidate"),"--source-revision",APP,
            "--job",job,"--evidence",str(evidence),"--local-image-tag",f"dispatcharr-rust-arm64-local:{job}"]


def build(output):
    assert os.uname().sysname=="Linux" and os.uname().machine=="aarch64"
    assert all(shutil.which(program) for program in ("docker","git","tar","curl","bash","dpkg-deb"))
    output.mkdir(mode=0o700)
    job=f"{randbelow(9*10**17)+10**17}-1"
    evidence=output/"evidence"
    with TemporaryDirectory(prefix="private-native-context-",dir=output) as folder:
        with (output/"build.log").open("x") as log:
            actual=subprocess.run(command(Path(folder),evidence,job),env=child_env(),stdout=log,
                                  stderr=subprocess.STDOUT,check=False)
        if actual.returncode:
            raise SystemExit(f"Native local build exited {actual.returncode}. See the private build.log.")
        result=json.loads((evidence/"local-build.json").read_bytes())
        assert result["local_image_tag"]==f"dispatcharr-rust-arm64-local:{job}"
        assert result["nonroot_wrapper_smoke_exit"]==0 and result["image_pushed"] is False
    print(json.dumps(result,indent=2,sort_keys=True))


if __name__=="__main__":
    parser=ArgumentParser(description=__doc__)
    flags=parser.add_mutually_exclusive_group(required=True)
    flags.add_argument("--preflight",action="store_true")
    flags.add_argument("--build",action="store_true")
    parser.add_argument("--output",type=Path,help="Fresh local directory for private logs and text receipts")
    args=parser.parse_args()
    if args.preflight and args.output:
        parser.error("--output is used with --build")
    if args.build and (args.output is None or args.output.exists()):
        parser.error("--build requires a fresh --output directory")
    proof=preflight()
    if args.preflight:
        print(json.dumps(proof,indent=2,sort_keys=True))
    else:
        build(args.output.resolve())
