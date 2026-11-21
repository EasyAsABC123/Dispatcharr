"""Build and export the exact component slices inside one owned native job."""

import argparse
from collections.abc import Iterable
from hashlib import sha256
import json
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile

from assemble import load_manifest, materialize, named_entries, inventory_hash
from build_manifest import ROOT
from compose.qualification import exercise

HERE = Path(__file__).resolve().parent
BASES = {
    "donor": ("ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace",
              "sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43",
              17, "6fb7a5b77449c48fdf92aeb9eb8ec812b1902af77a8a8d57b22da02e2f934b39"),
    "debian": ("docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a",
               "sha256:5b6c2c6cbc90b6085e3f9b3086110b1eccedb643374137bfdcd1d2f5c78f6cd1", 1, None),
    "rust": ("rust:1.90.0-bookworm@sha256:4c632e493dfa97f0fe014c3910d1690c149bba85ed8678d47d3563ec6f258ead",
             "sha256:f6d52726c6c740d6ee8ea9ff348db1cb4bfbe325d1e86f3295388666500481ec", None, None),
    "node": ("node:24@sha256:0bab91d6a954a4a75de4a887edfda7386ed8711aa8b1eb5fb309ee0f0a2c544f",
             "sha256:087a47239071fc43868067cccb9d68f1a2b30aeb0042a4bc4b687481820e0516",
             8, "53078d645b593a5e03a57c4143660ec2554e0c2788c9e27d69bbc488f959a33b"),
}
RUST_INDEX = "rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f"
MANIFEST_REF = "5cbb7e1a61e68cad08ce0fa10eae9b26cccbcdfe"
COMPAT = ("rust/runtime/collect_regex_host.py", "rust/runtime/collect_python_host.py", "rust/runtime/regex_bridge",
          "rust/runtime/python_host_licenses", "rust/runtime/python_host_exclusions",
          "rust/runtime/plugin_compat_source", "rust/runtime/plugin_bridge")


def call(args, *, capture=False, **kwargs):
    return subprocess.run([os.fspath(value) for value in args], check=True, timeout=kwargs.pop("timeout",900),
                          text=capture, capture_output=capture, **kwargs)


def output(args, **kwargs):
    return call(args,capture=True,**kwargs).stdout.strip()


def docker(*args, **kwargs):
    return call(("docker",*args),**kwargs)


def image(reference):
    [value] = json.loads(output(("docker","image","inspect",reference)))
    return value


def base_authority(key, manifest):
    reference, config, count, layer_digest = BASES[key]
    docker("pull","--platform","linux/arm64",reference)
    value=image(reference)
    assert (value["Os"],value["Architecture"])==("linux","arm64")
    descriptor=value.get("Descriptor")
    if descriptor:
        assert descriptor["digest"]==reference.rsplit("@",1)[1] and value["Id"] in (config,descriptor["digest"])
    else:
        assert value["Id"]==config
    layers=value["RootFS"]["Layers"]
    if count:
        assert len(layers)==count
    if layer_digest:
        assert sha256(json.dumps(layers,separators=(",",":")).encode()).hexdigest()==layer_digest
    if key=="debian":
        assert layers==[manifest["final_base"]["diff_id"]]
    if key=="rust":
        assert sha256(("\n".join(layers)+"\n").encode()).hexdigest()==(
            "472b7a79d00e1038155e085f49b84167bde1d632468cba7edd0c8026d5742534")
    assert not value["Config"].get("Volumes")
    return value


def archived(checkout, target, paths: Iterable[str]):
    target.mkdir()
    with subprocess.Popen(["git","-C",str(ROOT),"archive",checkout,"--",*paths],
                          stdout=subprocess.PIPE) as archive:
        assert archive.stdout is not None
        call(("tar","-xp","-C",target),stdin=archive.stdout)
        assert archive.wait(timeout=60)==0


def recipe_directory(bundle, component, contract):
    root=bundle/component
    seal=(root/"SHA256SUMS").read_bytes()
    assert sha256(seal).hexdigest()==contract["recipe"]["sha256"]
    for line in seal.decode().splitlines():
        wanted,path=line.split("  ",1)
        relative=Path(path)
        if relative.parts[0]!="tree":
            continue
        assert ".." not in relative.parts and not relative.is_absolute()
        candidate=root/relative
        assert candidate.is_file() and not candidate.is_symlink()
        assert sha256(candidate.read_bytes()).hexdigest()==wanted
    roots=list((root/"tree/scripts/rust-platform-contracts").iterdir())
    assert len(roots)==1 and roots[0].is_dir()
    return roots[0]


def copy_recipe(source, target, *names):
    for name in names:
        file=source/name
        assert file.exists() and not file.is_symlink()
        if file.is_dir():
            shutil.copytree(file,target/name)
        else:
            shutil.copy2(file,target/name)


def context_rust(stage, recipes, checkout, owner, label):
    runtime=stage/"context"
    archived(checkout,runtime,("ATTRIBUTION.md","LICENSE","rust/Dockerfile.app","rust/Cargo.toml",
                               "rust/Cargo.lock","rust/rust-toolchain.toml","rust/crates","rust/vendor",
                               "rust/runtime/taskless-guard","rust/runtime/drf-static-source-1.json"))
    control=recipes["rust"]
    call((sys.executable,"-B",control/"helper/prepare.py","--source",runtime,"--output",stage/"helper"))
    seed=stage/"seed"; seed.mkdir(mode=0o700)
    tool="/usr/local/rustup/toolchains/1.90.0-aarch64-unknown-linux-gnu/bin"
    docker("run","--rm","--name",f"{owner}-cargo-seed","--label",label,"--pull","never","--platform","linux/arm64",
           "--read-only","--user",f"{os.getuid()}:{os.getgid()}","--cap-drop","ALL",
           "--security-opt","no-new-privileges","--tmpfs","/tmp:rw,nosuid,nodev",
           "--mount",f"type=bind,src={runtime},dst=/workspace,readonly",
           "--mount",f"type=bind,src={seed},dst=/seed","--workdir","/workspace",
           "--env","HOME=/seed","--env","CARGO_HOME=/seed/cargo-home","--env",f"RUSTC={tool}/rustc",
           "--entrypoint",f"{tool}/cargo",BASES["rust"][0],"vendor","--manifest-path",
           "/workspace/rust/Cargo.toml","--locked","--versioned-dirs","/seed/vendor")
    call((sys.executable,"-B",control/"prepare.py","admit","--workspace",stage,"--checkout",runtime))
    call((sys.executable,"-B",control/"prepare.py","compile-context","--workspace",stage,"--checkout",runtime))
    return runtime,runtime/"rust/Dockerfile.app"


def context_other(component, stage, recipes, checkout):
    recipe=recipes[component]
    runtime=stage/"context"
    if component=="react":
        archived(checkout,runtime,("ATTRIBUTION.md","LICENSE","frontend"))
        copy_recipe(recipe,runtime,"Dockerfile")
    elif component=="drf":
        archived(checkout,runtime,("rust/runtime/collect_static.py","rust/runtime/drf-static-source-1.json"))
        (runtime/"Dockerfile").write_text(
            f'FROM {BASES["donor"][0]} AS exact-donor-readonly-collector\n'
            'COPY --chmod=0444 rust/runtime/collect_static.py rust/runtime/drf-static-source-1.json /validation/\n'
            'USER 0:0\nRUN mkdir -p /media-runtime && chown 65532:65532 /media-runtime\nUSER 65532:65532\n'
            'RUN /dispatcharrpy/bin/python -I -B /validation/collect_static.py\n')
    elif component in ("ffmpeg-vlc","postgres","crypto"):
        paths=("ATTRIBUTION.md","LICENSE","rust/runtime/collect_crypto.py") if component=="crypto" else ("ATTRIBUTION.md",)
        archived(checkout,runtime,paths)
        if component=="ffmpeg-vlc":
            copy_recipe(recipe,runtime,"Dockerfile","assemble_vlc.py","inputs.json","ffmpeg")
        elif component=="postgres":
            copy_recipe(recipe,runtime,"Dockerfile","assemble.py","inventory.py","inputs.json")
        else:
            copy_recipe(recipe,runtime,"Dockerfile")
            shutil.copy2(runtime/"rust/runtime/collect_crypto.py",runtime/"collect_crypto.py")
    elif component=="python":
        archived(checkout,runtime,("ATTRIBUTION.md","LICENSE",*COMPAT))
        copy_recipe(recipe,runtime,"Dockerfile")
        call((sys.executable,"-B",recipe/"prepare.py",runtime,recipe/"inputs.json"))
    elif component=="comskip":
        source=stage/"source"
        archived(checkout,source,("ATTRIBUTION.md","LICENSE","rust/runtime/comskip-source.ini",
                                  "rust/runtime/comskip-Original-DispatcharrBase"))
        runtime.mkdir()
        call((sys.executable,"-B",HERE/"acquire_comskip.py","--sealed-builder",recipe/"abi/stage.py",
              "--root",source,"--context",runtime),timeout=1200)
        archive=runtime/"Comskip-source.tar.gz"
        call(("curl","--fail","--show-error","--silent","--proto","=https","--max-time","40","--max-filesize","1048576",
              "https://codeload.github.com/erikkaashoek/Comskip/tar.gz/a140b6ac8bc8f596729e9052819affc779c3b377","-o",archive))
        assert sha256(archive.read_bytes()).hexdigest()==(
            "5563d65c0cc5f76883e26b9e460348b4826cc496d10f8d53b25e7648e47f87c2")
        copy_recipe(recipe,runtime,"Dockerfile","assemble_comskip.py","ffmpeg")
        copy_recipe(recipe/"abi",runtime,"collect_comskip_arm64.py","comskip-arm64-abi-inputs.json")
        shutil.copy2(source/"ATTRIBUTION.md",runtime/"ATTRIBUTION.md")
    else:
        raise ValueError("unknown component")
    return runtime,runtime/"Dockerfile"


def stage_python_archive(stream, destination):
    # The passed producer has 0555 bridge directories. Stage regular files before
    # the merger renders directory metadata, while preserving their exact modes.
    seen = set()
    total = 0
    with tarfile.open(fileobj=stream, mode="r|") as archive:
        for member in archive:
            name = member.name.removeprefix("./")
            if name == ".":
                assert member.isdir() and name not in seen
                seen.add(name)
                continue
            path = PurePosixPath(name)
            assert name and path.as_posix() == name and not path.is_absolute()
            assert ".." not in path.parts and all(character.isprintable() for character in name)
            assert name not in seen and len(seen) < 60000
            seen.add(name)
            target = destination / name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            assert member.isfile() and not member.issparse()
            assert not member.mode & 0o7002 and member.mode & 0o004
            total += member.size
            assert 0 <= member.size and total <= 10 * 1024 ** 3
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open("xb") as output:
                shutil.copyfileobj(source, output, 1024 * 1024)
            target.chmod(member.mode)


def export_python(container, target):
    with subprocess.Popen(
        ("docker", "cp", f"{container}:/media-runtime/.", "-"), stdout=subprocess.PIPE
    ) as copy:
        try:
            stage_python_archive(copy.stdout, target)
            assert copy.wait(timeout=360) == 0, "Python export Docker copy failed"
        finally:
            if copy.poll() is None:
                copy.kill()


def build_and_export(component, stage, context, dockerfile, contract, manifest, owner, exports, job, acquired):
    iid=stage/(component+".iid")
    args=["build","--platform","linux/arm64","--pull=false","--target",contract["same_job_stage"],
          "--iidfile",iid,"--file",dockerfile]
    if component!="react":
        args.extend(("--network","none"))
    os.environ["DOCKER_BUILDKIT"]="1"
    docker(*args,context,timeout=2400)
    identity=iid.read_text().strip()
    assert re.fullmatch(r"sha256:[a-f0-9]{64}", identity)
    acquired.append(identity)
    actual=image(identity)
    assert actual["Id"]==identity and (actual["Os"],actual["Architecture"])==("linux","arm64")
    label=f"org.dispatcharr.validation.job={owner}"
    container=output(("docker","container","create","--name",f"{owner}-{component}","--label",label,
                      "--platform","linux/arm64","--pull","never","--network","none","--read-only",
                      "--entrypoint","/bin/true",identity))
    assert re.fullmatch(r"[a-f0-9]{64}", container)
    export=exports/component
    fs=export/"rootfs"; fs.mkdir(parents=True)
    try:
        for source,destination in contract["export_paths"].items():
            target=fs/destination
            if source.endswith("/"):
                target.mkdir(parents=True,exist_ok=True)
                if component == "python":
                    assert source == "/media-runtime/" and target == fs
                    export_python(container, target)
                else:
                    docker("cp",f"{container}:{source}.",target)
            else:
                target.parent.mkdir(parents=True,exist_ok=True)
                docker("cp",f"{container}:{source}",target)
    finally:
        docker("container","rm","-fv",container)
    if component=="drf":
        # The exact donor collector also emits the corresponding runtime docs.
        assert (fs/"usr/share/doc/dispatcharr/drf-static-export.json").is_file()
    records=named_entries(fs,component,contract)
    proof={"component":component,"validation_job":job,"source_tree_sha256":manifest["application_input_tree_sha256"],
           "recipe_sha256":contract["recipe"]["sha256"],"stage":contract["same_job_stage"],
           "local_source_image_config":identity,"controller_exit":0,"inventory_sha256":inventory_hash(records)}
    (export/"export.json").write_text(json.dumps(proof,sort_keys=True,indent=2)+"\n")


def build_final(context, work, owner, acquired):
    iid=work/"candidate.iid"
    docker("build","--platform","linux/arm64","--network","none","--pull=false","--iidfile",iid,context,timeout=900)
    identity=iid.read_text().strip()
    assert re.fullmatch(r"sha256:[a-f0-9]{64}",identity)
    acquired.append(identity)
    actual=image(identity)
    assert actual["Id"]==identity and (actual["Os"],actual["Architecture"])==("linux","arm64")
    assert actual["Config"]["User"]=="65532:65532"
    assert actual["Config"]["Entrypoint"]==["/usr/local/bin/dispatcharr-rs"]
    assert actual["RootFS"]["Layers"][0]==(
        "sha256:c96c411090da09187c55ab267826b3903b542b5c03f1cafa912d60e060e7850c")
    commands=r"""
set -eu
export LC_ALL=C PATH=/usr/local/bin:/usr/bin:/bin
[ "$(id -u)" = 65532 ]
[ "$(uname -m)" = aarch64 ]
[ ! -e /dispatcharrpy ]
test -f /opt/dispatcharr-frontend/index.html
test -f /usr/share/doc/dispatcharr/drf-static-export.json
test -f /usr/share/doc/dispatcharr/ATTRIBUTION.md
test -x /usr/local/bin/comskip
test -x /usr/local/bin/cvlc
/usr/local/bin/dispatcharr-operator --help | grep -qF 'showmigrations --plan'
/opt/dispatcharr-pg-guarded-restore/bin/dispatcharr-pg-guarded-restore --version | grep -qF '17.11'
/usr/local/bin/pg_restore --version | grep -qF '17.11'
/usr/local/bin/ffmpeg -version | grep -m1 -qF 'ffmpeg version'
/usr/local/bin/gpg --version | grep -m1 -qF 'gpg (GnuPG) 2.4.8'
/usr/local/bin/dispatcharr-python -I -B -c 'import regex; assert regex.__version__ == "2026.9.29"'
/usr/local/bin/dispatcharr-python -I -B -c '
from hashlib import file_digest, sha256
from pathlib import Path
import json, os, stat
docs=Path("/usr/share/doc/dispatcharr/drf-static-source-1.json").read_bytes()
assert sha256(docs).hexdigest()=="d92ffe32744de2e730df0af9d4c8044e92496d25a6ab7aa532f5d4878f9bb8b9"
root=Path("/opt/dispatcharr-frontend/static/rest_framework")
files=json.loads(docs)["files"]
assert os.getuid()==65532 and len(files)==27
for row in files:
    asset=root/row["path"]
    info=asset.lstat()
    assert stat.S_ISREG(info.st_mode)
    assert (info.st_size,info.st_mtime_ns)==(row["bytes"],row["mtime_ns"])
    with asset.open("rb") as content:
        assert file_digest(content,"sha256").hexdigest()==row["sha256"]
'
printf 'exact-native-final-package-smoke\n'
"""
    status=output(("docker","run","--rm","--name",f"{owner}-final-smoke","--label",
        f"org.dispatcharr.validation.job={owner}","--read-only","--pull","never","--network","none",
        "--user","65532:65532","--cap-drop","ALL","--security-opt","no-new-privileges",
        "--entrypoint","/bin/sh",identity,"-ec",commands),timeout=120)
    assert status=="exact-native-final-package-smoke"
    return {"image_config":identity,"platform":"linux/arm64","uid":65532,
            "read_only_wrapper_smoke_exit":0,"compose_execution_verified":False,
            "source_application_started":False,"distribution_qualified":False,
            "exact_drf_asset_bytes_sizes_and_mtime_checked_nonroot":27}


def retire(owner, prior, acquired):
    label=f"org.dispatcharr.validation.job={owner}"
    containers=output(("docker","container","ls","-aq","--no-trunc","--filter",f"label={label}")).splitlines()
    for container in containers:
        assert re.fullmatch(r"[a-f0-9]{64}",container)
        docker("container","rm","-fv",container)
    current_images=set(output(("docker","image","ls","--no-trunc","--format","{{.ID}}")).splitlines())
    for identity in reversed(list(dict.fromkeys(acquired))):
        if identity not in prior and identity in current_images:
            docker("image","rm",identity)
    remainder=output(("docker","container","ls","-aq","--no-trunc","--filter",f"label={label}"))
    images=set(output(("docker","image","ls","--no-trunc","--format","{{.ID}}")).splitlines())
    assert not remainder and not (set(acquired)-prior)&images


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recipe-bundle",type=Path,required=True)
    parser.add_argument("--scratch",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--source-revision",required=True)
    parser.add_argument("--job",required=True)
    parser.add_argument("--evidence",type=Path,required=True)
    args=parser.parse_args()
    assert args.job.isascii() and args.job.replace("-","").isdigit() and len(args.job)<=24
    assert os.uname().machine=="aarch64"
    assert output(("docker","info","--format","{{.OSType}}/{{.Architecture}}")) in ("linux/arm64","linux/aarch64")
    manifest=load_manifest(args.source_revision)
    assert manifest["verified_docs_parent"]==MANIFEST_REF
    recipes={key:recipe_directory(args.recipe_bundle,key,row) for key,row in manifest["components"].items()}
    args.scratch.mkdir(mode=0o700)
    args.evidence.mkdir(mode=0o700)
    exports=args.scratch/"exports"; exports.mkdir()
    owner=f"dispatcharr-arm64-assembly-{args.job}"
    prior=set(output(("docker","image","ls","--no-trunc","--format","{{.ID}}")).splitlines())
    acquired=[]
    phase="base-identities"
    result=None
    component_progress=[]
    try:
        for key in BASES:
            base_authority(key,manifest)
        docker("pull","--platform","linux/arm64",RUST_INDEX)
        assert image(RUST_INDEX)["Id"]==BASES["rust"][1]
        rust=args.scratch/"rust"; rust.mkdir()
        phase="rust-locked-context"
        rust_context=context_rust(rust,recipes,args.source_revision,owner,f"org.dispatcharr.validation.job={owner}")
        for component in manifest["components"]:
            phase="component-"+component
            if component in ("rust","guarded-pg"):
                stage=rust
                context,dockerfile=rust_context
            else:
                stage=args.scratch/component
                stage.mkdir()
                context,dockerfile=context_other(component,stage,recipes,args.source_revision)
            build_and_export(component,stage,context,dockerfile,manifest["components"][component],
                             manifest,owner,exports,args.job,acquired)
            projection=json.loads((exports/component/"export.json").read_bytes())
            component_progress.append({"component":component,"controller_exit":projection["controller_exit"],
                                       "inventory_sha256":projection["inventory_sha256"]})
            (args.evidence/"component-progress.json").write_text(json.dumps(component_progress,sort_keys=True)+"\n")
        phase="combined-file-merge"
        merged=materialize(manifest,exports,args.out,args.job)
        phase="private-final-image-build-and-smoke"
        final=build_final(args.out,args.scratch,owner,acquired)
        assembled={"private_assembly":merged,"private_final_image":final}
        (args.evidence/"private-package.json").write_text(json.dumps(assembled,sort_keys=True,indent=2)+"\n")
        phase="same-job-owned-compose"
        ready=args.scratch/"local-final-receipt.json"
        ready.write_text(json.dumps(final,sort_keys=True)+"\n")
        compose_result=exercise(ready,final["image_config"],args.source_revision,args.scratch/"compose",args.job)
        (args.evidence/"compose.json").write_text(json.dumps(compose_result,sort_keys=True,indent=2)+"\n")
        assert compose_result["actual_exit"]==0 and compose_result["exact_cleanup"] is True
        result={"private_assembly":merged,"private_final_image":final,"native_compose":compose_result}
        phase="complete"
    finally:
        exact_cleanup=False
        try:
            retire(owner,prior,acquired)
            exact_cleanup=True
        finally:
            if result and exact_cleanup:
                (args.evidence/"combined-runtime.json").write_text(json.dumps(result,indent=2,sort_keys=True)+"\n")
            terminal={"last_phase":phase,"controller_exit":0 if result and exact_cleanup else 1,
                      "exact_container_and_owned_image_retirement":exact_cleanup}
            (args.evidence/"terminal.json").write_text(json.dumps(terminal,sort_keys=True)+"\n")



if __name__=="__main__":
    main()
