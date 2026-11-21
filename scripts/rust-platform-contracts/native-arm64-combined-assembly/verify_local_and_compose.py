"""Exercise the normal local ARM64 package CLI and retire exactly its one local image."""
from argparse import ArgumentParser
from hashlib import file_digest
import json
from os import environ
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory

from compose import qualification

HERE=Path(__file__).resolve().parent
PINNED_REF="9116a8cc54cebe988ab939de70e4f761d45a3db7"
SOURCE_TREE="c73544e9a2901eb61b21bdea7275497259f585b9715ddf69745d5ddf0ae9f15c"
PINNED_BASES=(
 "ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace",
 "docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a",
 "rust:1.90.0-bookworm@sha256:4c632e493dfa97f0fe014c3910d1690c149bba85ed8678d47d3563ec6f258ead",
 "rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f",
 "node:24@sha256:0bab91d6a954a4a75de4a887edfda7386ed8711aa8b1eb5fb309ee0f0a2c544f",
)
PREFIXES=("GITHUB_","RUNNER_","ACTIONS_","GIT_")
NATIVE_PROBE="test \"$(id -u)\" = 65532; test \"$(uname -m)\" = aarch64; test -s /usr/share/doc/dispatcharr/ATTRIBUTION.md; /usr/local/bin/pg_restore --version | grep -qF '17.11'; printf 'local-arm64-retained-image-ok\\n'"
TAG_PREFIX="dispatcharr-rust-arm64-local:"
ARM_LAYER="sha256:c96c411090da09187c55ab267826b3903b542b5c03f1cafa912d60e060e7850c"


def execute(args, *, env=None, timeout=30):
    return subprocess.run([str(x) for x in args],capture_output=True,timeout=timeout,env=env)


def required(args, *, timeout=30):
    value=execute(args,timeout=timeout)
    value.check_returncode()
    return value.stdout


def ids(kind):
    prefix=["docker",kind,"ls","-q","--no-trunc"]
    if kind=="container":
        prefix.append("-a")
    return set(required(prefix).decode().split())


def image(reference):
    result=execute(["docker","image","inspect",reference])
    if result.returncode:
        return None
    [row]=json.loads(result.stdout)
    return row


def tags():
    rows=required(["docker","image","ls","--format","{{.Repository}}:{{.Tag}}"]).decode().splitlines()
    return {row for row in rows if row.startswith(TAG_PREFIX)}


def base_ids():
    role_refs = tuple(row[0] for row in qualification.references(qualification.inputs()).values())
    return {row["Id"] for pin in (*PINNED_BASES, *role_refs) if (row:=image(pin))}


def write(path, obj):
    path.write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n")


def main():
    parser=ArgumentParser(description=__doc__)
    parser.add_argument("--evidence",required=True,type=Path)
    parser.add_argument("--work",required=True,type=Path)
    parser.add_argument("--owner",required=True)
    args=parser.parse_args()
    assert re.fullmatch(r"[0-9]{5,20}-[0-9]{1,3}",args.owner)
    args.evidence.mkdir(mode=0o700)
    report={"last_phase":"start","controller_exit":1,"cli_preflight_exit":None,"cli_build_exit":None,
            "exact_cleanup":False,"image_pushed":False,"redistribution_qualified":False}
    write(args.evidence/"terminal.json",report)
    env={key:value for key,value in environ.items() if not key.startswith(PREFIXES) and key not in {"CI","PYTHONPATH"}}
    assert not any(key.startswith(PREFIXES) for key in env)
    before=ids("image")
    containers=ids("container")
    existing_tags=tags()
    assert not existing_tags
    retained=None
    cleared=False
    candidate_id=None
    diagnostic=None
    with TemporaryDirectory(prefix="owned-local-cli-",dir=args.work) as directory:
        output=Path(directory)/"build"
        try:
            cli=[sys.executable,"-B",HERE/"local_build.py"]
            report["last_phase"]="engine-free-checkout-preflight"
            first=execute(cli+["--preflight"],env=env)
            report["cli_preflight_exit"]=first.returncode
            first.check_returncode()
            proof=json.loads(first.stdout)
            assert proof["application_revision"]==PINNED_REF and proof["application_tree_sha256"]==SOURCE_TREE
            assert proof["sealed_text_files"]==90 and proof["credits_checked"]==13 and proof["engine_invoked"] is False
            assert ids("image")==before and ids("container")==containers and tags()==existing_tags
            write(args.evidence/"preflight.json",proof)
            report["last_phase"]="actual-native-local-package"
            outcome=execute(cli+["--build","--output",output],env=env,timeout=7500)
            report["cli_build_exit"]=outcome.returncode
            outcome.check_returncode()
            result=json.loads(outcome.stdout)
            assert result==json.loads((output/"evidence/local-build.json").read_bytes())
            retained=result["local_image_tag"]
            candidate_id=result["image_config"]
            assert re.fullmatch(TAG_PREFIX+r"[0-9]{18}-1",retained)
            assert re.fullmatch(r"sha256:[a-f0-9]{64}",candidate_id) and candidate_id not in before
            assert tags()=={retained}
            report["last_phase"]="independent-retained-image"
            observed=image(retained)
            assert observed and observed["Id"]==candidate_id
            assert (observed["Os"],observed["Architecture"],observed["RootFS"]["Layers"][0])==("linux","arm64",ARM_LAYER)
            assert observed["Config"]["User"]=="65532:65532"
            assert observed["Config"]["Entrypoint"]==["/usr/local/bin/dispatcharr-rs"]
            assert observed["Config"]["Labels"]["org.opencontainers.image.licenses"]=="AGPL-3.0-only"
            assert result["closed_other_owned_images_and_containers"] is True and result["image_pushed"] is False
            assert {p.name for p in output.iterdir()}=={"build.log","evidence"}
            check_file=output/"evidence/private-package.json"
            package=json.loads(check_file.read_bytes())
            assert package["private_assembly"]["source_tree_sha256"]==SOURCE_TREE
            assert len(package["private_assembly"]["fresh_component_exports"])==9
            assert package["private_final_image"]["image_config"]==candidate_id
            assert package["private_final_image"]["exact_drf_asset_bytes_sizes_and_mtime_checked_nonroot"]==27
            assert package["private_final_image"]["read_only_wrapper_smoke_exit"]==0
            native=required(["docker","run","--rm","--name","dispatcharr-local-cli-proof-"+args.owner,
                             "--pull","never","--read-only","--network","none","--cap-drop","ALL","--user","65532:65532",
                             "--security-opt","no-new-privileges","--entrypoint","/bin/sh",candidate_id,"-ec",NATIVE_PROBE],
                            timeout=60)
            assert native==b"local-arm64-retained-image-ok\n"
            report["last_phase"]="same-retained-image-owned-compose"
            receipt=output/"evidence/native-final-image-for-compose.json"
            write(receipt,package["private_final_image"])
            runtime=qualification.exercise(receipt,candidate_id,PINNED_REF,
                                           Path(directory)/"compose",args.owner,copy_data=True)
            write(args.evidence/"same-image-compose.json",runtime)
            assert runtime["candidate_image_config"]==candidate_id
            assert runtime["actual_exit"]==0 and runtime["exact_cleanup"] is True
            assert runtime["performance_measured"] is False and runtime["published_host_port"] is False
            assert len(runtime["checks"])==12 and len(runtime["decoders"])==2
            assert all(row["audio_video_decoded"] for row in runtime["decoders"])
            assert runtime["copied_postgres_data"]["schema_tables"]==62
            assert runtime["copied_postgres_data"]["copy_initialization_disabled"] is True
            assert runtime["copied_postgres_data"]["normal_copied_account_login"] is True
            assert runtime["copied_postgres_data"]["ordinary_copied_channel_read"] is True
            pinned=base_ids()
            assert before<=ids("image") and ids("image")-before<={candidate_id}|pinned
            assert ids("container")==containers
            report["last_phase"]="test-owned-image-retirement"
            required(["docker","image","rm",retained])
            cleared=True
            assert image(retained) is None and candidate_id not in ids("image")
            assert tags()==existing_tags and before<=ids("image") and ids("image")-before<=pinned
            assert ids("container")==containers
            write(args.evidence/"qualified-local-image.json",{
                "source_revision":PINNED_REF,"source_tree_sha256":SOURCE_TREE,"image_config":candidate_id,
                "platform":"linux/arm64","uid":65532,"component_exports":9,"pinned_drf_metadata_files":27,
                "independent_nonroot_readonly_smoke_exit":0,"local_build_scratch_removed":True,
                "preexisting_engine_images_retained":True,"test_owned_local_image_removed":True,
                "image_pushed":False,"original_license_notice_verified":True})
            report.update(last_phase="complete",controller_exit=0,exact_cleanup=True)
        except (AssertionError,OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError) as error:
            diagnostic=type(error).__name__
            if retained and candidate_id and not cleared:
                row=image(retained)
                if row and row["Id"]==candidate_id and candidate_id not in before:
                    required(["docker","image","rm",retained])
            remaining=ids("image")
            report["exact_cleanup"]=(tags()==existing_tags and ids("container")==containers
                                     and before<=remaining and remaining-before<=base_ids())
        finally:
            log=output/"build.log"
            if log.is_file():
                with log.open("rb") as stream:
                    report["private_build_log_sha256"]=file_digest(stream,"sha256").hexdigest()
            if diagnostic:
                report["failure_type"]=diagnostic
            write(args.evidence/"terminal.json",report)
    if report["controller_exit"]:
        raise SystemExit(1)


if __name__=="__main__":
    main()
