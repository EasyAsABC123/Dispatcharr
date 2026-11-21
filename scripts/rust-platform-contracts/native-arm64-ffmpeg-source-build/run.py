"""Build and test the exact native FFmpeg source inputs without exporting any binaries."""
import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from context import acquire,inputs,materialize

HERE=Path(__file__).resolve().parent
PARENT="380551dc2dfa3adbad07f95f475909ad622d1098"
BRANCH="validation/m10-arm64-ffmpeg-source-build-380551dc-1"
STEM="native-arm64-ffmpeg-source-build"
CID=re.compile("sha256:[a-f0-9]{64}")
GUEST_STAGES={"notice-inputs","ffmpeg-elf-and-sonames","ffprobe-elf-and-sonames","generated-encode","generated-probe","generated-decode"}


def call(*cmd, capture=False, timeout=120):
    return subprocess.run([str(c) for c in cmd],stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None,check=True,timeout=timeout).stdout


def text(*cmd, **options):
    return call(*cmd,capture=True,**options).decode().strip()


def jsonfile(path,data):
    path.write_text(json.dumps(data,indent=2,sort_keys=True)+"\n")


def scoped_revision():
    revision=os.environ["GITHUB_SHA"]
    assert re.fullmatch("[a-f0-9]{40}",revision)
    assert os.environ["GITHUB_REF"]=="refs/heads/"+BRANCH
    assert text("git","rev-parse","HEAD")==revision and text("git","rev-parse","HEAD^")==PARENT
    changed=set(text("git","diff","--name-only","HEAD^","HEAD").splitlines())
    tracked={f"scripts/rust-platform-contracts/{STEM}/"+p.relative_to(HERE).as_posix()
             for p in HERE.rglob("*") if p.is_file()}
    assert changed==tracked|{f".github/workflows/{STEM}.yml"}
    return revision


def image(value):
    [data]=json.loads(text("docker","image","inspect",value))
    assert (data["Architecture"],data["Os"])==("arm64","linux")
    return data


def images():
    return set(text("docker","image","ls","--no-trunc","--format","{{.ID}}").splitlines())


def remove_owned(previous,owned,name):
    marker=subprocess.run(["docker","container","inspect",name],capture_output=True)
    if marker.returncode==0:
        call("docker","container","rm","-f",name)
    assert subprocess.run(["docker","container","inspect",name],capture_output=True).returncode!=0
    for ident in reversed(owned):
        if ident not in previous and ident in images():
            call("docker","image","rm",ident)
    assert not (set(owned)-previous)&images()


def qualify(raw,authority):
    assert 0<len(raw)<1024
    lines=raw.decode().splitlines()
    values=dict(row.split("=",1) for row in lines)
    assert len(values)==len(lines)
    assert set(values)=={"ffmpeg","ffprobe","runtime_package_versions","shared_sonames_per_tool",
                        "notice_hashes","configure_and_media_workflow"}
    expected={"runtime_package_versions":"25","shared_sonames_per_tool":"55","notice_hashes":"62",
              "configure_and_media_workflow":"passed"}
    assert all(values[k]==v for k,v in expected.items())
    hashes={k:values[k] for k in ("ffmpeg","ffprobe")}
    assert all(re.fullmatch("[0-9a-f]{64}",value) for value in hashes.values())
    match={name:hashes[name]==sha for name,sha in authority["prior_candidate_binary_sha256"].items()}
    assert set(match)==set(hashes)
    return {"executables_sha256":hashes,"original_binary_byte_match":match,
            "same_binary_bytes_for_both_tools":all(match.values()),
            "compatible_arm64_sonames_and_generated_media":True,"guest_exit":0,
            "exact_runtime_package_versions":25,"linked_sonames_per_executable":55,
            "exact_original_notice_and_configuration_hashes":62,
            "generated_software_h264_and_stripped_fdk_aac":True,
            "generated_stream_probed_and_both_types_decoded":True,"guest_receipt_sha256":sha256(raw).hexdigest(),
            "broader_hardware_app_or_distribution_qualified":False}


def main():
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--evidence",type=Path,required=True)
    args=cli.parse_args()
    evidence=args.evidence
    evidence.mkdir(mode=0o700)
    revision=scoped_revision()
    job=os.environ["GITHUB_RUN_ID"]+"-"+os.environ["GITHUB_RUN_ATTEMPT"]
    assert re.fullmatch("[0-9]+-[0-9]+",job)
    name="dispatcharr-ffmpeg-source-"+job
    assert os.uname().machine=="aarch64"
    assert text("docker","info","--format","{{.OSType}}/{{.Architecture}}") in ("linux/arm64","linux/aarch64")
    previous=images()
    owned=[]
    phase="verified-public-input-acquisition"
    status=1
    cleanup=False
    diagnosis={}
    with tempfile.TemporaryDirectory(prefix="dispatcharr-ffmpeg-native-build-") as scratch:
        path=Path(scratch)
        iid=path/"native.iid"
        try:
            authority,lock=inputs()
            cache,recipe=acquire(path)
            phase="canonical-signed-offline-build-context"
            context=path/"context"
            ctx=materialize(context,cache,recipe)
            jsonfile(evidence/"context.json",ctx)
            base=lock["ubuntu_image"]
            phase="native-ubuntu-base"
            call("docker","pull","--platform","linux/arm64",base,timeout=500)
            pinned=image(base)
            owned.append(pinned["Id"])
            assert pinned["RootFS"]["Layers"]==authority["base"]["diff_ids"]
            manifest=base.rsplit("@",1)[1]
            descriptor=pinned.get("Descriptor")
            if descriptor:
                assert descriptor["digest"]==manifest and pinned["Id"] in (manifest,authority["base"]["config"])
            else:
                assert pinned["Id"]==authority["base"]["config"] and any(value.endswith("@"+manifest) for value in pinned["RepoDigests"])
            jsonfile(evidence/"base.json",{"native":True,"manifest":manifest,"config":authority["base"]["config"],
                                          "exact_original_rootfs_diffs":True})
            phase="build-source-arm64"
            call("docker","build","--platform","linux/arm64","--network","none","--target","final","--pull=false",
                 "--iidfile",iid,context,timeout=5400)
            final=iid.read_text().strip()
            assert CID.fullmatch(final)
            owned.append(final)
            observed=image(final)
            assert observed["Id"]==final and observed["RootFS"]["Layers"][:2]==pinned["RootFS"]["Layers"]
            jsonfile(evidence/"build.json",{"build_exit":0,"native":True,"destination_image_id":final,
                                           "exact_original_base":True,"registry_upload":False})
            phase="nonroot-config-notice-soname-and-av"
            raw=call("docker","run","--rm","--name",name,"--platform","linux/arm64","--pull","never","--network","none",
                     "--read-only","--user","65532:65532","--cap-drop","ALL","--security-opt","no-new-privileges",
                     "--tmpfs","/tmp:rw,nosuid,nodev,mode=1777,size=64m",
                     "--mount",f"type=bind,src={HERE/'expected'},dst=/expected,readonly",
                     "--mount",f"type=bind,src={HERE/'guest.sh'},dst=/qualification.sh,readonly",
                     "--entrypoint","/bin/sh",final,"/qualification.sh",capture=True)
            jsonfile(evidence/"qualification.json",qualify(raw,authority))
            phase="complete"
            status=0
        except (AssertionError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as problem:
            diagnosis["type"]=type(problem).__name__
            if isinstance(problem,subprocess.CalledProcessError):
                diagnosis["command_exit"]=problem.returncode
                stdout=problem.stdout or b""
                stderr=problem.stderr or b""
                diagnosis["stdout_sha256"]=sha256(stdout).hexdigest()
                diagnosis["stderr_sha256"]=sha256(stderr).hexdigest()
                checkpoint=[line.removeprefix("guest-stage=") for line in stderr.decode(errors="replace").splitlines()
                            if line.startswith("guest-stage=")]
                diagnosis["guest_stage"]=next((value for value in checkpoint if value in GUEST_STAGES),None)
        finally:
            try:
                remove_owned(previous,owned,name)
                cleanup=True
            finally:
                jsonfile(evidence/"terminal.json",{"controller_exit":0 if status==0 and cleanup else 1,
                    "phase":phase,"exact_temporary_image_and_container_cleanup":cleanup,
                    "validation_revision":revision,"failure":diagnosis,
                    "binary_or_image_publication":False})
    return 0 if status==0 and cleanup else 1


if __name__=="__main__":
    raise SystemExit(main())
