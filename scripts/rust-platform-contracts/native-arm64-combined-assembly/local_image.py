"""Keep one local smoke-tested image after all other owned resources retire."""
import json
import re
import subprocess


def retain_local_image(owner, prior, acquired, image_id, tag, evidence, docker, image, retire):
    assert re.fullmatch(r"sha256:[a-f0-9]{64}",image_id)
    assert image_id in acquired
    assert re.fullmatch(r"dispatcharr-rust-arm64-local:[0-9]{18}-1",tag)
    retire(owner,prior,[item for item in acquired if item!=image_id])
    try:
        docker("image","tag",image_id,tag)
        row=image(tag)
        assert row["Id"]==image_id and (row["Os"],row["Architecture"])==("linux","arm64")
        assert row["Config"]["User"]=="65532:65532"
        receipt={
            "image_config":image_id,"local_image_tag":tag,"platform":"linux/arm64",
            "closed_other_owned_images_and_containers":True,"retained_exact_local_image":True,
            "nonroot_wrapper_smoke_exit":0,"compose_executed":False,"image_pushed":False,
        }
        destination=evidence/"local-build.json"
        with destination.open("x") as out:
            out.write(json.dumps(receipt,indent=2,sort_keys=True)+"\n")
    except (AssertionError,OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError):
        try:
            row=image(tag)
        except (OSError,ValueError,subprocess.SubprocessError):
            row={}
        if row.get("Id")==image_id:
            docker("image","rm",tag)
        raise
