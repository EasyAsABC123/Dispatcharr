"""Freeze build recipes, scope evidence, and the expected ARM64 runtime namespaces."""

from hashlib import sha256
import json
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ARTIFACTS = HERE.parent
APPLICATION = "fdede54c54ccb19300c01031537803a0b553f581"
BASE = "14948254883a48d6c2b628cbf9ab8b1857b8d6b6"
SCOPES = ("ATTRIBUTION.md","LICENSE","frontend","rust/Cargo.toml","rust/Cargo.lock","rust/rust-toolchain.toml",
          "rust/Dockerfile.app","rust/crates","rust/vendor","rust/runtime")
SCOPE_SHA = "757a0c4c4072d830a50395101634d780c8d5d3de0d4286305f164c551934b6ba"
DOCS = "usr/share/doc/dispatcharr"
CREDIT = "Original Dispatcharr and upstream authors and contributors retain full credit."


def stamp(raw):
    return sha256(raw).hexdigest()


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], timeout=30)


def path_pin(relative):
    path = ARTIFACTS / relative
    return {"kind": "retained-artifact", "path": relative, "sha256": stamp(path.read_bytes())}


def git_pin(path):
    return {"kind": "verified-git", "commit": BASE, "path": path, "sha256": stamp(git("show", f"{BASE}:{path}"))}


def recipe(name, digest):
    relative = name + "/SHA256SUMS"
    pin = path_pin(relative)
    assert pin["sha256"] == digest
    index = (ARTIFACTS / relative).read_text()
    for line in index.splitlines():
        expected, filename = line.split("  ", 1)
        assert not Path(filename).is_absolute() and ".." not in Path(filename).parts
        assert stamp((ARTIFACTS / name / filename).read_bytes()) == expected
    return pin


def unit(name, digest, evidence, stage, copy, files, prefixes, required, *, pending=False):
    return {"recipe": recipe(name, digest),"prior_evidence": evidence,"prior_standalone_terminal": "pending" if pending else "passed",
            "same_job_stage": stage, "export_paths": copy, "allowed_files": files, "allowed_prefixes": prefixes,
            "required_files": required, "requires_fresh_recipe_and_input_binding": True}


def integration(name):
    return git_pin(f"scripts/rust-integration/m10-native-arm64-{name}.json")


def make():
    for revision in (APPLICATION, BASE):
        source = git("ls-tree", "-rz", revision, "--", *SCOPES)
        assert source.count(b"\0") == 2020 and stamp(source) == SCOPE_SHA
    app_bin = ["usr/local/bin/dispatcharr-rs","usr/local/bin/dispatcharr-operator"]
    helper = "opt/dispatcharr-pg-guarded-restore/bin/dispatcharr-pg-guarded-restore"
    node_index = "opt/dispatcharr-frontend/index.html"
    fw = ["usr/local/bin/ffmpeg","usr/local/bin/ffprobe","usr/local/bin/vlc","usr/local/bin/cvlc"]
    pw = ["usr/local/bin/pg_dump","usr/local/bin/pg_restore","usr/local/bin/psql"]
    credit_paths = [f"{DOCS}/LICENSE", f"{DOCS}/ATTRIBUTION.md"]
    credits = {
        credit_paths[0]:("LICENSE","8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef"),
        credit_paths[1]:("ATTRIBUTION.md","1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7"),
        f"{DOCS}/Django-LICENSE":("rust/crates/output-http/data/Django-LICENSE",None),
        f"{DOCS}/Python-COPYRIGHT":("rust/crates/store/third-party/PYTHON-COPYRIGHT",None),
        f"{DOCS}/Requests-LICENSE":("rust/crates/store/third-party/REQUESTS-LICENSE",None),
        f"{DOCS}/Celery-LICENSE":("rust/crates/store/datasets/Celery-LICENSE",None),
        f"{DOCS}/Kombu-LICENSE":("rust/crates/store/datasets/Kombu-LICENSE",None),
        f"{DOCS}/libxml2-Copyright":("rust/crates/xmltv-parser/vendor/Copyright",None),
        f"{DOCS}/libxml2-NOTICES":("rust/crates/xmltv-parser/vendor/NOTICES",None),
    }
    for name in ("sqlx-core","sqlx-postgres"):
        for license in ("LICENSE-APACHE","LICENSE-MIT"):
            credits[f"{DOCS}/{name}/{license}"]=(f"rust/vendor/{name}-0.8.6/{license}",None)
    unit_docs = lambda path: f"{DOCS}/{path}"
    result = {
      "schema":1,"platform":"linux/arm64","application_revision":APPLICATION,"verified_docs_parent":BASE,
      "application_input_count":2020,"application_input_scope":SCOPES,"application_input_tree_sha256":SCOPE_SHA,
      "final_base":{"repository":"docker.io/library/debian:bookworm-slim",
                    "manifest":"sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a",
                    "config":"sha256:5b6c2c6cbc90b6085e3f9b3086110b1eccedb643374137bfdcd1d2f5c78f6cd1",
                    "diff_id":"sha256:c96c411090da09187c55ab267826b3903b542b5c03f1cafa912d60e060e7850c"},
      "credit":CREDIT,"redistribution_qualified":False,"assembled_runtime_verified":False,
      "credits":{},
      "compose_role_pins":path_pin("m10-arm64-compose-runtime-pins-1/receipt.json"),
      "compose_role_config":path_pin("m10-arm64-compose-runtime-pins-1/compose.clean-install.arm64.yml"),
      "components":{
        "rust":unit("m10-a7ccdd1f-arm64-rust-build-actions-1","504b2851608c829ab68fc3500b79fa56cde1c8f1471db68c951496c942828ded",
                    [integration("rust-build")],"build",
                    {"/out/dispatcharr-rs":app_bin[0],"/out/dispatcharr-operator":app_bin[1]},
                    app_bin,[],app_bin),
        "guarded-pg":unit("m10-2581-arm64-helper-actions-2","29a39605e439c7a0bdb6c5dc235a0e9212f532fbddca21ca5532701848682ba0",
                    [integration("guarded-helper"),integration("rust-build")],"taskless-restore-helper",
                    {"/taskless-out/opt/dispatcharr-pg-guarded-restore/":"opt/dispatcharr-pg-guarded-restore/",
                     "/taskless-out/usr/share/doc/dispatcharr/pg-guarded-restore/":unit_docs("pg-guarded-restore/")},
                    [],["opt/dispatcharr-pg-guarded-restore",unit_docs("pg-guarded-restore")],
                    [helper,unit_docs("pg-guarded-restore/provenance.json")]),
        "react":unit("m10-dd9c2450-arm64-node-frontend-actions-2","24721a67f62cb9747a483c6c89b68d4a3aafc9ac46baa3e66742f39bf9d4b746",
                    [integration("node-frontend")],"frontend",{"/dispatcharr-frontend/dist/":"opt/dispatcharr-frontend/"},
                    [],["opt/dispatcharr-frontend"],[node_index]),
        "drf":unit("m10-dd9c2450-arm64-drf-static-actions-1","49c147362b8c16efd7c5522d8da9959b11f392247fc56779e5be5ab74ae98eea",
                   [integration("drf-static")],"exact-donor-readonly-collector",
                   {"/media-runtime/opt/dispatcharr-frontend/static/rest_framework/":"opt/dispatcharr-frontend/static/rest_framework/",
                    "/media-runtime/usr/share/doc/dispatcharr/":DOCS+"/"},
                   [unit_docs(name) for name in ("DRF-LICENSE.md","drf-static-export.json","drf-static-source-1.json")],
                   ["opt/dispatcharr-frontend/static/rest_framework"],[unit_docs("drf-static-export.json")]),
        "ffmpeg-vlc":unit("m10-native-arm64-vlc-debian-wrapper-2","50c6465844521e478c85e7b4c4f269e0472863bd4a9ca0ec1e5aa64977424148",
                          [integration("ffmpeg-debian-wrapper"),integration("vlc-debian-wrapper")],"collect",
                          {"/closure/":"","/vlc-closure/":""},fw,
                          ["opt/dispatcharr-media","opt/dispatcharr-vlc",f"{DOCS}-standalone-media",f"{DOCS}-standalone-vlc"],fw),
        "postgres":unit("m10-a7ccdd1f-arm64-postgres-debian-actions-2","723a8ea79b99b2d0471be25c84a128f01dfa43ad53b95fc37b76dd9e65550cd5",
                        [git_pin("scripts/rust-platform-contracts/m10-native-arm64-postgres-debian-wrappers.json"),
                         git_pin("scripts/rust-platform-contracts/m10-native-arm64-postgres-client-roundtrip.json")],
                        "collect",{"/closure/":""},pw,["opt/dispatcharr-postgres",f"{DOCS}-standalone-postgres"],pw),
        "crypto":unit("m10-96e49a90-arm64-gpg-debian-actions-5","89b53b10dc402f4d662fcd640cff5dc5a5bfc644c6d06c15e3c9fb34e5ab308a",
                      [integration("gpg-debian")],"donor",{"/media-runtime/":""},
                      ["usr/local/bin/gpg",unit_docs("crypto-runtime.json"),*credit_paths],
                      ["opt/dispatcharr-crypto",unit_docs("crypto-copyright")],["usr/local/bin/gpg",unit_docs("crypto-runtime.json")]),
        "python":unit("m10-3344bef3-arm64-python-host-debian-actions-1","16cb34c5b0e2dc9a1281db4d9167fe0f7436af3ef0d85cf7a31f55f7a05649d6",
                      [path_pin("m10-3344bef3-arm64-python-host-debian-actions-1/qualified-publication-1/m10-native-arm64-python-host-debian.json"),
                       integration("python-host-inputs")],"donor",{"/media-runtime/":""},
                      ["usr/local/bin/dispatcharr-"+name for name in ("python","plugin-python","regex-python")]+credit_paths,
                      ["opt/dispatcharr-python",unit_docs("python-host"),unit_docs("regex-host")],
                      ["opt/dispatcharr-python/bin/python3.13",unit_docs("python-host/manifest.json"),
                       unit_docs("regex-host/manifest.json"),"usr/local/bin/dispatcharr-plugin-python"]),
        "comskip":unit("m10-3344bef3-arm64-comskip-debian-actions-1","6e77ec18e261564c73bbea35c62d7b14c400c0ddbae54910988ec674ccec387b",
                       [integration("comskip-collector"),
                        path_pin("m10-3344bef3-arm64-comskip-debian-actions-1/qualified-publication-1/m10-native-arm64-comskip-debian.json")],
                       "collect",{"/comskip-closure/":""},
                       ["app/docker/comskip.ini","usr/local/bin/comskip"],["opt/dispatcharr-comskip",f"{DOCS}-standalone-comskip"],
                       ["app/docker/comskip.ini","usr/local/bin/comskip","opt/dispatcharr-comskip/bin/comskip",
                        f"{DOCS}-standalone-comskip/unit-inputs.json"]),
      },
      "required_runtime_checks":["owned compose clean install on exact local image","native Postgres and Redis role identity",
                                 "normal browser and media startup on same image","guarded copied PostgreSQL restore",
                                 "exact process and container retirement"],
    }
    for destination,(source,expected) in credits.items():
        blob = git("show",f"{BASE}:{source}")
        digest = stamp(blob)
        assert expected is None or digest == expected
        result["credits"][destination] = {"source":source,"sha256":digest}
    comskip = result["components"]["comskip"]["prior_evidence"][-1]
    assert comskip["sha256"] == "13f45c1dba158f0e1506593df18be6bbb191266571460c88cf1d009dc58f76c8"
    observed = json.loads((ARTIFACTS / comskip["path"]).read_bytes())
    assert observed["github"]["run"] == 44877868206
    assert observed["controller_exit"] == observed["guest_exit"] == observed["build_exit"] == 0
    assert all(observed["cleanup"].values()) and observed["runtime"]["media"]["audio_video_decode_completed"]
    expected_roles = {"compose_role_pins":"ac18a2b82c773dea5d38ef540b135896e1672f6f160db8bd12260db47a5b30e2",
                      "compose_role_config":"c846945218a4955c19b5c825282f5246f3e6b9a1f78d2c556d008d94b1c9a67b"}
    assert all(result[key]["sha256"]==value for key,value in expected_roles.items())
    return result


if __name__ == "__main__":
    print(json.dumps(make(), indent=2, sort_keys=True))
