"""Run the exact scoped saved Stats live/ghost case under the one M6 dispatcher lease."""
import argparse
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
try:
    import recording61_owner as common
finally:
    sys.path.pop(0)

SCRIPT = Path(__file__).resolve()
NODE = "m6/stats-live-ghost"
PREDECESSOR = "m6/backend-cleanup-expiry"
STAGE_SEAL = "a3ef8011a90b7889cb311143d7c26e4e5e0b4b60295c1f866af45714031aa25b"
EXPECTED = {"provider.py","profile.json","run.py","saved_stats.py","stats_daphne.py","stats_ws.py"}
ORIGINAL_PROOF = "c4ef0dbf481932dc1df0c4a596682be4f514545f47f768c077ea8f091facd406"
require, read, digest = common.require, common.read, common.digest


def imported(stage):
    sys.path.insert(0,str(stage))
    try:
        spec=spec_from_file_location("exact_final_stats_workload",stage/"run.py")
        fixture=module_from_spec(spec)
        spec.loader.exec_module(fixture)
        return fixture
    finally:
        sys.path.pop(0)


def plan(stage, seal, receiving, common_path, custody, package_seal, candidate, repository):
    art=(repository/"artifacts/rust").resolve(strict=True)
    require(stage.resolve()==stage and stage.parent==art and seal==STAGE_SEAL,
            "exact reviewed ordinary Stats stage required")
    inputs,names = common.sealed_inputs(stage,seal)
    require(names==EXPECTED,"complete exact Stats stage")
    require(receiving.resolve()==receiving and receiving==art/"candidate54-isolated-publication-1/repo",
            "exact receiving sources for scoped Stats")
    require(common_path.resolve()==common_path and common_path.is_relative_to(art)
            and read(common_path)["candidate"]==candidate and custody.resolve()==custody and custody.is_relative_to(art),
            "exact same common Stats package")
    package_file=Path(candidate["build_manifest_file"])
    funds={str(common_path):digest(common_path),str(custody):digest(custody),
           str(package_file):digest(package_file),str(SCRIPT):digest(SCRIPT)}
    profile = read(stage/"profile.json")
    # Keep the generic manifest's bounded read budget. Some packages exceed
    # read()'s small structured receipt limit, so use the real build custody
    # with a wider explicit ceiling for this one already hashed file.
    require(package_file.stat().st_size <= 24*1024*1024, "bounded packaged Stats manifest")
    manifest = json.loads(package_file.read_bytes())
    require(funds[str(package_file)]==candidate["build_manifest_sha256"]
            and manifest["git_revision"]==candidate["git"] and manifest["image_id"]==candidate["image_id"]
            and all(manifest["source_sha256"].get(name)==expected for name,expected in
                    profile["required_application_leaves"].items()),
            "exact current Stats manifest and approved source leaf digests")
    args=SimpleNamespace(common=common_path,common_sha256=funds[str(common_path)],custody=custody,
                         package_seal=package_seal,receiving=receiving)
    return inputs|funds,args


def dispatch_spec(stage, seal, receiving, common_path, custody, package_seal, candidate, repository, entrypoint, output):
    require(output.resolve()==output and not output.is_relative_to(stage) and not stage.is_relative_to(output),
            "exact distinct coordinator and Stats owner paths")
    inputs,_=plan(stage,seal,receiving,common_path,custody,package_seal,candidate,repository)
    coordinator=repository/"scripts/rust-milestone-batches/coordinator.py"
    inputs[str(coordinator)]=digest(coordinator)
    tooling=repository/"artifacts/rust/tooling"
    head=["/usr/bin/python3","-I","-B",str(SCRIPT),str(repository),str(stage),seal,str(receiving),str(common_path),
          str(custody),package_seal,json.dumps(candidate,sort_keys=True)]
    return {"argv":[*head,"run",entrypoint,str(output),str(coordinator),digest(coordinator)],
            "cleanup_argv":[*head,"cleanup"],
            "evaluator_argv":[*head,"evaluate",entrypoint,str(output),"@EXECUTION@"],
            "cwd":str(repository),"environment":{"DOCKER_HOST":common.DAEMON,"DOCKER_CONFIG":str(tooling/"docker-config"),
                 "PATH":f"{tooling/'docker'}:/usr/bin:/bin"},
            "strip_inherited_prefixes":common.STRIP_ENV,"input_sha256":inputs,"seconds":900,
            "cleanup_seconds":60,"output_bytes":65536,"exclusive_heavy":True}


def load_scope(stage,args):
    runtime=imported(stage)
    receiver,base,ffmpeg=runtime.provider.owner()
    base.__file__=str(stage/"run.py")
    runtime.provider.setup(receiver,base,ffmpeg)
    source=runtime.authority(ffmpeg)
    require(source["source_proof_sha256"]==ORIGINAL_PROOF,"exact settled Original saved Stats authority")
    build, app = runtime.package(args,source)
    return runtime,base,ffmpeg,build,app


def exact_cleanup(output, ffmpeg):
    report=read(output/"report.json")
    execution=read(output/"execution.json")
    require(execution["process_group_settled"] is execution["stdout_EOF"] is execution["stderr_EOF"] is True
            and execution["remaining_group_members"]==[]
            and all(report["cleanup"][key] is True for key in ffmpeg.CLEANUP),"actual scoped Stats runner retired")
    roots=list((output/"fixtures").glob("Native-*/independent-cleanup.json"))
    require(len(roots)==1,"one real Native copied Stats context")
    path=roots[0]
    closure=read(path)
    require(all(closure[key] is True for key in ("exact_owned_absence","all_original_process_generations_absent","host_CLI_groups_settled"))
            and closure["inventories"]=={"container":[],"network":[],"volume":[]} and closure["errors"]==[]
            and bool(closure["exact_CID_absence"])
            and all(item["actual_missing_object"] is True and item["original_host_generation_absent"] is True
                    for item in closure["exact_CID_absence"]),"exact application/instance/direct cleanup")
    return digest(path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("repository","stage","seal","receiving","common_file","custody","package_seal","candidate","operation"):
        p.add_argument(name)
    p.add_argument("remainder",nargs="*")
    options=p.parse_args()
    repository,stage,receiving,common_path,custody=map(Path,(options.repository,options.stage,options.receiving,
                                                            options.common_file,options.custody))
    candidate=json.loads(options.candidate)
    _,args=plan(stage,options.seal,receiving,common_path,custody,options.package_seal,candidate,repository)
    runtime,base,ffmpeg,build,app=load_scope(stage,args)
    output=stage/"actual-native-1"
    marker=(repository/"artifacts/rust").resolve(strict=True).parents[1]/".dispatcharr-recording-cohort-cleanup-required.json"
    if options.operation=="cleanup":
        require(not options.remainder and not os.path.lexists(marker),"no unproved scope or prior marker")
        print(json.dumps({"scope":NODE,"exact_owned_absence":True,"cleanup_sha256":exact_cleanup(output,ffmpeg)},sort_keys=True))
        return 0
    if options.operation=="evaluate":
        entrypoint, parent_output, execution_path=options.remainder
        execution=read(Path(execution_path))
        require(execution["id"]==NODE and common.settled(execution["execution"])
                and common.settled(execution["cleanup"]) and execution["cleanup_verified"] is True
                and common.attempt_directory(Path(parent_output),NODE)==Path(execution_path).parent,
                "actual canonical scoped Stats termination and cleanup")
        observation=runtime.reduce(output,app,ffmpeg)
        print(json.dumps({"scope":NODE,"entrypoint":entrypoint,"candidate":candidate,"semantic_assertions_passed":True,
                         "full_required_workflow":True,"stats":observation,"cleanup_sha256":exact_cleanup(output,ffmpeg)},sort_keys=True))
        return 0
    require(options.operation=="run","known Stats fixture phase")
    entrypoint,parent_output,controller_path,parent_sha=options.remainder
    binding=dispatch_spec(stage,options.seal,receiving,common_path,custody,options.package_seal,candidate,repository,
                          entrypoint,Path(parent_output))
    require(binding["argv"]==sys.orig_argv and Path.cwd()==repository,
            "exact actual canonical argv and working tree")
    controller=Path(controller_path)
    require(controller==repository/"scripts/rust-milestone-batches/coordinator.py","exact coordinator identity")
    lock=common.coordinator_parent(controller,parent_sha,Path(parent_output),binding,candidate,node=NODE)
    require(lock.samefile(runtime.provider.LOCK),"same original engine dispatch inode")
    require(all(os.environ.get(name)==value for name,value in binding["environment"].items())
            and not os.environ.get("DOCKER_CONTEXT") and not os.environ.get("USE_NGINX_ACCEL")
            and not any(os.path.lexists(path) for path in (output,marker,lock.parent/marker.name,lock.parent/".cleanup-required.json")),
            "fresh engine scope and no older uncertain teardown")
    docker=(repository/"artifacts/rust/tooling/docker/docker").resolve(strict=True)
    require(Path(shutil.which("docker") or "").resolve()==docker==ffmpeg.base.require_local_docker(docker),
            "current isolated daemon only")
    os.umask(0o077)
    token={"scope":NODE,"candidate":candidate["git"],"output":str(output)}
    with marker.open("x") as stream:
        json.dump(token,stream)
    status=base.launch(output,"Native",build,app,ffmpeg,docker)
    exact_cleanup(output,ffmpeg)
    require(read(marker)==token,"exact scope quarantine ownership")
    marker.unlink()
    return status


if __name__=="__main__":
    raise SystemExit(main())
