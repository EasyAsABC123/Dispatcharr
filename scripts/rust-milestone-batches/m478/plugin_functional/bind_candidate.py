"""Bind the ONE completed common image; file work only, never app dispatch."""
import argparse,hashlib,importlib.util,json,subprocess,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent

def need(value,label):
    if not value:raise ValueError(label)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--build-dir',type=Path,required=True);parser.add_argument('--source-map',type=Path,required=True);parser.add_argument('--candidate',type=Path,required=True);parser.add_argument('--binding',type=Path,required=True);parser.add_argument('--run-output',type=Path,required=True);args=parser.parse_args()
    manifest=args.build_dir.absolute()/'build.json';archive=args.build_dir.absolute()/'source.tar';build=json.loads(manifest.read_text())
    need(build['passed'] is True and type(build['exit_code']) is int and build['exit_code']==0,'actual completed common image build0')
    candidate={'git':build['git_revision'],'source_map_file':str(args.source_map.absolute()),'source_map_sha256':sha(args.source_map),'build_manifest_file':str(manifest),'build_manifest_sha256':sha(manifest),'image_id':build['image_id'],'source_archive_file':str(archive),'source_archive_sha256':sha(archive)}
    spec=importlib.util.spec_from_file_location('candidate_validation',HERE.parent/'batch.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);need(module.candidate_ready(candidate),'same exact committed source/archive/build/image binding')
    need(not args.candidate.exists() and not args.binding.exists() and not args.run_output.exists(),'fresh Root candidate/binding/execution outputs')
    args.candidate.parent.mkdir(parents=True,exist_ok=True);args.binding.parent.mkdir(parents=True,exist_ok=True);args.candidate.write_text(json.dumps(candidate,indent=2)+'\n')
    result=subprocess.run([sys.executable,'-B',str(HERE/'run.py'),'--emit-binding','--candidate',str(args.candidate.absolute()),'--output',str(args.run_output.absolute())],capture_output=True,text=True,timeout=10,check=False);need(result.returncode==0,'actual file-only binding export0');binding=json.loads(result.stdout);args.binding.write_text(json.dumps(binding,indent=2)+'\n')
    print(json.dumps({'candidate':str(args.candidate),'binding':str(args.binding),'image_id':candidate['image_id'],'Source_API_calls':0,'application_dispatches':0,'requires_Root_physical_image_proof_and_slot_before_dispatch':True}))
if __name__=='__main__':main()
