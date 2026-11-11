"""Build exact selected logo component peers; no mutable source capture/fallback.
Original Dispatcharr/dependency authors retain attribution and licensing.
"""
import argparse,hashlib,json,os,shutil,subprocess
from pathlib import Path

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();source=a.source.resolve();target=a.target.resolve();out=a.output.resolve();expected=json.loads(Path(__file__).with_name('source-inputs-759.json').read_text())
 def inputs():return {str(f.relative_to(source)):sha(f)for f in sorted((source/'rust').rglob('*'))if f.is_file()}
 if len(expected)!=759 or inputs()!=expected:raise ValueError('Exact qualified759 source required')
 if source==target or source in target.parents or source==out or source in out.parents:raise ValueError('Outputs must remain outside frozen source')
 out.mkdir(parents=True,exist_ok=False);environment=dict(os.environ,CARGO_TARGET_DIR=str(target));command=['cargo','test','--manifest-path',str(source/'rust/Cargo.toml'),'--locked','--offline','-p','dispatcharr-store','-p','dispatcharr-job-transport','--lib','--no-run','--message-format=json']
 with(out/'compiler.jsonl').open('w')as stdout,(out/'compiler.log').open('w')as stderr:subprocess.run(command,env=environment,stdout=stdout,stderr=stderr,check=True)
 if inputs()!=expected:raise ValueError('Frozen source changed during build')
 artifacts=[json.loads(s)for s in(out/'compiler.jsonl').read_text().splitlines()];peers={v['target']['name']:v['executable']for v in artifacts if v.get('reason')=='compiler-artifact'and v.get('executable')and v.get('profile',{}).get('test')}
 if set(peers)!={'dispatcharr_store','dispatcharr_job_transport'}:raise ValueError('Exactly two peers required')
 for name,path in peers.items():shutil.copy2(path,out/name)
 proof={'passed':True,'scope':'Fresh source-bound fixture peers, no resource/ELF transfer claim','source_file_count':759,'input_manifest_sha256':sha(Path(__file__).with_name('source-inputs-759.json')),'elf_sha256':{name:sha(out/name)for name in peers},'rustc':subprocess.check_output(['rustc','-Vv'],text=True),'cargo':subprocess.check_output(['cargo','-V'],text=True),'command':command,'compiler_json_sha256':sha(out/'compiler.jsonl')};(out/'build.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
if __name__=='__main__':main()
