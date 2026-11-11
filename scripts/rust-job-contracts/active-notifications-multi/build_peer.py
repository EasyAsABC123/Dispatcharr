"""Build/copy a fixture peer from exact reconstructed production library sources.
Original Dispatcharr/dependency authors retain code/license credit. No Docker,
application registration or resource sampling is performed by this builder.
"""
import argparse,hashlib,json,os,shutil,subprocess
from pathlib import Path

def digest(path):
 h=hashlib.sha256()
 with path.open('rb')as stream:
  while chunk:=stream.read(1024*1024):h.update(chunk)
 return h.hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();source=a.source.resolve();target=a.target.resolve();out=a.output.resolve()
 record=json.loads(Path(__file__).with_name('source-overlays.json').read_text());expected=record['expected_full_file_map'];actual={str(v.relative_to(source)):digest(v)for v in(source/'rust').rglob('*')if v.is_file()}
 if actual!=expected or len(actual)!=713:raise ValueError('Source differs from qualified complete713 map')
 if target==source or source in target.parents:raise ValueError('Generated Cargo target must stay outside frozen source')
 out.mkdir(parents=True,exist_ok=False);environment=dict(os.environ,CARGO_TARGET_DIR=str(target));cmd=['cargo','test','--manifest-path',str(source/'rust/Cargo.toml'),'--locked','--offline','-p','dispatcharr-job-transport','--lib','--no-run','--message-format=json']
 with(out/'compiler.jsonl').open('w')as stdout,(out/'compiler.log').open('w')as stderr:subprocess.run(cmd,env=environment,stdout=stdout,stderr=stderr,check=True)
 artifacts=[json.loads(line)for line in(out/'compiler.jsonl').read_text().splitlines()]
 peers=[Path(v['executable'])for v in artifacts if v.get('reason')=='compiler-artifact'and v.get('target',{}).get('name')=='dispatcharr_job_transport'and v.get('profile',{}).get('test')and v.get('executable')]
 if len(peers)!=1:raise ValueError('Exactly one compiled peer expected')
 binary=out/'native-default-peer';shutil.copy2(peers[0],binary);binary.chmod(0o755)
 proof={'passed':True,'source_file_count':713,'source_file_map_sha256':hashlib.sha256(json.dumps(actual,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'source_overlay_manifest_sha256':digest(Path(__file__).with_name('source-overlays.json')),'compiler_json_sha256':digest(out/'compiler.jsonl'),'elf_sha256':digest(binary),'rustc':subprocess.check_output(['rustc','-Vv'],text=True),'cargo':subprocess.check_output(['cargo','-V'],text=True),'command':cmd,'scope':'Fresh locally rebuilt executable; not claim of identical ELF or resource-performance transfer from prior execution'}
 (out/'build.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps({'passed':True,'binary':str(binary),'sha256':proof['elf_sha256']}))
if __name__=='__main__':main()
