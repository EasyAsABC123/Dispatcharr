#!/usr/bin/env python3
"""Reconstruct exact historical source inputs offline; never builds or starts apps."""
import argparse,hashlib,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
REV="044f3ad0fb3ee68ffb17b23bbb796ccdae954298"
def sha(data):return hashlib.sha256(data).hexdigest()
def reconstruct(repo,output,variant):
 if output.exists():raise ValueError("FreshOutputRequired")
 expected=json.loads((ROOT/"executed-definition/inputs"/(variant+"-full898-map.json")).read_text())
 base=json.loads((ROOT/"base797-map.json").read_text());output.mkdir(parents=True)
 for name,digest in base.items():
  data=subprocess.run(["git","-C",str(repo),"show",REV+":"+name],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout
  if sha(data)!=digest:raise ValueError("ExactBaseGitBlob:"+name)
  target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 crate=ROOT/"sqlx-core-0.8.6.crate"
 if sha(crate.read_bytes())!="ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6":raise ValueError("ExactCrate")
 count=0
 with tarfile.open(crate)as archive:
  for member in archive:
   if not member.isfile():continue
   parts=Path(member.name).parts
   if parts[0]!="sqlx-core-0.8.6" or ".."in parts or member.size>8388608:raise ValueError("BoundedPackageMember")
   target=output/"rust/vendor/sqlx-core"/Path(*parts[1:]);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.extractfile(member).read());count+=1
 if count!=100:raise ValueError("Exact100PackageFiles")
 for overlay in (["both-overlay"]+(["candidate-overlay"]if variant=="candidate"else[])):
  for origin in (ROOT/overlay).rglob("*"):
   if origin.is_file():
    target=output/origin.relative_to(ROOT/overlay);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(origin.read_bytes())
 actual={str(p.relative_to(output)):sha(p.read_bytes())for p in output.rglob("*")if p.is_file()}
 if actual!=expected:raise ValueError("Exact898SourceDictionary")
 return {"variant":variant,"exact_source_files":len(actual),"base_revision":REV,"build_or_application_executed":False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument("--repository",required=True,type=Path);p.add_argument("--output",required=True,type=Path);p.add_argument("--variant",choices=["baseline","candidate"],required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repository,a.output,a.variant),indent=2))
if __name__=="__main__":main()
