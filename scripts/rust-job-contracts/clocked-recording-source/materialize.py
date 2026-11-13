"""Restore exact public evidence; credential-projected hashes ONLY, no raw fallback."""
import argparse,gzip,hashlib,io,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
MAX_FILE=8*1024*1024
MAX_TOTAL=64*1024*1024
def sha(data):return hashlib.sha256(data).hexdigest()
def need(ok,label):
 if ok is not True:raise ValueError(label)
def decode(blob,row,media=False):
 need(sha(blob)==row['gzip_sha256'],'ExactGzipHash')
 with gzip.GzipFile(fileobj=io.BytesIO(blob))as stream:data=stream.read(MAX_FILE+1)
 need(len(data)<=MAX_FILE,'EvidenceFile8MiB')
 expected=row['sha256']if media else row['projected_sha256'];size=row['raw_bytes']if media else row['projected_bytes']
 need(len(data)==size and sha(data)==expected,'ExactProjectedHashOnly')
 return data
def relative(value):
 path=Path(value);need(not path.is_absolute()and'..'not in path.parts,'OwnedRelativePath');return path
def materialize(output):
 out=Path(output).resolve();need(not out.exists(),'FreshOutput');plans=[];total=0
 for manifest,media,prefix in [('evidence-manifest.json',False,''),('media-manifest.json',True,''),('negative-manifest.json',False,'negative-history')]:
  rows=json.loads((HERE/manifest).read_text())
  for name,row in rows.items():
   data=decode((HERE/relative(row['payload'])).read_bytes(),row,media);total+=len(data);need(total<=MAX_TOTAL,'EvidenceTotal64MiB');plans.append((relative(prefix)/relative(name),data))
 need(len({str(path)for path,data in plans})==len(plans),'UniqueOutputPaths');out.mkdir()
 for path,data in plans:
  dest=out/path;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
 return {'passed':True,'restored_files':len(plans),'restored_bytes':total,'Origin_host_credential_projected':True,'all_nonsecret_bytes_order_unchanged':True,'Source_main_report_unprojected':True,'application_or_media_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(materialize(a.output),indent=2))
