#!/usr/bin/env python3
"""Verify and restore the bounded actual Source evidence. Starts no services."""
import argparse,gzip,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
MAX_TOTAL=16*1024*1024

def restore(output,root=ROOT):
 output=Path(output);mapping=json.loads((root/'evidence-map.json').read_text())
 decoded={};total=0
 for name,item in mapping.items():
  if Path(name).name!=name or name in ('','.','..'):raise ValueError('Unsafe evidence name')
  packed=(root/'evidence'/(name+'.gz')).read_bytes()
  if hashlib.sha256(packed).hexdigest()!=item['gzip_sha256']:raise ValueError('Packed evidence hash')
  if type(item['bytes'])is not int or not 0<=item['bytes']<=MAX_TOTAL:raise ValueError('Evidence size bound')
  # Size bound during inflation, rather than after an unbounded decompression.
  import io
  with gzip.GzipFile(fileobj=io.BytesIO(packed))as f:data=f.read(item['bytes']+1)
  total+=len(data)
  if total>MAX_TOTAL or len(data)!=item['bytes']or hashlib.sha256(data).hexdigest()!=item['sha256']:raise ValueError('Decoded evidence hash/size')
  decoded[name]=data
 output.mkdir(parents=True,exist_ok=False)
 for name,data in decoded.items():(output/name).write_bytes(data)
 return {'passed':True,'files':len(decoded),'bytes':total,'all_original_bytes_restored':True,'application_replay_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(restore(a.output)))
