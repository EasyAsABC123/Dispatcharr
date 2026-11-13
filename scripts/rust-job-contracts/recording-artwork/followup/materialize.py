#!/usr/bin/env python3
"""Restore exact retained reports; no services, builds or private fallback."""
import argparse,gzip,hashlib,io,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
MAX_FILE=32*1024*1024
MAX_TOTAL=128*1024*1024
def restore(output,root=ROOT):
 output=Path(output)
 if output.exists():raise FileExistsError('Fresh output required')
 mapping=json.loads((root/'evidence-map.json').read_text());decoded={};total=0
 for name,item in mapping.items():
  if Path(name).name!=name or name in('','.','..'):raise ValueError('Flat evidence names required')
  size=item['bytes']
  if type(size)is not int or not 0<=size<=MAX_FILE:raise ValueError('Declared file bound')
  packed=(root/'evidence'/(name+'.gz')).read_bytes()
  if hashlib.sha256(packed).hexdigest()!=item['gzip_sha256']:raise ValueError('Compressed hash')
  with gzip.GzipFile(fileobj=io.BytesIO(packed))as stream:data=stream.read(size+1)
  total+=len(data)
  if len(data)!=size or total>MAX_TOTAL or hashlib.sha256(data).hexdigest()!=item['sha256']:raise ValueError('Inflated size/hash/total')
  decoded[name]=data
 output.mkdir(parents=True,exist_ok=False)
 for name,data in decoded.items():(output/name).write_bytes(data)
 return {'passed':True,'files':len(decoded),'bytes':total,'captured_report_bytes_restored_exactly':True,'raw_authentication_wire_replay_verified':False,'new_app_execution':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(restore(a.output)))
