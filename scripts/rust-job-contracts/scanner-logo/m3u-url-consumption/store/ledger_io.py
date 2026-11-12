"""One owned-controller append ledger; no multiwriter atomicity guarantee.
Only successful return promises file fsync, plus parent-directory fsync on create.
Oversized records persist a small explicit refusal receipt instead of full data.
"""
import hashlib,json,os
from pathlib import Path

def append_receipt(path,entry,maximum=4194304):
 path=Path(path)
 data=(json.dumps(entry,default=str)+'\n').encode()
 full=len(data)<=maximum
 if not full:
  entry={'kind':'serialized-record-capacity-refusal','stage':str(entry.get('stage'))[:64],'case':str(entry.get('case'))[:64],'full_record_retained':False,'original_serialized_bytes':len(data),'original_serialized_sha256':hashlib.sha256(data).hexdigest(),'maximum':maximum,'prior_capacity_refusal_count':len(entry.get('capacity_refusals',[])),'capacity_refusals':['serialized-record-bytes']}
  data=(json.dumps(entry,default=str)+'\n').encode()
  if len(data)>4096:raise ValueError('Small capacity-refusal receipt exceeds4096')
 created=False
 try:
  fd=os.open(path,os.O_WRONLY|os.O_APPEND|os.O_CREAT|os.O_EXCL,0o600);created=True
 except FileExistsError:fd=os.open(path,os.O_WRONLY|os.O_APPEND)
 try:
  offset=0
  while offset<len(data):
   written=os.write(fd,memoryview(data)[offset:])
   if written<=0:raise OSError('Ledger zero-byte write')
   offset+=written
  os.fsync(fd)
 finally:os.close(fd)
 if created:
  directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(directory)
  finally:os.close(directory)
 return {'full_record_retained':full,'receipt_bytes':len(data),'receipt_sha256':hashlib.sha256(data).hexdigest(),'file_fsynced':True,'new_parent_directory_fsynced':created}
