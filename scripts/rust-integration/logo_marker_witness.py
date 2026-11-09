"""Shared Redis processed-file interoperability witness, no raw values emitted.
Original Dispatcharr contributors retain application credit.
"""
import os
def clear_markers(paths):
 from core.utils import RedisClient
 client=RedisClient.get_client()
 keys=['processed_file:'+path for path in paths]
 if keys:client.delete(*keys)
def marker_witness(paths):
 from core.utils import RedisClient
 client=RedisClient.get_client();result={}
 for path in paths:
  key='processed_file:'+path;value=client.get(key)
  if value is None:result[path]={'present':False};continue
  if isinstance(value,bytes):value=value.decode('utf-8')
  result[path]={'present':True,'three_day_ttl':259190<=client.ttl(key)<=259200,'mtime_matches_original_python':os.path.isfile(path)and value==str(os.path.getmtime(path))}
 return result
