#!/usr/bin/env python3
"""Narrow PG authentication privacy projection; no application-data normalization.
Private raw evidence remains immutable. This is NOT lossless authentication replay.
"""
import base64,copy,hashlib
FRAME_KEYS={'connection','direction','kind_hex','payload_base64','epoch'}

def auth_kind(value):
 if not isinstance(value,dict)or value.get('direction')not in ('frontend','backend'):return None
 if value.get('kind_hex')not in ('70','52'):return None
 if set(value)!=FRAME_KEYS:raise ValueError('ExactRawPGAuthenticationFrameShapeRequired')
 payload=base64.b64decode(value['payload_base64'],validate=True)
 if value['direction']=='frontend'and value['kind_hex']=='70':return 'frontend-PasswordMessage',payload
 if value['direction']=='backend'and value['kind_hex']=='52':
  if len(payload)<4:raise ValueError('PGAuthenticationCodeRequired')
  code=int.from_bytes(payload[:4],'big')
  if code not in (0,10,11,12):raise ValueError('UnqualifiedPGAuthenticationMethod')
  if code in (11,12):return ('backend-SASLContinue'if code==11 else'backend-SASLFinal'),payload
 return None

def project(value,path=()):
 auth=auth_kind(value)
 if auth:
  kind,payload=auth;result=copy.deepcopy(value);result['payload_base64']=None;result['authentication_payload_redacted']={'kind':kind,'raw_bytes':len(payload),'raw_sha256':hashlib.sha256(payload).hexdigest(),'scope':'PG authentication payload only; original bytes private, not replay-qualified'}
  return result,[{'JSON_path':list(path),'kind':kind,'raw_bytes':len(payload),'raw_sha256':hashlib.sha256(payload).hexdigest()}]
 if isinstance(value,dict):
  result={};receipts=[]
  for key,item in value.items():
   result[key],added=project(item,path+(key,));receipts+=added
  return result,receipts
 if isinstance(value,list):
  result=[];receipts=[]
  for n,item in enumerate(value):
   projected,added=project(item,path+(n,));result.append(projected);receipts+=added
  return result,receipts
 return value,[]
