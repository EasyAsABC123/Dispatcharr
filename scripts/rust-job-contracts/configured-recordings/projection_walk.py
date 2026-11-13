"""Independent walk: no imports/calls of the projection implementation."""
import base64,hashlib

def verify(original,projected,path=()):
 if isinstance(original,dict)and original.get('direction')in('frontend','backend')and original.get('kind_hex')in('70','52'):
  raw=base64.b64decode(original['payload_base64'],validate=True);code=int.from_bytes(raw[:4],'big')if original['direction']=='backend'else None
  target=(original['direction']=='frontend'and original['kind_hex']=='70')or(original['direction']=='backend'and original['kind_hex']=='52'and code in(11,12))
  if target:
   assert set(projected)==set(original)|{'authentication_payload_redacted'},path
   assert projected['payload_base64']is None,path
   for key in original:
    if key!='payload_base64':assert type(original[key])is type(projected[key])and original[key]==projected[key],(path,key)
   receipt=projected['authentication_payload_redacted'];assert set(receipt)=={'kind','raw_bytes','raw_sha256','scope'},path
   assert receipt['raw_bytes']==len(raw)and receipt['raw_sha256']==hashlib.sha256(raw).hexdigest(),path
   expected='frontend-PasswordMessage'if original['direction']=='frontend'else'backend-SASLContinue'if code==11 else'backend-SASLFinal';assert receipt['kind']==expected,path
   assert receipt['scope']=='PG authentication payload only; original bytes private, not replay-qualified',path
   return 1
 assert type(original)is type(projected),(path,'type')
 if isinstance(original,dict):
  assert set(original)==set(projected),(path,'keys');return sum(verify(v,projected[k],path+(k,))for k,v in original.items())
 if isinstance(original,list):
  assert len(original)==len(projected),(path,'length');return sum(verify(a,b,path+(n,))for n,(a,b)in enumerate(zip(original,projected)))
 assert original==projected,(path,'value');return 0
