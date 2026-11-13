"""Exact private-to-public projection and finite credential/namespace inventory."""
import base64,hashlib,json,sys,subprocess
from pathlib import Path
import prepare as P
def need(v,label):
 if not v:raise ValueError(label)
def main():
 secrets=set();usernames=set();comparisons=0;expected_attempts=[];bindings=json.loads((P.OUT/'private-input-hashes.json').read_text())
 for path,expected in bindings.items():need(P.sha(P.ROOT/path)==expected,'UnchangedExactPrivateInput')
 for n in(1,2,3):
  src=P.ROOT/f'artifacts/rust/m7-xc-late-ready-native-{n}';raw=json.loads((src/'faults-private.json').read_text())
  e=json.loads((src/'execution.json').read_text());expected_attempts.append({'attempt':n,'controller_exit':e['controller_exit'],'passed':e['passed'],'default_before':P.project(json.loads((src/'default-before-private.json').read_text())),'default_after':P.project(json.loads((src/'default-after-private.json').read_text())),'backend_after':json.loads((src/'owned-backends-after-Native.json').read_text()),'raw_capture_passed':raw['passed']})
  if n>1:
   need(json.loads((P.OUT/f'retained/attempt-{n}/faults.json').read_text())==P.faults(raw,n==2),'ExactFaultProjection');comparisons+=1
  def collect(v,key=''):
   if isinstance(v,dict):
    for k,x in v.items():collect(x,k)
   elif isinstance(v,list):
    for x in v:collect(x,key)
   elif isinstance(v,str):
    if key in('username','password','api_key','APIkey','database','nonce')and len(v)>=8:
     secrets.add(v.encode())
     if key=='username':usernames.add(v.encode())
    if key in('parameter_hex','first_parameter_hex'):
     try:
      b=bytes.fromhex(v)
      if len(b)>=12:secrets.add(b)
     except ValueError:pass
  collect(raw);seed=json.loads((src/'Native-legacy-seed-private.json').read_text());collect(seed)
 need(json.loads((P.OUT/'attempts.json').read_text())==expected_attempts,'ExactAttemptAuditProjection')
 public=b'\n'.join(p.read_bytes()for p in P.OUT.rglob('*')if p.is_file()and '__pycache__'not in p.parts)
 occurrences=[];public_source_collisions=[]
 base=json.loads((P.OUT/'current-union/selected-hunks.json').read_text())['base_revision'];source_path='rust/crates/xc-http/src/gateway_tests.rs';base_bytes=subprocess.run(['git','-C',str(P.ROOT),'show',base+':'+source_path],capture_output=True,check=True).stdout
 for b in secrets:
  for mode,value in(('plain',b),('hex',b.hex().encode()),('base64',base64.b64encode(b))):
   if value in public:
    matches=[p for p in P.OUT.rglob('*')if p.is_file()and '__pycache__'not in p.parts and value in p.read_bytes()]
    expected=P.OUT/'current-union/overlays'/source_path
    if b in usernames and mode=='plain'and value in base_bytes and matches==[expected]:public_source_collisions.append({'value_sha256':hashlib.sha256(b).hexdigest(),'kind':'static username literal already in immutable Git test preimage','base_revision':base,'path':source_path})
    else:occurrences.append({'credential_sha256':hashlib.sha256(b).hexdigest(),'encoding':mode})
 need(not occurrences,'NoCredentialOrDatabaseDerivationMaterial')
 return {'exact_fault_projections':comparisons,'finite_sensitive_values':len(secrets),'encodings':['plain','hex','base64'],'occurrences':occurrences,'public_Git_static_test_collisions':public_source_collisions,'private_raw_inputs_unchanged':True,'application_executed':False}
if __name__=='__main__':print(json.dumps(main(),indent=2))
