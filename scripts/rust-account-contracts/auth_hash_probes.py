"""Packaged real HTTP password-upgrade comparisons, with source DB witnesses.

Only disposable fixture users and explicitly trusted synthetic client IPs are
used. Two real attempts share each per-IP budget; no history reset or bypass.
Original application credit remains with Dispatcharr contributors.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import uuid
_spec=importlib.util.spec_from_file_location('auth_hash_token_oracle',Path(__file__).with_name('startup_probes.py'))
_tokens=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_tokens)
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def run(http,baseline,candidate,accounts,record,fixture):
    prefix='rust-probe-packaged-hash-'+uuid.uuid4().hex[:12]
    for index,kind in enumerate(['rounds','sha1','salt','preferred','inactive','wrong','scrypt','scrypt-wrong']):
        control={'kind':kind,'username':prefix+'-'+kind,'client_ip':'198.18.2.'+str(index+1)}
        owned=False
        try:
            identity=fixture({'action':'auth_package_prepare',**control});control['user_id']=identity['id'];owned=True
            outcomes=[]
            for backend in [baseline,candidate]:
                reset=fixture({'action':'auth_package_reset',**control})
                if reset!={'reset':True}:raise RuntimeError('Credential fixture reset failed')
                status,body=http(backend+'/api/accounts/token/',body={'username':control['username'],'password':'wrong-fixture-only' if kind in ['wrong','scrypt-wrong'] else 'packaged-hash-fixture-only'},method='POST',headers={'X-Real-IP':control['client_ip'],'User-Agent':'rust-probe-packaged-auth-UA'})
                if status==200:body=_tokens.token_pair(body,control['user_id'])
                witness=fixture({'action':'auth_package_inspect',**control})
                outcomes.append({'status':status,'body':body,'witness':witness})
            expected=401 if kind in ['inactive','wrong','scrypt-wrong'] else 200
            record('packaged-auth-hash:'+kind,outcomes[0]['status']==outcomes[1]['status']==expected and digest(outcomes[0])==digest(outcomes[1]),{'python_status':outcomes[0]['status'],'rust_status':outcomes[1]['status'],'python_outcome_sha256':digest(outcomes[0]),'rust_outcome_sha256':digest(outcomes[1]),'login_attempts_per_fixture_ip':2})
        except Exception as error:record('packaged-auth-hash:'+kind,False,{'error_type':type(error).__name__})
        finally:
            if owned:
                try:
                    removed=fixture({'action':'auth_package_cleanup',**control})
                    record('packaged-auth-hash:cleanup:'+kind,removed=={'removed':True},{'removed':removed.get('removed') is True})
                except Exception as error:record('packaged-auth-hash:cleanup:'+kind,False,{'error_type':type(error).__name__})
