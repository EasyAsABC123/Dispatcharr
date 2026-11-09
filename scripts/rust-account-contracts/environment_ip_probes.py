"""Packaged enabled-IP endpoint wiring, cached or lock-held only.

Background private-provider execution is covered by the separate 18-case
original-worker fixture. These package probes never start external lookups.
"""
import hashlib
import json
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()
def run(http,baseline,candidate,accounts,record,fixture):
    try:
        for mode in ['fresh','negative','locked-cold']:
            for role,status in [('admin',200),('standard',200),('viewer',200),('inactive',401),(None,401)]:
                results=[]
                for base in [baseline,candidate]:
                    fixture({'action':'environment_ip_prepare','mode':mode})
                    results.append(http(base+'/api/core/settings/env/',token=accounts[role]['access'] if role else None))
                a,b=results;name='environment-ip:'+mode+':'+str(role)
                passed=a[0]==b[0]==status and digest(a[1])==digest(b[1])
                if status==200:passed=passed and a[1].get('ip_lookup_enabled') is True and a[1].get('ip_lookup_pending') is (mode=='locked-cold')
                record(name,passed,{'python_status':a[0],'rust_status':b[0],'python_body_sha256':digest(a[1]),'rust_body_sha256':digest(b[1])})
    except Exception as error:record('environment-ip:fixture-failure',False,{'error_type':type(error).__name__})
    finally:fixture({'action':'environment_ip_restore'})
