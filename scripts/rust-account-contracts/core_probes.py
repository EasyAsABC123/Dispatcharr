"""Core settings/profile read contracts; original Dispatcharr authors retain credit.

Bodies can contain proxy credentials. Only canonical digests and statuses leave
this probe. No writes are performed by this read-only slice.
"""
import hashlib
import json


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def run(http, baseline, candidate, accounts, record):
    resources=('settings','useragents','streamprofiles','outputprofiles')
    for resource in resources:
        root='/api/core/'+resource+'/'
        for role,expected in [('admin',200),('standard',200),('viewer',403),
                              ('inactive',401),(None,401)]:
            name='core-read:'+resource+':'+str(role)
            try:
                token=accounts[role]['access'] if role else None
                old=http(baseline+root,token=token)
                new=http(candidate+root,token=token)
                record(name,old[0]==new[0]==expected and digest(old[1])==digest(new[1]),
                       {'python_status':old[0],'rust_status':new[0],
                        'python_body_sha256':digest(old[1]),'rust_body_sha256':digest(new[1])})
                if role=='standard' and old[0]==200 and isinstance(old[1],list):
                    ids=[str(old[1][0]['id'])] if old[1] else []
                    for suffix in ids+['2147483647','not-a-number']:
                        a=http(baseline+root+suffix+'/',token=token)
                        b=http(candidate+root+suffix+'/',token=token)
                        wanted=200 if suffix in ids else 404
                        record(name+':detail:'+suffix,a[0]==b[0]==wanted and digest(a[1])==digest(b[1]),
                               {'python_status':a[0],'rust_status':b[0],
                                'python_body_sha256':digest(a[1]),'rust_body_sha256':digest(b[1])})
            except Exception as error:
                record(name,False,{'error_type':type(error).__name__})
