"""Real Django/Rust profile-membership contracts, including side effects.

Creates only rust-probe-* profiles through the pinned original API. Does not
change fixture channels. Hashes alone record response bodies.
"""
import hashlib
import json
import uuid


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/profiles/'
    prefix='rust-probe-membership-'+uuid.uuid4().hex[:12]
    admin=accounts['admin']['access']
    def invoke(base,path,body=None,method=None,token=admin):
        return http(base+path,body=body,method=method,token=token)
    def capture(name,callback):
        try:return callback()
        except Exception as error:
            record('profile-memberships:'+name,False,{'error_type':type(error).__name__})
    def lifecycle(base,label):
        owned={};snapshots={};results={}
        def new_profile(name,empty=True):
            result=invoke(baseline,root,{'name':prefix+'-'+label+'-'+name,'start_empty':empty},'POST')
            if result[0]!=201 or not isinstance(result[1].get('id'),int):raise AssertionError('Profile fixture creation failed')
            owned[result[1]['id']]=result[1]['name'];return result[1]
        def state(profile):
            result=invoke(baseline,root+str(profile)+'/')
            if result[0]!=200:raise AssertionError('Profile fixture state unavailable')
            return 200,{'channels':result[1]['channels']}
        def add(step,response,expected):results[step]=(response,expected)
        try:
            inventory=new_profile('inventory',False)
            if not inventory['channels']:raise AssertionError('Fixture has no channels')
            channel=inventory['channels'][0]
            profile=new_profile('single')['id'];path=root+str(profile)+'/'
            single=path+'channels/'+str(channel)+'/'
            bulk=path+'channels/bulk-update/'
            for role in ('standard','viewer'):
                add('single-unassigned-'+role,invoke(base,single,{'enabled':True},'PATCH',accounts[role]['access']),404)
            add('single-invalid-creates-disabled',invoke(base,single,{'enabled':'not-a-boolean'},'PATCH'),400)
            add('after-invalid-disabled-state',state(profile),200)
            # updated=1/created=0 proves the rejected single request created a row.
            add('bulk-after-invalid-proves-row',invoke(base,bulk,{'channels':[{'channel_id':channel,'enabled':True}]},'PATCH'),200)
            add('single-empty-body',invoke(base,single,{},'PATCH'),200)
            add('single-float-boolean',invoke(base,single,{'enabled':0.0},'PATCH'),200)
            add('single-null-fields',invoke(base,single,{'channel':None,'enabled':None},'PATCH'),400)
            add('single-bool-pk',invoke(base,single,{'channel':True},'PATCH'),400)
            add('single-empty-pk',invoke(base,single,{'channel':''},'PATCH'),400)
            add('single-invalid-pk-string',invoke(base,single,{'channel':'1__0'},'PATCH'),400)
            add('single-channel-string-pk',invoke(base,single,{'channel':str(channel),'enabled':'on'},'PATCH'),200)
            add('single-missing-channel',invoke(base,path+'channels/2147483647/',{'enabled':True},'PATCH'),404)
            add('single-missing-profile',invoke(base,root+'2147483647/channels/'+str(channel)+'/',{'enabled':True},'PATCH'),404)
            for role in ('standard','viewer'):
                userpath='/api/accounts/users/'+str(accounts[role]['id'])+'/'
                before=invoke(baseline,userpath)
                if before[0]!=200:raise AssertionError('Probe user unavailable')
                snapshots[role]=before[1]['channel_profiles']
                if invoke(baseline,userpath,{'channel_profiles':snapshots[role]+[profile]},'PATCH')[0]!=200:raise AssertionError('Probe assignment failed')
                add('single-assigned-'+role,invoke(base,single,{'enabled':True},'PATCH',accounts[role]['access']),200)
                add('bulk-denied-'+role,invoke(base,bulk,{'channels':[{'channel_id':channel,'enabled':False}]},'PATCH',accounts[role]['access']),403)
            add('single-anonymous',invoke(base,single,{'enabled':True},'PATCH',None),401)
            add('bulk-anonymous',invoke(base,bulk,{'channels':[{'channel_id':channel,'enabled':False}]},'PATCH',None),401)
            cases=[('missing-list',{}),('empty-list',{'channels':[]}),('null-list',{'channels':None}),
                   ('wrong-list-type',{'channels':{}}),('null-item',{'channels':[None]}),
                   ('missing-nested-fields',{'channels':[{}]}),('wrong-nested-type',{'channels':[4]}),
                   ('boolean-id',{'channels':[{'channel_id':True,'enabled':True}]}),
                   ('bad-enabled',{'channels':[{'channel_id':channel,'enabled':None}]}),
                   ('bad-fractional-id',{'channels':[{'channel_id':1.5,'enabled':True}]}),
                   ('oversized-integer-string',{'channels':[{'channel_id':'1'*1001,'enabled':True}]}),
                   ('nested-errors',{'channels':[{}, {'channel_id':None,'enabled':None}]})]
            for name,body in cases:add('bulk-'+name,invoke(base,bulk,body,'PATCH'),400)
            big=123456789012345678901234567890
            add('bulk-large-invalid-id',invoke(base,bulk,{'channels':[{'channel_id':channel,'enabled':False},{'channel_id':big,'enabled':True}]},'PATCH'),400)
            add('invalid-bulk-preserves-previous-state',state(profile),200)
            add('bulk-duplicate-existing',invoke(base,bulk,{'channels':[{'channel_id':channel,'enabled':True},{'channel_id':str(channel)+'.0','enabled':False}]},'PATCH'),200)
            add('after-duplicate-existing-last-value',state(profile),200)
            second=new_profile('bulk')['id'];bulk2=root+str(second)+'/channels/bulk-update/'
            add('bulk-create',invoke(base,bulk2,{'channels':[{'channel_id':channel,'enabled':1.0}]},'PATCH'),200)
            add('bulk-update',invoke(base,bulk2,{'channels':[{'channel_id':channel,'enabled':False}]},'PATCH'),200)
            add('bulk-final-state',state(second),200)
            return results
        finally:
            for role,memberships in snapshots.items():
                path='/api/accounts/users/'+str(accounts[role]['id'])+'/'
                if invoke(baseline,path,{'channel_profiles':memberships},'PATCH')[0]!=200:raise AssertionError('User profile restore failed')
            for profile,name in owned.items():
                path=root+str(profile)+'/'
                result=invoke(baseline,path)
                if result[0]!=200 or result[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe fixture cleanup refused')
                if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Profile fixture cleanup failed')
    old=capture('python-lifecycle',lambda:lifecycle(baseline,'python'))
    new=capture('rust-lifecycle',lambda:lifecycle(candidate,'rust'))
    if old is None or new is None:return
    for name in sorted(set(old)|set(new)):
        if name not in old or name not in new:
            record('profile-memberships:'+name,False,{'missing_result':True});continue
        a,wanted=old[name];b,expected=new[name]
        record('profile-memberships:'+name,wanted==expected and a[0]==b[0]==wanted and digest(a[1])==digest(b[1]),
               {'python_status':a[0],'rust_status':b[0],
                'python_body_sha256':digest(a[1]),'rust_body_sha256':digest(b[1])})
