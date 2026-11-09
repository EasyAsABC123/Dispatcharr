"""Disposable channel-profile lifecycle oracles against original Dispatcharr."""
import copy
import hashlib
import json
import uuid


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/profiles/'
    prefix='rust-probe-profile-'+uuid.uuid4().hex[:12]
    admin=accounts['admin']['access'];standard=accounts['standard']['access']
    def invoke(base,path,body=None,method=None,token=admin):
        return http(base+path,body=body,method=method,token=token)
    def capture(label,callback):
        try:return callback()
        except Exception as error:
            record('channel-profiles:'+label,False,{'error_type':type(error).__name__})
    def normalized(result):
        status,body=result;body=copy.deepcopy(body)
        if isinstance(body,dict) and 'id' in body:body['id']='<generated-profile-id>'
        return status,body
    def lifecycle(base):
        results={};owned={}
        try:
            for name,payload in [('default',{'name':prefix}),
                                 ('empty',{'name':prefix+'-empty','start_empty':True})]:
                result=invoke(base,root,payload,'POST');results['create-'+name]=(result,201)
                if result[0]!=201 or not isinstance(result[1].get('id'),int):raise AssertionError('Profile creation failed')
                owned[result[1]['id']]=result[1]['name']
            ids=list(owned);path=root+str(ids[0])+'/'
            results['retrieve']=(invoke(base,path),200)
            results['standard-unassigned']=(invoke(base,path,token=standard),404)
            results['duplicate-name']=(invoke(base,root,{'name':prefix},'POST'),400)
            results['missing-name']=(invoke(base,root,{},'POST'),400)
            results['bad-name']=(invoke(base,root,{'name':'x'*101},'POST'),400)
            results['null-start-empty']=(invoke(base,root,{'name':prefix+'-bad','start_empty':None},'POST'),400)
            results['readonly-channels']=(invoke(base,path,{'channels':[]},'PATCH'),200)
            results['put-required']=(invoke(base,path,{},'PUT'),400)
            results['rename']=(invoke(base,path,{'name':prefix+'-renamed'},'PATCH'),200)
            owned[ids[0]]=prefix+'-renamed'
            results['ignored-start-empty-on-update']=(invoke(base,path,{'start_empty':True},'PATCH'),200)
            results['duplicate-missing-name']=(invoke(base,path+'duplicate/',{},'POST'),400)
            results['duplicate-conflict-before-missing-source']=(invoke(base,root+'2147483647/duplicate/',{'name':prefix+'-empty'},'POST'),400)
            for name,source in [('default',ids[0]),('empty',ids[1])]:
                result=invoke(base,root+str(source)+'/duplicate/',{'name':prefix+'-copy-'+name},'POST')
                results['duplicate-'+name]=(result,201)
                if result[0]==201:owned[result[1]['id']]=result[1]['name']
            for role,expected in [('standard',403),('viewer',403),('inactive',401)]:
                results['denied-delete-'+role]=(invoke(base,path,method='DELETE',token=accounts[role]['access']),expected)
            # Prove delete clears the user M2M relation as Django's collector does.
            userpath='/api/accounts/users/'+str(accounts['standard']['id'])+'/'
            before=invoke(baseline,userpath)
            if before[0]!=200:raise AssertionError('Fixture user missing')
            memberships=before[1]['channel_profiles']
            if invoke(baseline,userpath,{'channel_profiles':memberships+[ids[0]]},'PATCH')[0]!=200:raise AssertionError('Fixture assignment failed')
            try:
                results['standard-assigned']=(invoke(base,path,token=standard),200)
                results['delete']=(invoke(base,path,method='DELETE'),204)
                results['read-deleted']=(invoke(base,path),404)
                remaining=invoke(baseline,userpath)
                if remaining[0]!=200 or remaining[1]['channel_profiles']!=memberships:raise AssertionError('Profile deletion left user relation')
                results['cascade-user-membership']=((200,{'cleared':True}),200)
            finally:
                if invoke(baseline,userpath,{'channel_profiles':memberships},'PATCH')[0]!=200:raise AssertionError('Fixture restore failed')
            return results
        finally:
            for object_id,name in owned.items():
                path=root+str(object_id)+'/'
                value=invoke(baseline,path)
                if value[0]==404:continue
                if value[0]!=200 or value[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe fixture cleanup refused')
                if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Profile cleanup failed')
    old=capture('python-lifecycle',lambda:lifecycle(baseline))
    new=capture('rust-lifecycle',lambda:lifecycle(candidate))
    if old is None or new is None:return
    for step in sorted(set(old)|set(new)):
        if step not in old or step not in new:
            record('channel-profiles:'+step,False,{'missing_result':True});continue
        a,wanted=old[step];b,expected=new[step];na=normalized(a);nb=normalized(b)
        record('channel-profiles:'+step,wanted==expected and a[0]==b[0]==wanted and digest(na)==digest(nb),
               {'python_status':a[0],'rust_status':b[0],
                'python_body_sha256':digest(na[1]),'rust_body_sha256':digest(nb[1])})
