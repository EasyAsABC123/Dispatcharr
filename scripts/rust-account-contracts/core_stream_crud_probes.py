"""No-job StreamProfile source goldens. Only disposable owned rows are mutated.

Only the created object's ID is normalized; existing FK IDs retain exact values.
Original Dispatcharr authors and contributors retain full application credit.
"""
import copy
import hashlib
import json
import uuid

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()
def normalize(value):
    value=copy.deepcopy(value)
    if isinstance(value,dict) and 'name' in value and 'id' in value:value['id']='<created-stream-profile-id>'
    return value

def run(http,baseline,candidate,accounts,record,fixture):
    root='/api/core/streamprofiles/';prefix='rust-probe-stream-profile-'+uuid.uuid4().hex[:8];token=accounts['admin']['access']
    agent=http(baseline+'/api/core/useragents/',token=token)
    if agent[0]!=200 or not agent[1]:record('core-stream-crud:fixture',False,{'user_agent_available':False});return
    agent=agent[1][0]['id']
    def invoke(base,path,body=None,method=None,role='admin'):return http(base+path,body=body,method=method,token=accounts[role]['access'] if role else None)
    def lifecycle(base):
        owned={};out={}
        def add(name,result,status):out[name]=(result,status)
        def create(suffix,**fields):
            result=invoke(base,root,{'name':prefix+'-'+suffix,'id':2147483647,**fields},'POST')
            if result[0]!=201 or result[1].get('id')==2147483647 or result[1].get('name')!=prefix+'-'+suffix:raise ValueError('Fixture creation failed')
            owned[result[1]['id']]=result[1]['name'];return result[1]['id'],result
        try:
            primary,result=create('primary');path=root+str(primary)+'/'
            add('create-defaults',result,201)
            duplicate,duplicate_result=create('primary');add('duplicate-name-allowed',duplicate_result,201)
            locked,locked_result=create('locked',locked=True);locked_path=root+str(locked)+'/'
            add('locked-create',locked_result,201)
            for role in ['standard','viewer','inactive',None]:
                expected=401 if role in ['inactive',None] else 403
                add('create-denied-'+str(role),invoke(base,root,{'name':prefix+'-denied'},'POST',role),expected)
                add('patch-denied-'+str(role),invoke(base,path,{'name':'denied'},'PATCH',role),expected)
                add('delete-denied-'+str(role),invoke(base,path,method='DELETE',role=role),expected)
            for name,body in [('missing',{}),('blank',{'name':' '}),('null',{'name':None}),('bool',{'name':True}),('too-long',{'name':'x'*256}),('invalid-multiple',{'name':None,'command':None,'parameters':None,'is_active':None,'locked':'bad','user_agent':2147483647}),('nonobject',[])]:
                add('create-validation-'+name,invoke(base,root,body,'POST'),400)
            add('put-required',invoke(base,path,{},'PUT'),400)
            readonly=invoke(base,path,{'id':2147483647,'unknown':True},'PATCH')
            if readonly[0]==200 and readonly[1].get('id')!=primary:raise ValueError('Readonly identity changed')
            add('patch-noop',readonly,200)
            add('strings-and-booleans',invoke(base,path,{'name':' '+owned[primary]+' ','command':' command ','parameters':' --unsafe [ ','is_active':'off','user_agent':str(agent)},'PUT'),200)
            add('parameters-blank-and-useragent-null',invoke(base,path,{'parameters':'  ','user_agent':None},'PATCH'),200)
            add('locked-noop',invoke(base,locked_path,{},'PATCH'),200)
            add('locked-useragent-allowed',invoke(base,locked_path,{'user_agent':agent},'PATCH'),200)
            for name,body in [('name',{'name':'changed'}),('command',{'command':'new'}),('parameters',{'parameters':'new'}),('active',{'is_active':False}),('unlock',{'locked':False}),('model-field-order',{'locked':False,'is_active':False})]:add('locked-'+name,invoke(base,locked_path,body,'PATCH'),400)
            add('locked-delete',invoke(base,locked_path,method='DELETE'),400)
            add('missing-before-validation',invoke(base,root+'2147483647/',{},'PUT'),404)
            before=fixture({'action':'link_stream_profile','profile_id':primary,'profile_name':owned[primary]})
            add('delete-normal',invoke(base,path,method='DELETE'),204)
            after=fixture({'action':'verify_stream_profile_removed'})
            if before!=after or after.get('relations')!=4:raise ValueError('Collector relations or timestamps changed')
            add('four-collector-relations',(200,{'relations':4,'timestamps_unchanged':True}),200)
            add('deleted-get',invoke(base,path),404)
            add('deleted-delete',invoke(base,path,method='DELETE'),404)
            return out
        finally:
            for object_id,name in owned.items():
                path=root+str(object_id)+'/';current=invoke(baseline,path)
                if current[0]==404:continue
                if current[0]!=200 or current[1].get('name')!=name or not name.startswith(prefix):raise ValueError('Unsafe fixture cleanup')
                # Locked fixture is deliberately undeletable through the API.
                if current[1].get('locked'):continue
                if invoke(baseline,path,method='DELETE')[0]!=204:raise ValueError('Cleanup failed')
    results=[]
    for label,base in [('python',baseline),('rust',candidate)]:
        try:results.append(lifecycle(base))
        except Exception as error:record('core-stream-crud:'+label+':lifecycle',False,{'error_type':type(error).__name__});results.append(None)
    if None in results:return
    for name in sorted(set(results[0])|set(results[1])):
        a,expected=results[0][name];b,other=results[1][name]
        record('core-stream-crud:'+name,a[0]==b[0]==expected==other and digest(normalize(a[1]))==digest(normalize(b[1])),{'python_status':a[0],'rust_status':b[0],'python_body_sha256':digest(normalize(a[1])),'rust_body_sha256':digest(normalize(b[1]))})
