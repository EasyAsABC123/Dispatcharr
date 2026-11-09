"""Queued no-job ServerGroup CRUD oracles from original Dispatcharr semantics.

Original authors retain credit. Only disposable rust-probe-* rows are mutated;
field values and response bodies are not recorded.
"""
import copy
import hashlib
import json
import uuid


def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()
def normalize(value):
    value=copy.deepcopy(value)
    if isinstance(value,dict) and 'id' in value and 'name' in value:value['id']='<created-server-group-id>'
    return value


def run(http,baseline,candidate,accounts,record,fixture):
    root='/api/m3u/server-groups/';prefix='rust-probe-server-crud-'+uuid.uuid4().hex[:10]
    admin=accounts['admin']['access'];account=accounts['_fixtures']['inactive_provider_id']
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    def lifecycle(base):
        owned={};results={}
        def add(name,result,status):results[name]=(result,status)
        def create(suffix):
            name=prefix+'-'+suffix;result=invoke(base,root,{'name':name,'id':2147483647},'POST')
            if result[0]!=201 or result[1].get('name')!=name or result[1].get('id')==2147483647:raise ValueError('Fixture creation failed')
            owned[result[1]['id']]=name;return result[1]['id'],result
        try:
            primary,created=create('primary');add('create',created,201)
            duplicate,_=create('duplicate');path=root+str(primary)+'/'
            add('get-created',invoke(base,path),200)
            for role in ['standard','viewer']:
                token=accounts[role]['access']
                add('denied-create-'+role,invoke(base,root,{'name':prefix+'-bad'},'POST',token),403)
                add('denied-update-'+role,invoke(base,path,{'name':prefix+'-bad'},'PATCH',token),403)
                add('denied-delete-'+role,invoke(base,path,method='DELETE',token=token),403)
            for name,body in [('missing-name',{}),('null-name',{'name':None}),('blank-name',{'name':' '}),
                              ('boolean-name',{'name':True}),('long-name',{'name':'x'*101}),
                              ('null-character',{'name':'bad\0name'}),('nonobject',[])]:
                add('create-validation-'+name,invoke(base,root,body,'POST'),400)
            add('duplicate-create',invoke(base,root,{'name':owned[duplicate]},'POST'),400)
            add('duplicate-update',invoke(base,path,{'name':owned[duplicate]},'PATCH'),400)
            add('put-required-name',invoke(base,path,{},'PUT'),400)
            add('patch-empty',invoke(base,path,{},'PATCH'),200)
            readonly=invoke(base,path,{'id':2147483647,'unknown':True},'PATCH')
            if readonly[0]==200 and readonly[1].get('id')!=primary:raise ValueError('Readonly identity changed')
            add('readonly-and-unknown-noop',readonly,200)
            renamed=prefix+'-renamed'
            add('trimmed-put',invoke(base,path,{'name':' '+renamed+' '},'PUT'),200);owned[primary]=renamed
            before=invoke(baseline,'/api/m3u/accounts/'+str(account)+'/')
            if before[0]!=200 or before[1]['server_group'] is not None:raise ValueError('Unsafe fixture account')
            linked=fixture({'action':'link_server_group','account_id':account,'group_id':primary,'group_name':renamed})
            if linked!={'linked':True,'updated_at_preserved':True}:raise ValueError('Fixture linking failed')
            add('delete-linked-group',invoke(base,path,method='DELETE'),204)
            after=invoke(baseline,'/api/m3u/accounts/'+str(account)+'/')
            if (after[0]!=200 or after[1]['server_group'] is not None
                or before[1]['updated_at']!=after[1]['updated_at']):raise ValueError('Collector SET_NULL or timestamp differs')
            add('set-null-preserves-account',(200,{'server_group':None,'updated_at_unchanged':True}),200)
            add('read-deleted',invoke(base,path),404)
            add('delete-missing',invoke(base,path,method='DELETE'),404)
            add('missing-before-validation',invoke(base,path,{},'PUT'),404)
            return results
        finally:
            for object_id,name in owned.items():
                path=root+str(object_id)+'/';current=invoke(baseline,path)
                if current[0]==404:continue
                if current[0]!=200 or current[1].get('name')!=name or not name.startswith(prefix):raise ValueError('Unsafe cleanup')
                if invoke(baseline,path,method='DELETE')[0]!=204:raise ValueError('Cleanup failed')
    results=[]
    for label,base in [('python',baseline),('rust',candidate)]:
        try:results.append(lifecycle(base))
        except Exception as error:record('server-group-crud:'+label+':lifecycle',False,{'error_type':type(error).__name__});results.append(None)
    if None in results:return
    left,right=results
    for name in sorted(set(left)|set(right)):
        if name not in left or name not in right:record('server-group-crud:'+name,False,{'missing_result':True});continue
        a,expected=left[name];b,other=right[name]
        a_body,b_body=normalize(a[1]),normalize(b[1])
        record('server-group-crud:'+name,a[0]==b[0]==expected==other and digest(a_body)==digest(b_body),
               {'python_status':a[0],'rust_status':b[0],'python_body_sha256':digest(a_body),'rust_body_sha256':digest(b_body)})
