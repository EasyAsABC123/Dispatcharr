"""Original M3UFilter API CRUD oracles, including literal regex validation.

Original Dispatcharr authors retain credit. No filter refresh/import job is
triggered; bodies and provider credential properties never enter reports.
"""
import copy
import hashlib
import json
import uuid


def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()
def normalized(value):
    value=copy.deepcopy(value)
    if isinstance(value,dict) and 'id' in value and 'regex_pattern' in value:value['id']='<created-filter-id>'
    return value


def run(http,baseline,candidate,accounts,record):
    account=accounts['_fixtures']['provider_id'];other=accounts['_fixtures']['inactive_provider_id']
    root='/api/m3u/accounts/'+str(account)+'/filters/';other_root='/api/m3u/accounts/'+str(other)+'/filters/'
    prefix='rust-probe-filter-'+uuid.uuid4().hex[:12];admin=accounts['admin']['access']
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    def lifecycle(base):
        owned={};results={}
        def add(name,response,status):results[name]=(response,status)
        try:
            payload={'id':2147483647,'regex_pattern':' ['+prefix+' ',
                     'm3u_account':other,'custom_properties':{'n':1e23,'fraction':1.5,'integral':2.0}}
            created=invoke(base,root,payload,'POST');add('create-invalid-regex-model-clean-not-called',created,201)
            if (created[0]!=201 or created[1].get('id')==2147483647
                or created[1].get('regex_pattern')!='['+prefix):raise ValueError('Source regex or readonly identity contract differs')
            object_id=created[1]['id'];owned[object_id]='['+prefix;path=root+str(object_id)+'/'
            add('create-defaults',(200,{key:created[1][key] for key in ['filter_type','exclude','order']}),200)
            if {key:created[1][key] for key in ['filter_type','exclude','order']}!={'filter_type':'group','exclude':True,'order':0}:raise ValueError('Source defaults differ')
            add('jsonb-following-get',invoke(base,path),200)
            add('wrong-parent-get',invoke(base,other_root+str(object_id)+'/'),404)
            add('wrong-parent-patch',invoke(base,other_root+str(object_id)+'/',{'regex_pattern':'bad'},'PATCH'),404)
            add('wrong-parent-delete',invoke(base,other_root+str(object_id)+'/',method='DELETE'),404)
            for role in ['standard','viewer']:
                token=accounts[role]['access']
                add('denied-create-'+role,invoke(base,root,{'regex_pattern':prefix},'POST',token),403)
                add('denied-update-'+role,invoke(base,path,{'regex_pattern':'bad'},'PATCH',token),403)
                add('denied-delete-'+role,invoke(base,path,method='DELETE',token=token),403)
            invalid=[('required',{}),('null-regex',{'regex_pattern':None}),('blank-regex',{'regex_pattern':' '}),
                     ('long-regex',{'regex_pattern':'x'*201}),('nul-regex',{'regex_pattern':'bad\0name'}),
                     ('null-choice',{'regex_pattern':prefix,'filter_type':None}),
                     ('bad-choice',{'regex_pattern':prefix,'filter_type':'bad'}),
                     ('untrimmed-choice',{'regex_pattern':prefix,'filter_type':' group '}),
                     ('boolean-choice',{'regex_pattern':prefix,'filter_type':True}),
                     ('list-choice',{'regex_pattern':prefix,'filter_type':[1]}),
                     ('dict-choice',{'regex_pattern':prefix,'filter_type':{'a':1}}),
                     ('null-exclude',{'regex_pattern':prefix,'exclude':None}),
                     ('bad-exclude',{'regex_pattern':prefix,'exclude':'invalid'}),
                     ('negative-order',{'regex_pattern':prefix,'order':-1}),
                     ('bool-order',{'regex_pattern':prefix,'order':True}),
                     ('large-order',{'regex_pattern':prefix,'order':2147483648}),('nonobject',[])]
            for name,body in invalid:add('validation-'+name,invoke(base,root,body,'POST'),400)
            add('put-regex-required',invoke(base,path,{},'PUT'),400)
            add('patch-empty',invoke(base,path,{},'PATCH'),200)
            readonly=invoke(base,path,{'id':2147483647,'m3u_account':other,'unknown':True},'PATCH')
            if readonly[0]==200 and readonly[1]['id']!=object_id:raise ValueError('Readonly identity changed')
            add('readonly-parent-and-id-ignored',readonly,200)
            add('coerced-numeric-order-and-bool',invoke(base,path,{'filter_type':'url','order':'2.0','exclude':'off'},'PATCH'),200)
            add('nullable-json-scalar',invoke(base,path,{'custom_properties':None},'PATCH'),200)
            add('json-scalar-false',invoke(base,path,{'custom_properties':False},'PATCH'),200)
            renamed='('+prefix
            add('put-preserves-omitted-optionals',invoke(base,path,{'regex_pattern':renamed},'PUT'),200);owned[object_id]=renamed
            add('following-put-get',invoke(base,path),200)
            add('delete',invoke(base,path,method='DELETE'),204)
            add('read-deleted',invoke(base,path),404)
            add('delete-missing',invoke(base,path,method='DELETE'),404)
            add('missing-before-validation',invoke(base,path,{},'PUT'),404)
            return results
        finally:
            for object_id,pattern in owned.items():
                path=root+str(object_id)+'/';row=invoke(baseline,path)
                if row[0]==404:continue
                if row[0]!=200 or row[1].get('regex_pattern')!=pattern or prefix not in pattern:raise ValueError('Unsafe filter cleanup')
                if invoke(baseline,path,method='DELETE')[0]!=204:raise ValueError('Filter cleanup failed')
    outcomes=[]
    for label,base in [('python',baseline),('rust',candidate)]:
        try:outcomes.append(lifecycle(base))
        except Exception as error:record('filter-crud:'+label+':lifecycle',False,{'error_type':type(error).__name__});outcomes.append(None)
    if None in outcomes:return
    left,right=outcomes
    for name in sorted(set(left)|set(right)):
        if name not in left or name not in right:record('filter-crud:'+name,False,{'missing_result':True});continue
        a,status=left[name];b,other_status=right[name];av,bv=normalized(a[1]),normalized(b[1])
        record('filter-crud:'+name,a[0]==b[0]==status==other_status and digest(av)==digest(bv),
               {'python_status':a[0],'rust_status':b[0],'python_body_sha256':digest(av),'rust_body_sha256':digest(bv)})
