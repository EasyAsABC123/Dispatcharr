"""Channel CRUD/relations oracle using only disposable rust-probe-* resources.

Normalize generated root channel IDs/UUIDs only. Shared reference fixtures keep
relation IDs and timestamps identical; credentials and bodies never enter logs.
"""
import copy
import hashlib
import json
import uuid
from urllib.parse import urlencode


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def normalized(value):
    value=copy.deepcopy(value)
    if isinstance(value,dict) and 'uuid' in value and 'id' in value:
        uuid.UUID(value['uuid'])
        value['id']='<generated-channel-id>';value['uuid']='<generated-channel-uuid>'
    return value


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/channels/';profiles='/api/channels/profiles/'
    streamroot='/api/channels/streams/';logoroot='/api/channels/logos/';grouproot='/api/channels/groups/'
    prefix='rust-probe-channel-crud-'+uuid.uuid4().hex[:10]
    admin=accounts['admin']['access'];fixtures=[];profile_ids=[];stream_ids=[];logo_id=None;group_id=None
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    def capture(name,callback):
        try:return callback()
        except Exception as error:
            record('channel-crud:'+name,False,{'error_type':type(error).__name__})
    def fixture(path,body):
        value=invoke(baseline,path,body,'POST')
        if value[0]!=201 or not isinstance(value[1].get('id'),int):raise AssertionError('Fixture creation failed')
        fixtures.append((path,value[1]['id'],value[1]['name']));return value[1]['id']
    def cleanup(path,object_id,name):
        path=path+str(object_id)+'/'
        value=invoke(baseline,path)
        if value[0]==404:return
        if value[0]!=200 or value[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe fixture cleanup refused')
        if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Fixture cleanup failed')
    def lifecycle(base):
        owned={};results={};uuids={}
        def add(name,response,status):results[name]=(response,status)
        def create(name,body,expected=201):
            name=prefix+'-'+name;body={'name':name,**body}
            value=invoke(base,root,body,'POST');add('create-'+name[len(prefix)+1:],value,expected)
            if value[0]==201:
                owned[value[1]['id']]=value[1]['name'];uuids[value[1]['id']]=value[1]['uuid'];return value[1]['id']
            if expected==400:
                found=invoke(baseline,root+'?'+urlencode({'name':name,'visibility_filter':'all'}))
                if found[0]!=200 or not isinstance(found[1],list):raise AssertionError('Creation side-effect state unavailable')
                exact=[item for item in found[1] if item['name']==name]
                if len(exact)==1:owned[exact[0]['id']]=name;return exact[0]['id']
            return None
        def memberships(object_id):
            found=[]
            for profile in profile_ids:
                value=invoke(baseline,profiles+str(profile)+'/')
                if value[0]!=200:raise AssertionError('Profile fixture disappeared')
                found.append(object_id in value[1]['channels'])
            return 200,{'memberships':found}
        try:
            if invoke(baseline,grouproot+str(group_id)+'/unclaim-custom/',{},'POST')[0]!=200:raise AssertionError('Manual group fixture reset failed')
            before_group=invoke(baseline,grouproot+str(group_id)+'/')
            if before_group[0]!=200 or before_group[1]['is_custom']:raise AssertionError('Manual group fixture not reset')
            primary=create('primary',{'channel_number':7421.5,'channel_group_id':group_id,'logo_id':logo_id,'tvg_id':'tvg-probe',
                    'tvc_guide_stationid':'station-probe','epg_data_id':None,'streams':stream_ids,
                    'user_level':2,'is_radio':True,'hidden_from_output':False,'channel_profile_ids':[]})
            if primary is None:raise AssertionError('Primary creation failed')
            path=root+str(primary)+'/'
            add('primary-empty-memberships',memberships(primary),200)
            group_state=invoke(baseline,grouproot+str(group_id)+'/')
            if group_state[0]!=200 or not group_state[1]['is_custom']:raise AssertionError('Manual channel did not claim its group')
            add('manual-channel-claims-group',(200,{'is_custom':group_state[1]['is_custom']}),200)
            add('primary-get',invoke(base,path),200)
            for role in ('standard','viewer'):
                add('denied-patch-'+role,invoke(base,path,{'name':prefix+'-unauthorized'},'PATCH',accounts[role]['access']),403)
                add('denied-delete-'+role,invoke(base,path,method='DELETE',token=accounts[role]['access']),403)
            add('put-name-required',invoke(base,path,{},'PUT'),400)
            add('bad-null-group',invoke(base,path,{'channel_group_id':None},'PATCH'),400)
            add('bad-empty-group',invoke(base,path,{'channel_group_id':''},'PATCH'),400)
            add('empty-nullable-relations',invoke(base,path,{'logo_id':'','epg_data_id':'','stream_profile_id':'','auto_created_by':''},'PATCH'),200)
            add('bad-zero-stream-profile',invoke(base,path,{'stream_profile_id':0},'PATCH'),400)
            add('bad-name-and-override',invoke(base,path,{'name':None,'override':{'channel_number':0}},'PATCH'),400)
            add('bad-boolean-user-level',invoke(base,path,{'user_level':True},'PATCH'),400)
            add('bad-stream-list',invoke(base,path,{'streams':'wrong'},'PATCH'),400)
            add('bad-stream-reference',invoke(base,path,{'streams':[2147483647]},'PATCH'),400)
            add('manual-override-rejected',invoke(base,path,{'override':{'name':'Cannot apply'}},'PATCH'),400)
            add('manual-clear-override-allowed',invoke(base,path,{'override':None},'PATCH'),200)
            add('unknown-fields-noop',invoke(base,path,{'unknown':True,'channel_profile_ids':[profile_ids[0]]},'PATCH'),200)
            readonly=invoke(base,path,{'id':2147483647,'uuid':'00000000-0000-0000-0000-000000000000'},'PATCH')
            if readonly[0]==200 and (readonly[1]['id']!=primary or readonly[1]['uuid']!=uuids[primary]):raise AssertionError('Readonly identity fields changed')
            add('readonly-id-uuid',readonly,200)
            add('reverse-stream-order',invoke(base,path,{'streams':list(reversed(stream_ids))},'PATCH'),200)
            add('reverse-stream-order-get',invoke(base,path+'?include_streams=true'),200)
            add('remove-catchup-stream',invoke(base,path,{'streams':[stream_ids[0]]},'PATCH'),200)
            add('remove-catchup-stream-get',invoke(base,path),200)
            add('clear-all-streams',invoke(base,path,{'streams':[]},'PATCH'),200)
            add('clear-all-streams-get',invoke(base,path),200)
            add('replace-scalars',invoke(base,path,{'name':prefix+'-renamed','channel_number':None,
                'user_level':'1.0','is_adult':1.0,'is_radio':False,'hidden_from_output':True,
                'logo_id':None,'tvg_id':None,'tvc_guide_stationid':None},'PUT'),200)
            owned[primary]=prefix+'-renamed'
            add('hidden-detail-readable',invoke(base,path),200)
            add('update-filter-before-rename',invoke(base,path+'?'+urlencode({'name':prefix+'-renamed'}),{'name':prefix+'-after-filter'},'PATCH'),200)
            owned[primary]=prefix+'-after-filter'
            auto=create('auto',{'channel_number':7422.5,'auto_created':True,'streams':stream_ids,
                    'override':{'name':prefix+'-effective','channel_number':2.0,'logo_id':logo_id,
                                'is_radio':True},'channel_profile_ids':[]})
            if auto is None:raise AssertionError('Auto channel creation failed')
            autopath=root+str(auto)+'/'
            add('override-get',invoke(base,autopath),200)
            effective_params=urlencode({'name':prefix+'-effective','visibility_filter':'all'})
            summary=invoke(base,root+'summary/?'+effective_params)
            if summary[0]!=200 or not isinstance(summary[1],list) or len(summary[1])!=1:
                raise AssertionError('Effective summary fixture missing')
            if summary[1][0]['id']!=auto or summary[1][0]['name']!=prefix+'-effective' or summary[1][0]['channel_number']!=2.0:
                raise AssertionError('Effective summary values differ')
            add('effective-summary',(200,summary[1][0]),200)
            reserved=create('override-reservation-default',{})
            if reserved is None:raise AssertionError('Reserved default-number fixture missing')
            add('override-number-reserved-get',invoke(base,root+str(reserved)+'/'),200)
            add('override-partial-clear',invoke(base,autopath,{'override':{'name':None}},'PATCH'),200)
            add('override-empty-noop',invoke(base,autopath,{'override':{}},'PATCH'),200)
            add('override-invalid-number',invoke(base,autopath,{'override':{'channel_number':0}},'PATCH'),400)
            add('override-invalid-reference',invoke(base,autopath,{'override':{'logo_id':2147483647}},'PATCH'),400)
            add('override-all-null-drops-row',invoke(base,autopath,{'override':{'channel_number':None,'logo_id':None,'is_radio':None}},'PATCH'),200)
            add('override-recreate',invoke(base,autopath,{'override':{'name':prefix+'-again'}},'PATCH'),200)
            add('override-explicit-null-delete',invoke(base,autopath,{'override':None},'PATCH'),200)
            add('override-clear-get',invoke(base,autopath),200)
            for name,body in [('default',{}),('explicit-empty',{'channel_profile_ids':[]}),
                             ('explicit-one',{'channel_profile_ids':[profile_ids[0]]}),
                             ('string-one',{'channel_profile_ids':[str(profile_ids[0])]}),
                             ('sentinel-zero',{'channel_profile_ids':0}),
                             ('false-sentinel',{'channel_profile_ids':[False]})]:
                object_id=create(name,body)
                if object_id is None:raise AssertionError('Profile-semantics creation failed')
                add('memberships-'+name,memberships(object_id),200)
                add('default-number-subsequent-get-'+name,invoke(base,root+str(object_id)+'/'),200)
            for name,ids in [('missing-profile',[2147483647]),('duplicate-profile',[profile_ids[0],profile_ids[0]]),
                             ('null-profile',[None]),('invalid-profile-type',['not-a-number'])]:
                object_id=create(name,{'channel_profile_ids':ids},400)
                add('profile-error-commits-channel-'+name,(200,{'created':object_id is not None}),200)
                if object_id is None:raise AssertionError('Source creation side effect missing')
                add('profile-error-no-memberships-'+name,memberships(object_id),200)
            add('delete-primary',invoke(base,path,method='DELETE'),204)
            add('deleted-primary-404',invoke(base,path),404)
            add('delete-missing-404',invoke(base,path,method='DELETE'),404)
            add('delete-auto',invoke(base,autopath,method='DELETE'),204)
            add('deleted-auto-404',invoke(base,autopath),404)
            for name,body in [('missing-name',{}),('manual-override',{'name':prefix+'-invalid-manual','override':{'name':'bad'}}),
                             ('name-too-long',{'name':'x'*513}),('multiple-validation-errors',{'name':None,'channel_number':'bad','streams':False,'override':[]})]:
                add('invalid-create-'+name,invoke(base,root,body,'POST'),400)
            return results
        finally:
            for object_id,name in owned.items():cleanup(root,object_id,name)
    try:
        group_id=fixture(grouproot,{'name':prefix+'-group'})
        for i in range(2):profile_ids.append(fixture(profiles,{'name':prefix+'-profile-'+str(i),'start_empty':True}))
        for i in range(2):stream_ids.append(fixture(streamroot,{'name':prefix+'-stream-'+str(i),
              'url':'http://example.invalid/'+prefix+'-'+str(i)+'.ts','is_catchup':i==1,'catchup_days':3 if i==1 else 0}))
        logo_id=fixture(logoroot,{'name':prefix+'-logo','url':'http://example.invalid/'+prefix+'.png'})
        a=capture('python-lifecycle',lambda:lifecycle(baseline));b=capture('rust-lifecycle',lambda:lifecycle(candidate))
        if a is None or b is None:return
        for name in sorted(set(a)|set(b)):
            if name not in a or name not in b:record('channel-crud:'+name,False,{'missing_result':True});continue
            old,wanted=a[name];new,expected=b[name];na,nb=normalized(old[1]),normalized(new[1])
            record('channel-crud:'+name,wanted==expected and old[0]==new[0]==wanted and digest(na)==digest(nb),
                   {'python_status':old[0],'rust_status':new[0],
                    'python_body_sha256':digest(na),'rust_body_sha256':digest(nb)})
    except Exception as error:record('channel-crud:fixture-lifecycle',False,{'error_type':type(error).__name__})
    finally:
        for path,object_id,name in reversed(fixtures):capture('fixture-cleanup',lambda p=path,i=object_id,n=name:cleanup(p,i,n))
