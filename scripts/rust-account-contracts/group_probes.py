"""Channel-group CRUD/claim/provider-demotion oracle in the disposable DB."""
import copy
import hashlib
import json
import uuid


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def normalized(value):
    value=copy.deepcopy(value)
    if isinstance(value,dict) and 'id' in value:value['id']='<generated-group-id>'
    return value


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/groups/';channels='/api/channels/channels/'
    prefix='rust-probe-group-'+uuid.uuid4().hex[:12]
    admin=accounts['admin']['access'];standard=accounts['standard']['access']
    fixtures=accounts['_fixtures'];provider=fixtures['provider_group'];claimed=fixtures['claimed_provider_group']
    for fixture in (provider,claimed):
        if not isinstance(fixture['id'],int) or not fixture['name'].startswith('rust-probe-'):raise ValueError('Unsafe fixture group')
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    def capture(name,callback):
        try:return callback()
        except Exception as error:record('channel-groups:'+name,False,{'error_type':type(error).__name__})
    def emit(name,a,b,wanted,normalize=lambda value:value):
        na,nb=normalize(a[1]),normalize(b[1])
        record('channel-groups:'+name,a[0]==b[0]==wanted and digest(na)==digest(nb),
               {'python_status':a[0],'rust_status':b[0],
                'python_body_sha256':digest(na),'rust_body_sha256':digest(nb)})
    for role,status in [('admin',200),('standard',200),('viewer',403),('inactive',401),(None,401)]:
        token=accounts[role]['access'] if role else None
        a=capture('list-python',lambda:invoke(baseline,root,token=token));b=capture('list-rust',lambda:invoke(candidate,root,token=token))
        if a is not None and b is not None:emit('list-'+str(role),a,b,status)
    for suffix,status in [(str(provider['id']),200),(str(claimed['id']),200),('2147483647',404),('not-a-number',404)]:
        a=capture('detail-python',lambda:invoke(baseline,root+suffix+'/'));b=capture('detail-rust',lambda:invoke(candidate,root+suffix+'/'))
        if a is not None and b is not None:emit('detail-'+suffix,a,b,status)
    def lifecycle(base):
        results={};owned={};children={}
        def add(name,response,status):results[name]=(response,status)
        def child(name,group,level=0,auto=False):
            name=prefix+'-'+name;value=invoke(baseline,channels,{'name':name,'channel_group_id':group,
                'user_level':level,'auto_created':auto,'channel_profile_ids':[]},'POST')
            if value[0]!=201:raise AssertionError('Channel fixture creation failed')
            children[value[1]['id']]=name;return value[1]['id']
        def cleanup_channel(object_id,name):
            path=channels+str(object_id)+'/';value=invoke(baseline,path)
            if value[0]==404:return
            if value[0]!=200 or value[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe channel cleanup')
            if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Channel fixture cleanup failed')
        def cleanup_group(object_id,name):
            path=root+str(object_id)+'/';value=invoke(baseline,path)
            if value[0]==404:return
            if value[0]!=200 or value[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe group cleanup')
            if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Group fixture cleanup failed')
        try:
            if invoke(baseline,root+str(provider['id'])+'/unclaim-custom/',{},'POST')[0]!=200:raise AssertionError('Provider fixture reset failed')
            if invoke(baseline,root+str(claimed['id'])+'/claim-custom/',{},'POST')[0]!=200:raise AssertionError('Claimed fixture reset failed')
            create=invoke(base,root,{'name':prefix},'POST');add('create',create,201)
            if create[0]!=201:raise AssertionError('Group creation failed')
            group=create[1]['id'];owned[group]=prefix;path=root+str(group)+'/'
            add('retrieve',invoke(base,path),200)
            add('duplicate-name',invoke(base,root,{'name':prefix},'POST'),400)
            add('missing-name',invoke(base,root,{},'POST'),400)
            add('blank-name',invoke(base,root,{'name':'   '},'POST'),400)
            add('null-name',invoke(base,path,{'name':None},'PATCH'),400)
            add('put-name-required',invoke(base,path,{},'PUT'),400)
            add('readonly-fields',invoke(base,path,{'is_custom':False,'channel_count':999},'PATCH'),200)
            for role in ('standard','viewer'):
                add('denied-update-'+role,invoke(base,path,{'name':prefix+'-bad'},'PATCH',accounts[role]['access']),403)
                add('denied-claim-'+role,invoke(base,path+'claim-custom/',{},'POST',accounts[role]['access']),403)
            add('unclaim-empty',invoke(base,path+'unclaim-custom/',{},'POST'),200)
            add('claim-empty',invoke(base,path+'claim-custom/',{},'POST'),200)
            visible=child('visible',group,1);hidden=child('higher-level',group,8)
            add('delete-associated-denied',invoke(base,path,method='DELETE'),400)
            add('unclaim-associated-denied',invoke(base,path+'unclaim-custom/',{},'POST'),400)
            add('standard-retrieve-includes-all-channel-count',invoke(base,path,token=standard),200)
            listed=invoke(base,root,token=standard)
            if listed[0]!=200:raise AssertionError('Standard group listing failed')
            current=[item for item in listed[1] if item['id']==group]
            if len(current)!=1:raise AssertionError('Visible group missing')
            add('standard-list-visible-channel-count',(200,current[0]),200)
            add('rename',invoke(base,path,{'name':prefix+'-renamed'},'PATCH'),200);owned[group]=prefix+'-renamed'
            cleanup_channel(visible,children[visible]);cleanup_channel(hidden,children[hidden])
            add('delete',invoke(base,path,method='DELETE'),204)
            add('read-deleted',invoke(base,path),404)
            add('delete-missing',invoke(base,path,method='DELETE'),404)
            ppath=root+str(provider['id'])+'/'
            add('provider-only-update-denied',invoke(base,ppath,{'name':prefix+'-blocked'},'PATCH'),400)
            add('provider-only-validation-denied-first',invoke(base,ppath,{'name':None},'PUT'),400)
            add('provider-only-delete-denied',invoke(base,ppath,method='DELETE'),400)
            add('claim-provider',invoke(base,ppath+'claim-custom/',{},'POST'),200)
            add('unclaim-provider',invoke(base,ppath+'unclaim-custom/',{},'POST'),200)
            cpath=root+str(claimed['id'])+'/'
            manual=child('provider-manual',claimed['id']);auto=child('provider-auto',claimed['id'],auto=True)
            rename=invoke(base,cpath,{'name':prefix+'-claimed-renamed'},'PATCH');add('claimed-provider-rename-demotes',rename,200)
            if rename[0]!=200 or 'demoted_group' not in rename[1]:raise AssertionError('Provider demotion failed')
            new=rename[1]['id'];owned[new]=prefix+'-claimed-renamed'
            for name,channel,expected_group in [('manual-moved',manual,new),('auto-preserved',auto,claimed['id'])]:
                value=invoke(baseline,channels+str(channel)+'/')
                if value[0]!=200:raise AssertionError('Provider rename channel state unavailable')
                add(name,(200,{'expected_group':value[1]['channel_group_id']==expected_group}),200)
                if value[1]['channel_group_id']!=expected_group:raise AssertionError('Provider identity changed incorrectly')
            add('demoted-provider-retrieve',invoke(base,cpath),200)
            add('renamed-manual-retrieve',invoke(base,root+str(new)+'/'),200)
            cleanup_channel(manual,children[manual]);cleanup_channel(auto,children[auto])
            add('delete-renamed-manual',invoke(base,root+str(new)+'/',method='DELETE'),204)
            add('claimed-old-now-blocked',invoke(base,cpath,{'name':prefix+'-blocked-again'},'PATCH'),400)
            return results
        finally:
            for object_id,name in children.items():cleanup_channel(object_id,name)
            for object_id,name in owned.items():cleanup_group(object_id,name)
            if invoke(baseline,root+str(provider['id'])+'/unclaim-custom/',{},'POST')[0]!=200:raise AssertionError('Provider fixture restore failed')
            if invoke(baseline,root+str(claimed['id'])+'/claim-custom/',{},'POST')[0]!=200:raise AssertionError('Claimed fixture restore failed')
    a=capture('python-lifecycle',lambda:lifecycle(baseline));b=capture('rust-lifecycle',lambda:lifecycle(candidate))
    if a is None or b is None:return
    # Fixed shared seed fixtures retain IDs; only newly created group roots vary.
    fixed_steps={'provider-only-update-denied','provider-only-validation-denied-first','provider-only-delete-denied',
                 'claim-provider','unclaim-provider','demoted-provider-retrieve','claimed-old-now-blocked'}
    for name in sorted(set(a)|set(b)):
        if name not in a or name not in b:record('channel-groups:'+name,False,{'missing_result':True});continue
        old,status=a[name];new,expected=b[name]
        if expected!=status:record('channel-groups:'+name,False,{'expected_status_mismatch':True});continue
        emit(name,old,new,status,(lambda value:value) if name in fixed_steps else normalized)
