"""Isolated global cleanup and SET_NULL edge contracts without changing real rows.

Before invoking global cleanup, protect every unrelated eligible fixture group
using an auto-created rust-probe channel. This leaves its custom/provider flags
unchanged and is removed afterward. Only rust-probe groups can be deleted.
"""
import copy
import hashlib
import json
import uuid


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def normalized(value):
    value=copy.deepcopy(value)
    if isinstance(value,dict) and isinstance(value.get('deleted_groups'),list):
        # The original .values_list('name') has no ordering. Fresh generated
        # group IDs/query plans may reorder these names; retain exact names,
        # multiplicity, count, message and all other JSON representations.
        value['deleted_groups']=sorted(value['deleted_groups'])
    return value


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/groups/';channels='/api/channels/channels/';streams='/api/channels/streams/'
    prefix='rust-probe-cleanup-'+uuid.uuid4().hex[:12];admin=accounts['admin']['access']
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    def capture(name,callback):
        try:return callback()
        except Exception as error:record('group-cleanup:'+name,False,{'error_type':type(error).__name__})
    def lifecycle(base):
        groups={};children={};stream=None;results={}
        def create_group(suffix):
            name=prefix+'-'+suffix;response=invoke(baseline,root,{'name':name},'POST')
            if response[0]!=201:raise AssertionError('Group fixture creation failed')
            groups[response[1]['id']]=name;return response[1]['id']
        def child(suffix,group,override=None):
            name=prefix+'-'+suffix;body={'name':name,'channel_group_id':group,
                'auto_created':True,'channel_profile_ids':[]}
            if override is not None:body['override']={'channel_group_id':override}
            response=invoke(baseline,channels,body,'POST')
            if response[0]!=201:raise AssertionError('Protected channel fixture creation failed')
            children[response[1]['id']]=name;return response[1]['id']
        def clean(path,object_id,name):
            response=invoke(baseline,path+str(object_id)+'/')
            if response[0]==404:return
            if response[0]!=200 or response[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe fixture cleanup refused')
            if invoke(baseline,path+str(object_id)+'/',method='DELETE')[0]!=204:raise AssertionError('Fixture cleanup failed')
        def add(name,response,status):results[name]=(response,status)
        try:
            initial=invoke(baseline,root)
            if initial[0]!=200 or not isinstance(initial[1],list):raise AssertionError('Group inventory unavailable')
            protect=[value for value in initial[1] if value['channel_count']==0 and value['m3u_account_count']==0]
            if len(protect)>64:raise AssertionError('Unexpected non-fixture group inventory')
            for index,value in enumerate(protect):child('protect-'+str(index),value['id'])
            loose=create_group('loose');custom=create_group('custom')
            if invoke(baseline,root+str(loose)+'/unclaim-custom/',{},'POST')[0]!=200:raise AssertionError('Loose fixture group not unclaimed')
            for role in ('standard','viewer'):
                add('denied-'+role,invoke(base,root+'cleanup/',{'include_custom':True},'POST',accounts[role]['access']),403)
            add('anonymous-denied',invoke(base,root+'cleanup/',{},'POST',None),401)
            response=invoke(base,root+'cleanup/',{},'POST');add('default-removes-only-loose',response,200)
            if response[0]!=200 or response[1].get('deleted_groups')!=[groups[loose]]:raise AssertionError('Cleanup touched unexpected groups')
            extra=create_group('extra');foreign=create_group('foreign');effective=create_group('effective')
            masked=child('masked-raw-group',foreign,effective)
            response=invoke(baseline,streams,{'name':prefix+'-stream','url':'http://example.invalid/'+prefix+'.ts','channel_group':foreign},'POST')
            if response[0]!=201:raise AssertionError('Stream fixture creation failed')
            stream=(response[1]['id'],response[1]['name'])
            add('spaced-true-is-false',invoke(base,root+'cleanup/',{'include_custom':' true '},'POST'),200)
            response=invoke(base,root+'cleanup/',{'include_custom':True},'POST');add('include-custom',response,200)
            if response[0]!=200 or set(response[1].get('deleted_groups',[]))!={groups[custom],groups[extra],groups[foreign]}:raise AssertionError('Custom cleanup touched unexpected groups')
            state=invoke(baseline,channels+str(masked)+'/')
            if state[0]!=200:raise AssertionError('Masked channel disappeared')
            add('raw-channel-set-null',(200,{'raw_is_null':state[1]['channel_group_id'] is None,
                'effective_is_preserved':state[1]['effective_channel_group_id']==effective}),200)
            if state[1]['channel_group_id'] is not None or state[1]['effective_channel_group_id']!=effective:raise AssertionError('Channel FK cleanup failed')
            state=invoke(baseline,streams+str(stream[0])+'/')
            if state[0]!=200 or state[1]['channel_group'] is not None:raise AssertionError('Stream FK cleanup failed')
            add('stream-set-null',(200,{'group_is_null':True}),200)
            add('already-clean-empty',invoke(base,root+'cleanup/',{},'POST'),200)
            remaining=invoke(baseline,root)
            if remaining[0]!=200:raise AssertionError('Restored inventory unavailable')
            byid={value['id']:value for value in remaining[1]}
            # Original groups remain present and keep custom/provider identity.
            same=all(value['id'] in byid and byid[value['id']]['name']==value['name']
                     and byid[value['id']]['is_custom']==value['is_custom']
                     and byid[value['id']]['m3u_account_count']==value['m3u_account_count'] for value in initial[1])
            if not same:raise AssertionError('Cleanup changed original group identity')
            add('unrelated-group-identities-preserved',(200,{'preserved':True}),200)
            return results
        finally:
            for object_id,name in children.items():clean(channels,object_id,name)
            if stream is not None:clean(streams,stream[0],stream[1])
            for object_id,name in groups.items():clean(root,object_id,name)
    a=capture('python-lifecycle',lambda:lifecycle(baseline))
    repeat=capture('python-repeat-lifecycle',lambda:lifecycle(baseline))
    b=capture('rust-lifecycle',lambda:lifecycle(candidate))
    if a is None or b is None or repeat is None:return
    old=a['include-custom'][0];again=repeat['include-custom'][0]
    record('group-cleanup:reference-repeat-unordered-names',old[0]==again[0]==200 and digest(normalized(old[1]))==digest(normalized(again[1])),
           {'raw_order_stable':digest(old[1])==digest(again[1]),
            'first_raw_sha256':digest(old[1]),'repeat_raw_sha256':digest(again[1]),
            'normalization':'deleted_groups only sorted because original queryset has no ordering; fresh generated IDs differ'})
    for name in sorted(set(a)|set(b)):
        if name not in a or name not in b:record('group-cleanup:'+name,False,{'missing_result':True});continue
        old,status=a[name];new,expected=b[name]
        record('group-cleanup:'+name,status==expected and old[0]==new[0]==status and digest(normalized(old[1]))==digest(normalized(new[1])),
               {'python_status':old[0],'rust_status':new[0],
                'python_body_sha256':digest(normalized(old[1])),'rust_body_sha256':digest(normalized(new[1])),
                'python_raw_body_sha256':digest(old[1]),'rust_raw_body_sha256':digest(new[1])})
