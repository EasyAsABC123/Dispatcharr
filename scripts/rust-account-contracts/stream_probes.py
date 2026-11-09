"""Real stream management read/CRUD/relations contracts from original Dispatcharr."""
import copy
from datetime import datetime,timezone
import hashlib
import json
import uuid
from urllib.parse import urlencode

FIXED='2020-01-02T03:04:05Z'


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def normalize_links(value,base):
    value=copy.deepcopy(value)
    if isinstance(value,dict):
        for key in ('next','previous'):
            if isinstance(value.get(key),str) and value[key].startswith(base+'/'):
                value[key]='<fixture-origin>'+value[key][len(base):]
    return value


def normalize_created(value):
    value=copy.deepcopy(value)
    if isinstance(value,list):return [normalize_created(item) for item in value]
    if isinstance(value,dict) and 'id' in value and 'stream_hash' in value:
        stamp=datetime.fromisoformat(value['updated_at'].replace('Z','+00:00'))
        if stamp.utcoffset().total_seconds()!=0 or abs((datetime.now(timezone.utc)-stamp).total_seconds())>120:
            raise AssertionError('Automatic timestamp is stale or invalid')
        value['updated_at']='<automatic-updated-at>'
        if value['stream_hash']==hashlib.sha256(('custom_stream_'+str(value['id'])).encode()).hexdigest():
            value['stream_hash']='<generated-custom-stream-hash>'
        value['id']='<generated-stream-id>'
    return value


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/streams/';channels='/api/channels/channels/';groups='/api/channels/groups/'
    prefix='rust-probe-stream-'+uuid.uuid4().hex[:12];admin=accounts['admin']['access'];standard=accounts['standard']['access']
    fixtures=accounts['_fixtures'];provider=fixtures['provider_id'];inactive=fixtures['inactive_provider_id']
    owned_shared={};group=None;shared_channel=None
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    safe_reasons={'Source creation signal response differs':'create_signal_response','Generated hash absent from DB':'generated_hash_missing',
        'Source stream save unexpectedly rolled up channel':'stream_save_rollup','Provider readonly fields changed':'provider_fields_changed',
        'Stream deletion did not cascade and roll up':'delete_rollup','Stream creation failed':'stream_create',
        'Rollup fixture creation failed':'rollup_fixture_create','Readonly identity changed':'readonly_identity',
        'Update regenerated cleared hash':'hash_regenerated','Unsafe stream cleanup':'unsafe_stream_cleanup',
        'Stream fixture cleanup failed':'stream_cleanup','Unsafe channel cleanup':'unsafe_channel_cleanup',
        'Channel fixture cleanup failed':'channel_cleanup','Assigned fixture creation failed':'assigned_fixture_create'}
    def capture(name,callback):
        try:return callback()
        except Exception as error:
            details={'error_type':type(error).__name__}
            if type(error) is AssertionError and str(error) in safe_reasons:details['reason']=safe_reasons[str(error)]
            record('streams:'+name,False,details)
    def emit(name,old,new,expected,normalize=lambda value:value):
        try:
            a,b=normalize(old[1]),normalize(new[1])
            record('streams:'+name,old[0]==new[0]==expected and digest(a)==digest(b),
                   {'python_status':old[0],'rust_status':new[0],
                    'python_body_sha256':digest(a),'rust_body_sha256':digest(b)})
        except Exception as error:record('streams:'+name,False,{'error_type':type(error).__name__})
    def compare(name,path,expected=200,token=admin):
        try:
            old=invoke(baseline,path,token=token);new=invoke(candidate,path,token=token)
            a,b=normalize_links(old[1],baseline),normalize_links(new[1],candidate)
            emit(name,(old[0],a),(new[0],b),expected)
        except Exception as error:record('streams:'+name,False,{'error_type':type(error).__name__})
    def create_reference(suffix,body):
        name=prefix+'-'+suffix;response=invoke(baseline,root,{'name':name,'last_seen':FIXED,**body},'POST')
        if response[0]!=201:raise AssertionError('Stream fixture creation failed')
        owned_shared[response[1]['id']]=name;return response[1]['id']
    def clean_stream(object_id,name):
        result=invoke(baseline,root+'?ids='+str(object_id))
        if result[0]==404:return
        if result[0]!=200 or len(result[1])!=1 or result[1][0].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe stream cleanup')
        if invoke(baseline,root+'bulk-delete/',{'stream_ids':[object_id]},'DELETE')[0]!=204:raise AssertionError('Stream fixture cleanup failed')
    def clean_channel(object_id,name):
        path=channels+str(object_id)+'/';result=invoke(baseline,path)
        if result[0]==404:return
        if result[0]!=200 or result[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe channel cleanup')
        if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Channel fixture cleanup failed')
    def lifecycle(base):
        owned={};children={};results={}
        def add(name,response,status):results[name]=(response,status)
        def create(suffix,body):
            name=prefix+'-'+suffix;result=invoke(base,root,{'name':name,'last_seen':FIXED,**body},'POST');add('create-'+suffix,result,201)
            if result[0]!=201:raise AssertionError('Stream creation failed')
            owned[result[1]['id']]=name;return result[1]['id'],result[1]
        try:
            manual,created=create('manual',{'url':'http://host/path','channel_group':group,'is_catchup':True,'catchup_days':3,
                'stream_stats':{'codec':'probe','large':1e23,'fraction':1.5},'stream_stats_updated_at':FIXED})
            if created['stream_hash']!=hashlib.sha256(('custom_stream_'+str(manual)).encode()).hexdigest() or not created['is_custom']:raise AssertionError('Source creation signal response differs')
            path=root+str(manual)+'/'
            loaded=invoke(base,path)
            if loaded[0]!=200 or loaded[1]['stream_hash']!=hashlib.sha256(('custom_stream_'+str(manual)).encode()).hexdigest():raise AssertionError('Generated hash absent from DB')
            add('generated-hash-following-get',loaded,200)
            add('put-empty-preserves',invoke(base,path,{},'PUT'),200)
            readonly=invoke(base,path,{'id':2147483647,'updated_at':FIXED},'PATCH')
            if readonly[0]==200 and readonly[1]['id']!=manual:raise AssertionError('Readonly identity changed')
            add('readonly-id-timestamp',readonly,200)
            for name,body in [('blank-name',{'name':''}),('null-name',{'name':None}),('bad-url',{'url':'not a URL'}),
                              ('bad-current-viewers',{'current_viewers':-1}),('bad-boolean-integer',{'stream_id':True}),
                              ('bad-null-last-seen',{'last_seen':None}),('bad-timestamp',{'stream_stats_updated_at':'bad'}),
                              ('json-file-path',{'local_file':'/data/should-not-be-readable.ts'}),
                              ('bad-profile',{'stream_profile_id':2147483647}),('duplicate-hash',{'stream_hash':prefix+'-provided-hash'})]:
                add('validation-'+name,invoke(base,path,body,'PATCH'),400)
            add('long-logo-text',invoke(base,path,{'logo_url':'x'*3000},'PATCH'),200)
            add('nullable-relations-empty',invoke(base,path,{'channel_group':'','stream_profile_id':'','local_file':None},'PATCH'),200)
            for index,url in enumerate(['http://host/','HTTP://valid.example/path','http://','http:///path','http://.','udp://:1234','http://host:999999/path','rtsp://u:p@host/path','rtp://192.168.1.1','ftp://valid.example/path','ftp://u:@valid.example/','http://[::1]/','http://localhost?x']):
                add('accepted-url-'+str(index),invoke(base,path,{'url':url},'PATCH'),200)
            for index,url in enumerate(['http://host?x','HTTP://host/','rtmp://host/path','ftp://host/path','rtsp://[::1]/','https://user@local/path','http://-bad/']):
                add('rejected-url-'+str(index),invoke(base,path,{'url':url},'PATCH'),400)
            add('restore-valid-url',invoke(base,path,{'url':'http://example.invalid/'+prefix+'.ts'},'PATCH'),200)
            add('clear-hash-no-update-regeneration',invoke(base,path,{'stream_hash':None},'PATCH'),200)
            after=invoke(base,path);add('cleared-hash-get',after,200)
            if after[0]!=200 or after[1]['stream_hash'] is not None:raise AssertionError('Update regenerated cleared hash')
            for role in ('standard','viewer'):
                add('denied-update-'+role,invoke(base,path,{'name':prefix+'-bad'},'PATCH',accounts[role]['access']),403)
                add('denied-delete-'+role,invoke(base,path,method='DELETE',token=accounts[role]['access']),403)
                add('denied-bulk-'+role,invoke(base,root+'bulk-delete/',{'stream_ids':[manual]},'DELETE',accounts[role]['access']),403)
            response=invoke(baseline,channels,{'name':prefix+'-rollup-channel','streams':[manual],'channel_profile_ids':[]},'POST')
            if response[0]!=201:raise AssertionError('Rollup fixture creation failed')
            channel=response[1]['id'];children[channel]=response[1]['name']
            add('toggle-catchup-field',invoke(base,path,{'is_catchup':False},'PATCH'),200)
            current=invoke(baseline,channels+str(channel)+'/')
            if current[0]!=200 or not current[1]['is_catchup']:raise AssertionError('Source stream save unexpectedly rolled up channel')
            add('stream-save-preserves-old-channel-rollup',(200,{'is_catchup':True}),200)
            protected,_=create('protected',{'url':'http://example.invalid/provider.ts','m3u_account':provider,
                'channel_group':group,'tvg_id':'provider-tvg','is_custom':False})
            ppath=root+str(protected)+'/'
            add('provider-readonly-fields',invoke(base,ppath,{'name':prefix+'-ignored','url':'bad url',
                'm3u_account':inactive,'tvg_id':'ignored','channel_group':None},'PATCH'),200)
            unchanged=invoke(baseline,root+'?ids='+str(protected))
            if unchanged[0]!=200 or unchanged[1][0]['name']!=owned[protected] or unchanged[1][0]['m3u_account']!=provider:raise AssertionError('Provider readonly fields changed')
            add('provider-other-fields-mutable',invoke(base,ppath,{'is_radio':True,'stream_id':'42.0','stream_chno':2.5},'PATCH'),200)
            add('provider-switch-custom-flag',invoke(base,ppath,{'is_custom':True},'PATCH'),200)
            renamed=prefix+'-protected-now-custom'
            add('custom-after-flag-can-rename',invoke(base,ppath,{'name':renamed},'PATCH'),200);owned[protected]=renamed
            add('custom-move-to-inactive-account',invoke(base,ppath,{'m3u_account':inactive},'PATCH'),200)
            add('inactive-normal-get-hidden',invoke(base,ppath),404)
            add('inactive-ids-get-visible',invoke(base,root+'?ids='+str(protected)),200)
            extra,_=create('bulk-extra',{'url':None})
            add('bulk-empty',invoke(base,root+'bulk-delete/',{'stream_ids':[]},'DELETE'),204)
            add('bulk-includes-inactive',invoke(base,root+'bulk-delete/',{'stream_ids':[protected,extra]},'DELETE'),204)
            add('bulk-rows-deleted',invoke(baseline,root+'?ids='+str(protected)+','+str(extra)),404)
            add('delete-manual',invoke(base,path,method='DELETE'),204)
            add('read-deleted',invoke(base,path),404)
            add('delete-missing',invoke(base,path,method='DELETE'),404)
            current=invoke(baseline,channels+str(channel)+'/')
            if current[0]!=200 or current[1]['streams'] or current[1]['is_catchup'] or current[1]['catchup_days']!=0:raise AssertionError('Stream deletion did not cascade and roll up')
            add('delete-rollup-and-links',(200,{'streams_empty':True,'is_catchup':False,'catchup_days':0}),200)
            return results
        finally:
            for object_id,name in children.items():clean_channel(object_id,name)
            for object_id,name in owned.items():clean_stream(object_id,name)
    try:
        response=invoke(baseline,groups,{'name':prefix+'-group'},'POST')
        if response[0]!=201:raise AssertionError('Group fixture creation failed')
        group=response[1]['id']
        alpha=create_reference('alpha',{'url':'http://example.invalid/alpha.ts','channel_group':group,'is_catchup':True,'catchup_days':3,
                                      'stream_stats':{'codec':'probe-alpha'},'stream_stats_updated_at':FIXED})
        beta=create_reference('beta',{'url':'http://example.invalid/beta.ts','is_stale':True,'is_radio':True,'tvg_id':'probe-beta'})
        provided=create_reference('provider',{'url':'http://example.invalid/provider.ts','m3u_account':provider,
                    'is_custom':False,'channel_group':group,'stream_hash':prefix+'-provided-hash'})
        hidden=create_reference('inactive',{'url':'http://example.invalid/inactive.ts','m3u_account':inactive,'is_custom':False,'channel_group':group})
        response=invoke(baseline,channels,{'name':prefix+'-shared-channel','streams':[alpha,provided,hidden],
                                          'hidden_from_output':True,'channel_profile_ids':[]},'POST')
        if response[0]!=201:raise AssertionError('Assigned fixture creation failed')
        shared_channel=(response[1]['id'],response[1]['name'])
        relation=channels+str(shared_channel[0])+'/streams/'
        for role,status in [('admin',200),('standard',200),('viewer',403),('inactive',401),(None,401)]:
            token=accounts[role]['access'] if role else None
            compare('channel-streams-'+str(role),relation,status,token)
            compare('channel-stats-'+str(role),relation+'stats/',status,token)
        for name,params,status in [
            ('since-before',{'since':'2020-01-02T03:04:04Z'},200),
            ('since-exact-exclusive',{'since':FIXED},200),
            ('since-naive',{'since':'2020-01-02T03:04:04'},200),
            ('since-date',{'since':'2020-01-02'},200),
            ('invalid-since',{'since':'invalid'},400),
            ('ids-selected',{'ids':str(alpha)+','+str(hidden)},200),
            ('ids-large',{'ids':'99999999999999999999999999999999999999999'},200),
            ('ids-empty',{'ids':' , , '},200),
            ('ids-numeric-format',{'ids':' +'+str(alpha)+',1_000'},200),
            ('ids-invalid',{'ids':'1.0'},400),
            ('ids-and-since',{'ids':str(hidden),'since':'2020-01-01'},200),
            ('empty-query-ignored',{'ids':'','since':''},200)]:
            compare('channel-stats-'+name,relation+'stats/?'+urlencode(params),status)
        compare('missing-channel-before-invalid-stats',channels+'2147483647/streams/stats/?since=invalid&ids=invalid',404)
        compare('missing-channel-streams',channels+'2147483647/streams/',404)
        # Besides body equality, assert the source ordering and null-delta
        # semantics so a mutual empty result cannot count as successful proof.
        def relation_reference():
            ordered=invoke(baseline,relation)
            stats=invoke(baseline,relation+'stats/')
            delta=invoke(baseline,relation+'stats/?since=2020-01-02T03:04:04Z')
            passed=(ordered[0]==stats[0]==delta[0]==200
                    and [row['id'] for row in ordered[1]]==[alpha,provided,hidden]
                    and [row['id'] for row in stats[1]]==[hidden,provided,alpha]
                    and [row['id'] for row in delta[1]]==[alpha]
                    and delta[1][0]['stream_stats']=={'codec':'probe-alpha'})
            record('streams:channel-relations-reference-semantics',passed,{'ordered_stream_count':len(ordered[1]) if ordered[0]==200 else None})
        capture('channel-relations-reference-semantics',relation_reference)
        for role,status in [('admin',200),('standard',200),('viewer',403),('inactive',401),(None,401)]:compare('list-'+str(role),root,status,accounts[role]['access'] if role else None)
        for name,params in [('name',{'name':prefix}),('search',{'search':'"'+prefix+'" "alpha"'}),
             ('group',{'channel_group':prefix+'-group'}),('group-name-exact',{'channel_group_name':prefix+'-group'}),
             ('group-name-not-substring',{'channel_group_name':prefix}),('account-id',{'m3u_account':str(provider)}),
             ('account-name',{'m3u_account_name':'rust-probe-group-provider'}),('active-false-empty',{'m3u_account_is_active':'false'}),
             ('tvg',{'tvg_id':'probe'}),('assigned',{'assigned':str(shared_channel[0])}),('unassigned',{'unassigned':'true'}),
             ('hide-stale',{'hide_stale':'true'}),('catchup',{'is_catchup':'yes'}),('radio',{'is_radio':'on'}),
             ('page-first',{'page_size':'1'}),('page-second',{'page':'2','page_size':'1'}),('page-last',{'page':'last','page_size':'1'}),
             ('order-name',{'ordering':'name'}),('order-account',{'ordering':'m3u_account__name,name'}),
             ('order-group',{'ordering':'channel_group__name,name'}),('order-tvg',{'ordering':'tvg_id,name'}),
             ('order-invalid-fallback',{'ordering':'id'}),('ids-bypass',{'ids':str(hidden)+','+str(provided),'name':'no-match','page':'999','hide_stale':'true'})]:compare('read-'+name,root+'?'+urlencode(params))
        compare('standard-ids-bypass-url-redacted',root+'?ids='+str(hidden)+','+str(provided),token=standard)
        compare('ids-action',root+'ids/')
        compare('ids-action-ignores-list-ids-param',root+'ids/?ids='+str(hidden))
        compare('ids-action-filtered',root+'ids/?'+urlencode({'name':prefix,'ordering':'name'}))
        for suffix,status in [(str(alpha),200),(str(hidden),404),('2147483647',404),('not-a-number',404)]:compare('detail-'+suffix,root+suffix+'/',status)
        compare('unknown-ids-404',root+'?ids=2147483647',404)
        compare('invalid-page',root+'?page=bad',404)
        compare('nul-search',root+'?'+urlencode({'search':'\0'}),400)
        a=capture('python-crud-lifecycle',lambda:lifecycle(baseline));b=capture('rust-crud-lifecycle',lambda:lifecycle(candidate))
        if a is not None and b is not None:
            for name in sorted(set(a)|set(b)):
                if name not in a or name not in b:record('streams:'+name,False,{'missing_result':True});continue
                old,status=a[name];new,expected=b[name]
                if status!=expected:record('streams:'+name,False,{'expected_status_mismatch':True});continue
                emit(name,old,new,status,normalize_created)
    except Exception as error:record('streams:fixture-lifecycle',False,{'error_type':type(error).__name__})
    finally:
        if shared_channel is not None:capture('fixture-channel-cleanup',lambda:clean_channel(*shared_channel))
        for object_id,name in owned_shared.items():capture('fixture-stream-cleanup',lambda i=object_id,n=name:clean_stream(i,n))
        if group is not None:
            def cleanup_group():
                response=invoke(baseline,groups+str(group)+'/')
                if response[0]!=200 or response[1].get('name')!=prefix+'-group':raise AssertionError('Unsafe group cleanup')
                if invoke(baseline,groups+str(group)+'/',method='DELETE')[0]!=204:raise AssertionError('Group cleanup failed')
            capture('fixture-group-cleanup',cleanup_group)
