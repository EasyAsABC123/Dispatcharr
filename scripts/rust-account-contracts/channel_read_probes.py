"""Channel serializer/filter/access/page oracles using disposable Django fixtures.

Only next/previous URLs have their distinct test-server origin normalized. All
other response fields and numeric representations compare exactly. No raw
bodies, provider URLs, credentials, or exception payloads enter evidence.
"""
import copy
import hashlib
import json
import uuid
from urllib.parse import urlencode


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,
                                    separators=(',',':')).encode()).hexdigest()


def normalized(value,origin):
    value=copy.deepcopy(value)
    if isinstance(value,dict):
        for key in ('next','previous'):
            if isinstance(value.get(key),str) and value[key].startswith(origin+'/'):
                value[key]='<fixture-origin>'+value[key][len(origin):]
    return value


def run(http,baseline,candidate,accounts,record):
    root='/api/channels/channels/';profiles='/api/channels/profiles/'
    prefix='rust-probe-channel-read-'+uuid.uuid4().hex[:12]
    admin=accounts['admin']['access'];standard=accounts['standard']['access']
    created=[];profile=None;original_user=None
    def invoke(base,path,body=None,method=None,token=admin):return http(base+path,body=body,method=method,token=token)
    def compare(name,path,expected=200,token=admin):
        try:
            a=invoke(baseline,path,token=token);b=invoke(candidate,path,token=token)
            na,nb=normalized(a[1],baseline),normalized(b[1],candidate)
            record('channel-read:'+name,a[0]==b[0]==expected and digest(na)==digest(nb),
                   {'python_status':a[0],'rust_status':b[0],
                    'python_body_sha256':digest(na),'rust_body_sha256':digest(nb)})
        except Exception as error:
            record('channel-read:'+name,False,{'error_type':type(error).__name__})
    def restore_user():
        if original_user is not None:
            userpath='/api/accounts/users/'+str(accounts['standard']['id'])+'/'
            props=original_user['custom_properties'] or {}
            body={'channel_profiles':original_user['channel_profiles'],
                  'custom_properties':{**{'hide_adult_content':None},**props}}
            if invoke(baseline,userpath,body,'PATCH')[0]!=200:raise AssertionError('User restore failed')
    try:
        for suffix,number,level,adult,hidden in [('adult',9998.25,1,True,False),
                                               ('high',9999.25,8,False,True)]:
            value=invoke(baseline,root,{'name':prefix+'-'+suffix,'channel_number':number,
                                      'user_level':level,'is_adult':adult,
                                      'hidden_from_output':hidden,'channel_profile_ids':[]},'POST')
            if value[0]!=201:raise AssertionError('Channel fixture creation failed')
            created.append((value[1]['id'],value[1]['name']))
        for role,expected in [('admin',200),('standard',200),('viewer',403),('inactive',401),(None,401)]:
            compare('list-'+str(role),root,expected,accounts[role]['access'] if role else None)
            for action in ['ids','summary']:
                compare(action+'-'+str(role),root+action+'/',expected,accounts[role]['access'] if role else None)
        cases=[('all',{'visibility_filter':'all'}),('hidden',{'visibility_filter':'hidden'}),
               ('nested-streams',{'include_streams':'true','visibility_filter':'all'}),
               ('name-filter',{'name':prefix,'visibility_filter':'all'}),
               ('search',{'search':prefix,'visibility_filter':'all'}),
               ('quoted-search',{'search':'\"'+prefix+'\" \"adult\"','visibility_filter':'all'}),
               ('only-streamless',{'only_streamless':'true','visibility_filter':'all'}),
               ('truthy-false-streamless',{'only_streamless':'false','visibility_filter':'all'}),
               ('only-stale',{'only_stale':'1','visibility_filter':'all'}),
               ('only-overrides',{'only_has_overrides':'1','visibility_filter':'all'}),
               ('only-catchup',{'only_catchup':'1','visibility_filter':'all'}),
               ('only-radio',{'only_radio':'1','visibility_filter':'all'}),
               ('no-epg',{'epg':'null','visibility_filter':'all'}),
               ('epg-name',{'epg':'no-such-epg','visibility_filter':'all'}),
               ('group-name',{'channel_group':'Default Group','visibility_filter':'all'}),
               ('order-number',{'ordering':'channel_number','visibility_filter':'all'}),
               ('order-name',{'ordering':'-name','visibility_filter':'all'}),
               ('order-group',{'ordering':'channel_group__name,name','visibility_filter':'all'}),
               ('order-epg',{'ordering':'epg_data__name,-name','visibility_filter':'all'}),
               ('order-invalid-fallback',{'ordering':'id','visibility_filter':'all'}),
               ('page-first',{'page':'1','page_size':'1','visibility_filter':'all'}),
               ('page-second',{'page':'2','page_size':'1','visibility_filter':'all'}),
               ('page-last',{'page':'last','page_size':'1','visibility_filter':'all'}),
               ('page-size-bad-fallback',{'page_size':'bad','visibility_filter':'all'}),
               ('page-size-max',{'page_size':'10001','visibility_filter':'all'})]
        for name,params in cases:
            compare(name,root+'?'+urlencode(params))
            for action in ['ids','summary']:
                compare(action+'-'+name,root+action+'/?'+urlencode(params))
        compare('nul-search-validation',root+'?'+urlencode({'search':'\0'}),400)
        for action in ['ids','summary']:
            compare(action+'-nul-search-validation',root+action+'/?'+urlencode({'search':'\0'}),400)
            compare(action+'-ignores-invalid-pagination',root+action+'/?page=invalid&page_size=1')
        for name,params in [('invalid-page',{'page':'bad'}),('zero-page',{'page':'0'}),
                            ('missing-page',{'page':'999999'})]:compare(name,root+'?'+urlencode(params),404)
        for object_id,_ in created:
            compare('admin-detail-'+str(object_id),root+str(object_id)+'/')
        compare('standard-adult-detail',root+str(created[0][0])+'/',token=standard)
        compare('standard-high-detail-hidden',root+str(created[1][0])+'/',404,standard)
        compare('missing-detail',root+'2147483647/',404)
        compare('malformed-detail',root+'not-a-number/',404)
        compare('standard-nested-no-provider-url',root+'?include_streams=true',token=standard)
        userpath='/api/accounts/users/'+str(accounts['standard']['id'])+'/'
        before=invoke(baseline,userpath)
        if before[0]!=200:raise AssertionError('Fixture user missing')
        original_user=before[1]
        if invoke(baseline,userpath,{'custom_properties':{'hide_adult_content':True}},'PATCH')[0]!=200:raise AssertionError('Adult preference seed failed')
        compare('standard-hide-adult-list',root,token=standard)
        for action in ['ids','summary']:
            compare(action+'-standard-hide-adult',root+action+'/',token=standard)
        compare('standard-hide-adult-detail',root+str(created[0][0])+'/',404,standard)
        restore_user()
        value=invoke(baseline,profiles,{'name':prefix+'-profile','start_empty':True},'POST')
        if value[0]!=201:raise AssertionError('Profile fixture creation failed')
        profile=(value[1]['id'],value[1]['name'])
        membership=profiles+str(profile[0])+'/channels/'+str(created[0][0])+'/'
        if invoke(baseline,membership,{'enabled':True},'PATCH')[0]!=200:raise AssertionError('Profile fixture assignment failed')
        if invoke(baseline,userpath,{'channel_profiles':[profile[0]]},'PATCH')[0]!=200:raise AssertionError('User fixture assignment failed')
        compare('standard-assigned-profile-only',root,token=standard)
        for action in ['ids','summary']:
            compare(action+'-standard-assigned-profile',root+action+'/',token=standard)
            compare(action+'-standard-other-profile-hidden',root+action+'/?channel_profile_id=2147483647',token=standard)
        compare('standard-explicit-profile',root+'?channel_profile_id='+str(profile[0]),token=standard)
        compare('standard-show-disabled-retains-own-scope',root+'?channel_profile_id='+str(profile[0])+'&show_disabled=',token=standard)
        compare('standard-other-profile-hidden',root+'?channel_profile_id=2147483647',token=standard)
        compare('admin-explicit-profile',root+'?channel_profile_id='+str(profile[0]))
        compare('admin-explicit-show-disabled',root+'?channel_profile_id='+str(profile[0])+'&show_disabled=false')
    except Exception as error:
        record('channel-read:fixture-lifecycle',False,{'error_type':type(error).__name__})
    finally:
        try:
            restore_user()
            if profile is not None:
                path=profiles+str(profile[0])+'/'
                response=invoke(baseline,path)
                if response[0]!=200 or response[1].get('name')!=profile[1]:raise AssertionError('Unsafe profile cleanup')
                if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Profile cleanup failed')
            for object_id,name in created:
                path=root+str(object_id)+'/'
                response=invoke(baseline,path)
                if response[0]!=200 or response[1].get('name')!=name or not name.startswith(prefix):raise AssertionError('Unsafe channel cleanup')
                if invoke(baseline,path,method='DELETE')[0]!=204:raise AssertionError('Channel cleanup failed')
        except Exception as error:
            record('channel-read:fixture-cleanup',False,{'error_type':type(error).__name__})
