"""M3U/EPG management read oracles from original Dispatcharr source contracts.

Responses may contain provider credentials. Record only status/scalar/hash data;
original authors and contributors retain full application credit.
"""
import hashlib
import json
from urllib.parse import urlencode


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()


def list_diagnostics(left,right):
    """Order/field diagnostics only; never return body values or nested keys."""
    if not isinstance(left,list) or not isinstance(right,list):return {}
    if not all(isinstance(row,dict) and type(row.get('id')) is int for row in left+right):return {}
    a={row['id']:row for row in left};b={row['id']:row for row in right}
    shared=set(a)&set(b)
    allowed={'id','name','server_url','file_path','max_streams','is_active','created_at','updated_at',
             'filters','user_agent','profiles','locked','channel_groups','refresh_interval','cron_expression',
             'custom_properties','account_type','username','password','stale_stream_days','priority','hash_key',
             'status','last_message','enable_vod','auto_enable_new_groups_live','auto_enable_new_groups_vod',
             'auto_enable_new_groups_series','earliest_expiration','all_expirations','exp_date','server_group',
             'source_type','epg_data_count','has_channels'}
    return {'list_order_equal':[row['id'] for row in left]==[row['id'] for row in right],
            'same_item_id_set':set(a)==set(b),'items_equal_by_id':all(digest(a[key])==digest(b[key]) for key in shared),
            'different_item_count':sum(digest(a[key])!=digest(b[key]) for key in shared),
            'changed_known_fields':sorted({field for key in shared for field in allowed if digest(a[key].get(field))!=digest(b[key].get(field))})}


def run(http,baseline,candidate,accounts,record):
    fixture=accounts['_fixtures'];f=fixture['provider_reads'];provider=fixture['provider_id']
    roots=[('accounts','/api/m3u/accounts/',[provider,f['xc_id']]),
           ('profiles','/api/m3u/accounts/'+str(provider)+'/profiles/',f['profile_ids']),
           ('filters','/api/m3u/accounts/'+str(provider)+'/filters/',f['filter_ids']),
           ('server-groups','/api/m3u/server-groups/',[f['server_group_id']]),
           ('epg-sources','/api/epg/sources/',[f['epg_source_id'],f['override_epg_source_id']]),
           ('epg-data','/api/epg/epgdata/',[f['epg_data_id'],f['override_epg_data_id']])]
    def compare(name,path,expected=200,role='admin'):
        try:
            token=accounts[role]['access'] if role else None
            a=http(baseline+path,token=token);b=http(candidate+path,token=token)
            record('provider-read:'+name,a[0]==b[0]==expected and digest(a[1])==digest(b[1]),
                   {'python_status':a[0],'rust_status':b[0],
                    'python_body_sha256':digest(a[1]),'rust_body_sha256':digest(b[1]),
                    **(list_diagnostics(a[1],b[1]) if digest(a[1])!=digest(b[1]) else {})})
        except Exception as error:record('provider-read:'+name,False,{'error_type':type(error).__name__})
    for resource,root,ids in roots:
        for role,expected in [('admin',200),('standard',200),('viewer',403),('inactive',401),(None,401)]:
            compare(resource+':'+str(role),root,expected,role)
            if role in ('admin','standard'):
                for object_id in ids:
                    compare(resource+':'+role+':detail:'+str(object_id),root+str(object_id)+'/',role=role)
        for name,params in [('pagination-ignored',{'page':'bad','page_size':'1'}),
                            ('filters-ignored',{'search':'no-such-row','name':'missing','is_active':'false','ordering':'-id','epg_source':'2147483647'})]:
            compare(resource+':'+name,root+'?'+urlencode(params))
        compare(resource+':missing-detail',root+'2147483647/',404)
        compare(resource+':big-integer-missing-detail',root+('9'*100)+'/',404)
        compare(resource+':malformed-detail',root+'not-a-number/',404)
    for resource in ['profiles','filters']:
        compare(resource+':missing-parent-list','/api/m3u/accounts/2147483647/'+resource+'/')
        compare(resource+':big-integer-parent-list','/api/m3u/accounts/'+('9'*100)+'/'+resource+'/')
        compare(resource+':wrong-parent-detail','/api/m3u/accounts/'+str(f['xc_id'])+'/'+resource+'/'+str((f['profile_ids'] if resource=='profiles' else f['filter_ids'])[0])+'/',404)
    # Assert expected source values as well as cross-implementation equality.
    # This prevents an empty list or missing security field from passing merely
    # because both implementations accidentally produced the same shape.
    for label,base in [('python',baseline),('rust',candidate)]:
        try:
            admin=accounts['admin']['access'];standard=accounts['standard']['access']
            path='/api/m3u/accounts/'+str(provider)+'/'
            a=http(base+path,token=admin);s=http(base+path,token=standard)
            if a[0]!=s[0] or a[0]!=200:raise ValueError('Fixture account inaccessible')
            value=a[1]
            expirations=value['all_expirations']
            passed=(value['password']=='fixture-provider-secret' and 'password' not in s[1]
                    and value['cron_expression']=='5 3 * * 1' and value['exp_date']=='2030-01-02T03:04:05Z'
                    and value['earliest_expiration']=='2030-01-02T03:04:05Z'
                    and [entry['exp_date'] for entry in expirations]==['2029-01-01T00:00:00Z','2030-01-02T03:04:05Z','2031-01-01T00:00:00Z']
                    and [entry['order'] for entry in value['filters']]==[1,9]
                    and len(value['profiles'])==3 and value['enable_vod'] is True
                    and value['auto_enable_new_groups_live'] is False
                    and any(group['custom_properties'].get('channel_profile_ids')==[1,1,2] for group in value['channel_groups']))
            record('provider-read:'+label+':account-derived-source-semantics',passed,{'profile_count':len(value['profiles']),'filter_count':len(value['filters'])})
            profile=next(row for row in value['profiles'] if row['id']==f['usage_profile_id'])
            groups={row['channel_group']:row for row in value['channel_groups']}
            numbers=value['custom_properties']['probe_numbers']
            record('provider-read:'+label+':usage-sources-nonzero-counts-json-types',
                   type(profile['current_viewers']) is int and profile['current_viewers']==1
                   and profile['current_viewers']!=f['redis_profile_connections']==7
                   and profile['custom_properties']['user_info']['active_cons']=='11'
                   and profile['max_streams']==3 and profile['custom_properties']['user_info']['max_connections']=='13'
                   and groups[fixture['provider_group']['id']]['stream_count']==2
                   and groups[fixture['claimed_provider_group']['id']]['stream_count']==1
                   and type(numbers['integer']) is int and numbers['integer']==1
                   and type(numbers['fraction']) is float and numbers['fraction']==1.5
                   and type(numbers['integral_float']) is float and numbers['integral_float']==2.0
                   and type(numbers['large']) is int and numbers['large']==10**23,
                   {'db_current_viewers':profile['current_viewers'],'fixture_redis_profile_connections':f['redis_profile_connections'],
                    'group_stream_counts':[groups[fixture['provider_group']['id']]['stream_count'],groups[fixture['claimed_provider_group']['id']]['stream_count']]})
            sources=http(base+'/api/epg/sources/',token=admin)
            raw=next(row for row in sources[1] if row['id']==f['epg_source_id'])
            override=next(row for row in sources[1] if row['id']==f['override_epg_source_id'])
            record('provider-read:'+label+':epg-counts-raw-mapping-not-override',
                   raw['epg_data_count']==override['epg_data_count']==1 and raw['has_channels'] is True
                   and override['has_channels'] is False and raw['cron_expression']=='5 3 * * 1'
                   and 'password' not in raw and 'password' not in override,
                   {'raw_epg_count':raw['epg_data_count'],'override_epg_count':override['epg_data_count']})
        except Exception as error:record('provider-read:'+label+':fixture-semantics',False,{'error_type':type(error).__name__})
