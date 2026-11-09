"""Queued real-oracle harness checks; fake HTTP does not prove schema parity."""
import copy
import json
import unittest
from urllib.parse import urlsplit
import provider_read_probes


FIXTURE={'provider_id':11,'provider_group':{'id':71},'claimed_provider_group':{'id':72},
    'provider_reads':{'xc_id':12,'server_group_id':31,'usage_profile_id':21,'redis_profile_connections':7,
    'profile_ids':[21,22,23],'filter_ids':[41,42],'epg_source_id':51,
    'override_epg_source_id':52,'epg_data_id':61,'override_epg_data_id':62}}
ACCOUNTS={role:{'access':role} for role in ['admin','standard','viewer','inactive']}
ACCOUNTS['_fixtures']=FIXTURE


class ReadHTTP:
    def __init__(self,mismatch=False,raises=False,wrong_usage=None):
        self.mismatch=mismatch;self.raises=raises;self.wrong_usage=wrong_usage;self.calls=[]
    def __call__(self,url,token=None,**unused):
        self.calls.append(url)
        parsed=urlsplit(url)
        if self.raises and parsed.port==2:raise RuntimeError('provider-secret-must-not-escape')
        if token in (None,'inactive'):return 401,{'detail':'Authentication failed'}
        if token=='viewer':return 403,{'detail':'Permission denied'}
        filters=[{'id':41,'order':9},{'id':42,'order':1}]
        profiles=[{'id':21,'current_viewers':1,'max_streams':3,
                   'custom_properties':{'user_info':{'active_cons':'11','max_connections':'13'}}},{'id':22},{'id':23}]
        if self.wrong_usage is not None and parsed.port==2:profiles[0]['current_viewers']=self.wrong_usage
        account={'id':11,'name':'fixture-provider','priority':1,'password':'fixture-provider-secret',
                 'cron_expression':'5 3 * * 1','exp_date':'2030-01-02T03:04:05Z',
                 'earliest_expiration':'2030-01-02T03:04:05Z','profiles':profiles,
                 'filters':list(reversed(filters)),'enable_vod':True,'auto_enable_new_groups_live':False,
                 'custom_properties':{'probe_numbers':{'integer':1,'fraction':1.5,'integral_float':2.0,'large':10**23}},
                 'channel_groups':[{'channel_group':71,'stream_count':2,'custom_properties':{'channel_profile_ids':[1,1,2]}},
                                   {'channel_group':72,'stream_count':1,'custom_properties':{}}],
                 'all_expirations':[{'exp_date':date} for date in ['2029-01-01T00:00:00Z','2030-01-02T03:04:05Z','2031-01-01T00:00:00Z']]}
        if token=='standard':account.pop('password')
        if self.mismatch and parsed.port==2:account['priority']=True
        sources=[{'id':51,'epg_data_count':1,'has_channels':True,'cron_expression':'5 3 * * 1'},
                 {'id':52,'epg_data_count':1,'has_channels':False,'cron_expression':''}]
        parts=parsed.path.strip('/').split('/')
        rows=[];detail=None
        if parts[:3]==['api','m3u','accounts']:
            if len(parts)>=5:
                rows=(profiles if parts[4]=='profiles' else filters) if parts[3]=='11' else []
                if len(parts)>5:detail=parts[5]
            else:
                rows=[account,{'id':12,'name':'fixture-xc'}]
                if len(parts)>3:detail=parts[3]
        elif parts[:3]==['api','m3u','server-groups']:
            rows=[{'id':31,'name':'fixture-server-group'}]
            if len(parts)>3:detail=parts[3]
        elif parts[:3]==['api','epg','sources']:
            rows=sources
            if len(parts)>3:detail=parts[3]
        elif parts[:3]==['api','epg','epgdata']:
            rows=[{'id':61},{'id':62}]
            if len(parts)>3:detail=parts[3]
        if detail is not None:
            row=next((row for row in rows if str(row['id'])==detail),None)
            return (200,copy.deepcopy(row)) if row is not None else (404,{'detail':'Missing fixture object'})
        return 200,copy.deepcopy(rows)


class ProviderReadTests(unittest.TestCase):
    def execute(self,**options):
        records=[];http=ReadHTTP(**options)
        provider_read_probes.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',ACCOUNTS,
                                lambda *row:records.append(row))
        return records,http
    def test_all_routes_roles_details_ignored_filters_and_reference_semantics(self):
        rows,http=self.execute()
        self.assertEqual(len(rows),96)
        self.assertTrue(all(row[1] for row in rows))
        self.assertTrue(any('page=bad' in url for url in http.calls))
        self.assertTrue(any('/accounts/2147483647/profiles/' in url for url in http.calls))
        self.assertNotIn('fixture-provider-secret',json.dumps(rows))
    def test_number_boolean_difference_fails_exact_body_comparison(self):
        rows,_=self.execute(mismatch=True)
        self.assertTrue(any(not row[1] and ':accounts:' in row[0] for row in rows))
    def test_zero_redis_xc_and_boolean_usage_cannot_masquerade_as_db_viewers(self):
        for wrong in [0,7,11,'7','11',True]:
            rows,_=self.execute(wrong_usage=wrong)
            self.assertTrue(any(not row[1] and row[0]=='provider-read:rust:usage-sources-nonzero-counts-json-types' for row in rows))
    def test_failure_capture_does_not_emit_provider_body_or_exception_message(self):
        rows,_=self.execute(raises=True)
        self.assertTrue(any(not row[1] for row in rows))
        self.assertNotIn('provider-secret',json.dumps(rows))


if __name__=='__main__':unittest.main()
