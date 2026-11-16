"""Future compact slice in the SAME auto-sync context and owned HTTP Origin.
Actual Source API/task characterization required; no runtime on import.
"""
import base64,json,traceback
from pathlib import Path

def save(path,value):Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
def prepare_context(fixture):
    if fixture.case!='auto-sync':return
    fixture._compact=fixture.sql('''
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.channels.models import Channel,ChannelGroup,ChannelGroupM3UAccount
account=M3UAccount.objects.bulk_create([M3UAccount(name='Owned Compact',server_url='http://127.0.0.1:18098/regex.m3u',account_type='STD',stale_stream_days=0,custom_properties={'hash_key':'url','orphan_channel_cleanup':'never'})])[0]
M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Default',is_default=True,is_active=True,search_pattern='',replace_pattern='')])
group,_=ChannelGroup.objects.get_or_create(name='Owned Regex')
target=ChannelGroup.objects.create(name='Owned Compact target')
props={'compact_numbering':True,'channel_numbering_mode':'provider','channel_sort_order':'name','group_override':target.pk}
relation=ChannelGroupM3UAccount.objects.create(m3u_account=account,channel_group=group,auto_channel_sync=True,enabled=True,auto_sync_channel_start=30000.9,custom_properties=props)
manual=Channel.objects.create(name='Owned compact reservation',channel_number=30001)
print(json.dumps({'account':account.pk,'group':group.pk,'target':target.pk,'relation':relation.pk,'manual':manual.pk}))
''',secret=True)

def run(fixture):
    if fixture.case!='auto-sync':raise ValueError('SameAutoContextRequired')
    setup=fixture._compact;account=setup['account'];output=Path(fixture.output)/'compact';output.mkdir(exist_ok=False);private=output/'private';private.mkdir(mode=0o700);cells=[]
    prefix=fixture._auto_backend_prefix
    def terminal(task):
        raw=fixture.redis(2,"local n=redis.call('STRLEN',ARGV[1]);if n>262144 then return redis.error_reply('compact small result bound')end;return redis.call('GET',ARGV[1])",prefix+task)
        return None if raw is None else json.loads(raw)
    def snapshot():
        return fixture.sql("from apps.channels.models import Channel,ChannelOverride;rows=list(Channel.objects.filter(auto_created_by_id="+str(account)+").order_by('id').values('id','name','uuid','channel_number','hidden_from_output','channel_group_id','updated_at'));print(json.dumps({'rows':rows,'overrides':list(ChannelOverride.objects.filter(channel_id__in=[r['id']for r in rows]).values('channel_id','channel_number')),'manual_number':Channel.objects.get(pk="+str(setup['manual'])+").channel_number},default=str))",secret=True)
    def invoke(label,api=False):
        if api:
            response=fixture.api('POST','/api/m3u/refresh/'+str(account)+'/',{});save(private/(label+'-API.json'),response);assert response['status']==202
            def actual_parent():
                ledger=fixture.case_data/'auto-publications.jsonl'
                if ledger.stat().st_size>33554432:raise RuntimeError('DeclaredSharedAutoJournalLimit')
                for line in ledger.read_text().splitlines():
                    try:
                        row=json.loads(line);frame=json.loads(row['raw_frame']);h=frame['headers']
                        if h['task']=='apps.m3u.tasks.refresh_single_m3u_account'and json.loads(base64.b64decode(frame['body']))[0]==[account]:return h['id']
                    except(json.JSONDecodeError,KeyError):continue
                return False
            task=fixture.wait(actual_parent,20,'actual-compact-normal-API-parent')
        else:
            task=fixture.sql("from apps.m3u.tasks import sync_auto_channels;from apps.channels.models import Stream;from celery import current_app;from urllib.parse import urlsplit;import datetime;assert urlsplit(current_app.conf.broker_url).path=='/1';start=Stream.objects.filter(m3u_account_id="+str(account)+").order_by('last_seen').values_list('last_seen',flat=True).first()-datetime.timedelta(seconds=1);result=sync_auto_channels.apply_async(args=["+str(account)+",start.isoformat()]);print(json.dumps(result.id))",secret=True)
        backend=fixture.wait(lambda:terminal(task),90,'actual-compact-task-backend');save(private/(label+'-backend.json'),backend);after=snapshot();save(private/(label+'-SQL.json'),after);assert backend['status']=='SUCCESS'
        return backend,after
    def initial():
        _,after=invoke('initial',True);rows=after['rows'];assert len(rows)==5 and all(r['channel_group_id']==setup['target']for r in rows)
        assert sorted(r['channel_number']for r in rows)==[30000,30002,30003,30004,30005]
        fixture._compact_initial=after
        return {'actual_visible':5,'fractional_start_truncated':True,'global_reservation_skipped':after['manual_number']==30001,'source_group_override_target_used':True}
    def hidden_pinned_exhaustion():
        rows=snapshot()['rows'];hidden,pinned=rows[0]['id'],rows[1]['id']
        fixture.sql("from apps.channels.models import Channel,ChannelOverride,ChannelGroupM3UAccount;Channel.objects.filter(pk="+str(hidden)+").update(hidden_from_output=True);ChannelOverride.objects.bulk_create([ChannelOverride(channel_id="+str(pinned)+",channel_number=30002)]);ChannelGroupM3UAccount.objects.filter(pk="+str(setup['relation'])+").update(auto_sync_channel_end=30002);print(json.dumps({'hidden':"+str(hidden)+",'pinned':"+str(pinned)+"}))",secret=True)
        value,after=invoke('exhausted');by_id={r['id']:r for r in after['rows']};assert by_id[hidden]['channel_number']is None and by_id[pinned]['channel_number']is None
        visible=[r for r in after['rows']if r['id']not in(hidden,pinned)]
        assert sorted(r['channel_number']for r in visible if r['channel_number']is not None)==[30000]
        assert value['result']['channels_failed']==2 and value['result']['channels_deleted']==0
        detail=value['result']['failed_stream_details'];assert len(detail)==1 and detail[0]['stream_id']is None and detail[0]['reason']=='RANGE_EXHAUSTED'
        assert after['overrides']==[{'channel_id':pinned,'channel_number':30002}]
        fixture._compact_special=(hidden,pinned)
        return {'visible_failed':2,'hidden_raw_released':True,'pinned_raw_cleared_override_untouched':True,'auto_failed_detail':detail}
    def reverse_and_stability():
        fixture.sql("from apps.channels.models import ChannelGroupM3UAccount;r=ChannelGroupM3UAccount.objects.get(pk="+str(setup['relation'])+");p=dict(r.custom_properties);p['channel_sort_reverse']=True;ChannelGroupM3UAccount.objects.filter(pk=r.pk).update(auto_sync_channel_end=None,custom_properties=p);print(json.dumps({'actual_reverse':True}))",secret=True)
        value,first=invoke('reverse');_,second=invoke('stable');hidden,pinned=fixture._compact_special
        visible=[r for r in first['rows']if r['id']not in(hidden,pinned)];mapping={r['name']:r['channel_number']for r in visible}
        assert mapping=={'Straße':30000,'foo':30003,'Auto 00002':30004}
        assert first==second and value['result']['channels_failed']==0
        original={r['id']:r['uuid']for r in fixture._compact_initial['rows']};assert all(original[r['id']]==r['uuid']for r in first['rows'])
        return {'natural_tuple_reverse':True,'repeated_pack_stable':True,'UUIDs_preserved':True,'raw_updates_did_not_touch_updated_at':True}
    for name,body in [('API-inline-compact-fractional-global-reservation-override-group',initial),('same-worker-hidden-pin-range-failure',hidden_pinned_exhaustion),('reverse-natural-pack-stable-UUID-time',reverse_and_stability)]:
        row={'name':name,'status':'running'};cells.append(row);save(output/'report.json',{'status':'running','cases':cells})
        try:row.update(status='passed',actual=body())
        except BaseException as error:row.update(status='failed',error_type=type(error).__name__);(private/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
        save(output/'report.json',{'status':'running','cases':cells})
    report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'new_Source_characterization_required':True,'performance_verified':False,'remaining_required':['manual repack HTTP','immediate hide/unhide model/API callbacks','compact numeric/sort invalid domains','held transaction physical rollback']};save(output/'report.json',report);return report
