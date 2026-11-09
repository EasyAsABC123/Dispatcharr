"""Original Django M3U batch oracle; disposable playback fixture only.

Executes the unchanged batch writer and captures projected input plus normalized
database state for the independent Rust sink. No application credentials leave
the process; all rows are local synthetic fixture data.
"""
import ast
import copy
import datetime
import json
import os
from pathlib import Path
import sys
import uuid

if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':
    raise RuntimeError('This oracle requires the disposable playback fixture')
sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.channels.models import Channel,ChannelOverride,ChannelStream,ChannelGroup,ChannelGroupM3UAccount,Stream
from apps.m3u.models import M3UAccount
from apps.m3u import tasks
from core.models import StreamProfile
from django.utils import timezone


def serialize(value):
    if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
    raise TypeError(type(value).__name__)


def cleanup_oracle(template,other):
    prefix='rust-cleanup-'+uuid.uuid4().hex
    account=copy.copy(template);account.pk=None;account.name=prefix;account.locked=False
    account.is_active=True;account.stale_stream_days=7
    M3UAccount.objects.bulk_create([account])
    enabled=ChannelGroup.objects.create(name=prefix+' enabled')
    disabled=ChannelGroup.objects.create(name=prefix+' disabled')
    ChannelGroupM3UAccount.objects.bulk_create([
        ChannelGroupM3UAccount(channel_group=enabled,m3u_account=account,enabled=True),
        ChannelGroupM3UAccount(channel_group=disabled,m3u_account=account,enabled=False)])
    scan=timezone.now()
    definitions=[('fresh-enabled',account,enabled,1,3),('fresh-disabled',account,disabled,1,0),
                 ('old-disabled',account,disabled,30,7),('old-enabled',account,enabled,30,0),
                 ('fresh-null-group',account,None,1,0),('exact-retention-boundary',account,enabled,7,0),
                 ('other-account-untouched',other,disabled,30,0)]
    streams=[]
    for suffix,owner,group,days,catchup in definitions:
        streams.append(Stream(name=prefix+' '+suffix,tvg_id=prefix+' '+suffix,url='http://origin:8080/live/0.ts',
            m3u_account=owner,channel_group=group,last_seen=scan-datetime.timedelta(days=days),
            is_catchup=bool(catchup),catchup_days=catchup))
    Stream.objects.bulk_create(streams)
    channel=Channel(name=prefix+' channel',channel_group=enabled)
    Channel.objects.bulk_create([channel])
    ChannelStream.objects.bulk_create([ChannelStream(channel=channel,stream=streams[0],order=1),
                                      ChannelStream(channel=channel,stream=streams[2],order=2)])
    from apps.channels.utils import rollup_catchup_for_channels
    rollup_catchup_for_channels([channel.pk])
    initial_streams=list(Stream.objects.filter(tvg_id__startswith=prefix).values())
    initial_links=list(ChannelStream.objects.filter(channel=channel).values())
    def snapshot():
        return {'streams':list(Stream.objects.filter(tvg_id__startswith=prefix).order_by('tvg_id').values('tvg_id','m3u_account_id','channel_group_id','is_catchup','catchup_days')),
                'channel':Channel.objects.filter(pk=channel.pk).values('is_catchup','catchup_days').get(),
                'links':list(ChannelStream.objects.filter(channel=channel).order_by('stream__tvg_id').values_list('stream__tvg_id',flat=True))}
    initial_channel=snapshot()['channel']
    steps=[]
    for name,days,active in [('overlap-and-null-group',7,True),('idempotent-repeat',7,True),
                             ('zero-retention',0,True),('overflow-retention',2147483647,True),('inactive-account-refusal',0,False)]:
        M3UAccount.objects.filter(pk=account.pk).update(stale_stream_days=days,is_active=active)
        before=Stream.objects.filter(m3u_account=account).count()
        selected_groups=ChannelGroup.objects.filter(m3u_accounts__m3u_account=account,m3u_accounts__enabled=True).values_list('id',flat=True)
        disabled_matches=Stream.objects.filter(m3u_account=account).exclude(channel_group__in=selected_groups).count()
        try:stale_matches=Stream.objects.filter(m3u_account=account,last_seen__lt=scan-datetime.timedelta(days=days)).count()
        except OverflowError:stale_matches=None
        error=False;reported=None
        try:reported=tasks.cleanup_streams(account.pk,scan)
        except Exception:error=True
        after=Stream.objects.filter(m3u_account=account).count()
        steps.append({'name':name,'stale_stream_days':days,'is_active':active,'error':error,'reported_total':reported,
            'disabled_group_matches':disabled_matches,'stale_matches':stale_matches,'deleted_streams':before-after,'expected':snapshot()})
    ChannelStream.objects.filter(channel=channel).delete()
    Stream.objects.filter(tvg_id__startswith=prefix).delete()
    Stream.objects.bulk_create([Stream(**row) for row in initial_streams])
    ChannelStream.objects.bulk_create([ChannelStream(**row) for row in initial_links])
    Channel.objects.filter(pk=channel.pk).update(**initial_channel)
    M3UAccount.objects.filter(pk=account.pk).update(stale_stream_days=7,is_active=True)
    return {'prefix':prefix,'account_id':account.pk,'channel_id':channel.pk,'scan_time':scan,'steps':steps}


def group_cleanup_oracle(template,other):
    prefix='rust-group-cleanup-'+uuid.uuid4().hex;scan=timezone.now()
    account=copy.copy(template);account.pk=None;account.name=prefix;account.locked=False
    account.is_active=True;account.stale_stream_days=7;M3UAccount.objects.bulk_create([account])
    groups=[ChannelGroup(name=prefix+' '+suffix) for suffix in ['orphan','stream-only','override-only','shared','direct','boundary']]
    ChannelGroup.objects.bulk_create(groups)
    ChannelGroupM3UAccount.objects.bulk_create([ChannelGroupM3UAccount(channel_group=group,m3u_account=account,
        last_seen=scan-datetime.timedelta(days=7 if index==5 else 30)) for index,group in enumerate(groups)]+
        [ChannelGroupM3UAccount(channel_group=groups[3],m3u_account=other,enabled=False)])
    stream=Stream(name=prefix,tvg_id=prefix,url='http://origin:8080/live/0.ts',m3u_account=other,channel_group=groups[1])
    Stream.objects.bulk_create([stream])
    channel=Channel(name=prefix,channel_group=groups[4]);Channel.objects.bulk_create([channel])
    override=ChannelOverride(channel=channel,channel_group=groups[2]);ChannelOverride.objects.bulk_create([override])
    stream.refresh_from_db();channel.refresh_from_db();override.refresh_from_db()
    times={'stream':stream.updated_at,'channel':channel.updated_at,'override':override.updated_at}
    initial_groups=list(ChannelGroup.objects.filter(name__startswith=prefix).values())
    initial_relations=list(ChannelGroupM3UAccount.objects.filter(channel_group__name__startswith=prefix).values())
    def snapshot():
        stream.refresh_from_db();channel.refresh_from_db();override.refresh_from_db()
        return {'groups':list(ChannelGroup.objects.filter(name__startswith=prefix).order_by('name').values_list('name',flat=True)),
                'relations':list(ChannelGroupM3UAccount.objects.filter(channel_group__name__startswith=prefix)
                    .order_by('channel_group__name','m3u_account_id').values('channel_group__name','m3u_account_id','enabled','last_seen','is_stale')),
                'references':{'stream':stream.channel_group_id,'channel':channel.channel_group_id,'override':override.channel_group_id},
                'timestamps_unchanged':{'stream':stream.updated_at==times['stream'],'channel':channel.updated_at==times['channel'],'override':override.updated_at==times['override']}}
    steps=[]
    for name,locked,account_name,days in [('locked-custom-skip-before-overflow',True,'custom',2147483647),
        ('orphan-and-protected-groups',False,prefix,7),('idempotent-repeat',False,prefix,7),
        ('zero-retention-boundary',False,prefix,0),('overflow-refusal',False,prefix,2147483647)]:
        selected=copy.copy(account);selected.locked=locked;selected.name=account_name;selected.stale_stream_days=days
        error=False;reported=None
        try:reported=tasks.cleanup_stale_group_relationships(selected,scan)
        except Exception:error=True
        steps.append({'name':name,'account':{'id':account.pk,'name':account_name,'locked':locked,'stale_stream_days':days},
                      'error':error,'reported':reported,'expected':snapshot()})
    existing=set(ChannelGroup.objects.filter(name__startswith=prefix).values_list('id',flat=True))
    ChannelGroup.objects.bulk_create([ChannelGroup(**row) for row in initial_groups if row['id'] not in existing])
    ChannelGroupM3UAccount.objects.filter(channel_group__name__startswith=prefix).delete()
    ChannelGroupM3UAccount.objects.bulk_create([ChannelGroupM3UAccount(**row) for row in initial_relations])
    Stream.objects.filter(pk=stream.pk).update(channel_group=groups[1],updated_at=times['stream'])
    ChannelOverride.objects.filter(pk=override.pk).update(channel_group=groups[2],updated_at=times['override'])
    return {'prefix':prefix,'scan_time':scan,'stream_id':stream.pk,'channel_id':channel.pk,'override_id':override.pk,'times':times,'steps':steps}


def main():
    prefix='rust-ingest-'+uuid.uuid4().hex
    account=M3UAccount.get_custom_account()
    group=ChannelGroup.objects.create(name=prefix+' group')
    groups={'Default Group':group.id}
    keys=['tvg_id']
    original=Path('/app/apps/m3u/tasks.py').read_text()
    tree=ast.parse(original)
    function=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='process_m3u_batch_direct')
    loop=next(node for node in function.body if isinstance(node,ast.For) and isinstance(node.target,ast.Name) and node.target.id=='stream_info')
    projection=compile(ast.Module(body=[loop],type_ignores=[]),'<original-M3U-batch-projection>','exec')
    other=M3UAccount.objects.get(pk=account.pk)
    other.pk=None;other.name=prefix+' other provider';other.locked=False
    M3UAccount.objects.bulk_create([other])
    def projected(batch,selected_groups=groups,selected_account=account):
        rows=[]
        for entry in batch:
            scope=dict(vars(tasks),account=selected_account,account_id=selected_account.id,batch=[entry],
                       groups=selected_groups,hash_keys=keys,compiled_filters=[],
                       name_max_length=Stream._meta.get_field('name').max_length,stream_hashes={})
            exec(projection,scope)
            for stream_hash,props in scope['stream_hashes'].items():
                row={key:value for key,value in props.items() if key not in {'m3u_account','is_stale'}}
                rows.append(row)
        return rows
    def raw(suffix,name=None,**attributes):
        return {'name':name or 'Fixture '+suffix,'url':'http://origin:8080/live/0.ts',
                'attributes':{'tvg-id':prefix+' '+suffix,**attributes}}
    fields=[field.attname for field in Stream._meta.concrete_fields if field.name!='id']
    def database_rows():
        return list(Stream.objects.filter(tvg_id__startswith=prefix).order_by('stream_hash').values(*fields))
    def normalized(rows,previous):
        old={row['stream_hash']:row for row in previous}
        result=[]
        for row in rows:
            before=old.get(row['stream_hash'])
            result.append({**{key:value for key,value in row.items() if key not in {'updated_at','last_seen'}},
                           'updated_at_changed':before is None or before['updated_at']!=row['updated_at'],
                           'last_seen_advanced':before is None or before['last_seen']<row['last_seen']})
        return result
    # Existing row keeps its custom/profile/stats/local-file ownership. Only
    # explicit import fields and last_seen may be updated by the source batch.
    unchanged=raw('unchanged',**{'numeric':1})
    existing=projected([unchanged])[0]
    created=Stream(m3u_account=account,**existing,is_custom=True,current_viewers=7,
                   stream_profile=StreamProfile.get_locked('Proxy'),local_file='uploads/fixture.ts',
                   stream_stats={'fixture':True})
    Stream.objects.bulk_create([created])
    old_time=timezone.now()-datetime.timedelta(days=2)
    Stream.objects.filter(pk=created.pk).update(last_seen=old_time,updated_at=old_time,is_stale=True,
                                               custom_properties={**unchanged['attributes'],'numeric':True})
    initial=database_rows()
    changed=raw('unchanged',name='Changed Café 😀',**{'tv_archive':'1','tv_archive_duration':'7','radio':'True'})
    new=raw('new',**{'tvg-chno':'2','is_adult':'1'})
    duplicate={**new,'name':'Discarded duplicate'}
    steps=[]
    for name,batch,selected_groups,selected_account in [
        ('create-touch-deduplicate',[unchanged,new,duplicate],groups,account),
        ('unchanged-touch',[unchanged,new],groups,account),
        ('change-selected-fields',[changed,new],groups,account),
        ('cross-account-hash-retains-original-account',[raw('unchanged',name='Other provider refresh')],groups,other),
        ('transaction-rollback',[raw('rollback-good'),raw('rollback-bad',**{'group-title':'bad'})],{**groups,'bad':-1},account),
    ]:
        before=database_rows();error=False;counts=None
        try:
            message=tasks.process_m3u_batch_direct(selected_account.id,batch,selected_groups,keys,[])
            counts=tasks._parse_batch_stream_counts(message)
        except Exception:
            error=True
        after=database_rows()
        steps.append({'name':name,'account_id':selected_account.id,'rows':projected(batch,selected_groups,selected_account),'counts':counts,'error':error,
                      'expected':normalized(after,before)})
    playlist=(f'#EXTM3U\n#EXTINF:-1 tvg-id="{prefix} parsed-a" group-title="Default Group" tvg-chno="٢",Café 😀\n'
              '#EXTVLCOPT:http-referrer=http://fixture\nhttp://origin:8080/live/0.ts\n'
              f'#EXTINF:-1 tvg-id="{prefix} parsed-a",Discarded duplicate\nhttp://origin:8080/live/1.ts\n'
              f'#EXTINF:-1 tvg-id="{prefix} parsed-b" tv_archive="True" tv_archive_duration="٧" radio="true",Radio\n'
              '#EXTGRP:Default Group\nhttpGarbage\n'
              f'#EXTINF:-1 tvg-id="{prefix} no-url",Discard at EOF\n')
    batch=list(tasks.iter_m3u_entries(playlist.splitlines()))
    before=database_rows()
    message=tasks.process_m3u_batch_direct(account.id,batch,groups,keys,[])
    steps.append({'name':'plain-parser-projection-and-database-pipeline','account_id':account.id,
                  'playlist':playlist,'projection_context':{'account_type':account.account_type,'groups':groups,'hash_keys':keys,'max_name_characters':512},
                  'rows':projected(batch),'counts':tasks._parse_batch_stream_counts(message),'error':False,
                  'expected':normalized(database_rows(),before)})
    # Raw-entry slices precede projection; a fully disabled first batch still
    # forms a batch even though it contributes no database writes.
    playlist = '#EXTM3U\n' + ''.join(
        f'#EXTINF:-1 tvg-id="{prefix} skipped-{index}" group-title="excluded",Skip\nhttp://origin:8080/live/0.ts\n'
        for index in range(1500))
    playlist += f'#EXTINF:-1 tvg-id="{prefix} boundary",Boundary\nhttp://origin:8080/live/0.ts\n'
    batch = list(tasks.iter_m3u_entries(playlist.splitlines()))
    before = database_rows()
    counts = [0, 0, 0]
    for offset in range(0, len(batch), 1500):
        result = tasks._parse_batch_stream_counts(tasks.process_m3u_batch_direct(
            account.id, batch[offset:offset+1500], groups, keys, []))
        counts = [left+right for left, right in zip(counts, result)]
    steps.append({'name':'parsed-cache-raw-boundaries-before-disabled-groups','account_id':account.id,
                  'playlist':playlist,'projection_context':{'account_type':account.account_type,'groups':groups,'hash_keys':keys,'max_name_characters':512},
                  'rows':projected(batch),'counts':counts,'error':False,'expected_batches':2,
                  'expected':normalized(database_rows(),before)})
    # Real database failures: other batches finish before one sequential retry.
    # A nontransactional sequence makes the transient trigger fail exactly once;
    # committed audit order distinguishes retry-after-all from inline retry.
    from django.db import connection
    suffix=uuid.uuid4().hex[:12]
    retry_trigger={key:'rust_retry_'+key+'_'+suffix for key in ['sequence','audit','function','trigger']}
    retry_trigger.update(transient_id=prefix+' retry-transient',valid_id=prefix+' retry-transient-valid')
    q=connection.ops.quote_name
    with connection.cursor() as cursor:
        cursor.execute('CREATE SEQUENCE '+q(retry_trigger['sequence'])+' START 1')
        cursor.execute('CREATE TABLE '+q(retry_trigger['audit'])+' (id bigint DEFAULT nextval(\''+retry_trigger['sequence']+'\'),tvg_id text NOT NULL)')
        cursor.execute(f"""CREATE FUNCTION {q(retry_trigger['function'])}() RETURNS trigger AS $$ BEGIN
            IF NEW.tvg_id IN ('{retry_trigger['transient_id']}','{retry_trigger['valid_id']}') THEN
                IF NEW.tvg_id='{retry_trigger['transient_id']}' AND nextval('{retry_trigger['sequence']}')=1 THEN
                    RAISE EXCEPTION 'disposable transient import fault';
                END IF;
                INSERT INTO {q(retry_trigger['audit'])}(tvg_id) VALUES (NEW.tvg_id);
            END IF; RETURN NEW; END; $$ LANGUAGE plpgsql""")
        cursor.execute('CREATE TRIGGER '+q(retry_trigger['trigger'])+' BEFORE INSERT ON dispatcharr_channels_stream FOR EACH ROW EXECUTE FUNCTION '+q(retry_trigger['function'])+'()')
    for mode in ['permanent','transient']:
        selected_groups={**groups,'bad':-1}
        title='bad' if mode=='permanent' else 'Default Group'
        playlist=f'#EXTM3U\n#EXTINF:-1 tvg-id="{prefix} retry-{mode}" group-title="{title}",Retry {mode}\nhttp://origin:8080/live/0.ts\n'
        playlist+=''.join(f'#EXTINF:-1 tvg-id="{prefix} retry-{mode}-skip-{i}" group-title="excluded",Skip\nhttp://origin:8080/live/0.ts\n' for i in range(1499))
        playlist+=f'#EXTINF:-1 tvg-id="{prefix} retry-{mode}-valid",Later batch\nhttp://origin:8080/live/0.ts\n'
        batch=list(tasks.iter_m3u_entries(playlist.splitlines()))
        before=database_rows();counts=[0,0,0];failed=[]
        for index,offset in enumerate(range(0,len(batch),1500)):
            part=batch[offset:offset+1500]
            try:
                result=tasks._parse_batch_stream_counts(tasks.process_m3u_batch_direct(account.id,part,selected_groups,keys,[]))
                counts=[a+b for a,b in zip(counts,result)]
            except Exception:failed.append((index,part))
        assert len(failed)==1
        created,updated,unchanged,still_failed=tasks._retry_failed_batches(account.id,failed,selected_groups,keys,[])
        counts=[a+b for a,b in zip(counts,[created,updated,unchanged])]
        step={'name':'parsed-cache-retry-after-all-'+mode,'account_id':account.id,'playlist':playlist,
              'projection_context':{'account_type':account.account_type,'groups':selected_groups,'hash_keys':keys,'max_name_characters':512},
              'rows':projected(batch,selected_groups),'counts':counts,'error':False,'expected_input_batches':2,
              'expected_batches':2-still_failed,'expected_failed_batches':still_failed,
              'expected':normalized(database_rows(),before)}
        if mode=='transient':
            with connection.cursor() as cursor:
                cursor.execute('SELECT tvg_id FROM '+q(retry_trigger['audit'])+' ORDER BY id')
                step['retry_audit']=[row[0] for row in cursor.fetchall()]
            assert step['retry_audit']==[retry_trigger['valid_id'],retry_trigger['transient_id']]
        steps.append(step)
    # Restore the same initial logical dataset for the independent Rust writer.
    Stream.objects.filter(tvg_id__startswith=prefix).delete()
    restored=[]
    for row in initial:
        data={key:value for key,value in row.items() if key not in {'updated_at','last_seen'}}
        stream=Stream(**data);Stream.objects.bulk_create([stream])
        Stream.objects.filter(pk=stream.pk).update(updated_at=row['updated_at'],last_seen=row['last_seen'])
        restored.append(stream.pk)
    # Separate group-sync oracle: preserve the task's actual bulk-write order,
    # legacy JSON normalization and global-group/provider-link distinction.
    initial_relation=ChannelGroupM3UAccount(channel_group=group,m3u_account=account,
        custom_properties={'xc_id':True,'keep':'user-setting'},enabled=False,
        auto_channel_sync=True,auto_sync_channel_start=1.5,auto_sync_channel_end=10.5,
        last_seen=old_time,is_stale=True)
    ChannelGroupM3UAccount.objects.bulk_create([initial_relation])
    initial_relation_data=ChannelGroupM3UAccount.objects.filter(pk=initial_relation.pk).values().get()
    def group_snapshot():
        names=list(ChannelGroup.objects.filter(name__startswith=prefix).order_by('name').values_list('name',flat=True))
        relations=list(ChannelGroupM3UAccount.objects.filter(channel_group__name__startswith=prefix)
            .order_by('channel_group__name','m3u_account_id').values('channel_group__name','m3u_account_id','custom_properties',
                'enabled','auto_channel_sync','auto_sync_channel_start','auto_sync_channel_end','last_seen','is_stale'))
        return {'groups':names,'relations':relations}
    group_steps=[]
    missing=copy.copy(account);missing.pk=-1
    for index,(name,selected_account,account_props,incoming,mutation) in enumerate([
        ('create-disabled-preserve-bool-xc',account,{'auto_enable_new_groups_live':False},
         {group.name:{'xc_id':1},prefix+' B':{'xc_id':11,'incoming':'new'},prefix+' A':{}},None),
        ('change-xc-preserve-user-settings',account,{}, {group.name:{'xc_id':2}},None),
        ('remove-xc-preserve-disabled',account,{}, {group.name:{},prefix+' B':{},prefix+' C':{}},None),
        ('normalize-legacy-string',account,{}, {group.name:{'xc_id':4}},json.dumps({'xc_id':4,'keep':'legacy-user'})),
        ('normalize-invalid-legacy-string',account,{}, {group.name:{}},'not-json'),
        ('retain-unchanged-nonobject',account,{}, {group.name:{}},[1,2]),
        ('retain-unchanged-null',account,{}, {group.name:{}},{'__fixture_sql_null__':True}),
        ('other-provider-no-manual-link',other,{}, {prefix+' D':{'xc_id':17}},None),
        ('group-create-survives-later-fk-error',missing,{}, {prefix+' E':{}},None),
        ('legacy-account-auto-enable',account,json.dumps({'auto_enable_new_groups_live':False}), {prefix+' F':{}},None),
        ('group-create-survives-invalid-auto-enable',account,{'auto_enable_new_groups_live':None}, {prefix+' G':{}},None),
    ]):
        if mutation=={'__fixture_sql_null__':True}:mutation=None;mutate_null=True
        else:mutate_null=False
        if mutation is not None or mutate_null:
            ChannelGroupM3UAccount.objects.filter(pk=initial_relation.pk).update(custom_properties=mutation,is_stale=True)
        selected_account.custom_properties=account_props
        scan_time=timezone.now()+datetime.timedelta(seconds=index+1)
        error=False
        try:tasks.process_groups(selected_account,incoming,scan_time)
        except Exception:error=True
        group_steps.append({'name':name,'account_id':selected_account.pk,'account_properties':account_props,
            'groups':[{'name':key,'custom_properties':value} for key,value in incoming.items()],
            'scan_time':scan_time,'mutation':None if mutation is None and not mutate_null else {'group_id':group.pk,'account_id':account.pk,'custom_properties':mutation},
            'error':error,'expected':group_snapshot()})
    ChannelGroupM3UAccount.objects.filter(channel_group__name__startswith=prefix).delete()
    ChannelGroup.objects.filter(name__startswith=prefix).exclude(pk=group.pk).delete()
    ChannelGroupM3UAccount.objects.bulk_create([ChannelGroupM3UAccount(**initial_relation_data)])
    cleanup=cleanup_oracle(account,other)
    group_cleanup=group_cleanup_oracle(account,other)
    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE TABLE '+q(retry_trigger['audit']))
        cursor.execute('ALTER SEQUENCE '+q(retry_trigger['sequence'])+' RESTART WITH 1')
    print(json.dumps({'retry_trigger':retry_trigger,'account_id':account.id,'prefix':prefix,'initial':initial,'steps':steps,'group_steps':group_steps,'cleanup':cleanup,'group_cleanup':group_cleanup,
                      'group_id':group.id,'restored_ids':restored},default=serialize))


if __name__=='__main__':main()
