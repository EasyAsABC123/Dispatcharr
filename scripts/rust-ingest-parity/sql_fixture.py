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
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
from apps.m3u.models import M3UAccount
from apps.m3u import tasks
from core.models import StreamProfile
from django.utils import timezone


def serialize(value):
    if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
    raise TypeError(type(value).__name__)


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
    print(json.dumps({'account_id':account.id,'prefix':prefix,'initial':initial,'steps':steps,'group_steps':group_steps,
                      'group_id':group.id,'restored_ids':restored},default=serialize))


if __name__=='__main__':main()
