"""Original atomic programme staging/swap oracle on disposable PostgreSQL.

All application and algorithm credit belongs to Dispatcharr authors. Synthetic
per-case sources remain isolated; before rows are restored for the native sink.
"""
import copy
import datetime as dt
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch
import uuid

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('requires the disposable playback fixture')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db import connection, transaction
from apps.epg import tasks, utils
from apps.epg.models import EPGSource, EPGData, ProgramData
from apps.channels.models import Channel, ChannelOverride, ChannelGroup
from core.models import EpgHistoryPolicy

NOW=dt.datetime(2026,11,9,tzinfo=dt.timezone.utc)
FIELDS=['id','epg_id','start_time','end_time','title','sub_title','description','tvg_id','program_id','custom_properties']


def serialize(value):
    if isinstance(value,dt.datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def snapshot(epgs):
    return list(ProgramData.objects.filter(epg_id__in=epgs).order_by('id').values(*FIELDS))


def main():
    cases=[]
    definitions=[('replace',1,30),('empty-preserves',1,30),('only-old',1,30),
                 ('first-boundary',1,30),('spanning-history',1,30),('retention-override',1,30),
                 ('ceiling',1,3),('zero-retention',0,0),('orphan-cleanup',1,30),
                 ('late-mapping',1,30),('removed-mapping',1,30),('unmapped-staged-literal',1,30),
                 ('multi-batch',1,30),('mapping-null-empty',1,30),('rollback',1,30)]
    for ordinal,(name,min_days,max_days) in enumerate(definitions):
        prefix='rust-guide-'+uuid.uuid4().hex
        source=EPGSource.objects.create(name=prefix,source_type='xmltv',is_active=False)
        other=EPGSource.objects.create(name=prefix+' other',source_type='xmltv',is_active=False)
        epgs=[EPGData.objects.create(tvg_id=prefix+str(i),name=prefix+str(i),epg_source=source if i<3 else other) for i in range(4)]
        if name=='mapping-null-empty':
            epgs[0].tvg_id=None;epgs[0].save(update_fields=['tvg_id'])
            epgs[1].tvg_id='';epgs[1].save(update_fields=['tvg_id'])
        group=ChannelGroup.objects.create(name=prefix)
        channel=Channel.objects.create(name=prefix,channel_number=700000+ordinal,channel_group=group,
                                       epg_data=epgs[0],is_catchup=name in ['retention-override','ceiling'],catchup_days=10)
        override_channel=None
        if name=='mapping-null-empty':
            ChannelOverride.objects.create(channel=channel,epg_data=epgs[1])
        if name in ['retention-override','ceiling']:
            override_channel=Channel.objects.create(name=prefix+' override',channel_number=710000+ordinal,
                channel_group=group,epg_data=epgs[1],is_catchup=True,catchup_days=5)
            ChannelOverride.objects.create(channel=override_channel,epg_data=epgs[0])
        mapped=list(tasks._epg_ids_mapped_to_channels(source))
        for i,epg in enumerate(epgs):
            for label,start,end in [('old',-48,-36),('cutoff',-26,-24),('boundary',-2,0),('future',0,2)]:
                ProgramData.objects.create(epg=epg,start_time=NOW+dt.timedelta(hours=start),end_time=NOW+dt.timedelta(hours=end),
                    title=f'{i}-{label}',sub_title=None,description='stored',tvg_id=epg.tvg_id,program_id='legacy-'+label,
                    custom_properties={'keep':True})
        staged=[]
        if name not in ['empty-preserves','mapping-null-empty']:
            times=[(-1,1),(1,3)]
            if name=='only-old':times=[(-72,-60)]
            if name=='first-boundary':times=[(0,1)]
            if name=='spanning-history':times=[(-26,-24),(-25,1),(1,2)]
            if name=='multi-batch':times=[(i,i+1) for i in range(12)]
            for index,(start,end) in enumerate(times):
                staged.append(ProgramData(epg=epgs[0],start_time=NOW+dt.timedelta(hours=start),end_time=NOW+dt.timedelta(hours=end),
                    title='synthetic-swap-failure' if name=='rollback' else 'incoming-'+str(index),sub_title='' if index else None,
                    description='incoming',tvg_id=epgs[0].tvg_id,custom_properties=None if index==0 else {'season':0,'values':[True,1]}))
        if name=='unmapped-staged-literal':
            staged.append(ProgramData(epg=epgs[2],start_time=NOW,end_time=NOW+dt.timedelta(hours=1),title='unmapped-incoming',tvg_id=epgs[2].tvg_id))
        if name=='late-mapping':
            Channel.objects.create(name=prefix+' late',channel_number=720000+ordinal,channel_group=group,epg_data=epgs[2])
        if name=='removed-mapping':
            channel.epg_data=None;channel.save(update_fields=['epg_data'])
        before=snapshot([epg.id for epg in epgs])
        tasks._prepare_epg_program_staging_table()
        tasks._flush_epg_program_staging_batch(staged)
        failure=None;deleted=None
        if name=='rollback':
            with connection.cursor() as cursor:
                cursor.execute("CREATE FUNCTION pg_temp.rust_guide_swap_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.title = 'synthetic-swap-failure' THEN RAISE EXCEPTION 'synthetic swap failure'; END IF; RETURN NEW; END $$")
                cursor.execute('CREATE TRIGGER rust_guide_swap_fail BEFORE INSERT ON epg_programdata FOR EACH ROW EXECUTE FUNCTION pg_temp.rust_guide_swap_fail()')
        try:
            with patch.object(utils.dj_timezone,'now',return_value=NOW),patch('core.models.CoreSettings.get_epg_history_policy',return_value=EpgHistoryPolicy(min_days,max_days,0,0)):
                with transaction.atomic():
                    deleted=tasks._swap_staged_epg_programs(mapped,source,batch_size=2 if name=='multi-batch' else 5000)
        except Exception as error:
            failure=type(error).__name__
        finally:
            if name=='rollback':
                with connection.cursor() as cursor:cursor.execute('DROP TRIGGER rust_guide_swap_fail ON epg_programdata')
        expected=snapshot([epg.id for epg in epgs])
        tasks._clear_epg_program_staging_table()
        ProgramData.objects.filter(epg_id__in=[epg.id for epg in epgs]).delete()
        ProgramData.objects.bulk_create([ProgramData(**row) for row in before])
        current_mapped=tasks._epg_ids_mapped_to_channels(source)
        lookup={row.tvg_id:row.id for row in EPGData.objects.filter(id__in=current_mapped) if row.tvg_id}
        cases.append({'name':name,'source_id':source.id,'other_source_id':other.id,'epg_ids':[epg.id for epg in epgs],
            'current_mapped':sorted(current_mapped),'lookup':lookup,
            'mapped_ids':mapped,'now':NOW,'policy':{'min_days':min_days,'max_days':max_days,'default_prev_days':0,'default_days':0},
            'before':before,'staged':[{key:getattr(row,key) for key in ['epg_id','start_time','end_time','title','sub_title','description','tvg_id','custom_properties']} for row in staged],
            'expected':expected,'deleted':deleted,'error':failure,'swap_batch_size':2 if name=='multi-batch' else 5000})
    print(json.dumps({'scope':'original-real-PostgreSQL-EPG-staging-swap','cases':cases},default=serialize))


if __name__=='__main__':
    main()
