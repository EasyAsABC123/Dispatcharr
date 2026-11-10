"""Original EPG automatic-logo helper flags, bulk effects and failure policy.
Full application and algorithm credit belongs to original Dispatcharr authors.
Synthetic database fixture only; media fetching is not invoked by these helpers.
"""
import json
import os
import sys
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.channels.models import Channel,ChannelOverride,Logo
from apps.channels import utils
from apps.epg.models import EPGSource,EPGData
from django.db import connection
PREFIX='rust-epg-auto-logo-'
if EPGSource.objects.filter(name__startswith=PREFIX).exists():raise RuntimeError('Fresh namespace required')

def main():
    cases=[]
    definitions=[('absent',{}),('false',{'auto_apply_epg_logos':False}),('zero',{'auto_apply_epg_logos':0}),('empty-string',{'auto_apply_epg_logos':''}),('empty-array',{'auto_apply_epg_logos':[]}),('true',{'auto_apply_epg_logos':True}),('truthy-string',{'auto_apply_epg_logos':'false'}),('truthy-array',{'auto_apply_epg_logos':[False]}),('null',None),('root-empty-array',[]),('root-array',[False]),('root-string','enabled'),('root-number',1),('root-bool',True),('bulk-name-too-long',{'auto_apply_epg_logos':True}),('batch-501',{'auto_apply_epg_logos':True}),('large-current-logo',{'auto_apply_epg_logos':True}),('forced-db-error',{'auto_apply_epg_logos':True})]
    for ordinal,(name,props) in enumerate(definitions):
        source=EPGSource(name=PREFIX+name,is_active=False,source_type='xmltv',custom_properties=props);EPGSource.objects.bulk_create([source])
        prefix='https://fixture.invalid/'+PREFIX+name+'/'
        old=Logo.objects.create(name=PREFIX+name+' old',url=prefix+'old')
        current=Logo.objects.create(name=PREFIX+name+' current',url=prefix+'current')
        existing=Logo.objects.create(name=PREFIX+name+' reused',url=prefix+'reuse')
        if name=='large-current-logo':
            Logo.objects.filter(pk=current.pk).update(url=prefix+'current-'+'x'*(160*1024))
            current.refresh_from_db()
        count=501 if name=='batch-501' else 5
        channels=[]
        for n in range(count):
            url=prefix+('new' if n%5==0 else 'reuse' if n%5==1 else 'current' if n%5==2 else 'override-only' if n%5==3 else 'blank')
            icon=('\u001c\u2003'+url+'\u001f\u2003') if n%5==1 else '' if n%5==3 else ' \u2003 ' if n%5==4 else url
            epg=EPGData.objects.create(epg_source=source,tvg_id='id-'+str(n),name='X'*256 if name=='bulk-name-too-long' and n==0 else '' if n==0 else 'Guide '+str(n),icon_url=icon)
            channel=Channel(name=PREFIX+name+'-'+str(n),channel_number=1_780_000+ordinal*1000+count-n,epg_data=epg,logo=current if n%5==2 else old);Channel.objects.bulk_create([channel]);channels.append(channel.pk)
            if n==3:
                override=EPGData.objects.create(epg_source=source,tvg_id='override-only',name='Override',icon_url=prefix+'override-only')
                ChannelOverride.objects.create(channel=channel,epg_data=override)
        def state():
            return {'channels':list(Channel.objects.filter(pk__in=channels).order_by('id').values('id','epg_data_id','logo_id','logo__url')),'logos':list(Logo.objects.filter(url__startswith=prefix).order_by('id').values('id','name','url'))}
        before=state();sql=[]
        def capture(execute,statement,params,many,context):
            sql.append({'sql':statement,'params':params})
            return execute(statement,params,many,context)
        result=None;failure=None
        try:
            with connection.execute_wrapper(capture):
                if name=='forced-db-error':
                    with connection.cursor() as cursor:cursor.execute("ALTER TABLE dispatcharr_channels_logo ADD CONSTRAINT rust_epg_auto_logo_fixture CHECK(url NOT LIKE %s) NOT VALID",[prefix+'new%'])
                    try:result=utils.maybe_auto_apply_epg_logos(source)
                    finally:
                        with connection.cursor() as cursor:cursor.execute("ALTER TABLE dispatcharr_channels_logo DROP CONSTRAINT rust_epg_auto_logo_fixture")
                else:result=utils.maybe_auto_apply_epg_logos(source)
        except Exception as error:failure={'type':type(error).__name__,'message':str(error)}
        cases.append({'name':name,'source_id':source.pk,'properties':props,'before':before,'after':state(),'result':result,'failure':failure,'sql':sql})
    print(json.dumps({'format_version':1,'scope':'Original automatic EPG-logo helper flags/bulk mutation/failure policy','cases':cases,'native_verified':False,'full_channel_job_verified':False,'performance_verified':False},default=str))
main()
