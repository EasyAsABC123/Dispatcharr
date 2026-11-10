"""Original current-file lookup preconditions, clock/index ordering and dispatch.
Source-only until a separate native SQL/service gate passes. Original authors
retain all application and algorithm credit. Disposable fixture/owned files only.
"""
import base64,datetime as dt,json,os,sys,tempfile
from pathlib import Path
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db.models import JSONField,Value
from django.test import override_settings
from apps.epg import tasks
from apps.epg.models import EPGData,EPGSource,EPGSourceIndex
PREFIX='rust-epg-current-lookup-';NOW=dt.datetime(2026,11,10,12,tzinfo=dt.timezone.utc)
if EPGSource.objects.filter(name__startswith=PREFIX).exists():raise RuntimeError('Fresh source namespace required')

def main():
    def prog(title):return ('<programme channel="one" start="20261110110000 +0000" stop="20261110130000 +0000"><title>'+title+'</title></programme>').encode()
    first=prog('first');second=prog('second');content=first+second
    definitions=[
        {'name':'valid-inactive'}, {'name':'missing-epg'}, {'name':'no-source'},
        {'name':'dummy','source_type':'dummy'}, {'name':'schedules-direct','source_type':'schedules_direct'},
        {'name':'empty-tvg','tvg':''}, {'name':'null-tvg','tvg':None},
        {'name':'missing-file','missing_file':True,'index_mode':'missing'},
        {'name':'missing-index','index_mode':'missing'}, {'name':'sql-null-index','index_mode':'sql_null'}, {'name':'json-null-index','index_mode':'json_null'},
        {'name':'missing-channels','index':{}}, {'name':'missing-channel','index':{'channels':{'other':[0]}}},
        {'name':'empty-offsets','index':{'channels':{'one':[]}}},
        {'name':'extracted-preferred','extracted':True}, {'name':'cache-fallback','cache':True},
        {'name':'fresh-index-after-clock','clock_action':'replace'}, {'name':'deleted-index-after-clock','clock_action':'delete'},
        {'name':'fresh-clock-at-end','late':True},
        {'name':'malformed-root','index':[]}, {'name':'malformed-channels','index':{'channels':None}},
        {'name':'negative-offset','index':{'channels':{'one':[-1]}}},
        {'name':'float-offset','index':{'channels':{'one':[0.5]}}},
        {'name':'null-offsets','index':{'channels':{'one':None}}},
        {'name':'empty-interleaved','index':{'channels':{'one':[]},'interleaved_channels':['one']}},
    ]
    cases=[];original_index=EPGSource.programme_index.fget
    with tempfile.TemporaryDirectory(prefix='owned-current-lookup-') as directory,override_settings(MEDIA_ROOT=directory):
        for spec in definitions:
            name=spec['name'];source=EPGSource(name=PREFIX+name,source_type=spec.get('source_type','xmltv'),is_active=False);EPGSource.objects.bulk_create([source])
            path=Path(directory)/(name+'.xml');path.write_bytes(content);source.file_path=str(path)
            if spec.get('extracted'):
                extracted=Path(directory)/(name+'-extracted.xml');extracted.write_bytes(second);source.extracted_file_path=str(extracted);path.write_bytes(first)
            if spec.get('cache'):
                source.file_path=None;source.extracted_file_path=None;cache=Path(source.get_cache_file());cache.write_bytes(content);path=cache
            if spec.get('missing_file'):path.unlink()
            EPGSource.objects.filter(pk=source.pk).update(file_path=source.file_path,extracted_file_path=source.extracted_file_path)
            epg=EPGData.objects.create(name=PREFIX+name,tvg_id=spec.get('tvg','one'),epg_source=None if name=='no-source' else source)
            mode=spec.get('index_mode','value');initial=spec.get('index',{'channels':{'one':[0]},'interleaved_channels':[]})
            if mode!='missing':
                EPGSourceIndex.objects.create(source=source,data=None if mode in ('sql_null','json_null') else initial)
                if mode=='json_null':EPGSourceIndex.objects.filter(source=source).update(data=Value(None,output_field=JSONField()))
            calls=[];dispatch=[]
            def clock():
                calls.append('clock')
                if spec.get('clock_action')=='replace':EPGSourceIndex.objects.filter(source=source).update(data={'channels':{'one':[len(first)]}})
                if spec.get('clock_action')=='delete':EPGSourceIndex.objects.filter(source=source).delete()
                return NOW.replace(hour=13) if spec.get('late') else NOW
            def index(instance):calls.append('index');return original_index(instance)
            def defer(source_id):dispatch.append(source_id);calls.append('dispatch')
            result=None;failure=None
            with patch.object(tasks.timezone,'now',side_effect=clock),patch.object(EPGSource,'programme_index',new=property(index)),patch.object(tasks.build_programme_index_task,'delay',side_effect=defer):
                try:result=tasks.find_current_program_for_tvg_id(-987654321 if name=='missing-epg' else epg)
                except Exception as error:failure={'type':type(error).__name__,'message':str(error)}
            cases.append({'name':name,'source_id':source.pk,'epg_entry_id':epg.pk,'source_type':source.source_type,'tvg_id':epg.tvg_id,'file_path':source.file_path,'extracted_file_path':source.extracted_file_path,'cache_file_path':str(Path(directory)/'cached_epg'/f'{source.pk}.tmp'),'file_bytes_base64':base64.b64encode(content).decode(),'spec':spec,'index_mode':mode,'initial_index':initial,'now':NOW.isoformat(),'calls':calls,'dispatch':dispatch,'result':result,'failure':failure})
    print(json.dumps({'format_version':1,'scope':'Original current-file lookup eligibility/fresh clock/index/dispatch only; no native acceptance','cases':cases,'native_verified':False,'performance_verified':False}))
main()
