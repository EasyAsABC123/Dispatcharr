"""Original raw programme byte index: literal scanning and task lease boundaries.
Full application and algorithm credit belongs to original Dispatcharr authors.
Synthetic fixture only. Input specs avoid copying multi-MiB padding into JSON.
"""
import base64
import datetime as dt
import hashlib
import html.entities
import json
import os
from pathlib import Path
import sys
import uuid
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import override_settings
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGSourceIndex
from core.utils import RedisClient
PREFIX='rust-epg-byte-index-';NOW=dt.datetime(2026,11,9,12,34,56,123456,tzinfo=dt.timezone.utc)
MEDIA='/tmp/'+PREFIX+'media-'+uuid.uuid4().hex
if EPGSource.objects.filter(name__startswith=PREFIX).exists():raise RuntimeError('Fresh namespace required')

def raw(value):return {'bytes_base64':base64.b64encode(value).decode()}
def pad(length):return {'repeat':32,'length':length}
def tag(channel,tail=b''):return b'<programme channel="'+channel+b'">'+tail+b'</programme>'

def main():
    chunk=8*1024*1024;head=b'<programme channel="'
    entities=b''.join(tag(('&'+key).encode()) for key in sorted(html.entities.html5))
    numeric=b''.join(tag(value) for value in [b'&#0;',b'&#1;',b'&#9;',b'&#31;',b'&#127;',b'&#128;',b'&#129;',b'&#159;',b'&#160;',b'&#xD800;',b'&#x10FFFF;',b'&#x110000;',b'&#999999999999999999999999;',b'&#-1;',b'&#x;',b'&#65suffix',b'&notit;',b'&unknown;',b' A&amp;B ',b'\x1c\xc2\xa0A\xe2\x80\x83\x1f',b'\xff\xfe\xe0\x80'])
    definitions=[
        ('empty',[]),('grouped',[raw(tag(b'a')*3+tag(b'b')*2)]),
        ('reappearing',[raw(tag(b'a')+tag(b'b')+tag(b'a'))]),
        ('interleaved-over-ten',[raw((tag(b'a')+tag(b'b'))*12)]),
        ('missing-channel-keeps-previous',[raw(tag(b'a')+b'<programme/><programme channel=""/>'+tag(b'a'))]),
        ('false-prefix-and-namespaces',[raw(b'<programmeXYZ channel="bad"/><x:programme channel="bad"/>'+tag(b'good'))]),
        ('substring-attribute',[raw(b'<programme notchannel="first" channel="second"/><programme x:channel="third"/>')]),
        ('tag-follow-slash',[raw(b'<programme/channel="a"/>')]),
        ('quoted-greater-than',[raw(tag(b'a>b')+tag(b'next'))]),
        ('eof-no-close',[raw(b'<programme channel="a"')]),
        ('eof-tag-prefix',[raw(b'<programme')]),
        ('comments-scanned-literally',[raw(b'<!--'+tag(b'comment')+b'-->'+tag(b'real'))]),
        ('entities-all-html5',[raw(entities)]),('numeric-strip-invalid-utf8',[raw(numeric)]),
        ('start-tag-max-minus-one',[raw(head+b'a" '+b'x'*(4095-len(head)-3)+b'>')]),
        ('start-tag-max-overrun',[raw(head+b'a" '+b'x'*(4096-len(head)-3)+b'>'+tag(b'after'))]),
        ('chunk-prefix-split',[pad(chunk-5),raw(tag(b'a'))]),
        ('chunk-attribute-split',[pad(chunk-20),raw(tag(b'a-longer-id'))]),
        ('chunk-utf8-split',[pad(chunk-len(head)-1),raw(tag('🦀'.encode()))]),
        ('chunk-tag-length-limit',[pad(chunk-2048),raw(head+b'a" '+b'x'*4096+b'>'+tag(b'after'))]),
        ('extracted-path-preferred',[raw(tag(b'extracted'))]),
        ('missing-file',[]),('file-held',[raw(tag(b'not-built'))]),('defer-exhausted',[raw(tag(b'not-built'))]),
    ]
    cases=[];redis=RedisClient.get_client()
    for name,parts in definitions:
        source=EPGSource(name=PREFIX+name,source_type='xmltv',is_active=False);EPGSource.objects.bulk_create([source])
        path=Path('/tmp/'+PREFIX+name+'.xml');extracted=Path('/tmp/'+PREFIX+name+'-extracted.xml')
        digest=hashlib.sha256();size=0
        with path.open('wb') as stream:
            for part in parts:
                if 'bytes_base64'in part:
                    data=base64.b64decode(part['bytes_base64']);stream.write(data);digest.update(data);size+=len(data)
                else:
                    remaining=part['length'];block=bytes([part['repeat']])*65536
                    while remaining:
                        data=block[:min(remaining,len(block))];stream.write(data);digest.update(data);size+=len(data);remaining-=len(data)
        source.file_path=str(path)
        if name=='extracted-path-preferred':path.rename(extracted);path.write_bytes(tag(b'original-ignored'));source.extracted_file_path=str(extracted)
        if name=='missing-file':path.unlink()
        EPGSource.objects.filter(pk=source.pk).update(file_path=source.file_path,extracted_file_path=source.extracted_file_path)
        EPGSourceIndex.objects.create(source=source,data={'fixture':'old byte index'})
        file_key=f'task_lock_epg_source_file_{source.pk}';events=[]
        if name in ('file-held','defer-exhausted'):redis.set(file_key,'locked',ex=300)
        def state():
            row=EPGSourceIndex.objects.get(source=source)
            return {'data':row.data,'updated_at':row.updated_at.isoformat(),'file_held':bool(redis.exists(file_key))}
        def deferred(*args,**kwargs):events.append({'args':args,'kwargs':kwargs,'state':state()})
        try:
            before=state();failure=None;result=None
            with override_settings(MEDIA_ROOT=MEDIA),patch.object(tasks.timezone,'now',return_value=NOW),patch.object(tasks.build_programme_index_task,'apply_async',side_effect=deferred):
                try:result=tasks.build_programme_index_task.run(source.pk,_defer_retry=480 if name=='defer-exhausted' else 0)
                except Exception as error:failure={'type':type(error).__name__,'message':str(error)}
            cases.append({'name':name,'source_id':source.pk,'path':str(path),'extracted_path':str(extracted) if name=='extracted-path-preferred' else None,'spec':parts,'input_bytes':size,'input_sha256':digest.hexdigest(),'now':NOW.isoformat(),'before':before,'after':state(),'dispatch':events,'result':result,'failure':failure})
        finally:
            redis.delete(file_key);path.unlink(missing_ok=True);extracted.unlink(missing_ok=True)
    print(json.dumps({'format_version':1,'scope':'Original raw byte programme-index task and stored shape; no native/lookup/performance acceptance','chunk_bytes':chunk,'max_start_tag':4096,'offset_cap':10,'cases':cases,'native_verified':False,'lookup_verified':False,'performance_verified':False},default=str))
main()
