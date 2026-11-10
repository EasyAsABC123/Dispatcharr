"""Draft original XMLTV format/extraction oracle on disposable owned files.
Original Dispatcharr authors and contributors retain full application credit.
"""
import base64
import gzip
import hashlib
import io
import json
import lzma
import mimetypes
import os
from pathlib import Path
import sys
import tempfile
import warnings
import zipfile

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.epg.tasks import detect_file_format, extract_compressed_file

def encoded(data):
    return None if data is None else base64.b64encode(data).decode()
formats=[]
headers=[None,b'',b'<tv>',b'<tv></tv>',b'<?xml',b'<?xml version="1.0"?><tv/>',
         b' '*16+b'<tv>',b' '*17+b'<tv>',b'PK',b'PKbroken',b'\x1f\x8b',b'\xfd7zXZ\0',
         b'\xfd7zXZ',b'BZh9',b'\xef\xbb\xbf<tv></tv>',b'<tv source="fixture">',
         b'<TV></TV>',b'\0<tv></tv>',b'ordinary bytes']
names=[None,'fixture','fixture.xml','fixture.XML','fixture.xml.gz','fixture.gzip',
       'fixture.GZ','fixture.xz','fixture.zip','fixture.ZIP','fixture.lzma',
       'fixture.tar','fixture.svg','fixture.rdf','fixture.xsl','fixture.wsdl',
       'http://fixture.invalid/guide.xml','http://fixture.invalid/guide.xml?x=1',
       'http://fixture.invalid/a?filename=guide.xml','fixture.xml#fragment',
       'data:application/xml,<tv/>']
for name in names:
    for content in headers:
        formats.append({'path':name,'content_base64':encoded(content),
                        'mime_hint':mimetypes.guess_type(name)[0] if name else None,
                        'expected':list(detect_file_format(file_path=name,content=content))})

XML=b'<?xml version="1.0"?><tv><channel id="fixture"/></tv>'
LARGE=b'<tv>'+b' '*131073+b'</tv>'
def archive(entries,compression=zipfile.ZIP_DEFLATED):
    out=io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with zipfile.ZipFile(out,'w',compression=compression) as writer:
            for name,content in entries:
                writer.writestr(name,content)
    return out.getvalue()

blobs=[('plain.xml',XML),('empty.gz',b''),('empty.xz',b''),('empty.zip',archive([])),
       ('input.gz',gzip.compress(XML,mtime=0)),('input.gzip',gzip.compress(XML,mtime=0)),
       ('wrong.bin',gzip.compress(XML,mtime=0)),('wrong.gz',XML),
       ('input.xz',lzma.compress(XML)),('alone.xz',lzma.compress(XML,format=lzma.FORMAT_ALONE)),
       ('multi.gz',gzip.compress(XML,mtime=0)+gzip.compress(b'fixture-tail',mtime=0)),
       ('multi.xz',lzma.compress(XML)+lzma.compress(b'fixture-tail')),
       ('large.gz',gzip.compress(LARGE,mtime=0)),('large.xz',lzma.compress(LARGE)),
       ('large.gz',gzip.compress(LARGE,mtime=0)[:-4]),('large.xz',lzma.compress(LARGE)[:-4]),
       ('trailing.gz',gzip.compress(XML,mtime=0)+b'garbage'),
       ('trailing.xz',lzma.compress(XML)+b'garbage'),
       ('truncated.gz',gzip.compress(XML,mtime=0)[:-4]),
       ('truncated.xz',lzma.compress(XML)[:-4]),
       ('bad.zip',b'PKbroken'),('bad.xz',b'\xfd7zXZ\0broken'),('bad.gz',b'\x1f\x8bbroken')]
entries=[ [('guide.xml',XML)], [('guide.XML',XML)], [('guide.xml',b'not xml')],
          [('guide.xml',b'')], [('folder/',b''),('guide.dat',XML)],
          [('ordinary.txt',b'ordinary'),('guide.dat',XML)],
          [('ordinary.txt',b'ordinary')], [('first.xml',b'first'),('second.xml',b'second')],
          [('duplicate.xml',b'first'),('duplicate.xml',b'last')],
          [('../guide.XML',XML)], [('日本語.xml',XML)], [('guide.xml/',b'')]]
for compression in [zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED,zipfile.ZIP_BZIP2,zipfile.ZIP_LZMA]:
    for index,members in enumerate(entries):
        blobs.append((f'zip-{compression}-{index}.zip',archive(members,compression)))
blobs += [('prefixed.zip',b'fixture-prefix'+archive([('guide.xml',XML)])),
          ('truncated.zip',archive([('guide.xml',XML)])[:-8])]
extractions=[]
def digest(path):
    if not path.exists():return None
    total=0
    hashed=hashlib.sha256()
    with path.open('rb') as file:
        for chunk in iter(lambda:file.read(65536),b''):
            total+=len(chunk)
            hashed.update(chunk)
    return {'bytes':total,'sha256':hashed.hexdigest()}
with tempfile.TemporaryDirectory(prefix='rust-epg-extract-') as directory:
    root=Path(directory)
    for index,(name,data) in enumerate(blobs):
        for delete_original in [False,True]:
            for existing in [False,True]:
                base=root/f'case-{index}-{int(delete_original)}-{int(existing)}'
                base.mkdir()
                source=base/name
                source.write_bytes(data)
                output=base/'output.xml'
                if existing:output.write_bytes(b'previous-owned-output')
                result=extract_compressed_file(str(source),str(output),delete_original)
                extractions.append({'name':name,'source_base64':encoded(data),
                                    'delete_original':delete_original,'existing_output':existing,
                                    'expected':{'success':result is not None,'source_exists':source.exists(),
                                                'output':digest(output)}})
print(json.dumps({'scope':'original-XMLTV-format-and-owned-file-extraction-oracle',
                  'format_cases':formats,'extraction_cases':extractions},separators=(',',':')))
