"""Capture unchanged original XMLTV preparation helpers in its pinned runtime.

All application/algorithm credit remains with original Dispatcharr authors.
This is a component oracle, not a complete guide-refresh acceptance report.
"""
import argparse
import ast
import base64
import datetime as dt
import hashlib
import html.entities
import json
import os
from pathlib import Path
import random
import _strptime
import tempfile
import unicodedata
from django.utils import timezone
from lxml import etree


class Quiet:
    def __getattr__(self, _):
        return lambda *args, **kwargs: None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path('/source')
    original = (root/'apps/epg/tasks.py').read_bytes()
    names = {'parse_xmltv_time', '_xmltv_file_is_complete', '_PrependStream',
             '_open_xmltv_file', '_build_html_entity_doctype'}
    tree = ast.parse(original)
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    scope = {'datetime': dt.datetime, 'timedelta': dt.timedelta, 'dt_timezone': dt.timezone,
             'timezone': timezone, 'logger': Quiet(), 'os': os, 'html': html,
             '_XML_ENTITIES': {'amp', 'lt', 'gt', 'quot', 'apos'},
             '_XMLTV_COMPLETENESS_TAIL_BYTES': 8192}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<original-XMLTV-helpers>', 'exec'), scope)
    scope['_HTML_ENTITY_DOCTYPE'] = scope['_build_html_entity_doctype']()
    values = ['', '2026', '20261109', '20261109000000', '20261109000000 +0000',
              '00010101000000 +0001', '99991231235959 -0001', '00000101000000',
              '20260229000000', '20240229000000', '20261109000060',
              '20261109000061', '20261109000099', '20261109000000 garbage',
              '٢٠٢٦1109000000 +0000', '2026١١09000000 +0000',
              '2026110١000000 +0000', '202611090٠0000 +0000']
    base = '20261109000000'
    for sign in ['+', '-', 'X', ' ', '\n']:
        for hours in ['00', '01', '23', '24', '-1', '+1', ' 1', '1 ', '٠١', '０１', '\x1c1', '\u00851']:
            for minutes in ['00', '59', '60', '99', '-1', '+1', ' 1', '١٢']:
                values.append(base+' '+sign+hours+minutes)
    for count in range(9, 26):
        values.extend([base[:count], (base+' +0100ignore')[:count], '1'*count])
    rng = random.Random(20261111)
    for _ in range(1000):
        year = rng.choice([1, 99, 999, 2000, 2024, 2026, 9999])
        month, day = rng.randrange(1, 13), rng.randrange(1, 32)
        hour, minute, second = rng.randrange(24), rng.randrange(60), rng.randrange(63)
        padded = f'{year:04}{month:02}{day:02}{hour:02}{minute:02}{second:02}'
        short = f'{year:04}{month}{day}{hour}{minute}{second}'
        values.append(rng.choice([padded, short])+rng.choice(['', ' +0530', ' -0099', ' X0100', ' +2360', ' +0000junk']))
    times = []
    for value in values:
        try:
            result = {'ok': True, 'iso': scope['parse_xmltv_time'](value).isoformat()}
        except Exception as error:
            result = {'ok': False, 'error': type(error).__name__}
        times.append({'input': value, 'expected': result})
    # Pin numeric Unicode semantics rather than inheriting a Rust table update.
    digits = [cp for cp in range(0x110000) if chr(cp).isdecimal()]
    zeroes = [cp for cp in digits if unicodedata.decimal(chr(cp)) == 0]
    spaces = []
    for cp in range(0x110000):
        if not chr(cp).isspace():
            continue
        try:
            if int(chr(cp)+'1') == 1:
                spaces.append(cp)
        except ValueError:
            pass
    files = []
    bodies = [b'', b'<tv></tv>', b'not XML</tv>', b'<tv>', b'<tv></TV>',
              b'<tv></tv>\x1c', b'<tv></tv>\x85', b'<tv></tv>\t\n\v\f\r ',
              b'<tv></tv><!--tail-->', b'x'*8193+b'</tv>',
              b'</tv>'+b' '*8192, b'</tv>'+b' '*8191]
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp)/'guide.xml'
        missing = scope['_xmltv_file_is_complete'](str(path))
        for body in bodies:
            path.write_bytes(body)
            files.append({'input_base64': base64.b64encode(body).decode(),
                          'expected': scope['_xmltv_file_is_complete'](str(path))})
        prefixes = []
        for body in [b'<tv></tv>', b'<?xml version="1.0"?><tv></tv>',
                     b'\xef\xbb\xbf<?xml version="1.0"?>\n<tv></tv>',
                     b'<!DOCTYPE tv []><tv></tv>', b'<!dOcTyPe tv []><tv></tv>',
                     b' '*508+b'<!DOCTYPE tv []><tv></tv>',
                     b'<!-- <?xml ?> --><tv></tv>', b'x'*510+b'<?xml?><tv></tv>']:
            path.write_bytes(body)
            for chunk in [1, 7, 4096, -1]:
                with scope['_open_xmltv_file'](str(path)) as opened:
                    parts = []
                    while True:
                        part = opened.read(chunk)
                        if not part:
                            break
                        parts.append(part)
                prefixes.append({'input_base64': base64.b64encode(body).decode(),
                                 'chunk': chunk,
                                 'expected_sha256': hashlib.sha256(b''.join(parts)).hexdigest()})
    report = {'scope': 'original-XMLTV-time-completeness-and-prefix-components',
              'source_sha256': hashlib.sha256(original).hexdigest(),
              'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'python': os.sys.version, 'unicode_version': unicodedata.unidata_version,
              'lxml_version': etree.LXML_VERSION, 'libxml_version': etree.LIBXML_VERSION,
              'time_regex': _strptime.TimeRE().pattern('%Y%m%d%H%M%S'),
              'decimal_zeroes': zeroes, 'integer_whitespace': spaces,
              'html_entity_doctype_base64': base64.b64encode(scope['_HTML_ENTITY_DOCTYPE']).decode(),
              'times': times, 'completeness': files, 'missing_file_complete': missing,
              'prefixes': prefixes, 'full_guide_jobs_migrated': False}
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'times': len(times), 'completeness': len(files), 'prefixes': len(prefixes),
                      'unicode': report['unicode_version'], 'lxml': report['lxml_version'],
                      'libxml': report['libxml_version']}))


if __name__ == '__main__':
    main()
