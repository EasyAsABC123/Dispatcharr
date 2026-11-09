"""Capture actual original lxml iterparse recovery and XMLTV element trees.

Original Dispatcharr authors retain full application and algorithm credit.
"""
import argparse
import ast
import base64
import hashlib
import html.entities
import json
import os
import random
from pathlib import Path
import tempfile
from lxml import etree


class Fragmented:
    def __init__(self, source, chunk):
        self.source, self.chunk = source, chunk

    def read(self, size):
        return self.source.read(min(size, self.chunk))


def tree(node):
    return {'tag': node.tag, 'text': node.text, 'attrs': dict(node.attrib),
            'children': [tree(child) for child in node if isinstance(child.tag, str)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    original = Path('/source/apps/epg/tasks.py').read_bytes()
    names = {'_PrependStream', '_open_xmltv_file', '_build_html_entity_doctype', 'clear_element'}
    nodes = [node for node in ast.parse(original).body
             if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    scope = {'html': html, 'os': os, '_XML_ENTITIES': {'amp', 'lt', 'gt', 'quot', 'apos'}}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<original XMLTV parser>', 'exec'), scope)
    scope['_HTML_ENTITY_DOCTYPE'] = scope['_build_html_entity_doctype']()
    bodies = [b'', b'<tv/>', b'<tv></tv>', b'garbage', b'<tv><channel id="one"><display-name>News</display-name></channel></tv>',
              b'<tv><programme channel="one"><title> A &eacute; &amp; B </title></programme></tv>',
              b'<tv><programme><title>before<b>bold</b>after</title></programme></tv>',
              b'<tv><programme><title>before<!--comment-->after</title></programme></tv>',
              b'<tv><programme><title><![CDATA[a]]><![CDATA[b]]>c</title></programme></tv>',
              b'<tv><programme><title>  </title><desc> a\n b </desc></programme></tv>',
              b'<tv><programme><title>unknown &unknown; tail</title></programme></tv>',
              b'<tv><programme><title>bad & title</title></programme></tv>',
              b'<tv><programme><title>x</programme><programme><title>y</title></programme></tv>',
              b'<tv><programme><title>unclosed',
              b'<tv><programme><title>nul\x00x\x1cy</title></programme></tv>',
              b'<tv><programme><title>bad utf8\xfftail</title></programme></tv>',
              b'<?xml version="1.0" encoding="ISO-8859-1"?><tv><programme><title>caf\xe9</title></programme></tv>',
              b'<tv xmlns="urn:test"><programme><title>hidden</title></programme></tv>',
              b'<tv xmlns:a="urn:a"><programme a:key="value"><a:title>x</a:title><title a:lang="y">z</title></programme></tv>',
              b'<tv><programme/><channel/><programme><programme><title>inner</title></programme><title>outer</title></programme></tv>',
              b'<!DOCTYPE tv [<!ENTITY custom "world">]><tv><programme><title>Hello &custom;!</title></programme></tv>',
              b'<!DOCTYPE tv [<!ENTITY custom "<b>nested</b>">]><tv><programme><title>Hello &custom;!</title></programme></tv>',
              b'<!DOCTYPE tv [<!ENTITY ext SYSTEM "file:///etc/hostname">]><tv><programme><title>&ext;</title></programme></tv>',
              b'<!DOCTYPE tv SYSTEM "http://127.0.0.1:1/never"><tv><programme><title>local</title></programme></tv>',
              b'<?xml version="1.0"?><tv><!--rootcomment--><other>ignored</other><programme><title>&#x1F600;</title></programme></tv>',
              b'<tv><programme title="one" title="two"><title>x</title></programme></tv>',
              b'<tv><programme><title>one</title></programme></tv><tv><programme><title>extra</title></programme></tv>']
    for depth in [1, 10, 100, 255, 257]:
        bodies.append(b'<tv><programme>'+b'<x>'*depth+b'leaf'+b'</x>'*depth+b'</programme></tv>')
    for count in [1, 2, 20, 1000]:
        bodies.append(b'<tv>'+b'<programme channel="one"><title>News</title></programme>'*count+b'</tv>')
    fixed_count = len(bodies)
    rng = random.Random(20261113)
    for _ in range(500):
        parts = [b'<tv>']
        for _ in range(rng.randrange(1, 8)):
            parts.append(rng.choice([b'<programme channel="x">', b'<channel id="x">',
                                     b'<other><programme>', b'<programme a="1" a="2">']))
            for _ in range(rng.randrange(1, 7)):
                parts.append(rng.choice([b'<title>a</title>', b'<title>&eacute;&amp;</title>',
                                         b'<title><![CDATA[a]]>b</title>',
                                         b'<title>a<!--comment-->b</title>',
                                         b'<title>a<b>b</b>c</title>', b'<title> &bogus; </title>',
                                         b'<x/>', b'<!--root-->', b'<title>\xff</title>',
                                         b'<title>&#x0;&#x1C;&#x1F600;</title>']))
            parts.append(rng.choice([b'</programme>', b'</channel>', b'</programme></other>', b'']))
        parts.append(rng.choice([b'</tv>', b'', b'</tv>junk', b'<programme><title>unfinished']))
        bodies.append(b''.join(parts))
    cases = []
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp)/'guide.xml'
        for index, body in enumerate(bodies):
            for chunk in ([1, 7, 4096, 32768] if index < fixed_count else [1, 4096]):
                path.write_bytes(body)
                events, error = [], None
                try:
                    with scope['_open_xmltv_file'](str(path)) as opened:
                        iterator = etree.iterparse(Fragmented(opened, chunk), events=('end',),
                                                   tag=('channel', 'programme'), remove_blank_text=True, recover=True)
                        for _, node in iterator:
                            events.append(tree(node))
                            scope['clear_element'](node)
                except Exception as exception:
                    error = type(exception).__name__
                cases.append({'input_base64': base64.b64encode(body).decode(), 'chunk': chunk,
                              'expected': {'events': events, 'error': error}})
    report = {'scope': 'original-streaming-XMLTV-iterparse-component', 'cases': cases,
              'source_sha256': hashlib.sha256(original).hexdigest(),
              'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'lxml_version': etree.LXML_VERSION, 'libxml_version': etree.LIBXML_VERSION,
              'full_refresh_job_verified': False, 'application_performance_verified': False}
    args.output.write_text(json.dumps(report, ensure_ascii=True, indent=2)+'\n')
    print(json.dumps({'captured': len(cases)}))


if __name__ == '__main__':
    main()
