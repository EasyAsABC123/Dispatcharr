"""Capture original XMLTV metadata projection using pinned lxml and Python.

All application behavior and algorithm credit belongs to Dispatcharr's authors.
Structured element inputs isolate projection from parser recovery semantics.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import random
import re
import sys
from lxml import etree


def element(value):
    node = etree.Element(value['tag'], value.get('attrs', {}))
    node.text = value.get('text')
    for child in value.get('children', []):
        node.append(element(child))
    return node


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    scope = {'re': re}
    files = {'apps/epg/tasks.py': {'extract_custom_properties'},
             'apps/epg/utils.py': {'fill_original_air_date_if_missing'}}
    hashes = {}
    for name, names in files.items():
        path = Path('/source')/name
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        tree = ast.parse(path.read_bytes())
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        if name.endswith('utils.py'):
            nodes += [node for node in tree.body if isinstance(node, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == '_ONSCREEN_RE' for t in node.targets)]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), name, 'exec'), scope)
    def child(tag, text=None, **attrs):
        return {'tag': tag, 'text': text, 'attrs': attrs}
    inputs = [{'tag': 'programme'}]
    systems = ['', 'onscreen', 'xmltv_ns', 'dd_progid', 'original-air-date',
               'thetvdb.com', 'themoviedb.org', 'imdb.com', 'unknown']
    texts = [None, '', ' ', '\u2028  \u0085', 'S01E05', 's12 E6 P2/2', 'ſ٢\u2028E٣',
             'Ｓ1Ｅ2', '0.118.', '-2.+4.', '١.２.', '0/2.1/4.', ' . . ',
             '1_0.2_0.', '9223372036854775808.999999999999999999999.',
             '2026-06-24 00:00:00', '  Café 東京  ']
    for system in systems:
        for text in texts:
            for before in [[], [child('episode-num', '8.7.', system='xmltv_ns')]]:
                inputs.append({'tag': 'programme', 'children': before+[child('episode-num', text, system=system)]})
    rng = random.Random(20261112)
    flat = ['category', 'keyword', 'date', 'country', 'language', 'orig-language',
            'length', 'review', 'image', 'premiere', 'last-chance', 'previously-shown',
            'new', 'live', 'icon', 'episode-num']
    nested = {'rating': ['value'], 'star-rating': ['value'],
              'credits': ['director', 'actor', 'writer', 'adapter', 'producer', 'composer',
                          'editor', 'presenter', 'commentator', 'guest'],
              'video': ['present', 'colour', 'aspect', 'quality'],
              'audio': ['present', 'stereo'], 'subtitles': ['language']}
    attrs = ['system', 'role', 'guest', 'units', 'type', 'source', 'reviewer',
             'size', 'orient', 'src', 'start', 'channel']
    for _ in range(900):
        children = []
        for _ in range(rng.randrange(1, 35)):
            tag = rng.choice(flat+list(nested))
            value = child(tag, rng.choice(texts))
            value['attrs'] = {key: rng.choice(['', 'yes', 'no', '  role  ', 'onscreen', 'xmltv_ns'])
                              for key in rng.sample(attrs, rng.randrange(5))}
            if tag in nested:
                value['children'] = [child(rng.choice(nested[tag]), rng.choice(texts),
                                           role=rng.choice(['', 'test']), guest=rng.choice(['yes', 'no']))
                                     for _ in range(rng.randrange(6))]
            children.append(value)
        inputs.append({'tag': 'programme', 'children': children})
    # First-child semantics, namespaces and mixed-content .text are retained.
    inputs.append({'tag': 'programme', 'children': [child('{urn:test}category', 'hidden'),
                   {'tag': 'category', 'text': 'leading', 'children': [child('b', 'ignored')]},
                   child('rating'), {'tag': 'rating', 'children': [child('value', 'ignored')]}]})
    cases = []
    for value in inputs:
        try:
            expected = {'ok': True, 'value': scope['extract_custom_properties'](element(value))}
        except Exception as error:
            expected = {'ok': False, 'error': type(error).__name__}
        cases.append({'input': value, 'expected': expected})
    report = {'scope': 'original-XMLTV-element-metadata-projection', 'cases': cases,
              'python': sys.version, 'lxml_version': etree.LXML_VERSION,
              'libxml_version': etree.LIBXML_VERSION, 'source_sha256': hashes,
              'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'full_xml_parser_verified': False, 'application_performance_verified': False}
    args.output.write_text(json.dumps(report, ensure_ascii=True, indent=2)+'\n')
    print(json.dumps({'captured': len(cases)}))


if __name__ == '__main__':
    main()
