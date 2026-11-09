"""Capture original programme-row projection from its actual streaming loop.

The unchanged original Dispatcharr authors retain all application/algorithm credit.
No SQL executes: ProgramData records capture the original constructor arguments.
"""
import argparse
import ast
from collections import defaultdict
import copy
import datetime as dt
import gc
import hashlib
import json
from pathlib import Path
import random
import re
import sys
from django.utils import timezone
from metadata_source import element


class Quiet:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class Record:
    def __init__(self, **kwargs):
        self.value = kwargs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metadata', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    files = {name: Path('/source', name).read_bytes()
             for name in ['apps/epg/tasks.py', 'apps/epg/utils.py']}
    scope = {'re': re, 'datetime': dt.datetime, 'timedelta': dt.timedelta,
             'dt_timezone': dt.timezone, 'timezone': timezone, 'logger': Quiet(), 'gc': gc,
             'ProgramData': Record, 'send_epg_update': lambda *args, **kwargs: None}
    helpers = {'parse_xmltv_time', 'extract_custom_properties', 'clear_element',
               'fill_original_air_date_if_missing', 'extract_season_episode_from_description'}
    tasks = None
    for name, data in files.items():
        tree = ast.parse(data)
        if name.endswith('tasks.py'):
            tasks = tree
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in helpers]
        nodes += [node for node in tree.body if isinstance(node, ast.Assign)
                  and any(isinstance(target, ast.Name) and target.id in {'_ONSCREEN_RE', '_DESC_SE_PATTERNS'}
                          for target in node.targets)]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), name, 'exec'), scope)
    function = next(node for node in tasks.body if isinstance(node, ast.FunctionDef) and node.name == 'parse_programs_for_source')
    loop = next(node for node in ast.walk(function)
                if isinstance(node, ast.For) and isinstance(node.iter, ast.Name) and node.iter.id == 'program_parser')
    compiled = compile(ast.Module(body=[loop], type_ignores=[]), '<original streaming programme row loop>', 'exec')
    metadata = json.loads(args.metadata.read_bytes())
    base = {'channel': 'one', 'start': '20261109000000 +0000', 'stop': '20261109010000 +0000'}
    inputs = []
    for case in metadata['cases']:
        value = copy.deepcopy(case['input'])
        value['attrs'] = dict(base)
        inputs.append(value)
    descriptions = [None, '', 'Plain description', 'S01E02 Title', ' - : s1 e2 ... News ',
                    'Season1Episode2 subtitle', 'season١ episode２\u2028suffix',
                    'ſeaſon 4 Epİſode 5: test', '1x01 Fine', '1X01 unchanged', '1x1 unchanged',
                    'S١ E٢ P2/2 remainder', 'S01E2-P2/2 start', 'S'+'9'*4301+'E01 invalid int',
                    '\u0085S02\u2028E03\u2029more']
    rng = random.Random(20261114)
    for _ in range(1200):
        children = []
        for _ in range(rng.randrange(4)):
            children.append({'tag': 'title', 'text': rng.choice([None, '', ' ', 'News', '東京😀'*200])})
        for _ in range(rng.randrange(3)):
            children.append({'tag': 'desc', 'text': rng.choice(descriptions)})
        for _ in range(rng.randrange(3)):
            children.append({'tag': 'sub-title', 'text': rng.choice([None, '', ' ', 'Subtitle'])})
        for _ in range(rng.randrange(3)):
            children.append({'tag': 'episode-num', 'text': rng.choice(['0.1.', '.2.', '3..', 'S4E5', '  ']),
                             'attrs': {'system': rng.choice(['xmltv_ns', 'onscreen', ''])}})
        attrs = dict(base)
        channel = rng.choice(['one', 'ch', '', ' one', 'one ', 'unknown', None])
        if channel is None:
            attrs.pop('channel')
        else:
            attrs['channel'] = channel
        inputs.append({'tag': 'programme', 'attrs': attrs, 'children': children})
    for key in ['start', 'stop']:
        for value in [None, '', 'bad', '2026', '20261109', '20261109000000',
                      '20261109000000 +0530', '00010101000000 +0001', '99991231235959 -0001',
                      '٢٠٢٦1109000000 +0000', '20261108000000 +0000', '20261109000000 X0100']:
            attrs = dict(base)
            if value is None:
                attrs.pop(key)
            else:
                attrs[key] = value
            inputs.append({'tag': 'programme', 'attrs': attrs})
    cases = []
    for value in inputs:
        local = dict(scope, program_parser=[(None, element(value))], mapped_tvg_ids={'one', 'ch', ''},
                     tvg_id_to_epg_id={'one': 11, 'ch': 12, '': 13}, title_max_length=255,
                     skipped_programs=0, total_programs=0, programs_batch=[], programs_accumulator=[],
                     programs_by_channel=defaultdict(int), parse_batch_size=2500, use_staging=False,
                     last_progress_update=0)
        exec(compiled, local)
        rows = [{key: field.isoformat() if isinstance(field, dt.datetime) else field
                 for key, field in row.value.items()} for row in local['programs_batch']]
        cases.append({'input': value, 'expected': {'rows': rows, 'skipped': local['skipped_programs']}})
    report = {'scope': 'original-streaming-XMLTV-programme-row-projection', 'cases': cases,
              'source_sha256': {name: hashlib.sha256(data).hexdigest() for name, data in files.items()},
              'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'metadata_goldens_sha256': hashlib.sha256(args.metadata.read_bytes()).hexdigest(),
              'python': sys.version, 'full_refresh_job_verified': False,
              'application_performance_verified': False}
    args.output.write_text(json.dumps(report, ensure_ascii=True, indent=2)+'\n')
    print(json.dumps({'captured': len(cases)}))


if __name__ == '__main__':
    main()
