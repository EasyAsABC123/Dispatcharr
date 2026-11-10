#!/usr/bin/env python3
"""Capture the actual original XC integer-assignment statements.

Full application credit belongs to the original Dispatcharr authors and
contributors. This captures a pure helper block; it does not verify native HTTP.
"""
import argparse
import ast
import hashlib
import json
import math
import random
from pathlib import Path
from types import SimpleNamespace


def assignment(statement, name):
    return isinstance(statement, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == name
        for target in statement.targets
    )


def original_block(path):
    raw = path.read_bytes()
    module = ast.parse(raw, filename=str(path))
    function = next(
        node for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == '_xc_live_streams_setup'
    )
    begin = next(index for index, node in enumerate(function.body) if assignment(node, 'channel_num_map'))
    end = next(index for index, node in enumerate(function.body) if assignment(node, '_base_url'))
    statements = function.body[begin:end]
    # Fail visibly if a future Source refactor changes the extracted boundary.
    assert len(statements) == 5
    assert assignment(statements[1], 'used_numbers')
    assert assignment(statements[2], 'float_channels')
    assert all(isinstance(node, ast.For) for node in statements[3:])
    block = ast.Module(body=statements, type_ignores=[])
    return compile(block, str(path), 'exec'), {
        'path': 'apps/output/views.py',
        'file_sha256': hashlib.sha256(raw).hexdigest(),
        'block_ast_sha256': hashlib.sha256(ast.dump(block, include_attributes=False).encode()).hexdigest(),
        'first_line': statements[0].lineno,
        'last_line': statements[-1].end_lineno,
    }


def vectors():
    yield []
    for values in [
        [None] * 128,
        [1.0] * 64 + [1.5] * 64,
        [-3.5, -3.0, -2.0, -1.0, -0.0, None, 0.5, 1.0],
        [1e20, 1e20, 1e308, -1e308, 1e-308, -1e-308, None],
        [float('inf')], [float('-inf')], [float('nan')],
    ]:
        yield values
    rng = random.Random(1241)
    choices = [None, -0.0, 0.0, 1.0, -1.0, 1e20, -1e20, 1e308, -1e308]
    choices.extend(number / 4 for number in range(-64, 65))
    for _ in range(384):
        yield [rng.choice(choices) for _ in range(rng.randrange(1, 129))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='Original source checkout root')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    block, identity = original_block(args.source / 'apps/output/views.py')
    cases = []
    for index, values in enumerate(vectors()):
        namespace = {'channels': [SimpleNamespace(id=at + 1, effective_channel_number=value) for at, value in enumerate(values)]}
        case = {
            'case': index,
            # JSON has no non-finite number literals; retain explicit tags.
            'numbers': [value if value is None or math.isfinite(value) else {'nonfinite': repr(value)} for value in values],
        }
        try:
            exec(block, namespace)
        except (ValueError, OverflowError) as error:
            case['error'] = type(error).__name__
        else:
            case['assigned'] = {str(key): str(value) for key, value in namespace['channel_num_map'].items()}
        cases.append(case)
    report = {'scope': 'Actual original XC assignment block only', 'source': identity, 'cases': cases, 'native_verified': False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as file:
        json.dump(report, file, indent=2, allow_nan=False)
        file.write('\n')


if __name__ == '__main__':
    main()
