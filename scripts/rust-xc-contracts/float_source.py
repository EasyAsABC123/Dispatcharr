#!/usr/bin/env python3
"""Capture pinned original Python float grammar used by XC Redis timestamps.

Original Dispatcharr authors retain application credit. This runs inside the
pinned Source image and records builtin conversions, not a replacement parser.
"""
import hashlib
import json
import math
import platform
import struct
import unicodedata


def main():
    values = ['', ' ', '.', '+', '-', '0', '-0', '+0', '.1', '-.1', '1.', '1.e2', '.1e-2', '00.001', '1e9999', '-1e9999', '1e-9999', '-1e-9999', '1_000.5', '1__0', '1_.0', '1._0', '1e_2', '1_e2', '_1', '1_', '0x1p2', '1\x00', '1 2', '1,2', '1/2', '−1', '＋1', '١٠٠.٥', 'NaN', '+NaN', '-NaN', 'inf', '-INF', '+Infinity', 'INFINITY', 'nan(0)', '∞', 'ＮａＮ']
    for space in ['\t','\n','\v','\f','\r',' ','\x1c','\x1d','\x1e','\x1f','\x85','\xa0','\u1680',*[chr(c) for c in range(0x2000,0x200c)],'\u2028','\u2029','\u202f','\u205f','\u3000','\u200b','\ufeff']:
        values.extend([space+'1.5'+space, '1'+space+'.5'])
    for code in range(0x110000):
        c = chr(code)
        if unicodedata.category(c) == 'Nd':
            values.extend([c, c+'.5', c+'_1', '1e'+c])
    values.extend(['9' * 4301, '0.' + '0' * 5000 + '1'])
    cases = []
    for value in dict.fromkeys(values):
        row = {'input': value}
        try:
            parsed = float(value)
        except (ValueError, OverflowError) as error:
            row['error'] = type(error).__name__
        else:
            row['class'] = 'nan' if math.isnan(parsed) else 'inf' if math.isinf(parsed) else 'finite'
            row['negative'] = math.copysign(1, parsed) < 0
            if math.isfinite(parsed):
                row['bits'] = struct.pack('>d', parsed).hex()
        cases.append(row)
    print(json.dumps({'scope':'Pinned original Python float conversion used by get_user_active_connections', 'python':platform.python_version(), 'unicode':unicodedata.unidata_version, 'tool_sha256':hashlib.sha256(open(__file__,'rb').read()).hexdigest(), 'cases':cases, 'native_verified':False}, ensure_ascii=True,allow_nan=False))


if __name__ == '__main__':
    main()
