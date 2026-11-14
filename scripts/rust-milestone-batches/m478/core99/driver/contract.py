"""Socket-free exact Source canonicalization and finite owned DNS wire codec."""
import ast
import copy
import ipaddress
import json
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def canonical_manifest(manifest):
    tree = ast.parse((ROOT / 'Original/apps/plugins/api_views.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_verify_manifest_signature')
    target = next(n for n in ast.walk(function) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'manifest_bytes' for t in n.targets))
    expression = ast.Expression(copy.deepcopy(target.value))
    ast.fix_missing_locations(expression)
    return eval(compile(expression, '<pinned Original manifest_bytes>', 'eval'), {'json': json, 'manifest_obj': manifest, '__builtins__': {}})


def question(packet):
    """Accept only one ordinary uncompressed IN A/AAAA question; no forwarding."""
    if not isinstance(packet, bytes) or not 17 <= len(packet) <= 512:
        raise ValueError('DNS packet capacity')
    ident, flags, qd, an, ns, ar = struct.unpack('!6H', packet[:12])
    if flags & ~0x0100 or (qd, an, ns, ar) != (1, 0, 0, 0):
        raise ValueError('DNS header scope')
    labels, offset = [], 12
    while True:
        if offset >= len(packet):
            raise ValueError('DNS truncated label')
        size = packet[offset]
        offset += 1
        if size == 0:
            break
        if size > 63 or offset + size >= len(packet) or len(labels) >= 10:
            raise ValueError('DNS label scope')
        label = packet[offset:offset + size]
        if any(b not in b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-' for b in label):
            raise ValueError('DNS label bytes')
        labels.append(label.decode('ascii').lower())
        offset += size
    if not labels or offset + 4 != len(packet):
        raise ValueError('DNS question tail')
    qtype, qclass = struct.unpack('!2H', packet[offset:])
    if qclass != 1 or qtype not in (1, 28):
        raise ValueError('DNS query scope')
    name = '.'.join(labels)
    if not name.endswith('.plugin-fixture.test'):
        raise ValueError('unowned DNS name')
    return ident, flags, name, qtype, packet[12:]


def answer(packet, addresses):
    ident, flags, name, qtype, raw_question = question(packet)
    if not isinstance(addresses, list) or len(addresses) > 2:
        raise ValueError('DNS answer capacity')
    records = []
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if ip.version != (4 if qtype == 1 else 6):
            raise ValueError('DNS address family')
        packed = ip.packed
        records.append(b'\xc0\x0c' + struct.pack('!HHIH', qtype, 1, 0, len(packed)) + packed)
    return struct.pack('!6H', ident, 0x8400 | (flags & 0x0100), 1, len(records), 0, 0) + raw_question + b''.join(records)
