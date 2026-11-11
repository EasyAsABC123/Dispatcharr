"""Bounded local Docker one-shot statistics; no CLI waiting inside sampling.
Original Dispatcharr authors retain application credit. Linux working-set formula
matches Docker CLI: usage minus total_inactive_file(v1) / inactive_file(v2).
"""
import http.client
import json
import os
import socket
import time

MAX_REPLY = 1024 * 1024


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=2)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        try:
            self.sock.connect(self.path)
        except BaseException:
            self.sock.close()
            raise


def integer(value, label):
    if type(value) is not int or value < 0:
        raise ValueError('Missing/invalid Docker ' + label)
    return value


def decode(value, identifier):
    if value.get('id') != identifier:
        raise ValueError('Docker statistics identity changed')
    memory = value['memory_stats']
    usage = integer(memory.get('usage'), 'memory usage')
    stats = memory['stats']
    if 'total_inactive_file' in stats:
        inactive = integer(stats['total_inactive_file'], 'inactive file v1')
    elif 'inactive_file' in stats:
        inactive = integer(stats['inactive_file'], 'inactive file v2')
    else:
        raise ValueError('Docker memory working-set counter absent')
    # Docker CLI subtracts only when the inactive-file counter is below usage.
    working = usage - inactive if inactive < usage else usage
    cpu = integer(value['cpu_stats']['cpu_usage'].get('total_usage'), 'CPU usage')
    limit = integer(memory.get('limit'), 'memory limit')
    return dict(id=identifier, memory_bytes=working, memory_usage_bytes=usage,
                inactive_file_bytes=inactive, memory_limit_bytes=limit,
                cpu_usage_ns=cpu)


class Stats:
    def __init__(self, selected):
        address = os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock')
        if not address.startswith('unix:///') or '\x00' in address or len(address) > 256:
            raise ValueError('Local Unix Docker socket required')
        self.path = address[len('unix://'):]
        self.selected = selected
        for item in selected.values():
            identifier = item['id']
            if len(identifier) != 64 or any(c not in '0123456789abcdef' for c in identifier):
                raise ValueError('Inspected full Docker identity required')
        self.previous = {}

    def read(self, item):
        identifier = item['id']
        connection = UnixConnection(self.path)
        try:
            connection.request('GET', '/containers/' + identifier + '/stats?stream=false&one-shot=true',
                               headers={'Connection': 'close'})
            response = connection.getresponse()
            if response.status != 200:
                raise ValueError('Docker one-shot statistics rejected')
            raw = response.read(MAX_REPLY + 1)
            if len(raw) > MAX_REPLY:
                raise ValueError('Docker statistics reply exceeds1MiB')
            value = decode(json.loads(raw), identifier)
            value['sampled_monotonic'] = time.monotonic()
            if value['memory_limit_bytes'] != item['limits']['memory_bytes']:
                raise ValueError('Docker one-shot memory limit differs from inspected policy')
            return value
        finally:
            connection.close()

    def prime(self):
        # Outside the measured window: first diagnostic has an actual baseline.
        self.previous = {name: self.read(item) for name, item in self.selected.items()}

    def sample(self):
        if set(self.previous) != set(self.selected):
            raise ValueError('Docker CPU diagnostic must be primed outside timing')
        began = time.monotonic()
        rows = []
        for name, item in self.selected.items():
            current = self.read(item)
            before = self.previous[name]
            elapsed = current['sampled_monotonic'] - before['sampled_monotonic']
            change = current['cpu_usage_ns'] - before['cpu_usage_ns']
            if elapsed <= 0 or change < 0:
                raise ValueError('Docker CPU diagnostic clock/counter reset')
            current['cpu_percent'] = change / elapsed / 10000000
            self.previous[name] = current
            rows.append(current)
        return {'cpu_percent': sum(v['cpu_percent'] for v in rows),
                'memory_bytes': sum(v['memory_bytes'] for v in rows),
                'containers': rows, 'observer_seconds': time.monotonic() - began,
                'memory_metric': 'Docker one-shot working-memory sum: usage-inactive_file',
                'cpu_diagnostic_metric': 'Docker cumulative CPU nanosecond deltas/monotonic interval'}
