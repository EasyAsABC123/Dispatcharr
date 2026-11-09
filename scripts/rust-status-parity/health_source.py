"""Original StreamManager one-tick health oracle; no provider or DB traffic.

Original Dispatcharr authors retain all application and algorithm credit.
This isolates state rules; it does not accept supervisor/failover timing.
"""
import argparse
import ast
import hashlib
import itertools
import json
from pathlib import Path
import types


class EndTick(Exception):
    pass


class Config:
    @staticmethod
    def stream_startup_timeout(): return 8
    @staticmethod
    def connection_timeout(): return 10
    @staticmethod
    def channel_init_grace_period(): return 60


class Quiet:
    def __getattr__(self, _): return lambda *args, **kwargs: None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    original = Path('/source/apps/proxy/live_proxy/input/manager.py').read_bytes()
    node = next(node for node in ast.parse(original).body
                if isinstance(node, ast.ClassDef) and node.name == 'StreamManager')
    names = {'_connection_is_awaiting_data', '_startup_switch_enabled',
             '_arm_startup_no_data_clock', '_request_startup_stream_switch',
             '_health_inactivity_threshold', '_health_poll_interval', '_monitor_health'}
    methods = [item for item in node.body if isinstance(item, ast.FunctionDef) and item.name in names]
    cls = ast.ClassDef(name='OriginalHealth', bases=[], keywords=[], body=methods, decorator_list=[])
    clock = types.SimpleNamespace(now=100)
    sleeper = types.SimpleNamespace(delay=None)
    def sleep(delay):
        sleeper.delay = delay
        raise EndTick()
    scope = {'ConfigHelper': Config, 'time': types.SimpleNamespace(time=lambda: clock.now),
             'gevent': types.SimpleNamespace(sleep=sleep), 'logger': Quiet()}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])),
                 '<original-health-methods>', 'exec'), scope)
    cases = []
    for flags in itertools.product([False, True], repeat=7):
        healthy, connected, awaiting, exhausted, needs_switch, reconnect, switching = flags
        for index, inactive in itertools.product([0, 4], [0, 7.99, 8, 8.01, 9.99, 10, 10.01, 59.99, 60, 60.01]):
            manager = scope['OriginalHealth']()
            values = dict(healthy=healthy, connected=connected, last_data_time=100,
                          _connection_awaiting_data=awaiting, _startup_failover_exhausted=exhausted,
                          needs_stream_switch=needs_switch, needs_reconnect=reconnect,
                          url_switching=switching, running=True, last_health_action_time=0,
                          health_check_interval=5, connection_start_time=50,
                          channel_id='local-health-fixture', buffer=types.SimpleNamespace(index=index))
            for name, value in values.items(): setattr(manager, name, value)
            clock.now = 100+inactive
            sleeper.delay = None
            try:
                manager._monitor_health()
            except EndTick:
                pass
            cases.append({'state': {'healthy': healthy, 'connected': connected,
                'last_data_time': 100, 'awaiting': awaiting, 'exhausted': exhausted,
                'needs_switch': needs_switch, 'needs_reconnect': reconnect, 'switching': switching},
                'now': clock.now, 'index': index,
                'expected': {'healthy': manager.healthy, 'needs_switch': manager.needs_stream_switch,
                             'needs_reconnect': manager.needs_reconnect, 'poll_delay': sleeper.delay}})
    args.output.write_text(json.dumps({'scope': 'original-manager-one-tick-health-state',
        'source_sha256': hashlib.sha256(original).hexdigest(),
        'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'cases': cases, 'full_supervisor_timing_verified': False}, separators=(',', ':'))+'\n')
    print(json.dumps({'cases': len(cases)}))


if __name__ == '__main__':
    main()
