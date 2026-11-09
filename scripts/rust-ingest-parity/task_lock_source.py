"""Original task-lock helper peer; isolated Redis fixture, never a deployment."""
import ast
import json
import logging
import os
from pathlib import Path
import time
import redis

prefix = os.environ['TASK_LOCK_FIXTURE_PREFIX']
assert prefix.startswith('rust_task_fixture_')
client = redis.Redis(host='127.0.0.1', port=6379)

class RedisClient:
    @staticmethod
    def get_client():
        return client

source = Path('/source/core/utils.py').read_text()
names = {'acquire_task_lock', 'release_task_lock', 'is_task_lock_held'}
nodes = [node for node in ast.parse(source).body
         if isinstance(node, ast.FunctionDef) and node.name in names]
scope = {'RedisClient': RedisClient, 'logger': logging.getLogger('fixture')}
exec(compile(ast.Module(body=nodes, type_ignores=[]), '<original-task-locks>', 'exec'), scope)

def wait_for(suffix):
    deadline = time.monotonic() + 120
    while not client.exists(f'{prefix}:{suffix}'):
        if time.monotonic() > deadline:
            raise TimeoutError(suffix)
        time.sleep(.02)

assert scope['acquire_task_lock'](prefix+'_source', 1)
assert client.get(f'task_lock_{prefix}_source_1') == b'locked'
assert 295 <= client.ttl(f'task_lock_{prefix}_source_1') <= 300
wait_for('ready')
assert scope['is_task_lock_held'](prefix+'_native', 2)
assert not scope['acquire_task_lock'](prefix+'_native', 2)
scope['release_task_lock'](prefix+'_source', 1)
client.set(f'{prefix}:checked', '1')
wait_for('released')
assert scope['acquire_task_lock'](prefix+'_native', 2)
scope['release_task_lock'](prefix+'_native', 2)
assert not scope['is_task_lock_held'](prefix+'_native', 2)
client.set(f'{prefix}:done', '1')
print(json.dumps({'passed': True, 'original_default_ttl': 300,
                  'source_and_native_mutual_exclusion': True,
                  'source_reacquires_after_native_release': True}))
