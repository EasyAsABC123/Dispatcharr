"""Execute the exact custody scripts in Lua and evaluate fixture inputs offline."""
import json
from pathlib import Path
import subprocess
import sys
import time

def load_case(prepared):
    sys.path.insert(0, str(prepared/'support/cohort'))
    import hls_case
    return hls_case


def custody_controls(case):
    lua = r"""
local key = 'owned:profile'
local owner, playlist, expiry, writes
redis = {}
redis.sha1hex = function(value) return 'sha1-'..value end
redis.error_reply = function(message) error(message) end
redis.call = function(operation, key_arg, value, option)
    if operation == 'GET' and key_arg == key..':owner' then return owner end
    assert(key_arg == key..':playlist')
    if operation == 'GET' then return playlist end
    if operation == 'PEXPIRETIME' then return expiry end
    assert(operation == 'SET' and option == 'KEEPTTL')
    playlist, writes = value, writes+1
    return 'OK'
end
local function arm(original_expiry)
    owner, playlist, expiry, writes = 'worker-a', ' {original bytes} ', original_expiry, 0
end
local function cas(before, after, expected_expiry, expected_owner)
    ARGV = {key, before, after, tostring(expected_expiry), 'sha1-'..expected_owner}
    return (function()
__CAS__
    end)()
end
local function snapshot()
    ARGV = {key}
    return (function()
__SNAPSHOT__
    end)()
end
local original, replacement = ' {original bytes} ', '{"variant":true}'
for _, expiration in ipairs({1234567890123, -1}) do
    arm(expiration)
    local value = snapshot()
    assert(value[1] == original and value[2] == expiration and value[3] == 'sha1-worker-a')
    assert(cas(original, original, expiration, 'worker-a') == 'already-exact' and writes == 0)
    assert(cas(original, replacement, expiration, 'worker-a') == 'changed-exact')
    assert(playlist == replacement and expiry == expiration and writes == 1)
    assert(cas(replacement, original, expiration, 'worker-a') == 'changed-exact')
    assert(playlist == original and expiry == expiration and writes == 2)
    assert(cas(replacement, original, expiration, 'worker-a') == 'already-exact' and writes == 2)
end
arm(800)
owner = 'worker-b'
assert(cas(replacement, original, 800, 'worker-a') == 'owner-changed' and writes == 0)
arm(800)
playlist = 'new publisher descriptor'
assert(cas(replacement, original, 800, 'worker-a') == 'bytes-changed' and writes == 0)
arm(900)
playlist = replacement
assert(cas(replacement, original, 800, 'worker-a') == 'expiry-changed' and writes == 0)
owner = nil
assert(#snapshot() == 0)
print('Lua custody controls passed')
"""
    script = lua.replace('__CAS__', case.CAS).replace('__SNAPSHOT__', case.SNAPSHOT)
    subprocess.run(['lua', '-'], input=script, text=True, check=True, timeout=10)


def source_fixture_inputs(case, prepared):
    render, stale = case.oracle(prepared/'original.tar')
    now = time.time()
    raw = json.dumps({'window': [{'seq': 7, 'dur': 3.25}], 'target': 9,
                      'adv_target': 6, 'ts': now, 'disc_seq': 0})
    expected = []
    for label in case.CASES:
        descriptor = case.variant(raw, label, now)
        try:
            body = render(descriptor)
        except stale:
            assert label == 'aged'
            expected.append(404)
        else:
            assert case.media_sequences(body.encode()) == [7]
            target = 4 if label == 'legacy-no-advertised-target' else 6
            assert f'#EXT-X-TARGETDURATION:{target}\n' in body
            expected.append(200)
    assert expected == [200, 200, 200, 404]
    print('Four pinned Original fixture variants passed')


if __name__ == '__main__':
    prepared = Path(sys.argv[1]).resolve()
    case = load_case(prepared)
    custody_controls(case)
    source_fixture_inputs(case, prepared)
