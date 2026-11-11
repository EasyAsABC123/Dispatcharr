"""Actual native Stats SQL/Redis/held-frame/size ownership in an owned fixture.
No Source delay parity, canonical package or performance claim.
Original Dispatcharr authors retain application and algorithm credit.
"""
import contextlib
import http.client as http_client
import io
import json
import os
from pathlib import Path
import runpy
import signal
import subprocess
import time

assert os.environ.get('VOD_API_DISPOSABLE_FIXTURE') == 'playback-fixture-only'
binary = Path(os.environ.pop('COMBINED_STATS_NATIVE'))
assert binary.is_file()
with contextlib.redirect_stdout(io.StringIO()):
    ctx = runpy.run_path(str(Path(__file__).with_name('combined_stats_source.py')))
database = ctx['database']
before = database()
redis = ctx['redis']
movie = ctx['Movie'].objects.order_by('pk').first()
profile = ctx['M3UAccountProfile'].objects.filter(m3u_account__name__startswith=ctx['PREFIX']).first()
key = 'vod_persistent_connection:combined-stats-physical-owned'
assert not redis.exists(key)
redis.hset(key, mapping=dict(content_obj_type='movie', content_uuid=str(movie.uuid),
    m3u_profile_id=str(profile.pk), created_at=str(time.time()-10),
    last_activity=str(time.time()), client_user_agent='owned-physical',
    client_ip='127.0.0.1', position_seconds='0', bytes_sent='1880'))
redis.expire(key, 180)
token = ctx['tokens']['admin']
reports = []


def record(value):
    reports.append(value)
    path=Path('/evidence/combined-stats-physical')/(value['case']+'.json')
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def http(path, method='GET', extra=None):
    client = http_client.HTTPConnection('127.0.0.1', 9194, timeout=12)
    try:
        headers = {'Authorization':'Bearer '+token, 'Host':'localhost', 'Accept':'application/json'}
        if extra:
            headers.update(extra)
        client.request(method, path, headers=headers)
        response = client.getresponse()
        raw = response.read(1024*1024+1)
        assert len(raw) <= 1024*1024
        return response.status, json.loads(raw)
    finally:
        client.close()


def wait(predicate, seconds=10):
    deadline = time.monotonic()+seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.02)
    raise TimeoutError('Owned physical predicate')


def state():
    status, value = http('/owned/stats/state')
    assert status == 200
    return value


def peer_start(name, small=False):
    directory = Path('/evidence/combined-stats-physical')/name
    directory.mkdir(parents=True)
    log = (directory/'native-private.log').open('w')
    (directory/'native-private.log').chmod(0o600)
    env = dict(os.environ, COMBINED_STATS_DISPOSABLE_FIXTURE='playback-fixture-only',
               COMBINED_STATS_PEER_DIR=str(directory), COMBINED_STATS_SMALL_OUTPUT='1' if small else '0')
    process = subprocess.Popen([str(binary),
        'combined_statistics::physical_peer::actual_combined_stats_physical_peer',
        '--ignored', '--nocapture', '--test-threads=1'], env=env,
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    wait(lambda: (directory/'ready').exists() or process.poll() is not None)
    assert process.poll() is None, 'Owned peer exited before readiness'
    return process, directory, log


def peer_stop(peer):
    process, directory, log = peer
    (directory/'stop').write_text('stop')
    try:
        assert process.wait(12) == 0, 'Physical peer failed its drain assertion'
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(5)
        log.close()


peer = None
actor = monitor = None
try:
    import psycopg
    peer = peer_start('sql')
    actor = psycopg.connect('dbname=dispatcharr user=dispatch password=playback-fixture-only host=127.0.0.1')
    monitor = psycopg.connect('dbname=dispatcharr user=dispatch password=playback-fixture-only host=127.0.0.1', autocommit=True)
    with actor.cursor() as cursor:
        cursor.execute('LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE')
    assert http('/owned/stats/start-observer')[1]['started']

    def locked():
        with monitor.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT m.name,CASE WHEN octet_length%%'")
            return cursor.fetchone()[0] == 1

    wait(locked)
    aborted = http('/owned/stats/abort-observers', 'POST')[1]
    assert aborted['aborted_observer_futures'] == 1
    retained = state()
    assert retained['active'] == 1 and retained['retained_bytes'] == 92340224
    assert http('/owned/stats/close', 'POST')[1]['drained'] is False
    assert locked(), 'Actual SQL must remain waiting after logical observer abort'
    actor.rollback()
    wait(lambda: state()['active'] == 0)
    assert state()['retained_bytes'] == 0
    record(dict(case='actual-pg-observer-abort', passed=True, retained=retained,
        locked_query_observed=True, shutdown_false_before_actual_reply=True,
        final_memory_bytes=0, copied_models_sequences_unchanged=database() == before))
    actor.close(); actor = None
    monitor.close(); monitor = None
    peer_stop(peer); peer = None

    peer = peer_start('redis')
    began = time.monotonic()
    redis.execute_command('CLIENT', 'PAUSE', 3200, 'ALL')
    assert http('/owned/stats/start-observer')[1]['started']
    time.sleep(2.15)
    assert http('/owned/stats/abort-observers', 'POST')[1]['aborted_observer_futures'] == 1
    retained = state()
    assert retained['active'] == 1 and retained['retained_bytes'] == 92340224
    assert http('/owned/stats/close', 'POST')[1]['drained'] is False
    wait(lambda: state()['active'] == 0)
    drained_at = time.monotonic()-began
    assert drained_at >= 3.1 and state()['retained_bytes'] == 0
    record(dict(case='actual-redis-observer-abort', passed=True, redis_pause_ms=3200,
        observer_abort_after_seconds=2.15, retained=retained, actual_completion_seconds=drained_at,
        shutdown_false_before_actual_reply=True, final_memory_bytes=0,
        copied_models_sequences_unchanged=database() == before))
    peer_stop(peer); peer = None

    peer = peer_start('frames')
    assert http('/owned/stats/hold')[1]['held']
    assert http('/owned/stats/hold')[1]['held']
    retained = state()
    assert retained['active'] == 2 and retained['retained_bytes'] == 184680448 and retained['held_frames'] == 2
    assert http('/proxy/stats/')[0] == 503
    assert http('/owned/stats/close', 'POST')[1]['drained'] is False
    assert http('/owned/stats/release', 'POST')[1]['released']
    wait(lambda: state()['active'] == 0)
    assert state()['retained_bytes'] == 0
    record(dict(case='actual-http-projection-held-frames', passed=True, retained=retained,
        third_request_status=503, shutdown_false_while_frames_held=True, final_memory_bytes=0,
        scope='Actual service Response bodies held before polling; no TCP flush acknowledgement claim'))
    peer_stop(peer); peer = None

    peer = peer_start('bounds', small=True)
    redis.hset(key, 'client_user_agent', 'x'*70000)
    assert http('/proxy/stats/')[0] == 503
    wait(lambda: state()['active'] == 0)
    redis.hset(key, 'client_user_agent', 'owned-physical')
    assert http('/proxy/stats/', extra={'X-Owned-Large': 'x'*70000})[0] == 503
    assert state()['active'] == 0 and state()['retained_bytes'] == 0
    record(dict(case='actual-input-output-limits', passed=True, input_limit_bytes=65536,
        controlled_output_limit_bytes=65536, oversized_payload_bytes=70000,
        output_status=503, input_status=503, final_memory_bytes=0,
        scope='Explicit native resource extensions; no Source rejection parity claim'))
    peer_stop(peer); peer = None
    unchanged = database() == before
    assert unchanged
    result = dict(passed=True, scope=__doc__, records=reports, native_physical_cases=len(reports),
        restored18=True, selected_models=19, sequences_restored=True,
        models_sequences_unchanged=unchanged, Source_HTTP_parity=False, performance_claim=False)
finally:
    if actor is not None:
        actor.rollback(); actor.close()
    if monitor is not None:
        monitor.close()
    if peer is not None:
        peer_stop(peer)
    redis.delete(key)
    assert not redis.exists(key)
    assert database() == before
print(json.dumps(result))
