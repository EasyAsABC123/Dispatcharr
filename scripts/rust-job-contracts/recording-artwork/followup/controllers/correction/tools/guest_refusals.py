"""Unexecuted real guest helper; host integration/admission remain open.

No imports of Django or processes at module import. Dependencies are the actual
guest's models, owned connection, snapshot writer and unchanged compiled peer.
This is fixture control code, never a new production routing/parent capability.
"""
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from refusal_contract import REASONS, raw_child_context, require, validate


def mutate_owned(mode, context, connection, Recording, Movie, VODLogo):
    require(mode in REASONS, 'ExactRefusalMutation')
    database = os.environ.get('POSTGRES_DB', '')
    require(database.startswith('rust_probe_backup_jobs_') and os.environ.get('JOB_DISPOSABLE_FIXTURE') == 'playback-fixture-only', 'OwnedDisposableGuardBeforeMutation')
    with connection.cursor() as cursor:
        cursor.execute('SELECT current_database()')
        require(cursor.fetchone()[0] == database, 'ActualOwnedDatabaseBeforeMutation')
    recording = Recording.objects.get(pk=context['recording_id'])
    require(recording.custom_properties['program']['title'] == 'Owned Parented Artwork', 'ActualOwnedRecordingTitle')
    movies = list(Movie.objects.filter(name='Owned Parented Artwork')[:2])
    require(len(movies) == 1 and movies[0].logo_id is not None, 'OneOwnedMovieBeforeMutation')
    movie = movies[0]
    logo = VODLogo.objects.get(pk=movie.logo_id)
    require(logo.url == '/app/media/rust-owned-parented-artwork/logo.png', 'ActualOwnedLocalLogoBeforeMutation')
    receipt = {'mode': mode, 'database': database, 'recording_id': recording.pk, 'movie_id': movie.pk, 'logo_id': logo.pk, 'mutation_began_utc': datetime.now(timezone.utc).isoformat(), 'Native_task_started': False}
    if mode == 'missing-recording':
        deleted, models = recording.delete()
        receipt['actual_deleted_model_counts'] = models
        require(deleted > 0 and not Recording.objects.filter(pk=context['recording_id']).exists(), 'ActualRecordingRemovedByOwnedSetup')
    elif mode == 'duplicate-casefold-movie':
        duplicate = Movie.objects.create(name='OWNED PARENTED ARTWORK', logo=logo, year=2026)
        receipt['actual_duplicate_id'] = duplicate.pk
        require(duplicate.pk != movie.pk and Movie.objects.filter(name__iexact='Owned Parented Artwork').count() == 2, 'ActualTwoMovieRowsBeforeNative')
        with connection.cursor() as cursor:
            sql = 'SELECT m.id,m.name FROM vod_movie m WHERE UPPER(m.name::text)=UPPER(%s) ORDER BY m.name LIMIT 2'
            cursor.execute(sql, ['Owned Parented Artwork'])
            matched = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
        receipt['actual_casefold_SQL'] = {'sql': sql, 'parameters': ['Owned Parented Artwork'], 'rows': matched, 'Native_Store_predicate': 'UPPER(m.name::text)=UPPER($1)'}
        require(len(matched) == 2 and {row['id'] for row in matched} == {movie.pk, duplicate.pk}, 'ActualSameStoreSQLCasefoldTwoRows')
    elif mode == 'oversized-recording-properties':
        props = dict(recording.custom_properties)
        props['owned_oversized_refusal'] = 'x' * 8193
        recording.custom_properties = props
        recording.save(update_fields=['custom_properties'])
        with connection.cursor() as cursor:
            cursor.execute('SELECT octet_length(custom_properties::text) FROM dispatcharr_channels_recording WHERE id=%s', [context['recording_id']])
            actual_bytes = cursor.fetchone()[0]
        require(type(actual_bytes) is int and actual_bytes > 8192, 'ActualSerializedSQLPropertyBytesOverCap')
        receipt['actual_custom_properties_text_octets'] = actual_bytes
    elif mode == 'nonlocal-logo':
        # No server is started. The request guard must still reject/log any
        # attempted provider contact; refusal must happen solely in Store.
        nonlocal_url = 'http://127.0.0.1:61999/owned-uncontacted-logo.png'
        changed = VODLogo.objects.filter(pk=logo.pk).update(url=nonlocal_url)
        require(changed == 1 and VODLogo.objects.get(pk=logo.pk).url == nonlocal_url, 'ActualNonlocalURLBeforeNative')
        receipt['uncontacted_owned_URL'] = nonlocal_url
    receipt['mutation_finished_utc'] = datetime.now(timezone.utc).isoformat()
    return receipt


def execute(mode, config, root_task, parent_task, artwork_queue, snapshot, http_attempts, persist, peer_type, stop_owned):
    """Actual one-run driver, called only by a future reviewed host/guest closure.

    The caller must first apply exactly one owned mutation and retain its setup
    effects. config is derived from actual completed raw parents and original
    child bytes, never projected into a different lineage/queue.
    """
    started = time.monotonic()
    require(mode in REASONS, 'ExactRefusalExecution')
    before = snapshot()
    context = raw_child_context(before, parent_task, root_task, artwork_queue)
    require(config['artwork_frame_base64'] == context['frame_base64'], 'OriginalRawCandidateInPeerConfiguration')
    require(config['refresh_backend_base64'] == root_task['result']['raw_base64'] and config['series_backend_base64'] == parent_task['result']['raw_base64'], 'ActualCompletedRawMetadataInPeerConfiguration')
    frame = json.loads(base64.b64decode(context['frame_base64'], validate=True))
    delay = max(0, (datetime.fromisoformat(frame['headers']['eta']) - datetime.now(timezone.utc)).total_seconds())
    require(delay <= 2, 'ActualCapturedETABoundedWait')
    time.sleep(delay)
    report = {'passed': False, 'mode': mode, 'before': before, 'context': context, 'actual_ETA_wait_seconds': delay, 'native_process': {'spawn_state': 'not_started'}, 'Source_failure_result_parity_verified': False}
    persist(report)
    peer = None
    try:
        argv = ['/native-parented-peer', 'recording_artwork_peer::actual_local_artwork_peer', '--exact', '--ignored', '--nocapture', '--test-threads=1']
        peer = peer_type(argv, dict(os.environ), stop_owned)
        report['native_process'].update(spawn_state='actual_child_started', argv=argv, pid=peer.process.pid, starttime=Path('/proc', str(peer.process.pid), 'stat').read_text().rsplit(')', 1)[1].split()[19])
        peer.send(config)
        prepared = peer.receive(15)
        report['prepared'] = prepared
        persist(report)
        require(prepared.get('prepared') is True and prepared.get('task_id') == context['task_id'], 'ActualPreparedChildCapability')
        peer.send({'action': 'run'})
        response = peer.receive(35)
        report['response'] = response
        # Full metadata/effects are retained before checking refusal predicates.
        report['after_run'] = snapshot()
        persist(report)
        peer.send({'action': 'close'})
        report['closed'] = peer.receive(15)
        persist(report)
        require(report['closed'] == {'closed': True}, 'ActualNormalPeerClose')
        peer.finish(15)
        report['after'] = snapshot()
        report['HTTP_attempts'] = http_attempts()
        require(report['HTTP_attempts'] == [], 'NoProviderOrOwnedURLAttempt')
    finally:
        errors = []
        if peer is not None:
            if peer.writer is not None and peer.writer.is_alive():
                try: peer.commands.put_nowait(None)
                except BaseException as error: errors.append({'resource': 'writer-wake', 'error': type(error).__name__})
            if peer.poll() is None:
                try: peer.abort('RefusalFinally')
                except BaseException as error: errors.append({'resource': 'native-stop', 'error': type(error).__name__})
            deadline = time.monotonic() + 17
            for thread in peer.threads + ([peer.writer] if peer.writer else []):
                try: thread.join(max(0, deadline - time.monotonic()))
                except BaseException as error: errors.append({'resource': 'reader-writer-join', 'error': type(error).__name__})
            report['native_process'].update(exit=peer.poll(), settled=peer.physically_settled, pipes_settled=not any(t.is_alive() for t in peer.threads), writer_settled=peer.writer is None or not peer.writer.is_alive(), collector_invalid=peer.invalid)
            report['stdout_base64'] = base64.b64encode(bytes(peer.output['stdout'])).decode()
            report['stderr_base64'] = base64.b64encode(bytes(peer.output['stderr'])).decode()
        report['finally_errors'] = errors
        report['case_elapsed_seconds'] = time.monotonic() - started
        persist(report)
    require(report['finally_errors'] == [] and report['case_elapsed_seconds'] <= 90, 'ActualRefusalCaseSettlement90')
    report['derived_validation'] = validate(mode, before, report['after'], report['response'], report['native_process'], artwork_queue, context)
    report['passed'] = True
    persist(report)
    return report
