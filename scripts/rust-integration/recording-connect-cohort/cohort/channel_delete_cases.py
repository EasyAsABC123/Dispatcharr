"""Real M5 Channel DELETE with Recording/Beat Collector effects.
Reuse existing Fixture/Redirect preparation, app and media Origin.
"""
import datetime
import hashlib
import json
import uuid
import urllib.request
import urllib.error
from pathlib import Path
from output_profile_reader import Reader


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


class SourceAliasConsumerUnqualified(RuntimeError):
    pass


def run(f):
    out = f.output / 'Channel-Recording-Collector-capture'
    out.mkdir(mode=0o755)
    out.chmod(0o755)
    seed = f._redirect_seed
    report = {'status': 'running', 'cells': [], 'full_M5_passed': False, 'performance_verified': False,
              'scope': 'normal Channel API DELETE + actual future Booking/PT/ClockedSchedule + declared completed-file orphan; optional stop preserves Source playback boundary', 'internal_cache_parity': 'not established; Source inert deleted-row alias is reported separately from actual resource retirement', 'original_Source28_alias_absence_failure_preserved': True}
    original = f.sql("from apps.channels.models import Stream;s=Stream.objects.get(pk=" + str(seed['stream']) + ");print(json.dumps({'url':s.url}))", secret=True)
    f.sql("from apps.channels.models import Stream;Stream.objects.filter(pk=" + str(seed['stream']) + ").update(url=" + repr(seed['media_url']) + ");print(json.dumps({'actual_owned_media_URL_selected':True}))")

    def persist():
        save(out / 'report.json', report)

    def state(channel, worker):
        db = f.sql("from apps.channels.models import Channel,Recording;from django_celery_beat.models import PeriodicTask,ClockedSchedule;rows=list(Recording.objects.filter(channel_id=" + str(channel) + ").values('id','task_id'));names=[v['task_id']for v in rows if v['task_id']];print(json.dumps({'channel_exists':Channel.objects.filter(pk=" + str(channel) + ").exists(),'recordings':rows,'periodic':list(PeriodicTask.objects.filter(name__in=names).values('id','name','clocked_id'))}))")
        raw = f.redis(0, "local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),redis.call('GET',p..':owner')or'',redis.call('GET','profile_connections:'..ARGV[2])or'0',redis.call('GET','channel_stream:'..ARGV[3])or'',redis.call('GET','stream_profile:'..ARGV[4])or'',redis.call('HGET',p..':metadata','channel_id')or'',redis.call('HGET',p..':metadata','stream_id')or'',redis.call('HGET',p..':metadata','m3u_profile')or'',redis.call('PTTL','channel_stream:'..ARGV[3]),redis.call('EXISTS',p..':metadata'),redis.call('TYPE','channel_stream:'..ARGV[3]).ok}", worker, str(seed['profile']), str(channel), str(seed['stream']))
        return {'db': db, 'clients': raw[0], 'owner_present': bool(raw[1]), 'provider_count': int(raw[2]), 'Channel_alias': raw[3], 'Stream_alias': raw[4], 'metadata_Channel': raw[5], 'metadata_Stream': raw[6], 'metadata_provider': raw[7], 'Channel_alias_PTTL_ms': raw[8], 'metadata_exists': raw[9], 'Channel_alias_TYPE': raw[10]}

    def actual_media_processes():
        cid = f.native if f.backend == 'Native' else f.command(f.compose + ['ps', '-q', 'baseline']).strip()
        # Raw argv remains private; public proof records only matched actual PIDs.
        text = f.command([f.docker, 'top', cid, '-eo', 'pid,args'], secret=True)
        return [int(line.split()[0]) for line in text.splitlines()[1:]
                if seed['media_url'] in line and any(token in line.lower() for token in ['ffmpeg', 'cvlc', '/vlc'])]

    def deleted_stream_headers(worker):
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, response, code, message, headers, url):
                return None
        request = urllib.request.Request(f.base_url + '/proxy/ts/stream/' + worker,
                                         headers={'Authorization': 'Bearer ' + f.actual_admin_token()})
        try:
            response = urllib.request.build_opener(NoRedirect()).open(request, timeout=f.remaining(5))
        except urllib.error.HTTPError as error:
            response = error
        with response:
            # Status/headers only: unexpected200 never becomes an unbounded read.
            return {'status': response.status, 'body_read': False,
                    'location_present': bool(response.headers.get('Location'))}

    def orphan_file(name):
        return json.loads(f.python("from pathlib import Path;import json,hashlib;p=Path('/shared')/" + repr(f.case) + "/'recordings'/" + repr(name) + ";print(json.dumps({'exists':p.is_file(),'bytes':p.stat().st_size if p.is_file()else None,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()if p.is_file()else None}))"))

    def cell(stop_stream):
        label = 'with-stop' if stop_stream else 'default-preserve-viewer'
        title = 'Owned Collector ' + uuid.uuid4().hex
        profile = f.sql("from core.models import StreamProfile;print(json.dumps(StreamProfile.get_locked('Proxy').pk))")
        created = f.api('POST', '/api/channels/channels/', {'name': title, 'streams': [seed['stream']], 'stream_profile': profile}, 'admin')
        save(out / (label + '-Channel-create.json'), created)
        if created['status'] != 201:
            raise AssertionError('ActualAuxiliaryChannelAPICreate')
        channel = created['body']['id']
        worker = created['body']['uuid']
        reader = None
        file_name = 'collector-orphan-' + uuid.uuid4().hex + '.ts'
        future = None
        proof = {'stop_stream': stop_stream, 'Channel': channel, 'worker': worker,
                 'completed_file_is_declared_owned_metadata_fixture_not_decoder_authority': True}
        save(out / (label + '-proof.json'), proof)
        try:
            reader = Reader(f.base_url + '/proxy/ts/stream/' + worker, out / (label + '-viewer.ts'), f.actual_admin_token())
            f.wait(lambda: reader.snapshot()['media_bytes'] >= 750000 and not reader.snapshot()['errors'], 30, 'actual-Collector-ordinary-viewer')
            start = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
            booked = f.api('POST', '/api/channels/recordings/', {'channel': channel, 'start_time': start.isoformat(), 'end_time': (start + datetime.timedelta(minutes=1)).isoformat(), 'custom_properties': {'program': {'title': title}}}, 'admin')
            save(out / (label + '-actual-future-booking.json'), booked)
            if booked['status'] != 201:
                raise AssertionError('ActualFutureBookingBeforeChannelCollector')
            future = booked['body']['id']
            f.python("import os;from pathlib import Path;p=Path('/shared')/" + repr(f.case) + "/'recordings'/" + repr(file_name) + ";p.write_bytes(b'owned completed Recording file: Collector preserves this byte fixture');p.chmod(0o444);os.chown(p,65532,65532)")
            # Bulk input intentionally bypasses producer/media work. It tests
            # Collector vs Recording API.destroy file behavior only.
            complete = f.sql("from apps.channels.models import Recording;from django.utils import timezone;from datetime import timedelta;r=Recording(channel_id=" + str(channel) + ",start_time=timezone.now()-timedelta(minutes=2),end_time=timezone.now()-timedelta(minutes=1),task_id=None,custom_properties={'status':'completed','file_path':" + repr('/data/recordings/' + file_name) + "});Recording.objects.bulk_create([r]);print(json.dumps({'id':r.pk,'task_id':r.task_id,'declared_completed_file_fixture':True}))")
            before = state(channel, worker)
            proof.update(actual_completed_row=complete, before=before, completed_file_before=orphan_file(file_name))
            save(out / (label + '-proof.json'), proof)
            if len(before['db']['recordings']) != 2 or len(before['db']['periodic']) != 1 or before['clients'] != 1 or before['provider_count'] != 1 or not before['owner_present']:
                raise AssertionError('ActualRecordingPTAndOrdinaryViewerBeforeCollector')
            periodic = before['db']['periodic'][0]
            if periodic['clocked_id'] is None:
                raise AssertionError('ActualFutureClockedScheduleRequired')
            path = '/api/channels/channels/' + str(channel) + '/' + ('?stop_stream=1' if stop_stream else '')
            result = f.api('DELETE', path, token='admin')
            proof['actual_Channel_DELETE'] = result
            proof['immediate_after'] = state(channel, worker)
            save(out / (label + '-proof.json'), proof)
            if result['status'] != 204 or result['body'] is not None:
                raise AssertionError('ActualChannelCollector204Required')
            deleted = f.sql("from apps.channels.models import Channel,Recording;from django_celery_beat.models import PeriodicTask,ClockedSchedule;print(json.dumps({'channel_absent':not Channel.objects.filter(pk=" + str(channel) + ").exists(),'recordings_absent':not Recording.objects.filter(pk__in=" + repr([future, complete['id']]) + ").exists(),'actual_PT_absent':not PeriodicTask.objects.filter(pk=" + str(periodic['id']) + ").exists(),'actual_unused_clocked_absent':not ClockedSchedule.objects.filter(pk=" + str(periodic['clocked_id']) + ").exists()}))")
            proof['actual_Collector_effects'] = deleted
            proof['completed_file_after'] = orphan_file(file_name)
            save(out / (label + '-proof.json'), proof)
            if not all(deleted.values()) or proof['completed_file_after'] != proof['completed_file_before']:
                raise AssertionError('ActualCollectorRecordingBeatDeletionAndOrphanFileRequired')
            if stop_stream:
                f.wait(lambda: reader.snapshot()['ended'] and not state(channel, worker)['owner_present'] and state(channel, worker)['provider_count'] == 0, 30, 'actual-stop-before-Channel-delete-retirement')
                proof['actual_stop_stream_retirement'] = state(channel, worker)
            else:
                count = reader.snapshot()['media_bytes']
                f.wait(lambda: reader.snapshot()['media_bytes'] >= count + 188000 and not reader.snapshot()['ended'] and not reader.snapshot()['errors'], 10, 'actual-default-Channel-delete-preserves-viewer')
                proof['actual_default_delete_viewer_survives'] = {'before_nonnull_bytes': count, 'after': reader.snapshot(), 'state': state(channel, worker)}
                if proof['actual_default_delete_viewer_survives']['state']['provider_count'] != 1:
                    raise AssertionError('DefaultCollectorMustPreserveActualProviderSlot')
            return proof
        except BaseException as error:
            proof['primary_failure'] = {'type': type(error).__name__, 'label': str(error)[:160]}
            save(out / (label + '-proof.json'), proof)
            raise
        finally:
            if reader is not None:
                reader.close()
                proof['actual_reader_final'] = reader.snapshot()
            try:
                proof['before_final_stop'] = state(channel, worker)
                proof['actual_final_stop'] = f.api('POST', '/proxy/ts/stop/' + worker, {}, 'admin')
                # This uses Source's Redis/local worker lookup and metadata-only
                # slot release; the deleted Channel row is not recreated.
                # A404 is only an already-retired observation, never a stop ACK.
                if proof['actual_final_stop']['status'] not in (200, 404):
                    raise AssertionError('ActualDeletedChannelWorkerStopTransport')
                def retired():
                    actual = state(channel, worker)
                    upstream = json.loads(f.get(f.origin + '/stats')[1])['active']
                    proof['actual_retirement_observation'] = {'state': actual, 'origin_active': upstream}
                    save(out / (label + '-proof.json'), proof)
                    resources_retired = not actual['owner_present'] and actual['clients'] == 0 and actual['provider_count'] == 0 and not actual['Stream_alias'] and actual['metadata_exists'] == 0 and upstream == 0 and reader.snapshot()['ended']
                    if not resources_retired:
                        return False
                    processes = actual_media_processes()
                    proof['actual_retirement_observation']['matching_actual_input_process_PIDs'] = processes
                    save(out / (label + '-proof.json'), proof)
                    if processes:
                        return False
                    # Native keeps its original strict Channel-alias absence.
                    if f.backend == 'Native':
                        return not actual['Channel_alias']
                    # Source fallback cannot identify deleted numeric Channel ID
                    # when metadata.channel_id was absent. Retain literal key/TTL;
                    # only the EXACT selectedStream residual is a candidate.
                    return not actual['Channel_alias'] or (not actual['db']['channel_exists'] and actual['Channel_alias_TYPE'] == 'string' and actual['Channel_alias'] == str(seed['stream']))
                f.wait(retired, 30, 'actual-owned-Collector-case-worker-final-retirement')
                proof['actual_final_retired'] = state(channel, worker)
                actual = proof['actual_final_retired']
                proof['actual_original_strict_Channel_alias_absence'] = not actual['Channel_alias']
                if f.backend == 'Original' and actual['Channel_alias']:
                    proof['Source_inert_data_alias'] = {'raw_value': actual['Channel_alias'], 'PTTL_ms': actual['Channel_alias_PTTL_ms'], 'TYPE': actual['Channel_alias_TYPE'], 'metadata_exists': actual['metadata_exists'], 'Source_original_metadata_channel_id': proof['before']['metadata_Channel'], 'resource_retirement_is_NOT_cache_parity': True}
                    # Prove the actual public consumers cannot turn this data
                    # residual into playback after the row and live owner vanish.
                    before_probe = state(channel, worker)
                    proof['deleted_Channel_API'] = f.api('GET', '/api/channels/channels/' + str(channel) + '/', token='admin')
                    proof['deleted_Stream_hash_fallback_absent'] = f.sql("from apps.channels.models import Stream;print(json.dumps(not Stream.objects.filter(stream_hash=" + repr(worker) + ").exists()))")
                    proof['deleted_Channel_stream_headers'] = deleted_stream_headers(worker)
                    proof['after_deleted_consumer_probe'] = state(channel, worker)
                    proof['after_deleted_consumer_origin_active'] = json.loads(f.get(f.origin + '/stats')[1])['active']
                    proof['after_deleted_consumer_input_process_PIDs'] = actual_media_processes()
                    save(out / (label + '-proof.json'), proof)
                    after_probe = proof['after_deleted_consumer_probe']
                    if not(proof['deleted_Channel_API']['status'] == 404 and proof['deleted_Stream_hash_fallback_absent'] is True and proof['deleted_Channel_stream_headers']['status'] == 404 and {k:v for k,v in before_probe.items() if k != 'Channel_alias_PTTL_ms'} == {k:v for k,v in after_probe.items() if k != 'Channel_alias_PTTL_ms'} and proof['after_deleted_consumer_origin_active'] == 0 and not proof['after_deleted_consumer_input_process_PIDs']):
                        raise SourceAliasConsumerUnqualified('DeletedSourceAliasConsumerNotProvenInert')
                    proof['Source_alias_consumers_proven_inert_for_actual_deleted_fixture'] = True
                    proof['internal_cache_parity_gap'] = 'Source channel_stream residual remains; Native requires strict absence. Full M5/cache parity is not accepted.'
            except SourceAliasConsumerUnqualified:
                proof['Source_alias_consumer_blocked'] = True
                save(out / (label + '-proof.json'), proof)
                raise
            except Exception as error:
                proof['physical_cleanup_failure'] = type(error).__name__
            # Any row left after a failed business call is removed only as
            # declared fixture cleanup; never credited as Native HTTP effects.
            proof['fixture_row_cleanup'] = f.sql("from apps.channels.models import Channel;print(json.dumps({'cleanup_only_deleted':Channel.objects.filter(pk=" + str(channel) + ").delete()[0]}))")
            save(out / (label + '-proof.json'), proof)
            if proof.get('physical_cleanup_failure'):
                raise RuntimeError('ActualCollectorCasePhysicalCleanupFailed')

    persist()
    try:
        for stop_stream in (False, True):
            row = {'name': 'Channel-delete-stop_stream=' + str(stop_stream).lower(), 'status': 'running'}
            report['cells'].append(row)
            persist()
            try:
                row['observation'] = cell(stop_stream)
                row['status'] = 'passed'
            except Exception as error:
                row.update(status='blocked' if isinstance(error, SourceAliasConsumerUnqualified) else 'failed', error_type=type(error).__name__, error_label=str(error)[:160])
            persist()
    finally:
        f.sql("from apps.channels.models import Stream;Stream.objects.filter(pk=" + str(seed['stream']) + ").update(url=" + repr(original['url']) + ");print(json.dumps({'actual_owned_Stream_URL_restored':True}))")
        report['status'] = 'passed' if len(report['cells']) == 2 and all(row['status'] == 'passed' for row in report['cells']) else 'failed'
        persist()
    return report
