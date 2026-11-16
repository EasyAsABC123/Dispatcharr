"""Real booking/Core PATCH regression for Source date-only save signals.
Original Dispatcharr/Django/Celery authors retain all application credit.
Invoke inside the existing grouped-m6 fixture with its publication observer;
this module starts no applications and creates no synthetic task frames.
"""
import base64
import datetime as dt
import json


def run(fixture, core, module):
    if fixture.case != 'core-produced':
        raise ValueError('ExistingGroupedM6ContextRequired')
    fixture.sql('from apps.channels.models import Channel;Channel.objects.filter(pk='+str(fixture.channel_id)+').update(logo_id=None,epg_data_id=None);print(json.dumps(True))')
    now = dt.datetime.now(dt.timezone.utc)
    future = now + dt.timedelta(hours=2)
    programme_start = now - dt.timedelta(hours=1)
    programme_end = now - dt.timedelta(minutes=30)
    response = fixture.api('POST', '/api/channels/recordings/', {
        'channel': fixture.channel_id,
        'start_time': future.isoformat(),
        'end_time': (future + dt.timedelta(minutes=10)).isoformat(),
        'custom_properties': {'program': {
            'title': fixture.channel_name,
            'start_time': programme_start.isoformat(),
            'end_time': programme_end.isoformat(),
        }},
    })
    if response['status'] != 201:
        raise AssertionError(('ActualShiftedPastBookingHTTP', response))
    ident = response['body']['id']
    snapshot = "from django_celery_beat.models import PeriodicTask;row=Recording.objects.get(pk="+str(ident)+");print(json.dumps({'start':row.start_time.isoformat(),'end':row.end_time.isoformat(),'task_id':row.task_id,'periodic':list(PeriodicTask.objects.filter(name='dvr-recording-"+str(ident)+"').values('id','name','clocked_id'))}))"
    before = fixture.sql(snapshot)
    if not before['task_id'] or len(before['periodic']) != 1:
        raise AssertionError(('MissingRealInitialSchedule', before))
    began, patch = core.core_patch(fixture, 'dvr_settings', {
        'pre_offset_minutes': 5, 'post_offset_minutes': 7,
    })
    task = core.actual_task(fixture, module,
        'apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change', began)
    after = fixture.sql(snapshot)
    if dt.datetime.fromisoformat(after['start']) != programme_start-dt.timedelta(minutes=5):
        raise AssertionError(('ShiftedPastStart', after))
    if dt.datetime.fromisoformat(after['end']) != programme_end+dt.timedelta(minutes=7):
        raise AssertionError(('ShiftedPastEnd', after))
    if after['task_id'] != before['task_id'] or after['periodic']:
        raise AssertionError(('SourceUpdateFieldsTaskIdentity', before, after))
    parent = task['publication']['headers']['id']
    children = []
    for frame in core.frames(fixture):
        if frame['headers'].get('parent_id') != parent:
            continue
        envelope = json.loads(frame['raw_frame'])
        args = json.loads(base64.b64decode(envelope['body']))[0]
        if not args or args[0] != ident:
            continue
        if frame['headers']['task'] != 'apps.channels.tasks.prefetch_recording_artwork':
            raise AssertionError(('PastEndUnexpectedCapture', frame))
        if not frame['headers'].get('eta'):
            raise AssertionError(('MissingActualCountdown', frame))
        terminal = module.result(fixture, frame['headers']['id'], 40)
        if terminal['metadata']['status'] != 'SUCCESS' or terminal['metadata'].get('parent_id') != parent or terminal['metadata'].get('result') != 'ok':
            raise AssertionError(('ActualShiftedPastArtworkOrLineage', terminal))
        children.append({'publication': frame, 'backend': terminal})
    if len(children) != 1:
        raise AssertionError(('ExpectedOneActualArtworkNoCapture', children))
    expected_ids={item[0][0]for item in task['backend']['metadata']['children']}
    def all_publications():
        found=[frame for frame in core.frames(fixture)if frame['headers'].get('parent_id')==parent]
        return found if {frame['headers']['id']for frame in found}==expected_ids else None
    all_children=fixture.wait(all_publications,10,'ActualCompleteShiftedPastChildPublicationSet')
    all_outcomes=core.child_results(fixture,all_children,parent,expected_ids)
    result = {'all_parent_children':all_outcomes,'status':'passed','booking':response,'patch':patch,'before':before,
        'task':task,'after':after,'children':children,
        'scope':'real API booking -> Core PATCH -> worker -> date-only SQL save/revoke -> real countdown artwork; no capture for past-end window'}
    module.save(module.directory(fixture)/'core-offset-shifted-past.json', result)
    return result
