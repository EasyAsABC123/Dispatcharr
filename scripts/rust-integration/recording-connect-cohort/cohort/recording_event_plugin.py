"""Owned inert event-action plugin; no imports or changes to backend task bodies.
Original Dispatcharr, Celery and Django authors retain application/library credit.
"""
class Plugin:
    name = 'Owned recording event observation'
    version = '1'
    description = 'Observe actual Source recording event ordering'
    author = 'Original application authors retain full credit'
    actions = [{'id': 'observe', 'label': 'Observe recording event',
                'events': ['recording_start', 'recording_end']}]

    def __init__(self):
        import json
        import os
        import sys
        from celery._state import get_current_task
        from django.db import connection
        task = get_current_task()
        value = {'pid': os.getpid(), 'argv': list(sys.argv),
                 'task_id': None if task is None else task.request.id,
                 'task_name': None if task is None else task.name,
                 'phase': 'ordinary_constructor_observed'}
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO owned_recording_event_startup(payload) VALUES(%s::jsonb)',
                           [json.dumps(value)])

    def run(self, action, params, context):
        import hashlib
        import json
        import os
        import threading
        from celery._state import get_current_task
        from django.db import connection
        from apps.connect.models import DeliveryLog
        from core.models import SystemEvent
        if action != 'observe':
            raise ValueError('OwnedRecordingEventAction')
        event = params['event']
        payload = params['payload']
        if event not in ('recording_start', 'recording_end'):
            raise ValueError('OwnedRecordingEventName')
        task = get_current_task()
        request = None if task is None else task.request
        # Source inserts audit and completes Connect delivery before invoking
        # this ordinary event action. Persist their actual IDs inside the action.
        audit = list(SystemEvent.objects.filter(event_type=event).order_by('-id')
                     .values('id', 'details')[:1])
        deliveries = list(DeliveryLog.objects.filter(
            subscription__integration__name='Owned recording event webhook',
            subscription__event=event).order_by('-id')
            .values('id', 'status', 'subscription_id')[:1])
        children = [] if request is None else [c.as_tuple() for c in request.children]
        observation = {
            'event': event, 'task_id': None if request is None else request.id,
            'task_name': None if task is None else task.name,
            'task_parent_id': None if request is None else request.parent_id,
            'task_root_id': None if request is None else request.root_id,
            'task_children': children, 'pid': os.getpid(),
            'thread_id': threading.get_native_id(), 'audit': audit,
            'deliveries': deliveries, 'payload_keys': sorted(payload),
            # Complete owned-fixture payload is retained only in private SQL
            # evidence for the native event-port contract; public packets hash it.
            'actual_payload': payload,
            'payload_sha256': hashlib.sha256(json.dumps(
                payload, sort_keys=True, default=str).encode()).hexdigest(),
            'recording_id': payload.get('recording_id'),
            'source_event_callback_completed': True,
        }
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO owned_recording_event_probe(payload,backend_pid) '
                           'VALUES(%s::jsonb,pg_backend_pid())',
                           [json.dumps(observation, default=str)])
        return {'observed': event}
