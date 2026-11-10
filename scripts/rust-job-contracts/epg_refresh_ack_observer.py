"""Fixture-only refresh ACK observer; real Original task remains unchanged.
Original Dispatcharr and Celery/Kombu authors retain credit and licenses.
Imports the explicitly labeled owned child-route control, never child execution.
"""
import json
from epg_refresh_import_observer import app,TARGET,broker,append
from celery.signals import task_prerun
@task_prerun.connect
def ack_prerun(task_id=None,task=None,**unused):
 if task.name!=TARGET:return
 values=broker.hvals('unacked')
 if len(values)>64:raise RuntimeError('Owned reservation cap')
 own=[]
 for raw in values:
  if len(raw)>131072:raise RuntimeError('Reservation frame cap')
  envelope=json.loads(raw)[0]
  if envelope['headers']['id']==task_id:own.append(envelope['properties']['delivery_tag'])
 append('epg-refresh-ack-prerun.jsonl',{'id':task_id,'own_unacked_tags':own,'acks_late':task.acks_late,'reject_on_worker_lost':task.reject_on_worker_lost,'track_started':task.track_started,'ignore_result':task.ignore_result,'max_retries':task.max_retries,'time_limit':task.time_limit,'soft_time_limit':task.soft_time_limit})
