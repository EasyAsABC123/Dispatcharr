"""Owned Source dependency-failure WSGI observer. Original authors retain credit.
Only the missing-index broker dependency is faulted; Original view, middleware
and error rendering stay unchanged. No production authority or native HTTP claim.
"""
import json,os
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_'):raise RuntimeError('Owned copied fixture required')
from django.core.wsgi import get_wsgi_application
application=get_wsgi_application()
from apps.epg.tasks import build_programme_index_task
from kombu.exceptions import OperationalError
def unavailable(*args,**kwargs):
 with Path('/evidence/current-dispatch-attempts.jsonl').open('a')as file:file.write(json.dumps({'args':args,'kwargs':kwargs})+'\n')
 raise OperationalError('Owned fixture broker publication failure')
build_programme_index_task.delay=unavailable
