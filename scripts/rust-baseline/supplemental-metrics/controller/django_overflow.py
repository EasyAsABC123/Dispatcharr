"""Actual pinned Original Django + Celery Signal boundary; no app/task stub."""
import json,os,sys
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from django.apps import apps
from django.db import connection
from dispatcharr.celery import app
from apps.epg.tasks import refresh_epg_data
from apps.epg.models import EPGSource
from apps.accounts.models import User
from celery.signals import task_prerun
root=Path('/evidence/django-overflow');root.mkdir(exist_ok=True)
os.environ['M0_OBSERVER_EVIDENCE_ROOT']=str(root)
import concurrent_task_observer as observed
assert observed.app is app and app.main=='dispatcharr'and apps.ready
seed=json.loads(Path('/evidence/concurrent-seed-private.json').read_text())
assert User.objects.get(pk=seed['user_id']).username==seed['username']and EPGSource.objects.filter(pk=seed['epg_source']).exists()
with connection.cursor()as cursor:cursor.execute('SELECT current_database()');database=cursor.fetchone()[0]
assert database==seed['database']
(root/'worker-observer-budget.json').write_text(json.dumps({'records':1024,'bytes':0}))
refresh_epg_data.push_request(id='owned-overflow-signal',root_id='owned-overflow-signal',parent_id=None)
try:responses=task_prerun.send(sender=refresh_epg_data,task_id='owned-overflow-signal',task=refresh_epg_data,args=[seed['epg_source']],kwargs={})
finally:refresh_epg_data.pop_request()
errors=[type(value).__name__ for receiver,value in responses if isinstance(value,Exception)]
assert errors==['RuntimeError'],errors
assert json.loads((root/'worker-observer-capacity.json').read_text())['capacity_exceeded']
assert not(root/'workflow-prerun.jsonl').exists()
assert not Path('/evidence/worker-observer-capacity.json').exists()
(root/'proof.json').write_text(json.dumps({'passed':True,'actual_Original_Django':True,'actual_app_main':app.main,'actual_task_name':refresh_epg_data.name,'no_app_task_signal_stubs':True,'same_copied_database':database==seed['database'],'signal_exception_swallowed':True,'persistent_overflow_witness':True,'no_overcap_event_written':True,'normal_worker_budget_unaffected':True,'task_body_executed':False}))
print('actual-Original-Django-Celery-overflow-PASS')
