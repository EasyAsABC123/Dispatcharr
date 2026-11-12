"""Owned real relation lock, never replaces task/helper/callable semantics."""
import json,os,sys,time
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from django.db import connection,transaction
with transaction.atomic():
 with connection.cursor()as cursor:
  cursor.execute("SET LOCAL statement_timeout='10000ms'");cursor.execute('LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE');cursor.execute('SELECT pg_backend_pid()');pid=cursor.fetchone()[0]
 Path('/evidence/held-relation-ready.json').write_text(json.dumps({'pid':pid,'table':'epg_programdata','read_only_lock_no_model_mutation':True}))
 deadline=time.monotonic()+35
 while time.monotonic()<deadline:time.sleep(.1)
 raise RuntimeError('Held SQL gate was not aborted within35s')
