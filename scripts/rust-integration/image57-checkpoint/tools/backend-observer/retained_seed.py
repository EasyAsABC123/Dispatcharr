"""Setup-only exact retained rows. No model save/build_command/view/task calls."""
import hashlib,json,os,re,sys
from pathlib import Path

ORDER=['accounts_user','m3u_m3uaccount','vod_vodcategory','vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation','core_outputprofile','core_coresettings']
def validate_rows(rows):
 if type(rows)is not dict or set(rows)!=set(ORDER):raise ValueError('ExactRetainedTables')
 for table,records in rows.items():
  if type(records)is not list or not records or len(records)>1000:raise ValueError('BoundedRetainedRows')
  ids=set();columns=set(records[0])
  for row in records:
   if type(row)is not dict or set(row)!=columns or any(not re.fullmatch('[a-z_][a-z_0-9]*',k)for k in row):raise ValueError('ExactSafeColumns')
   if type(row.get('id'))is not int or row['id']<=0 or row['id']in ids:raise ValueError('UniquePositivePK')
   ids.add(row['id'])
 return True
def main():
 if os.environ.get('SOURCE_CAPTURE_FIXTURE')!='playback-fixture-only' or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or not re.fullmatch('rust_capture_[0-9a-f]{16}',os.environ.get('POSTGRES_DB','')):raise RuntimeError('OwnedDatabaseOnly')
 root=Path('/capture');pins=json.loads((root/'pinned-source.json').read_text());binding=json.loads((root/'reference-inputs.json').read_text())
 for path,expected in binding['private_inputs'].items():
  if hashlib.sha256((root/path).read_bytes()).hexdigest()!=expected:raise ValueError('RetainedInputBinding')
 for path,expected in pins['files'].items():
  if hashlib.sha256((Path('/app')/path).read_bytes()).hexdigest()!=expected:raise ValueError('PinnedOriginal')
 rows=json.loads((root/'retained-rows-private.json').read_text());validate_rows(rows)
 sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 from dispatcharr.app_initialization import should_skip_initialization
 if should_skip_initialization()is not True:raise RuntimeError('ManagementSkipBefore')
 import django;django.setup()
 if should_skip_initialization()is not True:raise RuntimeError('ManagementSkipAfter')
 from django.apps import apps
 from django.db import connection,connections,transaction
 from django.db.models import JSONField
 from psycopg.types.json import Jsonb
 models={model._meta.db_table:model for model in apps.get_models()}
 try:
  with transaction.atomic(),connection.cursor()as cursor:
   # No captured row or its FK is changed. Validate the complete model FK
   # closure before upserts and create only approved disabled parents through
   # the unchanged pinned Source scheduling helper.
   from retained_dependencies import prepare
   dependency_setup=prepare(rows,models,connection)
   for table in ORDER:
    model=models[table];fields={f.attname:f for f in model._meta.concrete_fields}
    columns=list(rows[table][0]);
    if set(columns)!=set(fields):raise ValueError('ExactPinnedModelColumns')
    quoted=','.join('"'+name+'"'for name in columns)
    update=','.join('"'+name+'"=EXCLUDED."'+name+'"'for name in columns if name!='id')
    statement='INSERT INTO "'+table+'"('+quoted+') VALUES('+','.join(['%s']*len(columns))+') ON CONFLICT(id) DO UPDATE SET '+update
    for row in rows[table]:
     values=[Jsonb(row[name])if isinstance(fields[name],JSONField)and row[name]is not None else row[name]for name in columns]
     cursor.execute(statement,values)
    # Reconcile sequences to the actual max after setup; never consume nextval.
    cursor.execute('SELECT pg_get_serial_sequence(%s,%s)',[table,'id']);sequence=cursor.fetchone()[0]
    if sequence:
     cursor.execute('SELECT max(id) FROM "'+table+'"');maximum=cursor.fetchone()[0]
     cursor.execute('SELECT setval(%s,%s,true)',[sequence,maximum])
  selected={}
  for table in ORDER:
   model=models[table];ids=[row['id']for row in rows[table]]
   actual=list(model.objects.filter(pk__in=ids).order_by('id').values())
   expected=sorted(rows[table],key=lambda r:r['id'])
   # Exact datetime/UUID parsing uses model field conversions. No timestamps
   # or properties are changed to force a body match.
   converted=[]
   for row in expected:
    values={}
    for field in model._meta.concrete_fields:
     value=row[field.attname]
     values[field.attname]=value if isinstance(field,JSONField)or value is None else field.to_python(value)
    converted.append(values)
   if actual!=converted:raise ValueError('ExactRetainedSQLReload:'+table)
   selected[table]={'rows':len(actual)}
  raw=(json.dumps({'tables':selected,'dependency_setup':dependency_setup,'setup_only':True,'Source_business_requests':0,'build_command_calls':0},indent=2)+'\n').encode()
  Path('/evidence/retained-setup-proof.json').write_bytes(raw)
  print(json.dumps({'exact_retained_row_reload':True,'table_count':len(ORDER),'Source_business_requests':0,'build_command_calls':0}))
 finally:connections.close_all()
if __name__=='__main__':main()
