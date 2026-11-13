"""Complete retained-row FK preflight and narrow Source-defined setup parents.

No Django import or DB work on import. Task payloads were not captured: only
required disabled task parents are produced by the unchanged pinned Source
helper. Their schedules/date_changed are fresh owned setup, not Source history.
Original Dispatcharr authors retain model/scheduling/algorithm credit.
"""
import json,re
from pathlib import Path
TASK='django_celery_beat_periodictask'
SCHEMA_PATH=Path(__file__).resolve().with_name('retained-fk-schema.json')

def references(rows,schema):
 if type(rows)is not dict or type(schema)is not dict or set(rows)!=set(schema):raise ValueError('CompleteRetainedFKTables')
 result=[]
 for table,fields in schema.items():
  if type(fields)is not list or len({f['column']for f in fields})!=len(fields):raise ValueError('UniqueFKColumns')
  for field in fields:
   if set(field)!={'column','target_table','target_column'}or any(type(v)is not str or re.fullmatch('[a-z_][a-z_0-9]*',v)is None for v in field.values()):raise ValueError('SafeCompleteFKDefinition')
   for row in rows[table]:
    if field['column']not in row:raise ValueError('MissingRetainedFKColumn')
    value=row[field['column']]
    if value is None:continue
    if type(value)is not int or value<=0:raise ValueError('TypedRetainedFK')
    target=field['target_table'];column=field['target_column']
    if target in rows and not any(record.get(column)==value for record in rows[target]):raise ValueError('MissingInternalRetainedParent')
    result.append({'table':table,'row_id':row['id'],**field,'value':value})
 return result

def task_inputs(row):
 if type(row)is not dict or row.get('id')not in[2,3,4]or row.get('refresh_task_id')!=row['id']or row.get('account_type')!='XC'or type(row.get('refresh_interval'))is not int or row['refresh_interval']!=0 or type(row.get('is_active'))is not bool:raise ValueError('OnlyCapturedDisabledSchedulingParents')
 return {'expected_id':row['refresh_task_id'],'task_name':'m3u_account-refresh-'+str(row['id']),'celery_task_path':'apps.m3u.tasks.refresh_single_m3u_account','kwargs':{'account_id':row['id']},'interval_hours':0,'cron_expression':'','enabled':row['is_active']}

def approved_external_references(refs):
 external=[r for r in refs if r['target_table']not in json.loads(SCHEMA_PATH.read_text())]
 for ref in external:
  if ref['table']!='m3u_m3uaccount'or ref['column']!='refresh_task_id'or ref['target_table']!=TASK or ref['target_column']!='id'or ref['row_id']!=ref['value']or ref['value']not in[1,2,3,4]:raise ValueError('UnapprovedExternalRetainedParent')
 if sorted(r['value']for r in external)!=[1,2,3,4]:raise ValueError('ExactFourCapturedTaskReferences')
 return external

def model_schema(models,tables):
 schema={}
 for table in tables:
  if table not in models:raise ValueError('ExactPinnedModelTable')
  fields=[]
  for field in models[table]._meta.concrete_fields:
   if field.remote_field is not None and (field.many_to_one or field.one_to_one):
    fields.append({'column':field.attname,'target_table':field.remote_field.model._meta.db_table,'target_column':field.target_field.attname})
  schema[table]=sorted(fields,key=lambda r:r['column'])
 return schema

def require_natural_task_sequence(last,called):
 if type(last)is not int or type(called)is not bool or last+int(called)!=2:raise ValueError('TaskSequenceMustNaturallyAllocateCaptured2NoForcing')

def require_existing_task1_preserved(before,after):
 if type(before)is not dict or not before or before!=after:raise ValueError('ExistingDefaultTask1Changed')

def verify_task(task,plan):
 if task.pk!=plan['expected_id']or task.name!=plan['task_name']or task.task!=plan['celery_task_path']or json.loads(task.kwargs)!=plan['kwargs']or task.enabled is not False or task.interval is None or task.interval.every!=1 or task.interval.period!='hours'or any(getattr(task,key,None)is not None for key in ['crontab_id','solar_id','clocked_id']):raise ValueError('ExactSourceDefinedDisabledTaskProjection')
 return {'id':task.pk,'name':task.name,'task':task.task,'kwargs':json.loads(task.kwargs),'enabled':task.enabled,'interval':{'id':task.interval_id,'every':task.interval.every,'period':task.interval.period},'date_changed':str(task.date_changed),'scope':'fresh definition-derived setup, not retained Source task payload'}

def prepare(rows,models,connection):
 expected=json.loads(SCHEMA_PATH.read_text());actual=model_schema(models,rows)
 if actual!=expected:raise ValueError('CompletePinnedModelFKSchema')
 refs=references(rows,actual);external=approved_external_references(refs)
 PeriodicTask=models[TASK]
 existing1=PeriodicTask.objects.filter(pk=1).values().first()
 if existing1 is None:raise ValueError('PreserveExistingDefaultTask1NoSubstitute')
 plans=[task_inputs(row)for row in rows['m3u_m3uaccount']if row['id']!=1]
 if [p['expected_id']for p in plans]!=[2,3,4]:raise ValueError('FixedSchedulingParentOrder')
 # Before writes, refuse collisions rather than remap PKs or guess that an
 # unrelated existing task can be replaced. No sequence value is forced.
 for plan in plans:
  if PeriodicTask.objects.filter(pk=plan['expected_id']).exists()or PeriodicTask.objects.filter(name=plan['task_name']).exists():raise ValueError('FreshDerivedTaskIdentityCollision')
 with connection.cursor()as cursor:
  cursor.execute('SELECT pg_get_serial_sequence(%s,%s)',[TASK,'id']);sequence=cursor.fetchone()[0]
  if type(sequence)is not str or not all(re.fullmatch('[a-z_][a-z_0-9]*',v)for v in sequence.split('.')):raise ValueError('ExactTaskSequenceIdentifier')
  quoted='.'.join(connection.ops.quote_name(v)for v in sequence.split('.'))
  cursor.execute('SELECT last_value,is_called FROM '+quoted);last,called=cursor.fetchone()
  require_natural_task_sequence(last,called)
 from core.scheduling import create_or_update_periodic_task
 created=[]
 for plan in plans:
  kwargs={k:v for k,v in plan.items()if k!='expected_id'}
  task=create_or_update_periodic_task(**kwargs)
  created.append(verify_task(task,plan))
  # Check the complete actual FK set of each generated task, not just its
  # interval field. Every nonnull parent must really exist before row imports.
  for field in task._meta.concrete_fields:
   if field.remote_field is not None and (field.many_to_one or field.one_to_one):
    value=getattr(task,field.attname)
    if value is not None and not field.remote_field.model.objects.filter(**{field.target_field.attname:value}).exists():raise ValueError('DerivedTaskFKClosure')
 require_existing_task1_preserved(existing1,PeriodicTask.objects.filter(pk=1).values().first())
 for ref in external:
  if not PeriodicTask.objects.filter(pk=ref['value']).exists():raise ValueError('RequiredRetainedTaskParentStillMissing')
 return {'complete_retained_FK_schema':actual,'nonnull_references':refs,'existing_task1_exact_unchanged':True,'created_disabled_task_parents':created,'historical_Source_task_payloads_claimed':False,'no_task_dispatch':True,'no_FK_null_or_remap':True,'task_sequence_forced':False}
