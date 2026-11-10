"""Actual pinned Django/psycopg P0001 diagnostics, copied database only.
Original Dispatcharr authors retain application credit; PostgreSQL/psycopg/Django
framework authors retain their code, credit and licenses. No task/worker/restore.
"""
import hashlib,importlib.metadata,json,os,sys,subprocess,uuid
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database):raise RuntimeError('Copied DB required')
sys.path[:0]=['/app'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from django.db import connection,DatabaseError
from psycopg import sql
specs=[('plain','owned primary',None,None),('detail','owned primary','owned detail',None),('hint','owned primary',None,'owned hint'),('both','owned primary','owned detail','owned hint'),('empty_detail','owned primary','',None),('empty_hint','owned primary',None,''),('trailing_newline','owned primary\n','owned detail\n','owned hint\n'),('leading_space','  owned primary  ','  detail  ','  hint  '),('multiline','owned\nmessage','detail\nsecond','hint\nsecond'),('unicode','owned á🦀','détail Русский','提示'),('empty_message','',None,None),('whitespace',' \n\t',' \t','\n'),('nested','owned nested','nested detail','nested hint')]
for codepoint in [0x09,0x0a,0x0b,0x0c,0x0d,0x1c,0x1d,0x1e,0x1f,0x20,0x85,0xa0,0x1680,0x180e,0x2000,0x2007,0x2028,0x2029,0x202f,0x205f,0x3000,0xfeff]:
 ch=chr(codepoint);specs.append(('ws_'+format(codepoint,'x'),ch+'owned boundary'+ch,None,None))
report={'passed':False,'scope':'Pinned Original P0001 message/DETAIL/HINT/CONTEXT rendering, copiedDB only; no native formatter/error classification','cases':[],'checks':[],'versions':{name:importlib.metadata.version(name)for name in ['Django','psycopg']},'functions_created':0}
def check(name,passed):
 report['checks'].append({'name':name,'passed':bool(passed)})
 if not passed:raise AssertionError(name)
created=[]
try:
 with connection.cursor()as cursor:
  cursor.execute('SHOW server_version');report['postgresql']=cursor.fetchone()[0]
  for label,message,detail,hint in specs:
   name='owned_pg_error_'+label;parts=[sql.SQL("ERRCODE='P0001'"),sql.SQL('MESSAGE=')+sql.Literal(message)]
   if detail is not None:parts.append(sql.SQL('DETAIL=')+sql.Literal(detail))
   if hint is not None:parts.append(sql.SQL('HINT=')+sql.Literal(hint))
   body=sql.SQL('BEGIN\n RAISE EXCEPTION USING ')+sql.SQL(', ').join(parts)+sql.SQL(';\nEND')
   statement=sql.SQL('CREATE FUNCTION {}() RETURNS void LANGUAGE plpgsql AS {}').format(sql.Identifier(name),sql.Literal(body.as_string(connection.connection)))
   cursor.execute(statement.as_string(connection.connection));created.append(name)
   if label=='nested':
    outer='owned_pg_error_nested_outer';outer_body=sql.SQL('BEGIN\n PERFORM {}();\nEND').format(sql.Identifier(name)).as_string(connection.connection)
    cursor.execute(sql.SQL('CREATE FUNCTION {}() RETURNS void LANGUAGE plpgsql AS {}').format(sql.Identifier(outer),sql.Literal(outer_body)).as_string(connection.connection));created.append(outer);name=outer
   try:cursor.execute(sql.SQL('SELECT {}()').format(sql.Identifier(name)).as_string(connection.connection))
   except DatabaseError as error:
    cause=error.__cause__;diag=cause.diag
    fields={key:getattr(diag,key)for key in ['sqlstate','message_primary','message_detail','message_hint','context','statement_position','internal_position','internal_query']}
    if any(isinstance(value,str)and len(value)>16384 for value in fields.values()):raise RuntimeError('Diagnostic bound')
    check('actual-programming-error-'+label,type(error).__module__=='django.db.utils'and type(error).__name__=='ProgrammingError'and fields['sqlstate']=='P0001')
    check('single-string-argument-'+label,len(error.args)==1 and isinstance(error.args[0],str)and error.args[0]==str(error))
    report['cases'].append({'name':label,'function':name,'fields':fields,'exception_module':type(error).__module__,'exception_class':type(error).__name__,'arguments':list(error.args),'rendered':str(error)})
    if os.environ.get('NATIVE_PG_FORMAT')=='1':
     nonce=uuid.uuid4().hex;path=Path('/evidence/pg-input-'+nonce+'.json');output=Path('/evidence/pg-output-'+nonce+'.json');path.write_text(json.dumps({'function':name,'output':str(output)}))
     peer=subprocess.run(['/evidence/native-pg-formatter','--ignored','--exact','pg_failure_format::tests::native_pg_failure_format_peer','--nocapture'],env=dict(os.environ,PG_FAILURE_INPUT_PATH=str(path)),text=True,capture_output=True,timeout=15)
     with Path('/evidence/native-peer.log').open('a')as file:file.write(peer.stdout+peer.stderr)
     peer.check_returncode();assert 'test result: ok. 1 passed; 0 failed'in peer.stdout
     native=json.loads(output.read_text());expected={key:fields[key]for key in ['sqlstate','message_primary','message_detail','message_hint','context']};expected['has_position']=fields['statement_position']is not None or fields['internal_position']is not None
     check('actual-native-sqlx-fields-'+label,{key:native[key]for key in expected}==expected)
     check('actual-native-rendered-args-'+label,native['rendered']==str(error))
     report['cases'][-1]['native_fields']=native
     path.unlink();output.unlink()

   else:raise AssertionError('Expected P0001')
 report.update(passed=True,functions_created=len(created),native_pg_formatter_verified=os.environ.get('NATIVE_PG_FORMAT')=='1')
finally:
 with connection.cursor()as cursor:
  for name in reversed(created):cursor.execute(sql.SQL('DROP FUNCTION {}()').format(sql.Identifier(name)).as_string(connection.connection))
 report['functions_removed']=len(created)
 Path('/evidence/source-pg-format-private.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
