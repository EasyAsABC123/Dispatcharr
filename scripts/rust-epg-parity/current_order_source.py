"""DRAFT original CurrentPrograms ordering/statement-sequence oracle.
Original Dispatcharr authors retain all application and algorithm credit.
Execute only in a fresh explicitly disposable baseline, after quiet ends.
"""
import datetime as dt,json,os,random,re,sys,tempfile,threading,queue,time
from pathlib import Path
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection,transaction,close_old_connections
from django.test.utils import CaptureQueriesContext
from django.urls import resolve
from rest_framework.test import APIRequestFactory,force_authenticate
from apps.accounts.models import User
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGData,ProgramData,EPGSourceIndex
PREFIX='rust-current-order-oracle-'
NOW=dt.datetime(2026,11,10,12,tzinfo=dt.timezone.utc)
if EPGSource.objects.filter(name__startswith=PREFIX).exists() or User.objects.filter(username=PREFIX+'admin').exists():raise RuntimeError('Fresh owned namespace required')

def main():
    rng=random.Random(20261110)
    user=User.objects.create(username=PREFIX+'admin',password='!',user_level=10,is_active=True)
    sources=[EPGSource(name=PREFIX+str(i),source_type='xmltv',is_active=False,last_message=''.join(rng.choices('abcdef0123456789',k=8192)),custom_properties={'fixture':'ordering','value':''.join(rng.choices('abcdef0123456789',k=2048))})for i in range(64)]
    EPGSource.objects.bulk_create(sources)
    entries=EPGData.objects.bulk_create([EPGData(name=PREFIX+str(i),tvg_id='current-order-'+str(i),epg_source=source)for i,source in enumerate(sources)])
    # Heap movement and wide joined columns deliberately distinguish plan shapes;
    # no desired ID sort is imposed. The original query's observed order is gold.
    moved=list(range(64));rng.shuffle(moved)
    for i in moved:EPGSource.objects.filter(pk=sources[i].pk).update(last_message=sources[i].last_message+'-moved')
    for i in moved[::3]:EPGData.objects.filter(pk=entries[i].pk).update(name=entries[i].name+'-moved')
    ProgramData.objects.bulk_create([ProgramData(epg=entry,start_time=NOW-dt.timedelta(hours=1),end_time=NOW+dt.timedelta(hours=1),title=PREFIX+str(i),tvg_id=entry.tvg_id,custom_properties={})for i,entry in enumerate(entries)])
    ids=[entry.pk for entry in entries];shuffled=ids.copy();rng.shuffle(shuffled)
    cases=[('ascending64',ids),('descending64',ids[::-1]),('shuffled64',shuffled),('shuffled50',shuffled[:50]),('duplicates-before-truncation',[ids[0]]*45+ids[1:21]),('duplicates-shuffled',[shuffled[i%31]for i in range(70)]),('missing-ids-before-truncation',[-987654321]*8+shuffled),('invalid-after50',ids[:50]+['invalid']),('integer-conversion-before-limit',[str(i)for i in shuffled])]
    factory=APIRequestFactory();captured=[]
    with connection.cursor() as cursor:
        cursor.execute('ANALYZE epg_epgsource');cursor.execute('ANALYZE epg_epgdata');cursor.execute('ANALYZE epg_programdata');cursor.execute('SHOW server_version');server_version=cursor.fetchone()[0]
    for number,(name,values) in enumerate(cases):
        request=factory.post('/api/epg/current-programs/',{'epg_data_ids':values},format='json');force_authenticate(request,user=user)
        match=resolve('/api/epg/current-programs/')
        with patch.object(tasks.timezone,'now',return_value=NOW),CaptureQueriesContext(connection) as queries:
            response=match.func(request,*match.args,**match.kwargs);response.render()
        if response.status_code!=(400 if name=='invalid-after50' else 200):raise RuntimeError('Unexpected Source authorization/status for '+name)
        record={'name':name,'input_ids':values,'status':response.status_code,'body':json.loads(response.content),'queries':[q['sql']for q in queries.captured_queries]}
        if response.status_code!=200:captured.append(record);continue
        converted=[int(value)for value in values];retained=converted[:50];unique=list(dict.fromkeys(retained));record['retained_positions']=retained;record['retained_unique_ids']=unique
        queryset=EPGData.objects.select_related('epg_source').filter(id__in=retained)
        original_sql,original_params=queryset.query.sql_with_params()
        native_sql='SELECT d.id::bigint AS id,s.source_type FROM epg_epgdata d LEFT JOIN epg_epgsource s ON s.id=d.epg_source_id WHERE d.id IN ('+','.join(['%s']*len(unique))+')'
        record['metadata_queryset_ordered']=queryset.ordered
        record['diagnostic_queries']={}
        with connection.cursor() as cursor:
            for label,sql,params in [('original_full_projection',original_sql,original_params),('native_narrowed_shape',native_sql,unique)]:
                cursor.execute('EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) '+sql,params);plan=cursor.fetchone()[0]
                cursor.execute(sql,params);columns=[col[0]for col in cursor.description];rows=cursor.fetchall();observed=[row[columns.index('id')]for row in rows]
                # The original full projection repeats id for the joined source;
                # list.index intentionally selects the first (EPGData) column.
                control={'sql':sql,'params':list(params),'columns':columns,'ids':observed,'plan':plan,'prepared_modes':{}}
                statement='current_order_'+str(number)+'_'+('source'if label.startswith('original')else'narrow')
                ordinal=iter(range(1,len(params)+1));prepared=re.sub(r'%s',lambda _: '$'+str(next(ordinal)),sql)
                cursor.execute('PREPARE '+statement+'('+','.join(['bigint']*len(params))+') AS '+prepared)
                execute='EXECUTE '+statement+'('+','.join(['%s']*len(params))+')'
                for mode in ['force_custom_plan','force_generic_plan','auto']:
                    cursor.execute('SET plan_cache_mode='+mode)
                    cursor.execute('EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) '+execute,params);mode_plan=cursor.fetchone()[0]
                    cursor.execute(execute,params);mode_columns=[col[0]for col in cursor.description];mode_rows=cursor.fetchall()
                    control['prepared_modes'][mode]={'ids':[row[mode_columns.index('id')]for row in mode_rows],'plan':mode_plan}
                cursor.execute('DEALLOCATE '+statement);cursor.execute('SET plan_cache_mode=auto')
                record['diagnostic_queries'][label]=control
        record['observed_body_ids']=[row['epg_data_id']for row in record['body']]
        if record['observed_body_ids']!=record['diagnostic_queries']['original_full_projection']['ids']:raise RuntimeError('Original full projection does not explain actual body order')
        captured.append(record)
    schedules=[]
    schedule_names=['delete-epg-after-programmes','delete-source-after-programmes','delete-stored-programme-after-batch','create-epg-and-programme-after-batch','create-programme-after-batch','source-dummy-to-xml-after-batch','source-becomes-null-after-batch','replace-extracted-path-after-batch','delete-index-after-batch','replace-index-after-batch','atomic-file-replace-after-batch','fresh-file-clock-after-global-clock','replace-index-at-fresh-clock','malformed-stored-deleted-before-metadata','malformed-stored-survives-metadata']
    with tempfile.TemporaryDirectory(prefix='owned-current-order-')as directory:
        for ordinal,name in enumerate(schedule_names):
            def programme(title):return ('<programme channel="schedule" start="20261110110000 +0000" stop="20261110130000 +0000"><title>'+title+'</title></programme>').encode()
            original_file=programme('original-file');replacement_file=programme('replacement-file')
            path=Path(directory)/(name+'.xml');path.write_bytes(original_file+replacement_file)
            extracted=Path(directory)/(name+'-extracted.xml');extracted.write_bytes(replacement_file)
            source=EPGSource(name=PREFIX+name,source_type='dummy'if name=='source-dummy-to-xml-after-batch'else'xmltv',file_path=str(path));EPGSource.objects.bulk_create([source])
            EPGSourceIndex.objects.create(source=source,data={'channels':{'schedule':[0]},'interleaved_channels':[]})
            target=(EPGData.objects.order_by('-id').values_list('id',flat=True).first()or 0)+10000+ordinal
            entry=None if name=='create-epg-and-programme-after-batch'else EPGData.objects.create(name=PREFIX+name,tvg_id='schedule',epg_source=source)
            target=target if entry is None else entry.pk
            stored=name in ['delete-epg-after-programmes','delete-source-after-programmes','delete-stored-programme-after-batch','source-becomes-null-after-batch','malformed-stored-deleted-before-metadata','malformed-stored-survives-metadata']
            if stored:ProgramData.objects.create(epg=entry,start_time=NOW-dt.timedelta(hours=1),end_time=NOW+dt.timedelta(hours=1),title='stored-before-batch',tvg_id='schedule',custom_properties=[1]if name.startswith('malformed-stored-')else{})
            events=[];dispatch=[];actor_queries=[];fired=False;clock_calls=0;original_index=EPGSource.programme_index.fget
            def actor():
                nonlocal entry
                with CaptureQueriesContext(connection)as actor_capture,patch.object(tasks.timezone,'now',return_value=NOW):
                    if name in ('delete-epg-after-programmes','malformed-stored-deleted-before-metadata'):EPGData.objects.filter(pk=target).delete()
                    elif name=='delete-source-after-programmes':EPGSource.objects.filter(pk=source.pk).delete()
                    elif name=='delete-stored-programme-after-batch':ProgramData.objects.filter(epg_id=target).delete()
                    elif name=='create-epg-and-programme-after-batch':
                        entry=EPGData.objects.create(pk=target,name=PREFIX+name,tvg_id='schedule',epg_source=source)
                        ProgramData.objects.create(epg=entry,start_time=NOW-dt.timedelta(hours=1),end_time=NOW+dt.timedelta(hours=1),title='stored-created-after-batch',tvg_id='schedule',custom_properties={})
                    elif name=='create-programme-after-batch':ProgramData.objects.create(epg=entry,start_time=NOW-dt.timedelta(hours=1),end_time=NOW+dt.timedelta(hours=1),title='stored-created-after-batch',tvg_id='schedule',custom_properties={})
                    elif name=='source-dummy-to-xml-after-batch':EPGSource.objects.filter(pk=source.pk).update(source_type='xmltv')
                    elif name=='source-becomes-null-after-batch':EPGData.objects.filter(pk=target).update(epg_source=None)
                    elif name=='replace-extracted-path-after-batch':EPGSource.objects.filter(pk=source.pk).update(extracted_file_path=str(extracted))
                    elif name=='delete-index-after-batch':EPGSourceIndex.objects.filter(source=source).delete()
                    elif name=='replace-index-after-batch':EPGSourceIndex.objects.filter(source=source).update(data={'channels':{'schedule':[len(original_file)]}})
                    elif name=='atomic-file-replace-after-batch':
                        temporary=path.with_suffix('.incoming');temporary.write_bytes(replacement_file);temporary.replace(path)
                actor_queries.extend(q['sql']for q in actor_capture.captured_queries)
            def execute(execute,sql,params,many,context):
                nonlocal fired
                result=execute(sql,params,many,context)
                if not fired and sql.lstrip().upper().startswith('SELECT')and 'FROM "epg_programdata"'in sql:
                    fired=True;events.append('programmes-statement-complete');events.append('mutation-begin');actor();events.append('mutation-complete')
                elif sql.lstrip().upper().startswith('SELECT')and 'FROM "epg_epgdata"'in sql and 'JOIN "epg_epgsource"'in sql:events.append('metadata-statement-complete')
                return result
            def clock():
                nonlocal clock_calls
                clock_calls+=1;events.append('clock-'+str(clock_calls))
                if clock_calls==2 and name=='replace-index-at-fresh-clock':EPGSourceIndex.objects.filter(source=source).update(data={'channels':{'schedule':[len(original_file)]}})
                return NOW+dt.timedelta(hours=2)if clock_calls==2 and name=='fresh-file-clock-after-global-clock'else NOW
            def fresh_index(instance):events.append('fresh-index-read');return original_index(instance)
            def defer(id):events.append('index-dispatch');dispatch.append(id)
            request=factory.post('/api/epg/current-programs/',{'epg_data_ids':[target]},format='json');force_authenticate(request,user=user)
            match=resolve('/api/epg/current-programs/');failure=None;response=None
            with connection.execute_wrapper(execute),patch.object(tasks.timezone,'now',side_effect=clock),patch.object(EPGSource,'programme_index',new=property(fresh_index)),patch.object(tasks.build_programme_index_task,'delay',side_effect=defer),CaptureQueriesContext(connection)as queries:
                try:response=match.func(request,*match.args,**match.kwargs);response.render()
                except Exception as error:failure={'type':type(error).__name__,'message':str(error)}
            if name=='malformed-stored-survives-metadata':
                if failure is None or failure['type']!='AttributeError' or 'metadata-statement-complete'not in events:raise RuntimeError('Expected original stored serialization exception after metadata')
            elif failure is not None or response is None or response.status_code!=200:raise RuntimeError('Source schedule setup failed for '+name)
            if not fired or not connection.get_autocommit():raise RuntimeError('Source schedule order/transaction setup failed for '+name)
            schedules.append({'name':name,'epg_entry_id':target,'source_id':source.pk,'autocommit':connection.get_autocommit(),'programmes_statement_seen':fired,'events':events,'clock_calls':clock_calls,'dispatch':dispatch,'status':response.status_code if response is not None else None,'body':json.loads(response.content)if response is not None else None,'failure':failure,'queries':[q['sql']for q in queries.captured_queries],'actor_queries':actor_queries})
    # A failed stored serialization must not erase earlier ordered file-lookup
    # effects. Capture actual original metadata order rather than imposing one.
    with tempfile.TemporaryDirectory(prefix='owned-current-error-order-')as directory:
        path=Path(directory)/'guide.xml';path.write_text('<tv/>')
        source=EPGSource(name=PREFIX+'lookup-before-malformed',source_type='xmltv',file_path=str(path));EPGSource.objects.bulk_create([source])
        lookup=EPGData.objects.create(name=PREFIX+'lookup-before-malformed',tvg_id='lookup-first',epg_source=source)
        malformed=EPGData.objects.create(name=PREFIX+'malformed-after-lookup',tvg_id='bad-second',epg_source=source)
        selected=[entry for entry in EPGData.objects.select_related('epg_source').filter(id__in=[lookup.pk,malformed.pk])]
        # Assign fixture roles to the actually encountered original order. Do
        # not sort results, rewrite EPG rows or assume request/insert order.
        lookup,malformed=selected
        ProgramData.objects.create(epg=malformed,start_time=NOW-dt.timedelta(hours=1),end_time=NOW+dt.timedelta(hours=1),title='malformed-after-lookup',custom_properties=[1])
        actual_metadata_ids=list(EPGData.objects.select_related('epg_source').filter(id__in=[lookup.pk,malformed.pk]).values_list('id',flat=True))
        # values_list narrows the query: record full joined object order too.
        actual_full_ids=[entry.pk for entry in EPGData.objects.select_related('epg_source').filter(id__in=[lookup.pk,malformed.pk])]
        events=[];dispatch=[];original_index=EPGSource.programme_index.fget
        def fresh(instance):events.append('fresh-index-read');return original_index(instance)
        def defer(id):events.append('index-dispatch');dispatch.append(id)
        request=factory.post('/api/epg/current-programs/',{'epg_data_ids':[lookup.pk,malformed.pk]},format='json');force_authenticate(request,user=user);match=resolve('/api/epg/current-programs/');failure=None
        with patch.object(tasks.timezone,'now',return_value=NOW),patch.object(EPGSource,'programme_index',new=property(fresh)),patch.object(tasks.build_programme_index_task,'delay',side_effect=defer),CaptureQueriesContext(connection)as queries:
            try:response=match.func(request,*match.args,**match.kwargs);response.render()
            except Exception as error:failure={'type':type(error).__name__,'message':str(error)}
        if actual_full_ids!=[lookup.pk,malformed.pk] or failure is None or failure['type']!='AttributeError' or dispatch!=[source.pk]:
            print(json.dumps({'source_oracle_failure':'serialization fixture order/effects','full_ids':actual_full_ids,'roles':[lookup.pk,malformed.pk],'events':events,'dispatch':dispatch,'failure':failure}),flush=True)
            raise RuntimeError('Original lookup-before-serialization fixture order/effects were not observed')
        serialization_effect_order={'role_assignment':'First/second encountered full metadata rows; no result sorting or EPG-row mutation','epg_entry_ids':[lookup.pk,malformed.pk],'full_metadata_ids':actual_full_ids,'narrow_values_ids':actual_metadata_ids,'events':events,'dispatch':dispatch,'failure':failure,'queries':[q['sql']for q in queries.captured_queries]}
    # Actual no-hook concurrent witness: the source relation blocks only the
    # lazy metadata query. The original programme query does not join sources.
    target=entries[0].pk;original_program=ProgramData.objects.get(epg_id=target)
    original_values={field.attname:getattr(original_program,field.attname)for field in ProgramData._meta.concrete_fields}
    def original_request():
        request=factory.post('/api/epg/current-programs/',{'epg_data_ids':[target]},format='json');force_authenticate(request,user=user);match=resolve('/api/epg/current-programs/');response=match.func(request,*match.args,**match.kwargs);response.render();return response
    with patch.object(tasks.timezone,'now',return_value=NOW):expected=original_request();expected_body=json.loads(expected.content)
    backend=queue.Queue(maxsize=1);finished=queue.Queue(maxsize=1)
    def original_worker():
        close_old_connections()
        try:
            with connection.cursor()as cursor:cursor.execute("SET statement_timeout='10000ms'");cursor.execute('SELECT pg_backend_pid()');backend.put(cursor.fetchone()[0],timeout=1)
            with CaptureQueriesContext(connection)as worker_queries:
                response=original_request()
            finished.put({'status':response.status_code,'body':json.loads(response.content),'queries':[q['sql']for q in worker_queries.captured_queries]},timeout=1)
        except Exception as error:finished.put({'error':type(error).__name__},timeout=1)
        finally:connection.close()
    worker=threading.Thread(target=original_worker,daemon=True);physical={};joined=False
    try:
        with patch.object(tasks.timezone,'now',return_value=NOW):
            with transaction.atomic():
                with connection.cursor()as cursor:cursor.execute('LOCK TABLE epg_epgsource IN ACCESS EXCLUSIVE MODE')
                worker.start();pid=backend.get(timeout=5);deadline=time.monotonic()+5;waiting=None;last_observed=None
                while time.monotonic()<deadline:
                    with connection.cursor()as cursor:
                        # Backend-status snapshots can persist in the actor's
                        # lock-holding transaction; refresh observation only.
                        cursor.execute('SELECT pg_stat_clear_snapshot()')
                        cursor.execute("SELECT query,wait_event_type,wait_event FROM pg_stat_activity WHERE pid=%s",[pid]);row=cursor.fetchone()
                    last_observed=row
                    if row and row[1]=='Lock'and 'FROM "epg_epgdata"'in row[0]and 'JOIN "epg_epgsource"'in row[0]:waiting={'query':row[0],'wait_event_type':row[1],'wait_event':row[2]};break
                    time.sleep(.01)
                if waiting is None:
                    print(json.dumps({'source_oracle_failure':'metadata lock not observed','last_observed':last_observed,'worker_finished':not finished.empty()}),flush=True)
                    raise RuntimeError('Original metadata relation lock was not observed')
                ProgramData.objects.filter(pk=original_program.pk).delete()
                physical={'backend_pid':pid,'waiting':waiting,'deleted_programme_id':original_program.pk,'expected_body':expected_body}
            # Commit releases the actor lock. Do not restore the row until the
            # request finishes, which would otherwise hide a reversed schedule.
            worker.join(timeout=12);joined=not worker.is_alive()
            if not joined:raise RuntimeError('Original request did not finish after lock release')
            physical['response']=finished.get(timeout=1)
            if physical['response'].get('status')!=200 or physical['response'].get('body')!=expected_body:raise RuntimeError('Original did not return cached stored programme after real deletion')
            physical['passed']=True
    finally:
        if worker.ident is not None and not joined:worker.join(timeout=12)
        if worker.is_alive():raise RuntimeError('Original worker still active after bounded lock cleanup')
        if not ProgramData.objects.filter(pk=original_program.pk).exists():ProgramData.objects.bulk_create([ProgramData(**original_values)])
    # SQL ordering is observable. Check that the later schedule fixtures did not
    # change the original initial expectations before any native replay.
    final_replays=[]
    for record in captured:
        request=factory.post('/api/epg/current-programs/',{'epg_data_ids':record['input_ids']},format='json');force_authenticate(request,user=user);match=resolve('/api/epg/current-programs/')
        with patch.object(tasks.timezone,'now',return_value=NOW),CaptureQueriesContext(connection)as replay_queries:response=match.func(request,*match.args,**match.kwargs);response.render()
        body=json.loads(response.content);same=response.status_code==record['status']and body==record['body'];final_replays.append({'name':record['name'],'status':response.status_code,'body':body,'matches_initial':same,'queries':[q['sql']for q in replay_queries.captured_queries]})
        if not same:raise RuntimeError('Source response order changed after schedule fixtures; no native conclusion')
    with connection.cursor()as cursor:
        cursor.execute('SELECT id,ctid::text,pg_column_size(epg_epgdata) FROM epg_epgdata WHERE id=ANY(%s)',[ids]);heap=cursor.fetchall()
    print(json.dumps({'format_version':1,'scope':'Source-only current programme metadata/order/statement sequence; narrow SQL is diagnostic, not native execution','original_authors_credit':'Original Dispatcharr authors and contributors','server_version':server_version,'clock':NOW.isoformat(),'owned_epg_ids':ids,'heap_layout':heap,'cases':captured,'mutation_schedules':schedules,'physical_metadata_lock_witness':physical,'post_schedule_replays':final_replays,'serialization_effect_order':serialization_effect_order,'mutation_scope':'Writes interposed after actual original programme SELECT completes and before original lazy metadata query. Original view/helper bodies unchanged; no independent-process timing claim.','native_verified':False,'performance_verified':False},default=str))
main()
