"""Missing Source domains inside the ONE milestone cohort.
Called by the common owned Source harness, never by import/export. Uses real
models, signal publication and Celery worker; ctx owns bounded observations,
request routing, waits and stop/cleanup. No application task method replacement.
"""
import json
from pathlib import Path
REQUIRED_CONTEXT_METHODS=('assert_copied_owned_context','assert_empty_plugins_and_integrations','capture','persist_partial','start_original_worker','consume_until_postrun','enqueue_actual','configure_owned_version_response','install_owned_insert_trigger','remove_owned_insert_trigger','stop_and_collect_original_worker','actual_natural_task_id')

def execute(ctx, *, versions_only=False):
    for method in REQUIRED_CONTEXT_METHODS:
        if not callable(getattr(ctx,method,None)):raise ValueError('MissingCommonHarnessBinding:'+method)
    ctx.assert_copied_owned_context();ctx.assert_empty_plugins_and_integrations()
    from apps.epg.models import EPGSource,EPGData
    from apps.channels.models import Channel
    from core.models import SystemNotification
    # Normal producer import registers the shared task in this management process.
    from core import tasks as core_tasks
    if core_tasks.check_for_version_update.name != "core.tasks.check_for_version_update":raise ValueError("ActualVersionTaskName")
    from version import __version__,__timestamp__
    # This body is pinned to actual archive version facts, not a patched task.
    if __version__!='0.33.0' or __timestamp__ is not None:raise ValueError('DeclaredStableVersionContextRequired')
    report={'passed':False,'application_task_methods_replaced':False,'cases':[], 'all_raw_before_after_retained':True,'scope':'new per-channel worker + owned version200 domains only; no repeat Source30/61 capture'}
    if not versions_only:
        root=Path(ctx.owned_input_directory);root.mkdir(exist_ok=False)
        xml=root/'milestone.xml';xml.write_bytes(ctx.predeclared_xmltv_bytes)
        if len(ctx.predeclared_xmltv_bytes)>16384 or not ctx.predeclared_xmltv_bytes.rstrip().endswith(b'</tv>'):raise ValueError('BoundedCompleteLocalInput')
        source=EPGSource.objects.create(name=ctx.prefix+'-xmltv',source_type='xmltv',file_path=str(xml),is_active=False,refresh_interval=0)
        epg=EPGData.objects.create(epg_source=source,tvg_id='m6.batch.owned',name='Owned M6 Channel')
        # Natural real Channel post_save queues parse_programs_for_tvg_id. No direct
        # task invocation substitutes for this first signal-produced task.
        channel=Channel.objects.create(name='Owned M6 Channel',channel_number=ctx.channel_number,epg_data=epg)
        before_worker=ctx.capture();ctx.persist_partial('after-real-model-signal',before_worker)
    worker=ctx.start_original_worker(queues=['celery'])
    try:
        def run(name,task=None,args=(),expected=None,natural=False):
            before=ctx.capture();ctx.persist_partial(name+'-before',before)
            task_id=ctx.actual_natural_task_id(before,'apps.epg.tasks.parse_programs_for_tvg_id')if natural else ctx.enqueue_actual(task,args=list(args),kwargs={})
            actual=ctx.consume_until_postrun(worker,task_id);after=ctx.capture()
            row={'name':name,'task_id':task_id,'before':before,'actual':actual,'after':after,'expected_declared':expected}
            report['cases'].append(row);ctx.persist_partial(name+'-actual',row)
            # Always collect before comparison; worker/state errors remain literal.
            return row
        if not versions_only:
            run('tvg-real-channel-signal',expected={'status':'SUCCESS','result':None},natural=True)
            run('tvg-repeat','apps.epg.tasks.parse_programs_for_tvg_id',[epg.pk],{'status':'SUCCESS','result':None})
            missing=ctx.predeclared_missing_epg_id
            if EPGData.objects.filter(pk=missing).exists():raise ValueError('OwnedMissingIDIsNotMissing')
            run('tvg-missing','apps.epg.tasks.parse_programs_for_tvg_id',[missing],{'status':'SUCCESS','result':'EPG not found'})
            dummy_source=EPGSource.objects.create(name=ctx.prefix+'-dummy',source_type='dummy',is_active=False,refresh_interval=0)
            dummy=EPGData.objects.create(epg_source=dummy_source,tvg_id='m6.batch.dummy',name='Owned Dummy')
            run('tvg-dummy','apps.epg.tasks.parse_programs_for_tvg_id',[dummy.pk],{'status':'SUCCESS','result':None})
            unmapped=EPGData.objects.create(epg_source=source,tvg_id='m6.batch.unmapped',name='Owned Unmapped')
            run('tvg-unmapped','apps.epg.tasks.parse_programs_for_tvg_id',[unmapped.pk],{'status':'SUCCESS','result':None})
            ctx.install_owned_insert_trigger(table='epg_programdata',sqlstate='P0001',message='owned M6 programme insert')
            try:run('tvg-insert-error','apps.epg.tasks.parse_programs_for_tvg_id',[epg.pk],{'status':'FAILURE','SQLSTATE':'only when actually observed; declaration is not measured code'})
            finally:ctx.remove_owned_insert_trigger()
        # No task method/version constant is replaced. Only the already qualified
        # requests dependency is routed to an owned fixed-response endpoint.
        SystemNotification.objects.create(notification_key='version-0.33.0',notification_type='version_update',title='Owned obsolete notification',message='Owned obsolete notification',is_active=True)
        for label,payload in [('no-update',{'tag_name':'v0.33.0','html_url':'https://owned.invalid/unchanged'}),('update',{'tag_name':'v0.34.0','html_url':'https://owned.invalid/update'}),('repeat',{'tag_name':'v0.34.0','html_url':'https://owned.invalid/update'}),('malformed',{'tag_name':'not-a-version','html_url':'https://owned.invalid/invalid'})]:
            ctx.configure_owned_version_response(status=200,body=json.dumps(payload,separators=(',',':')).encode())
            run('version200-'+label,'core.tasks.check_for_version_update',expected={'status':'SUCCESS','result':None})
        report['capture_complete']=True
    finally:
        report['worker_stop_actual']=ctx.stop_and_collect_original_worker(worker)
        ctx.persist_partial('Source-extension-complete-or-negative',report)
    # ctx's whole-batch acceptor validates all raw observations, no direct flags.
    return report
