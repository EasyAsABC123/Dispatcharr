"""Future real HTTP/model compact cells in the existing auto-sync context.
Original Dispatcharr/dependency authors retain application/algorithm credit.
No apps/network/SQL run on import. Run after compact_cases, before held contexts.
"""
from budget import bounded_read
import hashlib,json,traceback,uuid
from pathlib import Path

def save(path,value):Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')

def run(fixture):
    import urllib.request,urllib.error
    if fixture.case!='auto-sync':raise ValueError('ExistingAutoContextRequired')
    setup=fixture._compact;account=setup['account'];relation=setup['relation']
    output=Path(fixture.output)/'compact-api';output.mkdir(exist_ok=False);private=output/'private';private.mkdir(mode=0o700);cells=[]
    path='/api/m3u/accounts/'+str(account)+'/repack-group/'
    target=path+'?channel_group_id='+str(setup['group'])
    def api(method,route,body=None,authenticated=True):
        data=None if body is None else json.dumps(body).encode();headers={}
        if authenticated:headers['Authorization']='Bearer '+fixture.actual_admin_token()
        if data is not None:headers['Content-Type']='application/json'
        request=urllib.request.Request(fixture.base_url+route,data=data,headers=headers,method=method)
        try:response=urllib.request.urlopen(request,timeout=fixture.remaining(15))
        except urllib.error.HTTPError as error:response=error
        with response:
            raw=bounded_read(response,1048576,fixture.remaining)
            record={'method':method,'path':route,'authenticated':authenticated,'status':response.status,'headers':{key:response.headers.get(key)for key in ['content-type','allow','vary','content-length','www-authenticate']},'body_sha256':hashlib.sha256(raw).hexdigest(),'body_bytes':len(raw),'body_text':raw.decode('utf8',errors='replace')}
        # Persist MIME/status/raw body BEFORE trying JSON or asserting a status.
        save(private/('HTTP-'+uuid.uuid4().hex+'.json'),record)
        if len(raw)>1048576:raise RuntimeError('DeclaredCompactHTTPBodyLimit')
        try:record['json']=json.loads(raw)
        except(json.JSONDecodeError,UnicodeDecodeError):record['json']=None
        return record
    def snapshot():
        return fixture.sql("from apps.channels.models import Channel,ChannelOverride;rows=list(Channel.objects.filter(auto_created_by_id="+str(account)+").order_by('id').values('id','name','uuid','channel_number','hidden_from_output','channel_group_id','updated_at'));print(json.dumps({'rows':rows,'overrides':list(ChannelOverride.objects.filter(channel_id__in=[r['id']for r in rows]).order_by('channel_id').values('channel_id','name','channel_number'))},default=str))",secret=True)
    def one(name):
        return next(row for row in snapshot()['rows']if row['name']==name)
    def patch(ident,body):return api('PATCH','/api/channels/channels/'+str(ident)+'/',body)
    def source_lock(action):
        return fixture.sql("from core.utils import acquire_task_lock,release_task_lock,RedisClient;key='task_lock_refresh_single_m3u_account_"+str(account)+"';client=RedisClient.get_client();actual_db=int(client.connection_pool.connection_kwargs.get('db',0));assert actual_db==0;action="+repr(action)+";before=client.get(key);before=before.decode()if isinstance(before,bytes)else before;\nif action=='acquire':assert acquire_task_lock('refresh_single_m3u_account',"+str(account)+")\nelif action=='release':\n assert before=='locked';release_task_lock('refresh_single_m3u_account',"+str(account)+")\nprint(json.dumps({'before':before,'after':client.get(key),'actual_TTL':client.ttl(key),'actual_cache_db':actual_db}))",secret=True)
    def status_and_repack():
        statuses=[]
        for route,expected,detail in [(path,400,'channel_group_id is required'),(path+'?channel_group_id=not-an-int',400,'channel_group_id must be an integer'),(path+'?channel_group_id=999999999999999999999999999999',404,'Group is not associated with this account')]:
            response=api('POST',route,{});statuses.append({'status':response['status'],'json':response['json']});assert response['status']==expected and response['json']=={'detail':detail}
        denied=api('POST',target,{},False);assert denied['status']in(401,403)
        method=api('GET',target);assert method['status']==405
        fixture.sql("from apps.channels.models import Channel;Channel.objects.filter(auto_created_by_id="+str(account)+").update(channel_number=51000);print(json.dumps({'actual_raw_fixture_reassigned':True}))",secret=True)
        before=snapshot();response=api('POST',target,{})
        after=snapshot();save(private/'manual-before-after.json',{'before':before,'after':after})
        assert response['status']==200 and response['json']=={'assigned':3,'released':1,'failed':0}
        assert all(r['channel_number']is None for r in after['rows']if r['hidden_from_output']or r['id']==fixture._compact_special[1])
        assert {r['id']:r['uuid']for r in before['rows']}=={r['id']:r['uuid']for r in after['rows']}
        assert {r['id']:r['updated_at']for r in before['rows']}=={r['id']:r['updated_at']for r in after['rows']}
        return {'actual_result':response['json'],'actual_error_status_bodies':statuses,'observed_unauthenticated_status':denied['status'],'method_status':method['status'],'paired_permission_exact_comparison_required':True,'raw_only_changes_no_timestamp_bump':True}
    def immediate_hide_unhide():
        ident=one('foo')['id'];before=one('foo')
        hidden=patch(ident,{'hidden_from_output':True});after_hide=one('foo')
        shown=patch(ident,{'hidden_from_output':False});after_show=one('foo')
        assert hidden['status']==shown['status']==200
        assert after_hide['channel_number']is None and after_show['channel_number']==30003
        assert hidden['json']['channel_number']is None and shown['json']['channel_number']==30003
        assert before['uuid']==after_hide['uuid']==after_show['uuid']
        return {'actual_immediate_hide_raw':None,'actual_immediate_unhide_raw':30003,'same_request_serializer_refreshed':True,'UUID_preserved':True}
    def held_refresh_and_override_only():
        ident=one('foo')['id'];receipts=[]
        try:
            receipts.append(source_lock('acquire'))
            blocked=api('POST',target,{})
            assert blocked['status']==409 and blocked['json']=={'detail':'An M3U refresh is in progress for this account.'}
            assert patch(ident,{'hidden_from_output':True})['status']==200
            assert patch(ident,{'hidden_from_output':False})['status']==200
            assert one('foo')['channel_number']is None
            assert patch(ident,{'override':{'name':'Owned display pin'}})['status']==200
            assert one('foo')['channel_number']is None
            receipts.append(source_lock('observe'));assert receipts[-1]['after']=='locked'
        finally:receipts.append(source_lock('release'))
        assert patch(ident,{'override':None})['status']==200
        assert one('foo')['channel_number']is None # override-only never model.save
        response=patch(ident,{'name':'foo'});assert response['status']==200 and one('foo')['channel_number']==30003
        save(private/'actual-lock-receipts.json',receipts)
        return {'actual_manual_lock_409':True,'actual_unhide_deferred_while_Source_lock_held':True,'unrelated_lock_not_deleted':True,'override_only_no_model_signal':True,'subsequent_scalar_save_assigned':True}
    def pinned_signal():
        ident=fixture._compact_special[1]
        for hidden in(True,False):
            response=patch(ident,{'hidden_from_output':hidden});assert response['status']==200
            assert response['json']['channel_number']is None and response['json']['effective_channel_number']==30002
        return {'override_preserved':True,'pinned_unhide_never_assigns_raw':True}
    def source_atomic_rollback():
        ident=one('foo')['id'];nonce=uuid.uuid4().hex;function='owned_compact_fail_'+nonce;sequence='owned_compact_seq_'+nonce
        # A controlled real PG trigger fails AFTER the actual model signal. A
        # nontransactional sequence proves the trigger observed assigned raw
        # number inside the doomed outer transaction; no fabricated trace/result.
        ddl="from django.db import connection;from apps.channels.models import Channel;assert connection.settings_dict['NAME']=="+repr(fixture.database)+";Channel.objects.filter(pk="+str(ident)+").update(channel_number=None);\nwith connection.cursor()as c:\n c.execute('CREATE SEQUENCE "+sequence+"');c.execute(\"CREATE FUNCTION "+function+"() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE n double precision; BEGIN SELECT channel_number INTO n FROM dispatcharr_channels_channel WHERE id="+str(ident)+";IF n IS NULL THEN RAISE EXCEPTION 'owned assignment stage not reached';END IF;PERFORM nextval('"+sequence+"');RAISE EXCEPTION 'owned compact after-save failure';END; $$\");c.execute('CREATE TRIGGER "+function+" BEFORE INSERT OR UPDATE ON dispatcharr_channels_channeloverride FOR EACH ROW WHEN(NEW.channel_id="+str(ident)+") EXECUTE FUNCTION "+function+"()');print(json.dumps({'owned_trigger_installed':True}))"
        installed=False
        try:
            fixture.sql(ddl,secret=True);installed=True;before=snapshot()
            response=patch(ident,{'name':'Owned attempted rollback','override':{'name':'Owned forced failure'}})
            proof=fixture.sql("from django.db import connection;\nwith connection.cursor()as c:\n c.execute('SELECT is_called FROM "+sequence+"');print(json.dumps({'actual_assignment_observed_before_failure':c.fetchone()[0]}))",secret=True)
            after=snapshot();save(private/'actual-rollback-proof.json',{'response':response,'proof':proof,'before':before,'after':after})
            assert response['status']==500 and proof['actual_assignment_observed_before_failure']is True and before==after
            released=source_lock('observe');assert released['after']is None
            return {'actual_SQL_failure_status':500,'assignment_occurred_before_trigger':True,'outer_row_override_time_rolled_back':True,'shared_lock_released_before_outer_rollback':True}
        finally:
            if installed:fixture.sql("from django.db import connection;\nwith connection.cursor()as c:\n c.execute('DROP TRIGGER "+function+" ON dispatcharr_channels_channeloverride');c.execute('DROP FUNCTION "+function+"()');c.execute('DROP SEQUENCE "+sequence+"');print(json.dumps({'owned_trigger_sequence_removed':True}))",secret=True)
    def noncompact_manual():
        fixture.sql("from apps.channels.models import ChannelGroupM3UAccount,Channel;r=ChannelGroupM3UAccount.objects.get(pk="+str(relation)+");p=dict(r.custom_properties);p['compact_numbering']=False;ChannelGroupM3UAccount.objects.filter(pk=r.pk).update(custom_properties=p);Channel.objects.filter(auto_created_by_id="+str(account)+").update(channel_number=52000);print(json.dumps({'actual_compact_flag_off':True}))",secret=True)
        response=api('POST',target,{});assert response['status']==200 and response['json']=={'assigned':3,'released':1,'failed':0}
        return {'manual_one_shot_works_when_compact_flag_off':True,'actual_result':response['json']}
    for name,body in [('actual-manual-POST-statuses-errors-raw-pack',status_and_repack),('actual-PATCH-immediate-hide-unhide',immediate_hide_unhide),('Source-lock-held-defer-override-only-no-signal',held_refresh_and_override_only),('pinned-override-model-signal',pinned_signal),('actual-parent-atomic-rollback-after-unhide',source_atomic_rollback),('manual-repack-with-compact-disabled',noncompact_manual)]:
        row={'name':name,'status':'running'};cells.append(row);save(output/'report.json',{'status':'running','cases':cells})
        try:row.update(status='passed',actual=body())
        except BaseException as error:row.update(status='failed',error_type=type(error).__name__);(private/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
        save(output/'report.json',{'status':'running','cases':cells})
    report={'status':'passed'if all(r['status']=='passed'for r in cells)else'failed','cases':cells,'new_Source_characterization_required':True,'performance_verified':False,'remaining_required':['bulk-edit full endpoint','OPTIONS serializer metadata','physical held cache/SQL uncertain shutdown','paired exact permission/header/HTML comparison','noncanonical numeric/error domains']};save(output/'report.json',report);return report
