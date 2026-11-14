"""Pinned migration-owned built-in context; read-only correlated raw SQL rows."""
import hashlib,json
TABLES=['m3u_m3uaccount','m3u_m3uaccountprofile','django_celery_beat_periodictask','django_celery_beat_intervalschedule']
def need(v,label):
 if not v:raise ValueError(label)
def positive(v):return type(v)is int and 0<v<2**63
def validate(context,audit):
 need(context['SQL_read_only']is True and context['database']==audit['database']and set(context['table_rows'])==set(TABLES),'ExactReadonlyBuiltinProjectionCoverage')
 rows={}
 for name in TABLES:
  raw=context['table_rows'][name];need(type(raw)is list and len(raw)==1 and type(raw[0])is list and len(raw[0])==1 and type(raw[0][0])is dict,'ExactlyOneMigrationRow:'+name)
  need(audit['tables'][name]['rows']==1 and hashlib.sha256(json.dumps(raw,sort_keys=True,default=str,ensure_ascii=False).encode()).hexdigest()==audit['tables'][name]['sha256'],'SameReadFullSQLFingerprintCorrelation:'+name);rows[name]=raw[0][0]
 need(audit['tables']['accounts_user']['rows']==0 and audit['tables']['vod_movie']['rows']==0,'NoUserOrMovieRows')
 a,p,t,i=[rows[name]for name in TABLES]
 need(all(positive(r.get('id'))for r in [a,p,t,i]),'ActualPositiveBuiltinIDs')
 need(a['name']=='custom'and a['locked']is True and a['is_active']is True and type(a['max_streams'])is int and a['max_streams']==0,'Pinned0003AccountFields')
 need(a['server_url']is None and a['file_path']is None and a['username']is None and a['password']is None,'ModelDefaultNoConfiguredExternalURLFileOrCredentials')
 need(p['m3u_account_id']==a['id']and p['name']=='custom Default'and p['is_default']is True and p['is_active']is True and type(p['max_streams'])is int and p['max_streams']==0 and p['search_pattern']=='^(.*)$'and p['replace_pattern']=='$1','Pinned0003DefaultProfileFieldsAndFK')
 need(a['refresh_task_id']==t['id']and t['name']=='m3u_account-refresh-'+str(a['id'])and t['task']=='apps.m3u.tasks.refresh_single_m3u_account'and json.loads(t['kwargs'])=={'account_id':a['id']}and t['interval_id']==i['id'],'Pinned0006ActualTaskFKAndPayload')
 need(type(i['every'])is int and i['every']==24 and i['period']=='hours','Pinned0006Interval')
 return {'passed':True,'guard':'exact single migration-owned custom account/profile/task/interval, unchanged','account_active':True,'profile_active':True,'locked':True,'configured_external_URL':False,'SQL_projection_matches_full_audit':True,'dynamic_ids_and_dates':'retained actual SQL; no forced IDs or expected timestamps','periodic_task_enabled':'retained literal only; no disabled assumption or worker launch','row_or_task_mutations':0}
