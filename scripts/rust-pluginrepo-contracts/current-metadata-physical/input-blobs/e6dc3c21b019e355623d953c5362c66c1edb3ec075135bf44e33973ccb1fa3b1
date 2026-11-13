"""Full independent raw-PG/Redis before/after, source-existing seed context."""
import copy
from validate_physical import need,exact,imported_rows
from validate_reference import own_cache,cache
FIELDS=['repos','configs','settings','users','events','system_notifications','notification_dismissals']
def qualify(source,before,after,case,rust):
 need(source['actual_request_count']==24 and len(source['requests'])==24,'OriginalSource24NotReplayed')
 a,b=before['public_manifest'],after['public_manifest'];need(len(a['tables'])==len(b['tables'])==62 and len(a['sequences'])==len(b['sequences'])==59,'ActualFull62Tables59Sequences')
 expected=copy.deepcopy(a)
 if case in(1,2):
  expected['tables']['plugins_pluginrepo']=b['tables']['plugins_pluginrepo']
  if case==2:expected['tables']['plugins_pluginconfig']=b['tables']['plugins_pluginconfig']
 need(exact(expected,b),'AllFullManifestFieldsForeignTablesAnd59SequencesUnchanged')
 for field in FIELDS:
  source_table={'repos':'plugins_pluginrepo','configs':'plugins_pluginconfig','settings':'core_coresettings','users':'accounts_user','events':'core_systemevent','system_notifications':'core_systemnotification','notification_dismissals':'core_notificationdismissal'}[field];imported_rows(source_table,source['seed'][field],before[field],source['seed']['public_manifest']['columns'])
  table={'repos':'plugins_pluginrepo','configs':'plugins_pluginconfig','settings':'core_coresettings','users':'accounts_user','system_notifications':'core_systemnotification','notification_dismissals':'core_notificationdismissal'}.get(field)
  if table:need(exact(after[field],rust['after']['tables'][table]),'IndependentRawPGSameAsActualRustSnapshot:'+field)
  if field not in('repos','configs'):need(exact(before[field],after[field]),'NoAuthHistoryOrForeignRowEffect:'+field)
 own_cache(before['raw_Redis'],after['raw_Redis']);cache(source['seed']['raw_Redis'],before['raw_Redis']);serialized=cache(source['requests'][2]['after']['raw_Redis'],after['raw_Redis'])
 return {'passed':True,'actual_case':case,'full_default_audit_scope':'separately executed full62/59 +named11','owned_full62_59_foreign_and_all_sequences_unchanged':True,'exact_imported_existing_Source5_keys':True,'request_induced_decoded_group_and_unrelated_Redis_scope':True,'cache_serialization_byte_equal':serialized,'Main_or_Source_replay':False}

def validate_import(source,state):
 """Before ANY Native peer: exact imported Source rows/59 sequences/5 keys."""
 need(source['actual_request_count']==24 and len(source['requests'])==24,'UntouchedCapturedSource24')
 mapping={'repos':'plugins_pluginrepo','configs':'plugins_pluginconfig','settings':'core_coresettings','users':'accounts_user','events':'core_systemevent','system_notifications':'core_systemnotification','notification_dismissals':'core_notificationdismissal'};ledger=[]
 for field,table in mapping.items():ledger+=imported_rows(table,source['seed'][field],state[field],source['seed']['public_manifest']['columns'])
 need(exact(state['public_manifest']['sequences'],source['seed']['public_manifest']['sequences'])and len(state['public_manifest']['sequences'])==59,'ExactlyImportedAll59SequenceStates')
 cache(source['seed']['raw_Redis'],state['raw_Redis'])
 return {'passed':True,'before_any_Native_peer':True,'physical_seed_overlays':False,'source_row_fields_and_all59seq_and_existing5Redis_keys_qualified':True,'SQL_timestamp_lexical_differences_same_UTC_instant':ledger}
