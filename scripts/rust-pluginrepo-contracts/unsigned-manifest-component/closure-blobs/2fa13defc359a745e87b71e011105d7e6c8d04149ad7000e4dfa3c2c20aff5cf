"""Finite pinned helper variable coverage; no shell execution/application imports."""
import hashlib,re
from pathlib import Path
FIXED={'DISPATCHARR_ENV':'aio','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PUID':'1000','PGID':'1000','PYTHONDONTWRITEBYTECODE':'1','LANG':'C.UTF-8','LC_ALL':'C.UTF-8','TZ':'UTC','DISPATCHARR_TIME_ZONE':'UTC'}
DIRECT={'DISPATCHARR_ENV','POSTGRES_DB','POSTGRES_DIR','POSTGRES_HOST','POSTGRES_PASSWORD','POSTGRES_PORT','POSTGRES_USER'}
WRAPPER={'PG_VERSION','PG_BINDIR'}
def need(x,label):
 if not x:raise ValueError(label)
def validate_environment(environment):
 need(type(environment)is dict and environment==FIXED,'ExactStorageOnlyEnvironmentAllRequiredValues')
 return True
def required_variables(text):
 code='\n'.join(x for x in text.splitlines()if not x.lstrip().startswith('#'))
 refs={a or b for a,b in re.findall(r'(?<!\\)\$(?:\{([A-Za-z_][A-Za-z_0-9]*)[^}]*\}|([A-Za-z_][A-Za-z_0-9]*))',code)}
 assignments=set(re.findall(r'(?m)^\s*(?:(?:export|local)\s+)?([A-Za-z_][A-Za-z_0-9]*)=',code));assignments|=set(re.findall(r'\bfor\s+([A-Za-z_][A-Za-z_0-9]*)\s+in\b',code));return refs,assignments
def audit_helpers(root,environment):
 root=Path(root);validate_environment(environment);rows={};all_refs=set();all_assignments=set()
 for name in ['01-user-setup.sh','02-postgres.sh']:
  path=root/'inputs/pg-init'/name;raw=path.read_bytes();refs,assignments=required_variables(raw.decode());rows[name]={'sha256':hashlib.sha256(raw).hexdigest(),'all_executable_line_variable_references':sorted(refs),'assigned_or_loop_local_names':sorted(assignments)};all_refs|=refs;all_assignments|=assignments
 external=all_refs-all_assignments;need(external==DIRECT|WRAPPER,'AllPinnedSourceHelperExternalVariablesClassified');need(DIRECT<=set(environment),'AllCallerRequiredVariablesProvided')
 start=(root/'controller/start.sh').read_text();need('export PG_VERSION' in start and '[ "$PG_VERSION" = 17 ]'in start and 'export PG_BINDIR='in start,'ActualPG17WrapperAssignments');need(start.index('export PG_BINDIR=')<start.index('. /pg-bootstrap/02-postgres.sh'),'PGVariablesAssignedBeforeHelper02');need(start.index('FixtureRequiredEnvironment')<start.index('. /pg-bootstrap/01-user-setup.sh'),'AllRequiredEnvironmentGuardBeforeAnyHelper')
 return {'passed':True,'kind':'Static source-helper caller-variable coverage, NOT dynamic shell branch execution','helpers':rows,'external_required':sorted(external),'caller_environment_supplied':sorted(DIRECT),'wrapper_assigned_before_helper02':sorted(WRAPPER),'helper_assigned_local_or_defaulted':sorted(all_assignments),'Django_or_app_import':False}
def storage_environment():return FIXED.copy()
