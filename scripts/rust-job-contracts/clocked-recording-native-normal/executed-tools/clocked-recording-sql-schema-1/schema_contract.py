"""Literal prepare-only acceptance, never DVR task/media/Source parity authority."""
import math

def require(ok,label):
 if not ok:raise ValueError(label)
def validate(raw,expected):
 require(type(raw)is dict and raw.get('passed')is True,'ActualProbePass')
 require(raw.get('schema_only')is True and raw.get('DML_executed')is False and raw.get('task_or_media_executed')is False and raw.get('actual_readonly')=='on','ExactPrepareOnlyReadonlyScope')
 require(raw.get('ready')=='Ok(())'and raw.get('closed')=='Ok(())','ActualSameConnectionReadyAndClose')
 rows=raw.get('statements');require(type(rows)is list and len(rows)==len(expected)==29,'All29CompiledStatementOccurrences')
 for index,(actual,authority)in enumerate(zip(rows,expected)):
  require(type(actual)is dict and type(actual.get('index'))is int and actual['index']==index and actual.get('source')==authority['source']and actual.get('sql')==authority['sql'],'ExactCompiledStatementOrderAndBytes')
  require(actual.get('prepared')is True and'error'not in actual,'EveryActualPrepareSucceeded')
  types=actual.get('provided_parameter_types');columns=actual.get('columns');require(types==authority['bound_types'],'ExactActualBindTypeAuthority')
  require(type(columns)is list and len(columns)==authority['column_count'] and all(type(c)is dict and set(c)=={'name','type'}and all(type(c[k])is str and 0<len(c[k])<=256 for k in c)for c in columns),'RetainedActualResultColumnTypes')
 return {'schema_verified':True,'prepared_statement_occurrences':29,'DVR_normal_verified':False,'Source_verified':False,'task_media_or_transaction_execution_verified':False}
