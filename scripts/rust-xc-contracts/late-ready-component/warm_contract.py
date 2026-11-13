"""Actual same-backend statement-cache evidence, not synthetic SQL execution."""
import hashlib

def need(ok,label):
 if not ok:raise ValueError(label)
def target(snapshot,producer,first_parameter_hex):
 events=snapshot['events'];rows=[x for x in events if x['kind']=='execute'and type(x.get('sql'))is str and x['sql'].startswith(producer['sql_prefix'])]
 need(len(rows)==1,'OneActualPreludeTargetExecute');row=rows[0];params=row['parameter_hex'];need(type(params)is list and len(params)==producer['parameter_count']and params[0]==first_parameter_hex,'ActualPreludeParameterBinding')
 need(row['arm_epoch']==0 and row['execute_armed']is False and type(row['execute_generation'])is int and row['execute_generation']>0,'PreludeBeforeArm')
 ready=[x for x in events if x['kind']=='ready-observed'and x['connection']==row['connection']and x.get('sql')==row['sql']and x['monotonic']>=row['monotonic']]
 need(len(ready)==1 and ready[0]['errors']==[]and type(ready[0]['backend_pid'])is int and ready[0]['backend_pid']>0,'ActualPreludeTargetReadySuccess')
 need(snapshot['errors']==[]and snapshot['held']is None,'NoPreludeHeldOrInvalidReply')
 return {'connection':row['connection'],'backend_pid':ready[0]['backend_pid'],'execute_generation':row['execute_generation'],'execute_monotonic':row['monotonic'],'ready_monotonic':ready[0]['monotonic'],'sql_sha256':hashlib.sha256(row['sql'].encode()).hexdigest(),'parameter_count':len(params),'first_parameter_hex':first_parameter_hex,'before_arm':True}
