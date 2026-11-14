"""Actual worker settlement authority shared by guest and acceptor."""
import re
def valid_worker(row):
 required={'actual_exit','actual_wait_completed','pipes_settled','forced','invalid','starttime','parent_PID_namespace','worker_PID_namespace','same_PID_namespace_as_observer','known_PID_generations'}
 if type(row)is not dict or not required<=set(row):return False
 if type(row['actual_exit'])is not int or row['actual_exit']!=0:return False
 if row['actual_wait_completed']is not True or row['pipes_settled']is not True or row['forced']is not False or row['invalid']is not None:return False
 if type(row['starttime'])is not str or not row['starttime'].isdigit()or int(row['starttime'])<=0:return False
 namespace=row['parent_PID_namespace']
 if type(namespace)is not str or re.fullmatch(r'pid:\[[0-9]+\]',namespace)is None or row['worker_PID_namespace']!=namespace or row['same_PID_namespace_as_observer']is not True:return False
 known=row['known_PID_generations']
 if type(known)is not list or not 1<=len(known)<=256:return False
 for p in known:
  if type(p.get('pid'))is not int or p['pid']<=0 or type(p.get('starttime'))is not str or not p['starttime'].isdigit()or int(p['starttime'])<=0 or p.get('original_absent')is not True:return False
 return True
