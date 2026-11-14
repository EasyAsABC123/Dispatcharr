"""Pure owned-role admission; metadata only, never process settlement authority."""
from pathlib import Path
def unexpected(argv):
 if not isinstance(argv,list)or not argv or any(type(v)is not str for v in argv):raise ValueError('ActualArgvRequired')
 return any('uwsgi'in Path(v).name for v in argv)or('celery'in' '.join(argv)and any(v in ('worker','beat')for v in argv))
def validate(report):
 if report.get('passed')is not True:raise ValueError('RolePreflightActualPassRequired')
 rows=report.get('rows')
 if not isinstance(rows,list)or not 1<=len(rows)<=256:raise ValueError('BoundedNonemptyRoleRows')
 if len({v['pid']for v in rows})!=len(rows)or any(type(v['pid'])is not int or v['pid']<=0 for v in rows):raise ValueError('ActualUniquePIDs')
 bad=[v for v in rows if unexpected(v['argv'])]
 if bad or report.get('unexpected_web_worker_beat')!=[]:raise ValueError('NoUnownedWebWorkerBeat')
 if type(report.get('own_pid'))is not int or report['own_pid']not in{v['pid']for v in rows}:raise ValueError('ActualPreflightPIDPresent')
 return True
