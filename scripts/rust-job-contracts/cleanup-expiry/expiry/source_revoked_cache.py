"""Pinned Celery revoked-set implementation/factory witness, not worker execution.
Celery authors retain source/protocol/license credit. No Django, DB or network.
"""
import hashlib,inspect,json,time
from celery.utils.collections import LimitedSet
from celery.worker import state
names=['__init__','add','__contains__','purge','pop']
source={n:inspect.getsource(getattr(LimitedSet,n))for n in names}
cases=[];base=time.monotonic()
for label,items,purge in [('insertion-capacity',[('a',1),('b',2),('c',3),('d',4)],None),('old-membership-before-purge',[('a',1)],None),('explicit-purge',[('a',1)],20),('repeat-update',[('a',1),('b',2),('a',4),('c',5),('d',6)],None)]:
 value=LimitedSet(maxlen=3,expires=10,minlen=0)
 for key,now in items:value.add(key,now=base+now)
 before={key:key in value for key in ['a','b','c','d']}
 if purge is not None:value.purge(now=base+purge)
 cases.append({'name':label,'items':items,'explicit_purge_now':purge,'membership_before_purge':before,'membership_after':{key:key in value for key in ['a','b','c','d']},'length':len(value)})
print(json.dumps({'scope':'Pinned framework implementation + explicit-clock LimitedSet factory witness; not actual worker elapsed retention','revoked_default':{'maxlen':state.revoked.maxlen,'expires':state.revoked.expires,'minlen':state.revoked.minlen},'methods':source,'method_sha256':{n:hashlib.sha256(v.encode()).hexdigest()for n,v in source.items()},'cases':cases},indent=2))
