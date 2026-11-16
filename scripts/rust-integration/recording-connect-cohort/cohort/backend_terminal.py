"""Opt-in backend-result dependency observation, never a generic wait override.
Original Dispatcharr authors retain application/algorithm credit.
"""
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from budget import bounded_read
from sd_guide_cases import ActualNativeCeleryFault

class BlockedBackendDependency(Exception):
    pass

def _scope(fixture):
    key=(fixture.case,getattr(fixture,'native',None))
    states=getattr(fixture,'_backend_terminal_observations',None)
    if states is None:
        states={};fixture._backend_terminal_observations=states
    return states.setdefault(key,{'last_health':None,'fault':None})

def _actual_fault(status, health):
    if status!=503 or not isinstance(health,dict):return False
    workers=health.get('selected_workers')
    if not isinstance(workers,dict):return False
    celery=workers.get('celery')
    return workers.get('ready') is False and isinstance(celery,str) and celery.startswith('Fault(')

def _observe(fixture, private, label, task_id=None):
    if fixture.backend!='Native':return
    state=_scope(fixture)
    if state['fault'] is not None:
        error=ActualNativeCeleryFault if task_id is not None else BlockedBackendDependency
        raise error('ActualNativeCommonCeleryFault; evidence='+state['fault'])
    now=time.monotonic()
    if state['last_health'] is not None and now-state['last_health']<1:return
    state['last_health']=now
    try:
        request=urllib.request.Request(fixture.base_url+'/health/ready')
        try:response=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout=fixture.remaining(1))
        except urllib.error.HTTPError as error:response=error
        with response:
            raw=bounded_read(response,65536,lambda maximum:fixture.remaining(min(maximum,1)))
            status=response.status
        if len(raw)>65536:return
        health=json.loads(raw)
    except (OSError,ValueError,RuntimeError):
        return
    if not _actual_fault(status,health):return
    private=Path(private);private.mkdir(parents=True,exist_ok=True)
    path=private/('backend-fault-'+hashlib.sha256(label.encode()).hexdigest()[:16]+'.json')
    path.write_text(json.dumps({'status':status,'body_sha256':hashlib.sha256(raw).hexdigest(),
        'actual_health_body':health,'admitted_task_id':task_id,'label':label,
        'unproduced_dependency':task_id is None},indent=2)+'\n');path.chmod(0o600)
    state['fault']=str(path)
    error=ActualNativeCeleryFault if task_id is not None else BlockedBackendDependency
    raise error('ActualNativeCommonCeleryFault; evidence='+str(path))

def require_backend_dependency(fixture,private,label):
    """Call BEFORE producing a new dependent task; actual Fault means BLOCKED."""
    _observe(fixture,private,label)

def wait_backend_terminal(fixture,terminal,task_id,seconds,label,private):
    """Same fixture wait/cadence/deadline; fail only an admitted missing result."""
    def observed():
        value=terminal(task_id)
        if value is None:_observe(fixture,private,label,task_id)
        return value
    return fixture.wait(observed,seconds,label)
