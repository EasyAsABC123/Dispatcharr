"""Finite candidate Main normal-Bearer requests; no application work on import."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import socket
import time
from absolute_http import Observation
from qualify import validate


def request(base,address,path,headers,method,payload,deadline):
    seconds=min(8,deadline-time.monotonic())
    if seconds<=0:raise TimeoutError('NotificationPhaseDeadline')
    owner=Observation(base,seconds=seconds,address=address)
    row={'started':time.monotonic(),'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        raw=None if payload is None else json.dumps(payload,separators=(',',':')).encode()
        response=owner.request(path,headers,method,body=raw)
        row['status']=response.status_code;row['headers']=[list(x)for x in response.raw.getheaders()]
        data=response.raw.read(65537)
        if len(data)>65536:raise ValueError('NotificationResponseBodyCapacity')
        row.update(body_hex=data.hex(),body_sha256=hashlib.sha256(data).hexdigest(),absolute_observation=owner.complete())
    except Exception as error:row['transport_error']=type(error).__name__
    finally:
        row['physical_close']=owner.cancel();row['completed_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();row['completed']=time.monotonic()
    return row


def execute(seed,policy,base,address,snapshot,output,requester=request):
    output=Path(output);deadline=time.monotonic()+90
    value={'phase_started':time.monotonic(),'cases':[],'owned_notification_key':seed['owned_notification_key'],'admin_user_id':seed['admin_user_id'],'standard_user_id':seed['standard_user_id'],'actual_SQL_Bearer_roles_verified':False}
    def save():
        data=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode()
        if len(data)>8388608:raise ValueError('NotificationPhaseEvidenceCapacity')
        p=output/'notification-Main-private.json';p.write_bytes(data);p.chmod(0o600)
    value['actual_auth_requests']={}
    for role in ('admin','standard'):
        headers={'Authorization':'Bearer '+seed[role+'_token'],'Content-Type':'application/json'}
        row=requester(base,address,'/api/accounts/users/me/',headers,'GET',None,deadline);value['actual_auth_requests'][role]=row;save()
        if row.get('status')!=200 or json.loads(bytes.fromhex(row['body_hex']))['id']!=seed[role+'_user_id']:raise ValueError('ActualSQLBearerRole:'+role)
    value['actual_SQL_Bearer_roles_verified']=True
    notice_id=None
    for spec in policy['cases']:
        name=spec['name'];role='admin';method='POST';path='/api/core/notifications/'
        body={'notification_key':seed['owned_notification_key'],'title':'Hello','message':'World','is_active':True}
        if name.startswith('standard-')and name!='standard-create-denied':
            role='standard';path+=str(notice_id)+'/dismiss/';body={'action_taken':'accepted'}if name=='standard-truthy-repeat'else{}
        if name=='standard-create-denied':role='standard'
        if name in ('required-put','empty-patch','cascade-delete'):
            method={'required-put':'PUT','empty-patch':'PATCH','cascade-delete':'DELETE'}[name];path+=str(notice_id)+'/';body={}if method!='DELETE'else None
        before=snapshot()
        row=requester(base,address,path,{'Authorization':'Bearer '+seed[role+'_token'],'Content-Type':'application/json'},method,body,deadline)
        row.update(name=name,method=method,role=role,before=before,after=snapshot());value['cases'].append(row);save()
        if name=='admin-create'and row.get('status')==201:notice_id=json.loads(bytes.fromhex(row['body_hex']))['id']
        # Status influences only the required ID extraction, never alternate inputs/retries.
        if notice_id is None:raise ValueError('NoSQLLinkedCreatedNoticeForFixedSubsequentPaths')
    value['phase_completed']=time.monotonic()
    try:value['qualification']=validate(value,policy)
    except Exception as error:value['qualification']={'normal_Bearer_Main_notification_slice_qualified':False};value['error']=type(error).__name__+':'+str(error)[:256]
    save();return value


def main():
    if os.environ.get('NOTIFICATION_MAIN_FIXTURE')!='notification-fixture-only':raise RuntimeError('OwnedNotificationMainFixture')
    from db import setup,snapshot
    setup()
    from django.db import connections
    from urllib.parse import urlsplit
    root=Path('/evidence');seed=json.loads((root/'notification-seed-private.json').read_text());policy=json.loads(Path('/notification-root/policy.json').read_text());base='http://candidate:9192';address=socket.gethostbyname(urlsplit(base).hostname)
    try:result=execute(seed,policy,base,address,snapshot,root)
    finally:connections.close_all()
    raise SystemExit(0 if result['qualification']['normal_Bearer_Main_notification_slice_qualified']else 1)


if __name__=='__main__':main()
