"""Exact workflow projection with only declared runtime identifiers/time excluded.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
"""
import datetime as dt
import json

def project(data):
    result=[]
    for row in data['imports']:
        events=[]
        for original in row['events']:
            event=json.loads(json.dumps(original))
            detail=event.get('data')
            if isinstance(detail,dict) and 'updated_at' in detail:
                stamp=dt.datetime.fromisoformat(detail['updated_at'])
                if stamp.utcoffset()!=dt.timedelta(0):raise ValueError('UTC event timestamp required')
                detail['updated_at']='<observed-runtime-UTC>'
            events.append(event)
        result.append({'iteration':row['iteration'],'http':row['http'],
                       'completed':sorted((v['task'],v['state'],json.dumps(v['result'],sort_keys=True))for v in row['completed']),
                       'index':row['index'],'current':row['current'],'programmes':row['programmes'],
                       'file_sha256':row['file_sha256'],'events':events})
    if len(result)!=2:raise ValueError('Exactly two imports required')
    return result
