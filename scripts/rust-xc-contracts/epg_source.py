"""Actual original xc_get_epg corpus; no replacement algorithm. Upstream credit."""
import hashlib,json,os,subprocess
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
from django.http import QueryDict
from django.test import RequestFactory
from apps.accounts.models import User
from apps.output import views

def specifications(channels):
    rows=[]
    for role in ['viewer','standard','admin','limited','inactive']:
        for index in [0,1,2,3,5,6,7,8,9]:
            for short in [True,False]:
                for limit in [None,'0','1','-1','bad','١','1_0']:
                    pairs=[('stream_id',str(channels[index]))]
                    if limit is not None:pairs.append(('limit',limit))
                    rows.append({'role':role,'short':short,'params':pairs})
        target=str(channels[0]);arabic=''.join(chr(0x660+int(char))for char in target)
        for raw in [None,'','bad','99999999999999999999','-1',' '+target+' ','+'+target,'00'+target,arabic]:
            for short in [True,False]:rows.append({'role':role,'short':short,'params':[]if raw is None else [('stream_id',raw)]})
        for pairs in [[('stream_id',target),('days','1'),('prev_days','1')],[('stream_id',target),('days','0'),('prev_days','1')],[('stream_id',target),('stream_id','bad')]]:
            for short in [True,False]:rows.append({'role':role,'short':short,'params':pairs})
    return rows

def capture(users,channels,clock,start=0,count=32):
    assert 0<=start<10000 and 1<=count<=64
    now=datetime.fromisoformat(clock)
    cases=specifications(channels);result=[]
    for spec in cases[start:start+count]:
        user=User.objects.get(pk=users[spec['role']]['id'])
        request=RequestFactory().get('/player_api.php',HTTP_HOST='xc.fixture.invalid:8088')
        params=QueryDict('',mutable=True)
        for key,value in spec['params']:params.appendlist(key,value)
        request.GET=params
        row={**spec,'user_id':user.pk,'now':now.isoformat()}
        with patch.object(views.django_timezone,'now',return_value=now):
            try:value=views.xc_get_epg(request,user,short=spec['short'])
            except Exception as error:row['error']=type(error).__name__
            else:row['expected']=value
        result.append(row)
    return {'scope':'Actual original XC short/simple EPG helper at fixed clock; no auth/HTTP acceptance','source_sha256':hashlib.sha256(Path('/app/apps/output/views.py').read_bytes()).hexdigest(),'total':len(cases),'cases':result,'native_verified':False}

def hash_characterization():
    values=['','1_1794236400','2_1794236400',' 2 _1794236400','٢_1794236400','𝟚_1794236400','café_1794236400']
    code='import json;print(json.dumps([str(abs(hash(value)))for value in '+repr(values)+']))'
    samples=[json.loads(subprocess.check_output(['/dispatcharrpy/bin/python','-c',code],text=True,timeout=5))for _ in range(2)]
    zero=json.loads(subprocess.check_output(['/dispatcharrpy/bin/python','-c',code],env=dict(os.environ,PYTHONHASHSEED='0'),text=True,timeout=5))
    return {'zero_seed_outputs':zero,'scope':'Actual fresh original CPython processes, synthetic IDs are not stable across random hash seeds','input_strings':values,'process_results':samples,'differ_across_processes':samples[0]!=samples[1],'PYTHONHASHSEED':os.environ.get('PYTHONHASHSEED')}
