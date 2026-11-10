"""Actual original resolve_epg_output_window vectors, no replacement algorithm."""
import hashlib
from pathlib import Path
from types import SimpleNamespace
from django.http import QueryDict
from apps.channels.utils import resolve_epg_output_window

def capture():
    values=[None,0,1,-1,1.5,True,False,'','bad','١','+2','2_0','999999999999999999999999',[],{},' {"epg_days": 2, "epg_prev_days": 1} ']
    queries=[[],[('days','0')],[('days','1')],[('days','bad')],[('days','-1')],[('days','١')],[('days','365')],[('days','999999999999999999999')],[('prev_days','1')],[('prev_days','bad')],[('prev_days','365')],[('days','1'),('days','2')],[('days','1'),('prev_days','1')]]
    rows=[]
    for maximum in [0,1,30,365]:
        policy={'min_days':min(1,maximum),'max_days':maximum,'default_days':3,'default_prev_days':min(2,maximum)}
        for value in values:
            props={'epg_days':value,'epg_prev_days':value}
            for custom in [props, __import__('json').dumps(props)]:
                for pairs in queries:
                    params=QueryDict('',mutable=True)
                    for key,item in pairs:params.appendlist(key,item)
                    row={'params':pairs,'custom_properties':custom,'policy':policy}
                    try:window=resolve_epg_output_window(SimpleNamespace(GET=params),SimpleNamespace(custom_properties=custom),policy=SimpleNamespace(**policy))
                    except Exception as error:row['error']=type(error).__name__
                    else:row['expected']={'forward_days':window.forward_days,'prev_days':window.prev_days}
                    rows.append(row)
    path=Path('/app/apps/channels/utils.py')
    return {'scope':'Actual original XMLTV export window helper (not grid/API guide windows)','file_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'cases':rows,'native_verified':False}
