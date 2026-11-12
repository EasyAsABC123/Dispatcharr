"""Pinned Django response-header primitive only: no application/DB/network setup."""
import json
from django.http.response import ResponseHeaders
rows=[]
for value in ['host-42','é-42','☃-42','long-ascii-hostname-with-snowman-☃-42','😀-42','space host-42','bad\rhost-42','bad\nhost-42','bad\0host-42']:
    try:
        headers=ResponseHeaders({'X-Worker-ID':value})
        rows.append({'value':value,'success':True,'latin1_hex':headers['X-Worker-ID'].encode('latin1').hex()})
    except Exception as error:
        rows.append({'value':value,'success':False,'error_type':type(error).__name__,'error':str(error)})
print(json.dumps({'scope':'Django ResponseHeaders primitive; no Source application initialized or network/DB requests','rows':rows}))
