"""Additional neutral Source exception/result probes; no application writes.
Original Dispatcharr authors retain application credit and AGPL licensing.
"""
from worker_tasks import app,default,started
from datetime import datetime,date,timezone
from decimal import Decimal
from uuid import UUID
@app.task(name='rust_probe.result_value')
def value(kind):
 if kind=='key-error':raise KeyError('fixture missing')
 if kind=='multi-error':raise ValueError('fixture first',3)
 if kind=='empty-error':raise ValueError()
 if kind=='boolean':return True
 if kind=='integer':return 9007199254740993
 if kind=='float':return 1.5
 if kind=='date':return date(2026,1,2)
 if kind=='datetime':return datetime(2026,1,2,3,4,5,tzinfo=timezone.utc)
 if kind=='decimal':return Decimal('1.20')
 if kind=='bytes':return b'fixture bytes'
 if kind=='bytes-binary':return b'\x00\xff'
 if kind=='uuid':return UUID('12345678-1234-5678-1234-567812345678')
 raise ValueError('Unknown fixture kind')
