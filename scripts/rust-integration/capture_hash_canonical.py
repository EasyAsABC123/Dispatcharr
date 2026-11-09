"""Original low-cost hash spelling contracts; fixture-only password values."""
import json
from django.contrib.auth.hashers import PBKDF2PasswordHasher,ScryptPasswordHasher
p=PBKDF2PasswordHasher();s=ScryptPasswordHasher();s.work_factor=16;s.block_size=1;s.parallelism=1
rows=[]
for hasher,encoded in [(p,p.encode('password','salt',1)),(s,s.encode('password','salt'))]:
    for field,spelling in [(None,None),(1,'0'+encoded.split('$')[1]),(1,'+'+encoded.split('$')[1])]:
        fields=encoded.split('$')
        if field is not None:fields[field]=spelling
        candidate='$'.join(fields)
        rows.append({'password':'password','encoded':candidate,'expected':hasher.verify('password',candidate)})
print(json.dumps(rows,separators=(',',':')))
