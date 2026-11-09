"""Low-cost original Django Scrypt vectors; all credentials are fixture-only."""
import json
from django.contrib.auth.hashers import ScryptPasswordHasher
h=ScryptPasswordHasher();h.work_factor=16;h.block_size=1;h.parallelism=1
print(json.dumps([{'password':password,'encoded':h.encode(password,salt)} for password,salt in [('password','salt'),(' pãss🔑 ','sälty'),('','empty-salt')]],ensure_ascii=True,separators=(',',':')))
