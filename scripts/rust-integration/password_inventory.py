"""Pinned-runtime password capability inventory; no credential bytes emitted."""
import importlib.util
import json
import django
from django.contrib.auth.hashers import PBKDF2PasswordHasher, Argon2PasswordHasher, BCryptSHA256PasswordHasher, ScryptPasswordHasher
scrypt=ScryptPasswordHasher()
encoded=scrypt.encode('hashers-fixture-only','abcdefghijklmnopqrstuv')
print(json.dumps({'django_version':django.get_version(),'preferred_algorithm':'pbkdf2_sha256','preferred_iterations':PBKDF2PasswordHasher.iterations,'preferred_salt_entropy':PBKDF2PasswordHasher.salt_entropy,'argon2_library_available':importlib.util.find_spec('argon2') is not None,'bcrypt_library_available':importlib.util.find_spec('bcrypt') is not None,'listed_argon2_parameters':{'time_cost':Argon2PasswordHasher.time_cost,'memory_cost_kib':Argon2PasswordHasher.memory_cost,'parallelism':Argon2PasswordHasher.parallelism},'listed_bcrypt_rounds':BCryptSHA256PasswordHasher.rounds,'scrypt':{'default_roundtrip_verified':scrypt.verify('hashers-fixture-only',encoded),'encoded_length':len(encoded),'work_factor':scrypt.work_factor,'block_size':scrypt.block_size,'parallelism':scrypt.parallelism,'maxmem':scrypt.maxmem}},sort_keys=True))
