"""Guarded ephemeral credential fixture for packaged HTTP comparisons.

Original application credit remains with Dispatcharr contributors. No plaintext
password, encoded credential or token is returned by this control helper.
"""
from django.contrib.auth.hashers import PBKDF2PasswordHasher, PBKDF2SHA1PasswordHasher, ScryptPasswordHasher, check_password
from apps.accounts.models import User
from core.models import SystemEvent
CASES={
    'rounds':('sha256',10,'abcdefghijklmnopqrstuv',True),
    'sha1':('sha1',10,'abcdefghijklmnopqrstuv',True),
    'salt':('sha256',1200000,'short',True),
    'preferred':('sha256',1200000,'abcdefghijklmnopqrstuv',True),
    'inactive':('sha256',10,'abcdefghijklmnopqrstuv',False),
    'wrong':('sha256',10,'abcdefghijklmnopqrstuv',True),
    'scrypt':('scrypt',16384,'abcdefghijklmnopqrstuv',True),
    'scrypt-wrong':('scrypt',16384,'abcdefghijklmnopqrstuv',True),
}
PASSWORD='packaged-hash-fixture-only'
def original_hash(kind):
    algorithm,iterations,salt,_=CASES[kind]
    hasher=ScryptPasswordHasher() if algorithm=='scrypt' else PBKDF2SHA1PasswordHasher() if algorithm=='sha1' else PBKDF2PasswordHasher()
    return hasher.encode(PASSWORD,salt) if algorithm=='scrypt' else hasher.encode(PASSWORD,salt,iterations)
def dispatch(data):
    kind=data.get('kind');name=data.get('username','');action=data['action']
    if kind not in CASES or not name.startswith('rust-probe-packaged-hash-') or len(name)>100:
        raise RuntimeError('Owned credential fixture required')
    ip=data.get('client_ip','')
    if not ip.startswith('198.18.2.') or not ip.removeprefix('198.18.2.').isdigit():
        raise RuntimeError('Owned fixture client IP required')
    if action=='auth_package_prepare':
        if User.objects.filter(username=name).exists():raise RuntimeError('Fixture username collision')
        user=User.objects.create(username=name,password=original_hash(kind),user_level=1,is_active=CASES[kind][3],custom_properties={})
        return {'id':user.pk}
    user=User.objects.get(pk=int(data['user_id']),username=name)
    if action=='auth_package_cleanup':
        SystemEvent.objects.filter(details__client_ip=ip,details__user=name).delete()
        user.delete();return {'removed':True}
    if action=='auth_package_reset':
        User.objects.filter(pk=user.pk).update(password=original_hash(kind),last_login=None,is_active=CASES[kind][3])
        SystemEvent.objects.filter(details__client_ip=ip).delete()
        return {'reset':True}
    if action=='auth_package_inspect':
        fields=user.password.split('$')
        return {'changed':user.password!=original_hash(kind),'algorithm':fields[0],'iterations':int(fields[1]),'salt_characters':len(fields[2]),'password_valid':check_password(PASSWORD,user.password),'is_active':user.is_active,'last_login_set':user.last_login is not None}
    raise RuntimeError('Unknown fixture operation')
