"""Capture original OutputProfile HTTP/SQL/cache contracts.

Run only in the disposable pinned original image; native acceptance is separate.
Original Dispatcharr authors retain all application credit and AGPL licensing.
The output contains ephemeral fixture JWTs: keep it in ignored artifacts only.
"""
import base64
import hashlib
import json
import os
import sys
from urllib.parse import quote, urlencode

if (os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only'
        or os.environ.get('OUTPUT_PROFILE_DISPOSABLE_FIXTURE') != 'playback-fixture-only'):
    raise RuntimeError('Explicit disposable OutputProfile fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
import requests
from django.core.cache import cache
from django.db import connection
from apps.accounts.models import User
from core.models import CoreSettings, OutputProfile
from core.utils import RedisClient
from rest_framework_simplejwt.tokens import RefreshToken

PREFIX = 'rust-probe-output-profile-'
IDS = (91001, 91002, 91003)
if OutputProfile.objects.filter(id__gte=91000).exists() or User.objects.filter(username__startswith=PREFIX).exists():
    raise RuntimeError('Fresh owned namespace required')
roles = {}
for role, level, active in [('admin',10,True),('above_admin',20,True),('standard',1,True),('basic',0,True),('inactive',10,False)]:
    user = User.objects.create(username=PREFIX+role, password='!', user_level=level, is_active=active)
    roles[role] = str(RefreshToken.for_user(user).access_token)
users = []
for suffix in ['integer','string','float','other','null','array','boolean','padded']:
    users.append(User.objects.create(username=PREFIX+'reference-'+suffix, password='!'))
base = [
    {'id':91001,'name':PREFIX+'one','command':'ffmpeg','parameters':'-i pipe:0 -c copy -f mpegts pipe:1','locked':False,'is_active':True},
    {'id':91002,'name':PREFIX+'two','command':'other','parameters':'other parameters','locked':False,'is_active':False},
    {'id':91003,'name':PREFIX+'locked','command':'locked','parameters':'locked parameters','locked':True,'is_active':True},
]
for profile_id in [9007199254740992,9007199254740993,9007199254740994,9223372036854775807]:
    base.append({'id':profile_id,'name':PREFIX+'huge-'+str(profile_id),'command':'ffmpeg','parameters':'huge parameters','locked':False,'is_active':True})
KEYS = ['coresettings:group:stream_settings','coresettings:group:dvr_settings',
        'coresettings:groupver:stream_settings','coresettings:groupver:dvr_settings',
        'coresettings:default_user_agent','coresettings:default_user_agent:ver']

def clear_faults():
    with connection.cursor() as cursor:
        cursor.execute('DROP TRIGGER IF EXISTS rust_probe_output_fault ON accounts_user')
        cursor.execute('DROP TRIGGER IF EXISTS rust_probe_output_fault ON core_coresettings')
        cursor.execute('DROP FUNCTION IF EXISTS rust_probe_output_fault()')

def install_fault(target):
    body, when = {
        'users':("IF NEW.username='rust-probe-output-profile-reference-string' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;",'BEFORE UPDATE OF custom_properties ON accounts_user'),
        'stream_update':("IF NEW.key='stream_settings' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;",'BEFORE UPDATE ON core_coresettings'),
        'dvr_update':("IF NEW.key='dvr_settings' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;",'BEFORE UPDATE ON core_coresettings'),
        'stream_insert':("IF NEW.key='stream_settings' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;",'BEFORE INSERT ON core_coresettings'),
    }[target]
    with connection.cursor() as cursor:
        cursor.execute('CREATE FUNCTION rust_probe_output_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN '+body+' RETURN NEW; END $$')
        cursor.execute('CREATE TRIGGER rust_probe_output_fault '+when+' FOR EACH ROW EXECUTE FUNCTION rust_probe_output_fault()')

def restore():
    clear_faults()
    # Fixture-only bypass permits recreation of deliberately locked owned rows.
    # No baseline seeded profile or nonowned User is changed.
    OutputProfile.objects.filter(id__gte=91000).update(locked=False)
    OutputProfile.objects.filter(id__gte=91000).delete()
    OutputProfile.objects.bulk_create([OutputProfile(**row) for row in base])
    props = [{'output_profile':91001,'keep':'integer'}, {'output_profile':'91001','keep':'string'},
             {'output_profile':91001.0,'keep':'float'}, {'output_profile':91002,'keep':'other'},
             None, [91001], {'output_profile':True,'keep':'boolean'},
             {'output_profile':'091001','keep':'padded'}]
    for user, value in zip(users, props):
        User.objects.filter(pk=user.pk).update(custom_properties=value)
    for key, name, value in [
        ('stream_settings','Original stream name',{'hdhr_output_profile_id':91001,'keep':'stream'}),
        ('dvr_settings','Original DVR name',{'output_profile_id':91001,'keep':'dvr'}),
    ]:
        CoreSettings.objects.update_or_create(key=key, defaults={'name':name,'value':value})
    cache.delete_many(KEYS)
    cache.set('coresettings:group:stream_settings', {'hdhr_output_profile_id':91001,'keep':'stream'}, 300)
    cache.set('coresettings:group:dvr_settings', {'output_profile_id':91001,'keep':'dvr'}, 300)
    cache.set('coresettings:default_user_agent', 'Rust-Probe/output-stale', 300)
    for key in ['coresettings:groupver:stream_settings','coresettings:groupver:dvr_settings','coresettings:default_user_agent:ver']:
        cache.set(key, 123456, timeout=None)

def wire(spec):
    if spec is None:
        return b'', 'application/json'
    if 'json' in spec:
        return json.dumps(spec['json'], ensure_ascii=False, separators=(',',':')).encode(), 'application/json'
    if 'form' in spec:
        return urlencode(spec['form']).encode(), 'application/x-www-form-urlencoded'
    if 'raw' in spec:
        return base64.b64decode(spec['raw']), spec.get('content_type','application/json')
    parts = []
    for key, value in spec['multipart'].items():
        parts.append(f'--rust-probe-output-boundary\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
    return b''.join(parts)+b'--rust-probe-output-boundary--\r\n', 'multipart/form-data; boundary=rust-probe-output-boundary'

def witness():
    with connection.cursor() as cursor:
        cursor.execute('SELECT last_value,is_called FROM core_coresettings_id_seq')
        settings_sequence = cursor.fetchone()
        cursor.execute('SELECT last_value,is_called FROM core_outputprofile_id_seq')
        profile_sequence = cursor.fetchone()
    return {
        'settings_sequence':settings_sequence,'profile_sequence':profile_sequence,
        'profiles':list(OutputProfile.objects.filter(id__gte=91000).order_by('id').values('id','name','command','parameters','locked','is_active')),
        'users':[{'suffix':user.username[len(PREFIX):], 'custom_properties':User.objects.get(pk=user.pk).custom_properties,'sql_null':User.objects.filter(pk=user.pk,custom_properties__isnull=True).exists()}for user in users],
        'settings':list(CoreSettings.objects.filter(key__in=['stream_settings','dvr_settings']).order_by('key').values('id','key','name','value')),
    }

def cache_witness():
    result = {}
    client = RedisClient.get_client()
    for key in KEYS:
        redis_type = client.type(':1:'+key)
        if redis_type not in ['none','string']:
            if redis_type != 'set':
                raise RuntimeError('Unexpected fixture cache type')
            result[key] = {'redis_type':'set','members':sorted(client.smembers(':1:'+key))}
            continue
        value = cache.get(key)
        if key.endswith(':ver') or ':groupver:' in key:
            result[key] = {'type':type(value).__name__, 'old':value==123456,
                           'advanced':isinstance(value,int) and value>123456,
                           'persistent':client.ttl(':1:'+key)==-1}
        else:
            result[key] = value
    return result

cases = []
def case(name, method='PATCH', pk='91001', role='admin', spec=None, setup=None):
    restore()
    if setup is not None:
        setup()
    sequence = 110000+len(cases)*10
    with connection.cursor() as cursor:
        cursor.execute("SELECT setval('core_outputprofile_id_seq',%s,true)",[sequence])
        cursor.execute('SELECT last_value,is_called FROM core_coresettings_id_seq')
        settings_sequence = cursor.fetchone()
    path = quote('/api/core/outputprofiles/'+(str(pk)+'/' if pk is not None else ''),safe='/?=&')
    headers = {'Host':'localhost'}
    if role is not None:
        headers['Authorization'] = 'Bearer '+roles.get(role,role)
    raw, mime = wire(spec)
    before, before_cache = witness(), cache_witness()
    response = requests.request(method,'http://127.0.0.1:5656'+path,data=raw,
        headers={**headers,'Content-Type':mime},timeout=20,allow_redirects=False)
    selected = {key:response.headers[key] for key in ['content-type','content-length','www-authenticate','x-content-type-options'] if key in response.headers}
    output = {'status':response.status_code,'headers':selected}
    if 'json' in selected.get('content-type',''):
        output['json'] = response.json()
    else:
        output.update(body_sha256=hashlib.sha256(response.content).hexdigest(),body_length=len(response.content))
    response.close()
    cases.append({'name':name,'method':method,'path':path,'headers':headers,'body':spec,
        'sequence':sequence,'settings_sequence':settings_sequence,'request_body_sha256':hashlib.sha256(raw).hexdigest(),
        'setup':getattr(setup,'fixture_spec',None),'pre':{'db':before,'cache':before_cache},
        'expected':{'response':output,'db':witness(),'cache':cache_witness()}})

valid = {'name':PREFIX+'new','command':'ffmpeg','parameters':'-i pipe:0 -c copy -f mpegts pipe:1'}
for body in [{},{'name':'name'},dict(valid),{**valid,'name':PREFIX+'one'},
             {**valid,'command':' '},{**valid,'parameters':''},
             {'name':None,'command':None,'parameters':None,'is_active':None,'locked':[]},
             {**valid,'name':'x'*256,'command':'y'*256},
             {**valid,'name':42,'command':1.5,'parameters':2},
             {**valid,'is_active':False,'locked':True,'id':1,'unknown':'ignored'}]:
    case('post-json-'+str(len(cases)),method='POST',pk=None,spec={'json':body})
for body in [None,[],True,1.5,'text']:
    case('post-nonobject-'+str(len(cases)),method='POST',pk=None,spec={'json':body})
case('post-unicode',method='POST',pk=None,spec={'json':{**valid,'name':PREFIX+'é','parameters':' --title café 😃 '}})
for method in ['PUT','PATCH']:
    case('null-body-'+method,method=method,spec={'json':None})
for kind in ['form','multipart']:
    for booleans in [{},{'is_active':'','locked':''},{'is_active':'on','locked':'off'}]:
        case('post-'+kind+'-'+str(len(cases)),method='POST',pk=None,spec={kind:{**valid,**booleans}})
for body in [{},{'name':PREFIX+'renamed'},{'command':' other '},{'parameters':'  -new  '},
             {'is_active':False},{'locked':True},{'id':1,'unknown':True},{'name':PREFIX+'two'},
             {'parameters':None},{'name':True}]:
    case('patch-json-'+str(len(cases)),spec={'json':body})
for body in [{},dict(valid),{**valid,'is_active':False}]:
    case('put-json-'+str(len(cases)),method='PUT',spec={'json':body})
for method in ['PUT','PATCH']:
    for kind in ['form','multipart']:
        case('html-update-'+str(len(cases)),method=method,spec={kind:dict(valid)})
        case('html-blank-boolean-'+str(len(cases)),method=method,spec={kind:{**valid,'is_active':'','locked':''}})
for body in [{},{'name':base[2]['name']},{'command':'changed'},{'parameters':'changed'},
             {'locked':False},{'is_active':False},{'locked':False,'is_active':False},
             {'name':'changed','command':'changed'},{'name':base[1]['name']},{'command':None}]:
    case('locked-'+str(len(cases)),pk='91003',spec={'json':body})
for method in ['POST','PATCH','DELETE']:
    for role in [None,'basic','standard','inactive','invalid','above_admin']:
        case('role-'+method+'-'+str(role),method=method,pk=None if method=='POST' else '91001',role=role,spec={'json':dict(valid)})
for pk in ['999999','bad','-1','91_001','+91001',' 91001 ','٩١٠٠١','9'*100]:
    case('lookup-'+str(len(cases)),method='PUT',pk=pk,spec={'json':{}})
case('delete-locked',method='DELETE',pk='91003')
case('delete-referenced',method='DELETE')
case('delete-unreferenced',method='DELETE',pk='91002')
case('malformed-json',method='POST',pk=None,spec={'raw':base64.b64encode(b'{').decode()})
case('unsupported-mime',method='POST',pk=None,spec={'raw':base64.b64encode(b'x').decode(),'content_type':'text/plain'})

# Declarative setup metadata is retained so a native replay can reconstruct the
# same DB/cache divergence without invoking or patching original application code.
def divergent(spec):
    def setup():
        for key in ['stream_settings','dvr_settings']:
            database = spec.get('database',{}).get(key,'keep-existing')
            if database != 'keep-existing':
                if database == '<absent>':
                    # Fixture-only raw deletion preserves deliberately stale
                    # shared cache entries for the subsequent HTTP request.
                    with connection.cursor() as cursor:
                        cursor.execute('DELETE FROM core_coresettings WHERE key=%s',[key])
                else:
                    with connection.cursor() as cursor:
                        cursor.execute('UPDATE core_coresettings SET value=%s::jsonb WHERE key=%s',[json.dumps(database),key])
            if key in spec.get('cached',{}):
                cache.set('coresettings:group:'+key,spec['cached'][key],300)
        for key in spec.get('remove_cache',[]):
            cache.delete(key)
        client = RedisClient.get_client()
        for key in spec.get('cache_sets',[]):
            client.delete(':1:'+key)
            client.sadd(':1:'+key,'rust-probe-output-cache-wrong-type')
        if 'user_profile' in spec:
            profile_id=spec['user_profile']
            for user,raw in zip(users[:3],[profile_id,str(profile_id),float(profile_id)]):
                User.objects.filter(pk=user.pk).update(custom_properties={'output_profile':raw,'keep':'huge-reference'})
        if 'sql_failure' in spec:
            install_fault(spec['sql_failure'])
    setup.fixture_spec = spec
    return setup

for raw in [91001,'91001',91001.75,'91_001','٩١٠٠١',' +91001 ',True,False,None,'',[],{},'invalid',10**100]:
    case('delete-settings-id-'+str(len(cases)),method='DELETE',setup=divergent({
        'cached':{'stream_settings':{'hdhr_output_profile_id':raw,'keep':'cached-stream'},
                  'dvr_settings':{'output_profile_id':raw,'keep':'cached-dvr'}}}))
for database in [{'keep':'live'},[],None,{},'<absent>']:
    case('delete-cached-match-live-divergence-'+str(len(cases)),method='DELETE',setup=divergent({
        'database':{'stream_settings':database,'dvr_settings':database},
        'cached':{'stream_settings':{'hdhr_output_profile_id':91001},'dvr_settings':{'output_profile_id':91001}}}))
# Read-miss defaults and cached-empty/nonobject behavior need separate actual
# witnesses: a cached empty dict is authoritative, not a DB/default overlay.
for label,database,cached in [
    ('miss-matching',None,None),
    ('miss-nonmatching',{'stream_settings':{'hdhr_output_profile_id':91002,'keep':'live-stream'},'dvr_settings':{'output_profile_id':91002,'keep':'live-dvr'}},None),
    ('miss-empty',{'stream_settings':{},'dvr_settings':{}},None),
    ('miss-absent',{'stream_settings':'<absent>','dvr_settings':'<absent>'},None),
    ('cached-empty',None,{'stream_settings':{},'dvr_settings':{}}),
    ('cached-false',None,{'stream_settings':False,'dvr_settings':False}),
]:
    spec={}
    if database is not None:spec['database']=database
    if cached is None:spec['remove_cache']=['coresettings:group:stream_settings','coresettings:group:dvr_settings']
    else:spec['cached']=cached
    case('delete-cache-'+label,method='DELETE',setup=divergent(spec))

# Compare Python int(float) with exact BIGINT IDs, never float(id). The
# adjacent odd value and BIGINT_MAX deliberately cannot be represented in f64.
for profile_id,raw in [
    (9007199254740993,9007199254740992.0),
    (9007199254740993,9007199254740994.0),
    (9007199254740993,9007199254740993),
    (9007199254740993,'9007199254740993'),
    (9007199254740992,9007199254740992.0),
    (9007199254740994,9007199254740994.0),
    (9223372036854775807,9223372036854775808.0),
    (9223372036854775807,9223372036854775807),
    (9223372036854775807,'9223372036854775807'),
]:
    case('delete-huge-id-'+str(len(cases)),method='DELETE',pk=str(profile_id),setup=divergent({
        'user_profile':profile_id,
        'cached':{'stream_settings':{'hdhr_output_profile_id':raw,'keep':'huge-stream'},
                  'dvr_settings':{'output_profile_id':raw,'keep':'huge-dvr'}}}))

# Actual server WRONGTYPE errors exercise the original broad callback catch,
# without patching original code or replacing the HTTP/DB/cache implementations.
for key in ['coresettings:group:stream_settings','coresettings:group:dvr_settings']:
    case('delete-cache-wrongtype-'+key.rsplit(':',1)[-1],method='DELETE',setup=divergent({'cache_sets':[key]}))
case('delete-cache-version-wrongtype',method='DELETE',setup=divergent({
    'remove_cache':['coresettings:group:stream_settings'],
    'cache_sets':['coresettings:groupver:stream_settings']}))
for target in ['users','stream_update','dvr_update']:
    case('delete-sql-failure-'+target,method='DELETE',setup=divergent({'sql_failure':target}))
case('delete-sql-failure-stream-insert',method='DELETE',setup=divergent({
    'database':{'stream_settings':'<absent>'},
    'cached':{'stream_settings':{'hdhr_output_profile_id':91001}},
    'sql_failure':'stream_insert'}))
restore()
print(json.dumps({'format_version':1,'source_capture_only':True,'cases':cases,'profiles':base,'user_ids':[user.pk for user in users]},separators=(',',':')))
