"""Prepared Source-only actual HTTP context; imported by the existing Workflow.
Raw tokens, passwords, cookies and CSRF values stay in owned private0600 files.
No application is started here; run(f) requires an already owned Source Fixture.
"""
import base64
import hashlib
import http.cookiejar
import http.client
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

CASE = 'm0-auth-route-session'
MAX_RESPONSE = 1048576


def private_json(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = (json.dumps(value, sort_keys=True, default=str) + '\n').encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as output:
        output.write(data)
    return {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class CSRF(HTMLParser):
    def __init__(self):
        super().__init__(); self.values = []
    def handle_starttag(self, tag, attrs):
        fields = dict(attrs)
        if tag == 'input' and fields.get('name') == 'csrfmiddlewaretoken':
            self.values.append(fields.get('value'))


class Wire:
    def __init__(self, f, directory):
        self.f = f; self.directory = directory
        self.origin = f.base_url.rstrip('/')
        parsed = urllib.parse.urlsplit(self.origin)
        assert parsed.scheme == 'http' and parsed.hostname in ('127.0.0.1', 'localhost')
        assert not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPCookieProcessor(self.jar))
        self.index = 0

    def call(self, label, method, path, payload=None, token=None, form=False):
        assert path.startswith('/') and not path.startswith('//')
        assert urllib.parse.urlsplit(path).netloc == ''
        self.index += 1
        headers = {'User-Agent': 'Dispatcharr-owned-M0-actual-route-probe', 'Accept': 'application/json'}
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
        data = None
        if payload is not None:
            headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
            data = urllib.parse.urlencode(payload).encode() if form else json.dumps(payload).encode()
        if form:
            headers['Referer'] = self.origin + '/admin/login/?next=/admin/'
        req = urllib.request.Request(self.origin + path, data=data, headers=headers, method=method)
        request = {'method': method, 'path': path, 'headers': headers, 'body_base64': base64.b64encode(data or b'').decode(), 'cookies_before': [vars(c) for c in self.jar]}
        stem = f'{self.index:04d}-{label}'
        private_json(self.directory / (stem + '-request.json'), request)
        sql_before = snapshot(self.f)
        private_json(self.directory / (stem + '-SQL-before.json'), sql_before)
        started = time.monotonic()
        result = None
        raw = b''
        try:
            try:
                response = self.opener.open(req, timeout=min(20, self.f.remaining(20)))
            except urllib.error.HTTPError as error:
                response = error
            with response:
                result = {'status': response.code, 'headers': list(response.headers.items())}
                raw = response.read(MAX_RESPONSE + 1)
                result.update(body_base64=base64.b64encode(raw[:MAX_RESPONSE]).decode(), body_truncated=len(raw)>MAX_RESPONSE, cookies_after=[vars(c) for c in self.jar], elapsed_seconds=time.monotonic()-started)
                # Persist the completed or bounded-partial wire observation BEFORE observers.
                proof = private_json(self.directory / (stem + '-wire.json'), {'request':request,'response':result})
                if len(raw) > MAX_RESPONSE:
                    raise RuntimeError('AuthResponseBound')
        except Exception as error:
            if isinstance(error, http.client.IncompleteRead) and result is not None:
                partial = error.partial
                result.update(body_base64=base64.b64encode(partial[:MAX_RESPONSE]).decode(), body_truncated=len(partial)>MAX_RESPONSE, incomplete_read=True, partial_bytes_observed=len(partial))
            private_json(self.directory / (stem + '-failure.json'), {'stage':'actual_HTTP_transport_or_read','type':type(error).__name__,'message':str(error)[:512],'request':request,'partial_response':result,'unexposed_partial_bytes_unknown':not isinstance(error,http.client.IncompleteRead) and not (result is not None and 'body_base64' in result),'elapsed_seconds':time.monotonic()-started})
            raise
        try:
            sql_after = snapshot(self.f)
        except Exception as error:
            private_json(self.directory / (stem + '-observer-failure.json'), {'stage':'SQL_after_completed_HTTP','type':type(error).__name__,'message':str(error)[:512],'completed_wire':proof})
            raise
        private_json(self.directory / (stem + '-SQL-after.json'), sql_after)
        try: body = json.loads(raw)
        except (ValueError, UnicodeError): body = None
        return {'method': method, 'path': path, 'status': result['status'], 'private_raw': proof}, body, raw


def snapshot(f):
    return f.sql("""
from django.contrib.auth import get_user_model
from django.contrib.sessions.models import Session
from django.core.serializers.json import DjangoJSONEncoder
from datetime import datetime,date
class ExactEncoder(DjangoJSONEncoder):
 def default(self,obj):
  if isinstance(obj,(datetime,date)):return obj.isoformat()
  return super().default(obj)
from core.models import SystemEvent
names=['OwnedM0RouteAdmin','OwnedM0RouteStandard','OwnedM0RouteStreamer']
rows={'users':list(get_user_model().objects.filter(username__in=names).order_by('id').values()),'sessions':list(Session.objects.order_by('session_key').values()),'events':list(SystemEvent.objects.order_by('id').values())}
with connection.cursor() as cursor:
 cursor.execute("SELECT pg_get_serial_sequence(%s,'id'),pg_get_serial_sequence(%s,'id')",[get_user_model()._meta.db_table,SystemEvent._meta.db_table])
 sequences=cursor.fetchone()
 rows['sequences']={}
 for sequence in sequences:
  if sequence:
   cursor.execute('SELECT last_value,is_called FROM '+connection.ops.quote_name(sequence.split('.')[0])+'.'+connection.ops.quote_name(sequence.split('.')[1]))
   value=cursor.fetchone();rows['sequences'][sequence]={'last_value':value[0],'is_called':value[1]}
print(json.dumps(rows,cls=ExactEncoder))
""", secret=True)


def prepare_context(f):
    assert f.backend == 'Original' and f.case == CASE
    credentials = {name: secrets.token_urlsafe(24) for name in ('admin', 'standard', 'streamer')}
    script = """
from django.contrib.auth import get_user_model
User=get_user_model()
roles={roles}
created={{}}
for role,password in roles.items():
 name='OwnedM0Route'+role.capitalize()
 if User.objects.filter(username=name).exists():raise RuntimeError('OwnedAuthActorAlreadyExists')
 user=User.objects.create_user(username=name,password=password,is_active=True,is_staff=(role=='admin'),is_superuser=(role=='admin'),user_level={{'admin':10,'standard':1,'streamer':0}}[role])
 created[role]={{'id':user.pk,'username':name,'user_level':user.user_level,'is_staff':user.is_staff,'is_superuser':user.is_superuser}}
print(json.dumps(created))
""".format(roles=repr(credentials))
    before = snapshot(f)
    actors = f.sql(script, secret=True)
    f._m0_auth = {'actors': actors, 'credentials': credentials}
    folder = f.output / 'auth-route-session' / 'private'
    private_json(folder / 'declared-actor-setup.json', {'before': before, 'after': snapshot(f), 'actors': actors, 'credentials': credentials, 'scope': 'Three new owned users; bootstrap users unchanged; real password hashing, no issued/session token seed'})
    return {'owned_actor_ids': {k: v['id'] for k,v in actors.items()}, 'actual_HTTP_executed': False}


def run(f):
    assert f.backend == 'Original' and f.case == CASE
    setup = f._m0_auth
    directory = f.output / 'auth-route-session' / 'private'
    summaries = []; dependencies = []
    # Each role uses a separate actual cookie jar and its own issued JWTs.
    for role in ('admin', 'standard', 'streamer'):
        wire = Wire(f, directory / role)
        actor = setup['actors'][role]
        before = snapshot(f)
        observation, pair, _ = wire.call('actual-login-route', 'POST', '/api/accounts/auth/login/', {'username':actor['username'], 'password':setup['credentials'][role]})
        summaries.append(dict(observation, role=role, actor_id=actor['id']))
        if not (observation['status'] == 200 and isinstance(pair,dict) and isinstance(pair.get('access'),str) and isinstance(pair.get('refresh'),str)):
            dependencies.append({'role':role,'blocked':'Login route did not issue a usable pair; dependent chain NOT_RUN'})
            private_json(directory / role / 'SQL-login-boundary.json', {'before':before,'after':snapshot(f)})
            continue
        access = pair['access']
        admin_session_ready = False
        observation, own, _ = wire.call('actual-own-identity','GET','/api/accounts/users/me/',token=access)
        summaries.append(dict(observation,role=role,own_identity_matches=isinstance(own,dict) and own.get('id')==actor['id']))
        observation, refreshed, _ = wire.call('actual-refresh-route','POST','/api/accounts/token/refresh/',{'refresh':pair['refresh']})
        summaries.append(dict(observation,role=role,issued_access=isinstance(refreshed,dict) and isinstance(refreshed.get('access'),str)))
        if isinstance(refreshed,dict) and isinstance(refreshed.get('access'),str):
            observation, own, _ = wire.call('refreshed-JWT-own-identity','GET','/api/accounts/users/me/',token=refreshed['access'])
            summaries.append(dict(observation,role=role,refreshed_token_own_identity_matches=isinstance(own,dict) and own.get('id')==actor['id']))
        else: dependencies.append({'role':role,'blocked':'Refresh did not issue a usable access string; refreshed-token protected identity NOT_RUN'})
        if role == 'admin':
            observation, _, html = wire.call('admin-CSRF-page','GET','/admin/login/?next=/admin/')
            summaries.append(dict(observation,role=role))
            csrf = CSRF(); csrf.feed(html.decode('utf-8','replace'))
            if len(csrf.values)==1 and isinstance(csrf.values[0],str):
                observation, _, _ = wire.call('admin-real-session-login','POST','/admin/login/?next=/admin/',{'username':actor['username'],'password':setup['credentials'][role],'csrfmiddlewaretoken':csrf.values[0],'next':'/admin/'},form=True)
                summaries.append(dict(observation,role=role,session_cookie_present=any(c.name=='sessionid' for c in wire.jar)))
                observation, _, _ = wire.call('admin-session-before-logout','GET','/admin/')
                admin_session_ready = observation['status']==200 and any(c.name=='sessionid' for c in wire.jar)
                summaries.append(dict(observation,role=role,admin_session_verified=admin_session_ready))
                if not admin_session_ready: dependencies.append({'role':role,'blocked':'Real admin session was not verified; session logout/post-session chain NOT_RUN; JWT-only fallback remains explicit'})
            else: dependencies.append({'role':role,'blocked':'No unique CSRF form token; admin session chain NOT_RUN; JWT-only fallback remains explicit'})
        # This is the new cookie+JWT route chain, not replay of the direct-view logout authority.
        observation, _, _ = wire.call('actual-session-JWT-logout-route' if admin_session_ready else 'actual-JWT-only-logout-route','POST','/api/accounts/auth/logout/',{},token=access)
        summaries.append(dict(observation,role=role,session_chain_exercised=admin_session_ready,session_cookie_present=any(c.name=='sessionid' for c in wire.jar)))
        if role=='admin' and admin_session_ready:
            observation, _, _ = wire.call('admin-session-after-logout','GET','/admin/')
            summaries.append(dict(observation,role=role))
        observation, own, _ = wire.call('JWT-after-session-logout','GET','/api/accounts/users/me/',token=access)
        summaries.append(dict(observation,role=role,own_identity_matches=isinstance(own,dict) and own.get('id')==actor['id']))
        private_json(directory / role / 'SQL-chain-boundary.json', {'before':before,'after':snapshot(f)})
    # Run the one promised wrong-credential alias request only AFTER all valid
    # dependent chains. This owned context has a fresh Redis namespace; no
    # forwarded-IP spoofing/reset or replay of existing lockout vectors.
    wire = Wire(f,directory/'wrong-credential-alias')
    actor=setup['actors']['admin']
    observation, _, _ = wire.call('wrong-owned-credential-login-alias','POST','/api/accounts/auth/login/',{'username':actor['username'],'password':'wrong-'+secrets.token_urlsafe(24)})
    summaries.append(dict(observation,role='owned_admin_wrong_password',throttle_context='One invalid attempt after completed valid chains in fresh owned context; actual429 remains throttled, never relabeled401'))
    # Exact methods are observed independently; no successful-role inheritance or guessed implementation.
    wire = Wire(f,directory/'dispatch')
    for path in ('/api/accounts/auth/login/','/api/accounts/auth/logout/','/api/accounts/token/refresh/'):
        for method in ('POST','GET','HEAD','OPTIONS','PUT','PATCH','DELETE','TRACE'):
            observation, _, _ = wire.call(f'dispatch-{len(summaries):03d}',method,path)
            summaries.append(dict(observation,role='anonymous'))
        observation, _, _ = wire.call(f'invalid-JWT-{len(summaries):03d}','POST',path,{},token='owned.invalid.jwt')
        summaries.append(dict(observation,role='invalid_JWT'))
    report={'status':'OBSERVATIONS_CAPTURED_NOT_YET_QUALIFIED','actual_Source':True,'actual_Native':False,'observations':summaries,'dependent_NOT_RUN':dependencies,'raw_credentials_cookies_CSRF_JWT':'Private0600 only; public per-response SHA and selected typed facts','method_definition_binding':'Actual requests do not rewrite unresolved contract IDs; bind after independent Source review'}
    target=f.output/'auth-route-session'/'report.json'
    target.write_text(json.dumps(report,indent=2)+'\n')
    return report
