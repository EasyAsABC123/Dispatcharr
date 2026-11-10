"""Actual original generic serializer/nonobject body discovery, disposable only.
Original Dispatcharr authors retain full application credit and AGPL licensing.
Raw fixture includes ephemeral JWTs and must remain in ignored artifacts.
"""
import hashlib,json,os,sys
if (os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'
        or os.environ.get('SERIALIZER_NULL_DISPOSABLE_FIXTURE')!='playback-fixture-only'):
    raise RuntimeError('Explicit disposable serializer fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from django.core.serializers.json import DjangoJSONEncoder
from django.contrib.auth.models import Group
from apps.accounts.models import User
from apps.channels.models import Logo,Channel,Stream,ChannelGroup,ChannelProfile
from core.models import UserAgent,StreamProfile,OutputProfile,SystemNotification,CoreSettings
from rest_framework_simplejwt.tokens import RefreshToken
PREFIX='rust-probe-serializer-null-'
if User.objects.filter(username__startswith=PREFIX).exists():raise RuntimeError('Fresh owned namespace required')
roles={}
for label,level,active in [('admin',10,True),('standard',1,True),('basic',0,True),('inactive',10,False)]:
    user=User.objects.create(username=PREFIX+label,password='!',user_level=level,is_active=active)
    roles[label]={'id':user.pk,'access':str(RefreshToken.for_user(user).access_token)}
agent=UserAgent.objects.create(name=PREFIX+'agent',user_agent='Rust-Probe/null')
stream_profile=StreamProfile.objects.create(name=PREFIX+'stream',command='',parameters='')
output_profile=OutputProfile.objects.create(name=PREFIX+'output',command='ffmpeg',parameters='-i pipe:0')
logo=Logo.objects.create(name=PREFIX+'logo',url='https://example.invalid/null-fixture.png')
group=Group.objects.create(name=PREFIX+'group')
channel_group=ChannelGroup.objects.create(name=PREFIX+'channel-group')
channel_profile=ChannelProfile.objects.create(name=PREFIX+'channel-profile')
channel=Channel.objects.create(name=PREFIX+'channel',channel_number=90000)
stream=Stream.objects.create(name=PREFIX+'stream',url='http://example.invalid/null-fixture.ts')
notification=SystemNotification.objects.create(title=PREFIX+'notification',message='fixture only',notification_type='info')
resources=[
    ('useragents','/api/core/useragents/',agent.pk),
    ('streamprofiles','/api/core/streamprofiles/',stream_profile.pk),
    ('outputprofiles','/api/core/outputprofiles/',output_profile.pk),
    ('logos','/api/channels/logos/',logo.pk),
    ('users','/api/accounts/users/',roles['admin']['id']),
    ('groups','/api/accounts/groups/',group.pk),
    ('channel-groups','/api/channels/groups/',channel_group.pk),
    ('channel-profiles','/api/channels/profiles/',channel_profile.pk),
    ('channels','/api/channels/channels/',channel.pk),
    ('streams','/api/channels/streams/',stream.pk),
    ('notifications','/api/core/notifications/',notification.pk),
]
models=[User,Group,UserAgent,StreamProfile,OutputProfile,Logo,ChannelGroup,ChannelProfile,Channel,Stream,SystemNotification,CoreSettings,
        User.groups.through,User.user_permissions.through,User.channel_profiles.through,Group.permissions.through]
def fingerprint():
    rows={model._meta.db_table:list(model.objects.order_by('pk').values())for model in models}
    return hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':'),cls=DjangoJSONEncoder,allow_nan=False).encode()).hexdigest()
checks=[]
def case(name,path,method,body,role='admin'):
    before=fingerprint();headers={'Host':'localhost','Content-Type':'application/json'}
    if role is not None:headers['Authorization']='Bearer '+roles[role]['access']
    raw=json.dumps(body,ensure_ascii=False,separators=(',',':')).encode()
    response=requests.request(method,'http://127.0.0.1:5656'+path,data=raw,headers=headers,timeout=15,allow_redirects=False)
    selected={key:response.headers[key]for key in ['content-type','content-length','www-authenticate','x-content-type-options']if key in response.headers}
    output={'status':response.status_code,'headers':selected}
    if 'json'in selected.get('content-type',''):output['json']=response.json()
    else:output.update(body_length=len(response.content),body_sha256=hashlib.sha256(response.content).hexdigest(),body_hex=response.content.hex())
    response.close();after=fingerprint()
    if before!=after:raise RuntimeError('Unexpected mutation from nonobject body in '+name)
    checks.append({'name':name,'path':path,'method':method,'role':role,'body':body,'headers':headers,
                   'request_body_sha256':hashlib.sha256(raw).hexdigest(),'expected':output,'no_model_mutations':True})
for name,root,pk in resources:
    for method,path in [('POST',root),('PUT',root+str(pk)+'/'),('PATCH',root+str(pk)+'/')]:
        case(name+'-'+method+'-null',path,method,None)
    for body in [[],True,1.5,'text']:
        case(name+'-POST-'+type(body).__name__,root,'POST',body)
    for role in [None,'basic','standard','inactive']:
        case(name+'-POST-null-role-'+str(role),root,'POST',None,role)
    case(name+'-missing-before-null',root+'2147483647/','PUT',None)
for role in ['admin','standard','basic','inactive',None]:
    case('me-PATCH-null-'+str(role),'/api/accounts/users/me/','PATCH',None,role)
for body in [[],True,1.5,'text']:
    case('me-PATCH-'+type(body).__name__,'/api/accounts/users/me/','PATCH',body)
print(json.dumps({'format_version':1,'source_capture_only':True,'cases':checks,
                  'resources':resources,'protected_tables':[model._meta.db_table for model in models],'all_no_model_mutations':True},separators=(',',':')))
