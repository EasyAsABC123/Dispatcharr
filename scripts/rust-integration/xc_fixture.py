"""Original XC helper goldens for a disposable Django PG/Redis fixture only.

Credentials are fixed test inputs in code and never emitted to the evidence
file. This management helper uses original auth, throttle, ORM and channel ACL.
Original application credit belongs entirely to Dispatcharr contributors.
"""
import json
import os
import sys
import time
from datetime import datetime,timezone
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.cache import cache
from django.test import RequestFactory
from django.db import connection
from apps.accounts.models import User
from apps.accounts.xc_auth import authorize_xc_client
from apps.channels.models import Channel,ChannelOverride,ChannelProfile,ChannelProfileMembership
from apps.channels.access import get_channel_for_user
from core.models import CoreSettings,SystemEvent,SystemNotification,NotificationDismissal
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
PREFIX='rust-probe-xc-'
GOOD='xc-fixture-only';DJANGO='django-fixture-only'
PASSWORDS={'correct':GOOD,'wrong':'wrong-fixture-only','django':DJANGO,'empty':'','unicode':'caf\u00e9','number':'123','boolean':'True','array':'[True, None]','dict':"{'x': False}"}
USER_PROPS={'main':{'xc_password':GOOD,'enable_xc':False},'inactive':{'xc_password':GOOD},'missing':{},'null':None,'unicode':{'xc_password':'caf\u00e9'},'number':{'xc_password':123},'boolean':{'xc_password':True},'array':{'xc_password':[True,None]},'dict':{'xc_password':{'x':False}},'denied':{'xc_password':GOOD,'allowed_networks':{'XC_API':'192.0.2.0/24'}},'lenient':{'xc_password':GOOD,'allowed_networks':{'XC_API':'bad, 198.51.100.8/24'}},'hide-adult':{'xc_password':GOOD,'hide_adult_content':True},'admin':{'xc_password':GOOD}}
rows=[]
for kind,props in USER_PROPS.items():
    user=User(username=PREFIX+kind,user_level=10 if kind=='admin' else 1,is_active=kind!='inactive',custom_properties=props,password='!')
    if kind=='main':user.set_password(DJANGO)
    rows.append(user)
User.objects.filter(username__startswith=PREFIX).delete();User.objects.bulk_create(rows)
users={kind:User.objects.get(username=PREFIX+kind) for kind in USER_PROPS}
# Preparation bypasses scheduler/cache signals; only this owned fixture DB is used.
for key,value in [('network_access',{'XC_API':'0.0.0.0/0,::/0'}),('system_settings',{'max_system_events':10000})]:
    if not CoreSettings.objects.filter(key=key).exists():CoreSettings.objects.bulk_create([CoreSettings(key=key,name=PREFIX+key,value=value)])
    CoreSettings.objects.filter(key=key).update(value=value)
    cache.delete(CoreSettings.group_cache_key(key))
Channel.objects.filter(name__startswith=PREFIX).delete();ChannelProfile.objects.filter(name__startswith=PREFIX).delete()
Channel.objects.bulk_create([Channel(name=PREFIX+'safe',channel_number=9970,user_level=0,hidden_from_output=True),Channel(name=PREFIX+'adult',channel_number=9971,user_level=0,is_adult=True),Channel(name=PREFIX+'high',channel_number=9972,user_level=11),Channel(name=PREFIX+'outside',channel_number=9973,user_level=0)])
channels={name:Channel.objects.get(name=PREFIX+name) for name in ['safe','adult','high','outside']}
ChannelOverride.objects.bulk_create([ChannelOverride(channel=channels['safe'],name=PREFIX+'effective-safe')])
ChannelProfile.objects.bulk_create([ChannelProfile(name=PREFIX+'profile')])
profile=ChannelProfile.objects.get(name=PREFIX+'profile')
User.channel_profiles.through.objects.bulk_create([User.channel_profiles.through(user_id=users[kind].pk,channelprofile_id=profile.pk) for kind in ['main','hide-adult']])
ChannelProfileMembership.objects.bulk_create([ChannelProfileMembership(channel_profile=profile,channel=channels[name],enabled=True) for name in ['safe','adult']])
factory=RequestFactory()
ALLOW={'XC_API':'0.0.0.0/0,::/0'};DENY={'XC_API':'192.0.2.0/24'}
def prepare(mode,ip,prefill=0):
    cache.delete('throttle_xc_auth_'+ip)
    cache.delete(CoreSettings.group_cache_key('network_access'))
    if mode!='miss':cache.set(CoreSettings.group_cache_key('network_access'),DENY if mode=='deny' else ALLOW,300)
    if prefill:cache.set('throttle_xc_auth_'+ip,[time.time()-59.0]*prefill,60)
def call(kind,password_kind,ip,username_kind=None):
    req=factory.get('/player_api.php',REMOTE_ADDR=ip,HTTP_USER_AGENT='rust-probe-xc-UA')
    username='' if username_kind=='empty' else ('invalid\0username' if username_kind=='nul' else PREFIX+kind)
    user,error=authorize_xc_client(req,username,PASSWORDS[password_kind],'XC_API')
    return {'status':200,'user_id':user.pk,'is_active':user.is_active} if error is None else {'status':error.status_code,'body':json.loads(error.content),'retry_after':int(error['Retry-After']) if error.has_header('Retry-After') else None,'bearer_challenge':error.has_header('WWW-Authenticate')}
cases=[]
for name,kind,password,mode,prefill,username_kind in [
    ('active','main','correct','miss',0,None),('inactive-preserved','inactive','correct','allow',0,None),('no-django-fallback','main','django','allow',0,None),('wrong','main','wrong','allow',0,None),('empty-password','main','empty','allow',0,None),('missing-password-property','missing','correct','allow',0,None),('null-properties','null','correct','allow',0,None),('unicode-match','unicode','unicode','allow',0,None),('unicode-mismatch','main','unicode','allow',0,None),('legacy-number','number','number','allow',0,None),('legacy-boolean','boolean','boolean','allow',0,None),('legacy-array','array','array','allow',0,None),('legacy-dictionary','dict','dict','allow',0,None),('user-network-denied-correct','denied','correct','allow',0,None),('user-network-denied-wrong','denied','wrong','allow',0,None),('user-network-lenient','lenient','correct','allow',0,None),('system-network-before-lookup','main','correct','deny',0,'nul'),('system-network-before-throttle','main','correct','deny',10,None),('missing-username','main','correct','allow',0,'empty'),('rate-before-lookup','main','correct','allow',10,'nul'),('rate-many-no-retry-header','main','correct','allow',11,None)]:
    ip='198.51.100.'+str(len(cases)+1);prepare(mode,ip,prefill)
    cases.append({'name':name,'user_kind':kind,'password_kind':password,'mode':mode,'prefill':prefill,'username_kind':username_kind,'ip':ip,'expected':call(kind,password,ip,username_kind)})
channel_cases=[]
for kind,name in [('main','safe'),('main','adult'),('main','high'),('main','outside'),('inactive','safe'),('hide-adult','adult'),('admin','high')]:
    row=get_channel_for_user(users[kind],id=channels[name].pk)
    value=None if row is None else {'id':row.pk,'uuid':str(row.uuid),'name':row.name,'effective_name':getattr(getattr(row,'override',None),'name',None) if getattr(getattr(row,'override',None),'name',None) is not None else row.name}
    channel_cases.append({'user_kind':kind,'channel_id':channels[name].pk,'expected':value})
# Real original inbox publication: register a source channel and receive the
# actual channel-layer group message; the Rust unit adapter captures its sink.
import asyncio
async def receive_notice(layer,observer):
    for _ in range(16):
        message=await asyncio.wait_for(layer.receive(observer),10)
        if message.get('data',{}).get('notification',{}).get('notification_key')=='xc-auth-lockout':return message
    raise RuntimeError('Expected owned notification was not received')
layer=get_channel_layer();observer=async_to_sync(layer.new_channel)('rustprobexc.')
async_to_sync(layer.group_add)('updates',observer)
def normalize_notice(value):
    data=json.loads(json.dumps(value));notice=data['data']['notification'];datetime.fromisoformat(notice['created_at']);notice['id']='<notice-id>';notice['created_at']='<utc-iso>';return data
ip='198.51.100.240';prepare('allow',ip);cache.set(CoreSettings.group_cache_key('system_settings'),{'max_system_events':10000},300)
SystemEvent.objects.filter(event_type='login_failed',details__reason='XC auth rate limited',details__user=PREFIX+'main').delete()
SystemNotification.objects.filter(notification_key='xc-auth-lockout').delete();cache.delete('xc_auth_lockout_notice_cooldown')
sequence=[call('main','wrong',ip) for _ in range(9)];sequence.append(call('main','correct',ip));sequence.append(call('main','wrong',ip));sequence.append(call('main','correct',ip))
notice=SystemNotification.objects.get(notification_key='xc-auth-lockout');first_id=notice.pk
message=normalize_notice(async_to_sync(receive_notice)(layer,observer))
events=SystemEvent.objects.filter(event_type='login_failed',details__reason='XC auth rate limited',details__user=PREFIX+'main')
first={'sequence':sequence,'event_count':events.count(),'event_details':events.first().details,'notice':{field:getattr(notice,field) for field in ['notification_key','notification_type','priority','source','title','message','action_data','is_active','admin_only']},'cooldown':cache.get('xc_auth_lockout_notice_cooldown'),'published':message}
cache.delete('throttle_xc_auth_'+ip)
for _ in range(10):call('main','wrong',ip)
second={'event_count':events.count(),'notice_same_id':SystemNotification.objects.get(notification_key='xc-auth-lockout').pk==first_id,'notice_count':SystemNotification.objects.filter(notification_key='xc-auth-lockout').count()}
NotificationDismissal.objects.create(user=users['admin'],notification=notice)
cache.delete('xc_auth_lockout_notice_cooldown');cache.delete('throttle_xc_auth_'+ip)
# Preserve the source retention off-by-one: max=1 keeps the cutoff row too.
SystemEvent.objects.all().delete()
SystemEvent.objects.bulk_create([SystemEvent(event_type='channel_start',details={'owned_fixture':True}) for _ in range(3)])
for ordinal,row in enumerate(SystemEvent.objects.order_by('id'),1):SystemEvent.objects.filter(pk=row.pk).update(timestamp=datetime(2020,1,ordinal,tzinfo=timezone.utc))
cache.set(CoreSettings.group_cache_key('system_settings'),{'max_system_events':1},300)
for _ in range(10):call('main','wrong',ip)
replacement=SystemNotification.objects.get(notification_key='xc-auth-lockout')
third={'event_total':SystemEvent.objects.count(),'notice_changed_id':replacement.pk!=first_id,'dismissals_cleared':not NotificationDismissal.objects.filter(notification_id=first_id).exists(),'published':normalize_notice(async_to_sync(receive_notice)(layer,observer))}
async_to_sync(layer.group_discard)('updates',observer)
# Leave a clean replay start. The original case outcomes above remain evidence.
SystemEvent.objects.all().delete();SystemNotification.objects.filter(notification_key='xc-auth-lockout').delete();cache.delete('xc_auth_lockout_notice_cooldown')
for case in cases:cache.delete('throttle_xc_auth_'+case['ip'])
cache.delete('throttle_xc_auth_'+ip)
for key in ['network_access','system_settings']:cache.delete(CoreSettings.group_cache_key(key))
print(json.dumps({'users':{kind:{'id':user.pk,'username':user.username} for kind,user in users.items()},'auth_cases':cases,'channel_cases':channel_cases,'lockout':{'first':first,'second':second,'third':third}},default=str))
