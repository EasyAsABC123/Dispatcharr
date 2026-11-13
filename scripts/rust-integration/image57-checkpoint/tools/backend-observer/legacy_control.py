"""Owned original-schema XC read fixture. Original Dispatcharr author credit retained.

Credentials are returned only over parent-controlled stdin/stdout; never log them.
"""
import json,os,sys,uuid
from datetime import datetime,timezone
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.core.cache import cache
from apps.accounts.models import User
from apps.channels.models import Channel,ChannelGroup,ChannelOverride,ChannelProfile,ChannelProfileMembership,Logo,Stream
from apps.m3u.models import M3UAccount,M3UAccountProfile
from core.models import CoreSettings
from core.utils import RedisClient
state={}
PRIVATE='127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16'
def settings(key,value):CoreSettings.objects.update_or_create(key=key,defaults={'name':'XC read fixture','value':value})
def command(request):
    if request['op']=='seed':
        if state:return {'users':state['users'],'channels':state['channels'],'groups':state['groups'],'profile':state['profile']}
        groups=[ChannelGroup.objects.create_manual(name=name) for name in ['XC alpha & 東京','XC beta','XC empty']]
        # Pre-existing default group keeps identity stable between independent readers;
        # lazy creation/demotion gets its own explicit source-side-effect case.
        default=ChannelGroup.objects.get_or_create_manual(name='Default Group')[0]
        logo=Logo.objects.create(name='XC fixture logo',url='https://logo.invalid/logo.png')
        rows=[]
        specs=[(801,0,False,False,0),(801.5,0,False,False,0),(802,1,False,False,0),(None,1,False,False,1),(803,1,True,False,1),(804,10,False,False,1),(805,11,False,False,1),(806,0,False,True,0),(-1.5,0,False,False,0),(0,0,False,False,None)]
        for index,(number,level,adult,hidden,group) in enumerate(specs):
            row=Channel.objects.create(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'xc-read-fixture-'+str(index)),name='XC channel '+str(index),channel_number=number,user_level=level,is_adult=adult,hidden_from_output=hidden,channel_group=groups[group] if group is not None else None,is_catchup=index==1,catchup_days=4 if index==1 else 0,is_radio=index==2,logo=logo if index in [0,1] else None)
            Channel.objects.filter(pk=row.pk).update(created_at=datetime(2026,1,2,3,4,index,tzinfo=timezone.utc));rows.append(row)
        ChannelOverride.objects.create(channel=rows[1],name='XC overridden café',channel_number=800.5,channel_group=groups[1],is_radio=True)
        profile=ChannelProfile.objects.create(name='XC assigned profile');ChannelProfileMembership.objects.filter(channel_profile=profile).delete()
        for index in [0,1,2,4]:ChannelProfileMembership.objects.create(channel_profile=profile,channel=rows[index],enabled=index!=2)
        users={}
        for name,level,enabled,limit in [('viewer',0,True,0),('standard',1,True,0),('admin',10,True,0),('limited',1,True,2),('inactive',1,False,0)]:
            password=uuid.uuid4().hex
            user=User.objects.create(username='xc-read-'+name,user_level=level,is_active=enabled,stream_limit=limit,custom_properties={'xc_password':password});user.set_unusable_password();user.save();users[name]={'username':user.username,'password':password,'id':user.pk}
        User.objects.get(pk=users['limited']['id']).channel_profiles.add(profile)
        saved={key:(CoreSettings.objects.filter(key=key).values_list('value',flat=True).first() or {}) for key in ['network_access','system_settings']}
        state.update(users=users,channels=[row.pk for row in rows],groups=[group.pk for group in groups],profile=profile.pk,saved=saved,default_group=default.pk)
        return {'users':users,'channels':state['channels'],'groups':state['groups'],'profile':profile.pk}
    if request['op']=='preferences':
        user=User.objects.get(pk=state['users'][request['user']]['id']);props={'xc_password':state['users'][request['user']]['password']};props.update(request['values']);user.custom_properties=props;user.save(update_fields=['custom_properties']);return {'updated':True}
    if request['op']=='system':
        value=dict(state['saved']['system_settings']);value.update(request['values']);settings('system_settings',value);return {'updated':True}
    if request['op']=='network':
        value=dict(state['saved']['network_access']);value.update(request['values']);settings('network_access',value);return {'updated':True}
    if request['op']=='counts':
        redis=RedisClient.get_client();keys=['live:channel:xc-count.p5:clients:first','timeshift:channel:xc-count:clients:second','vod_persistent_connection:xc-count-third']
        redis.delete(*keys)
        if request.get('present'):
            redis.hset(keys[0],mapping={'user_id':state['users']['limited']['id'],'connected_at':'100.5'})
            redis.hset(keys[1],mapping={'user_id':state['users']['limited']['id'],'connected_at':'101.5'})
            redis.hset(keys[2],mapping={'user_id':state['users']['admin']['id'],'created_at':'102.5','content_uuid':'fixture-content'})
        if request.get('bad')=='timestamp':redis.hset(keys[0],mapping={'user_id':state['users']['limited']['id'],'connected_at':'invalid'})
        if request.get('bad')=='user':redis.hset(keys[0],mapping={'user_id':'invalid','connected_at':'100.5'})
        if request.get('bad')=='type':redis.delete(keys[0]);redis.set(keys[0],'wrong-type')
        return {'updated':True}
    if request['op']=='tuner':
        if not state.get('tuner_accounts'):
            accounts=M3UAccount.objects.bulk_create([M3UAccount(name='XC tuner active',is_active=True),M3UAccount(name='XC tuner inactive',is_active=False)])
            state['tuner_accounts']=[row.pk for row in accounts]
            M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=accounts[0],name='XC two',max_streams=2,search_pattern='(.*)',replace_pattern='$1'),M3UAccountProfile(m3u_account=accounts[0],name='XC four',max_streams=4,search_pattern='(.*)',replace_pattern='$1'),M3UAccountProfile(m3u_account=accounts[0],name='XC inactive',max_streams=0,is_active=False,search_pattern='(.*)',replace_pattern='$1'),M3UAccountProfile(m3u_account=accounts[1],name='XC disabled unlimited',max_streams=0,search_pattern='(.*)',replace_pattern='$1')])
            Stream.objects.create(name='XC disabled custom',url='http://origin:8080/never-requested.ts',m3u_account=accounts[1],is_custom=True)
        M3UAccountProfile.objects.filter(m3u_account_id=state['tuner_accounts'][0],name='XC two').update(max_streams=0 if request['unlimited'] else 2)
        from apps.m3u.utils import calculate_tuner_count
        return {'expected':calculate_tuner_count(minimum=1,unlimited_default=50)}
    if request['op']=='reset_tuner':
        if state.get('tuner_accounts'):M3UAccount.objects.filter(pk__in=state.pop('tuner_accounts')).delete()
        return {'reset':True}
    if request['op']=='default_group_probe':
        from django.db import transaction
        from django.test import RequestFactory
        from apps.output.views import _xc_live_streams_setup
        user=User.objects.get(pk=state['users']['standard']['id'])
        with transaction.atomic():
            ChannelGroup.objects.filter(name='Default Group').delete()
            missing=not ChannelGroup.objects.filter(name='Default Group').exists()
            req=RequestFactory().get('/player_api.php',HTTP_HOST='xc.fixture.invalid:8088')
            _,_,get_group,_,_=_xc_live_streams_setup(req,user,None)
            first=get_group();again=get_group();group=ChannelGroup.objects.get(pk=first)
            relation=group.m3u_accounts.get(m3u_account=M3UAccount.get_custom_account())
            result={'missing_before':missing,'repeated_get_same_id':first==again,'group_name':group.name,'matching_groups':ChannelGroup.objects.filter(name='Default Group').count(),'matching_custom_relations':group.m3u_accounts.filter(m3u_account=M3UAccount.get_custom_account()).count(),'relation':{'custom_properties':relation.custom_properties,'enabled':relation.enabled,'auto_channel_sync':relation.auto_channel_sync,'auto_sync_channel_start':relation.auto_sync_channel_start,'auto_sync_channel_end':relation.auto_sync_channel_end,'is_stale':relation.is_stale,'last_seen_present':relation.last_seen is not None}}
            transaction.set_rollback(True)
        return result
    if request['op']=='side_effects':
        return {'default_group_id':ChannelGroup.objects.filter(name='Default Group').values_list('id',flat=True).first(),'group_relations':list(ChannelGroup.objects.get(name='Default Group').m3u_accounts.values_list('id',flat=True))}
    if request['op']=='close':return {'closed':True}
    raise ValueError('unknown XC read control')
if __name__=='__main__':
    print(json.dumps({'ready':True}),flush=True)
    for line in sys.stdin:
        if len(line)>65536:raise ValueError('bounded fixture input')
        value=json.loads(line);print(json.dumps(command(value)),flush=True)
        if value['op']=='close':break
