"""Actual Original uWSGI UUID action corpus on the owned disposable database.
Original Dispatcharr authors retain all application/algorithm credit/licenses.
"""
import hashlib,json,os,sys,urllib.request,urllib.error
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel,ChannelProfile,ChannelProfileMembership,ChannelOverride
from rest_framework_simplejwt.tokens import RefreshToken
channels=list(Channel.objects.order_by('pk'))[:5];assert len(channels)==5
Channel.objects.filter(pk=channels[0].pk).update(hidden_from_output=True)
Channel.objects.filter(pk=channels[1].pk).update(is_adult=True)
Channel.objects.filter(pk=channels[2].pk).update(user_level=2)
ChannelOverride.objects.create(channel=channels[3],name='Owned UUID override',channel_number=0.5)
profile=ChannelProfile.objects.create(name='Owned UUID profile')
ChannelProfileMembership.objects.filter(channel_profile=profile).update(enabled=False)
ChannelProfileMembership.objects.update_or_create(channel_profile=profile,channel=channels[0],defaults={'enabled':True})
ChannelProfileMembership.objects.update_or_create(channel_profile=profile,channel=channels[1],defaults={'enabled':False})
users={}
for name,level,active,props in [('admin',10,True,{}),('standard',1,True,{}),('limited',1,True,{}),('adult-hidden',1,True,{'hide_adult_content':True}),('viewer',0,True,{}),('inactive',10,False,{})]:
 u=User.objects.create(username='uuid-action-'+name,user_level=level,is_active=active,custom_properties=props)
 if name=='limited':u.channel_profiles.add(profile)
 users[name]=u
ids=[str(c.uuid)for c in channels];missing='00000000-0000-0000-0000-000000000099'
body_cases=[('default',{}),('empty',{'uuids':[]}),('one-hidden',{'uuids':[ids[0]]}),('all',{'uuids':ids}),('reverse',{'uuids':ids[::-1]}),('duplicate',{'uuids':[ids[0],ids[0],ids[3]]}),('missing',{'uuids':[missing]}),('null-member',{'uuids':[None,ids[0]]}),('hex',{'uuids':[ids[0].replace('-','')]}),('uppercase',{'uuids':[ids[0].upper()]}),('braces',{'uuids':['{'+ids[0]+'}']}),('urn',{'uuids':['urn:uuid:'+ids[0]]}),('zero-integer',{'uuids':[0]}),('true-integer',{'uuids':[True]}),('wrong-list',{'uuids':'wrong'}),('null-list',{'uuids':None}),('dict-list',{'uuids':{}}),('bad-uuid',{'uuids':['invalid']}),('negative-uuid',{'uuids':[-1]}),('float-uuid',{'uuids':[1.5]}),('array-root',[]),('null-root',None)]
tables=['accounts_user','dispatcharr_channels_channel','dispatcharr_channels_channeloverride','dispatcharr_channels_channelprofile','dispatcharr_channels_channelprofilemembership','dispatcharr_channels_channelstream','dispatcharr_channels_stream']
from django.db import connection
def state():
 result={}
 with connection.cursor()as cursor:
  for table in tables:
   cursor.execute('SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.id)::text,\'[]\')) FROM '+table+' t');result[table]=cursor.fetchone()[0]
 return result
before=state()
cases=[]
def capture(label,role,value,query=''):
 token=str(RefreshToken.for_user(users[role]).access_token)if role in users else 'invalid'if role=='invalid'else None
 raw=json.dumps(value,separators=(',',':')).encode();headers={'Content-Type':'application/json','Accept':'application/json','Host':'127.0.0.1:5656'}
 if token:headers['Authorization']='Bearer '+token
 request=urllib.request.Request('http://127.0.0.1:5656/api/channels/channels/by-uuids/'+query,data=raw,headers=headers,method='POST')
 try:response=urllib.request.urlopen(request,timeout=10)
 except urllib.error.HTTPError as error:response=error
 with response:
  body=response.read(1024*1024);ctype=response.headers.get('Content-Type','');status=response.status
 try:expected={'json':json.loads(body)}
 except ValueError:expected={'text':body.decode()}
 cases.append({'label':label,'role':role,'user_id':users[role].pk if role in users else None,'body':value,'query':query,'status':status,'content_type':ctype,'expected':expected})
for label,body in body_cases:capture('admin-'+label,'admin',body)
for role in ['standard','limited','adult-hidden','viewer','inactive','anonymous','invalid']:
 for suffix,body in [('all',{'uuids':ids}),('bad-list',{'uuids':'wrong'})]:capture(role+'-'+suffix,role,body)
for query in ['?include_streams=true','?include_streams=False','?page=1&page_size=1&ordering=-name&search=no-match&visibility_filter=hidden']:
 capture('admin-query-'+str(len(cases)),'admin',{'uuids':ids},query)
after=state();assert before==after
print(json.dumps({'cases':cases,'row_fingerprints':before,'Original_readonly_unchanged':True,'actual_Original_HTTP':True,'credit':'Original Dispatcharr authors retain full credit/licenses.'}))
