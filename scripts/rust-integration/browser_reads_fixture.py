"""Original deferred React read contracts on owned disposable PostgreSQL rows.
Original Dispatcharr contributors retain full application credit.
"""
import json,os,sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Logo,Channel,Stream,ChannelGroup
from apps.m3u.models import M3UAccount
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import RefreshToken
if Logo.objects.filter(name__startswith='rust-probe-').exists() or User.objects.filter(username__startswith='rust-probe-browser-').exists():raise RuntimeError('Fixture namespace must be fresh')
users={}
for name,level,active in [('admin',10,True),('standard',1,True),('basic',0,True),('inactive',10,False)]:
 u=User.objects.create(username='rust-probe-browser-'+name,password='!',user_level=level,is_active=active);users[name]=str(RefreshToken.for_user(u).access_token)
Logo.objects.bulk_create([Logo(name='rust-probe-'+n,url=url)for n,url in [('A','https://fixture.invalid/a.png'),('B','/data/logos/no-such-fixture.png'),('C','https://fixture.invalid/c.svg')]])
logos=list(Logo.objects.order_by('name'))
Channel.objects.bulk_create([Channel(name='rust-probe-channel-'+str(i),channel_number=i+1,logo=logos[0]if i<7 else logos[1])for i in range(8)])
M3UAccount.objects.bulk_create([M3UAccount(name='rust-probe-browser-'+n,account_type='STD',is_active=active)for n,active in [('A',True),('B',True),('inactive',False)]])
providers=list(M3UAccount.objects.filter(name__startswith='rust-probe-browser-').order_by('name'))
ChannelGroup.objects.bulk_create([ChannelGroup(name='rust-probe-browser-'+n)for n in ['A','B','inactive']]);groups=list(ChannelGroup.objects.filter(name__startswith='rust-probe-browser-').order_by('name'))
Stream.objects.bulk_create([Stream(name='rust-probe-browser-'+str(i),url='http://fixture.invalid/'+str(i),stream_hash='rust-probe-browser-hash-'+str(i),m3u_account=providers[i%3]if i<3 else None,channel_group=groups[i%3],tvg_id='fixture-'+str(i),is_stale=i==1,is_radio=i==1,is_catchup=i==0)for i in range(4)])
factory=APIRequestFactory();cases=[]
def case(path,role='admin',headers=None):
 path=__import__('urllib.parse',fromlist=['quote']).quote(path,safe='/?=&')
 headers=headers or {};headers={**headers,'HTTP_HOST':headers.get('HTTP_HOST','localhost')}
 if role is not None:headers['HTTP_AUTHORIZATION']='Bearer '+users[role]
 request=factory.get(path,REMOTE_ADDR='127.0.0.1',**headers);match=resolve(path.split('?')[0]);result=match.func(request,*match.args,**match.kwargs);result.render()
 cases.append({'path':path,'headers':headers,'expected':{'status':result.status_code,'body':json.loads(result.content)}})
base='/api/channels/logos/'
for query in ['', '?no_pagination=true','?used=true','?used=false','?used=True','?name=PROBE-A','?name=missing','?page_size=1','?page_size=1&page=2','?page_size=1&page=last','?page=0','?page=bad','?ids=abc&ids=-1','?ids='+str(logos[0].pk)+'&ids='+str(logos[2].pk),'?ids=²','?ids='+''.join(chr(0x660+int(c))for c in str(logos[1].pk)), '?no_pagination=TRUE']:
 case(base+query)
case(base+str(logos[0].pk)+'/');case(base+str(logos[0].pk)+'/?used=false');case(base+'999999/');case(base+'bad/')
for role in ['standard','basic','inactive',None]:case(base,role)
case(base,headers={'HTTP_X_FORWARDED_PROTO':'https','HTTP_X_FORWARDED_HOST':'fixture.invalid','HTTP_X_FORWARDED_PORT':'9443'})
base='/api/channels/streams/filter-options/'
for query in ['', '?channel_group=missing','?search=missing','?hide_stale=true','?name=rust-probe-browser','?name=missing','?m3u_account='+str(providers[0].pk),'?m3u_account='+str(providers[1].pk)+'&hide_stale=true','?name=rust-probe-browser&channel_group=missing','?m3u_account_name=browser-B','?tvg_id=fixture-0','?m3u_account_is_active=false','?m3u_account_is_active=true&is_radio=true','?m3u_account_is_active=true&is_catchup=true']:
 case(base+query)
for role in ['standard','basic','inactive',None]:case(base,role)
print(json.dumps({'cases':cases,'unicode_version':__import__('unicodedata').unidata_version},separators=(',',':')))
