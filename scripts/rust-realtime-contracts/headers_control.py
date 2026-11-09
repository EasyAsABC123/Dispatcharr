"""Owned actual-source header fixture. Original Dispatcharr author credit retained."""
import hashlib,inspect,json,os,sys
sys.path.insert(0,'/hdhr-probe')
import control as fixture
from django.core.cache import cache
from apps.accounts.models import User
from core.models import CoreSettings,SystemEvent
from dispatcharr.utils import get_client_ip,proxy_auth_identity
PRIVATE='127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16'
def command(value):
    if value['op']=='seed':
        data=fixture.main(value)
        fixture.main({'op':'network','values':{key:PRIVATE for key in ['UI','STREAMS','M3U_EPG','XC_API','XC_STREAMS']}})
        CoreSettings.objects.update_or_create(key='reverse_proxy_auth',defaults={'name':'Header fixture','value':{'enabled':True,'header':'X-Header-Fixture-User'}})
        user=User.objects.create(username='header-café',user_level=1,is_active=True);user.set_unusable_password();user.save()
        data['source_functions']={name:hashlib.sha256(inspect.getsource(fn).encode()).hexdigest() for name,fn in [('get_client_ip',get_client_ip),('proxy_auth_identity',proxy_auth_identity)]}
        return data
    if value['op']=='reset_login_history':
        row=SystemEvent.objects.filter(event_type='login_success',details__user='header-café').order_by('-id').first()
        if row is None:raise ValueError('owned proxy login event required')
        ip=row.details['client_ip']
        import ipaddress
        if not ipaddress.ip_address(ip).is_private:raise ValueError('private owned fixture peer required')
        key='throttle_login_'+ip;history=cache.get(key,[])
        if not isinstance(history,list) or len(history)>3:raise ValueError('unexpected fixture login history')
        cache.delete(key)
        return {'reset':True,'scope':'only owned fixture login history','prior_entries':len(history),'peer':ip}
    if value['op']=='auth_history':
        allowed=['2001:db8::3%history-scope','203.0.113.43']
        ip=value['ip']
        if ip not in allowed:raise ValueError('unknown fixture history')
        history=cache.get('throttle_api_key_auth_'+ip,[])
        if not isinstance(history,list) or len(history)>10:raise ValueError('unexpected history')
        return {'entries':len(history)}
    if value['op']=='events':
        return {'events':[{'id':row.pk,'type':row.event_type,'details':{key:item for key,item in row.details.items() if key in ['client_ip','user_agent','profile','reason','user']}} for row in SystemEvent.objects.filter(pk__gt=value.get('after',0)).order_by('id')[:64]]}
    if value['op']=='checkpoint':return {'id':SystemEvent.objects.order_by('-id').values_list('pk',flat=True).first() or 0}
    if value['op']=='close':return {'closed':True}
    raise ValueError('unknown header fixture operation')
if __name__=='__main__':
    print(json.dumps({'ready':True}),flush=True)
    for line in sys.stdin:
        if len(line)>65536:raise ValueError('fixture input bound')
        value=json.loads(line);print(json.dumps(command(value)),flush=True)
        if value['op']=='close':break
