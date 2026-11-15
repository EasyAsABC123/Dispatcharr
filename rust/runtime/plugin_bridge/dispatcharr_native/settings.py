"""Source settings with explicit real native transport adapters, no services.
Not enabled until all required adapters and exact owning callbacks are installed.
"""
import copy,os
from dispatcharr.settings import *
DATABASES=copy.deepcopy(DATABASES)
if set(DATABASES)!={'default'}:raise RuntimeError('Multiple Django database aliases require owned native sessions')
from .wire import current
_native_database=current().call('sql','settings',{})
for _field in('NAME','USER','HOST','PORT','ENGINE'):DATABASES['default'][_field]=_native_database[_field]
# No Python libpq credential/client is used: actual existing native pool owns
# authentication/TLS. Retain the real target metadata rather than silently
# claiming a different POSTGRES_* target while Main uses DATABASE_URL.
DATABASES['default']['PASSWORD']=''
DATABASES['default']['CONN_MAX_AGE']=0
DATABASES['default']['OPTIONS']={}
CHANNEL_LAYERS={'default':{'BACKEND':'dispatcharr_native.channels_transport.NativeChannelLayer'}}
# Keep Source serializer/codec/layer logic. Actual Redis/Kombu adapters MUST be
# installed before django.setup. Do not point these at a new direct backend.
if os.environ.get('DISPATCHARR_NATIVE_PORTS_READY')!='sql,cache,jobs,events,process':
 raise RuntimeError('Lazy Python native transport callbacks are not ready')

# No Original URL router or WSGI/ASGI application is installed for this host.
ROOT_URLCONF='dispatcharr_native.urls'
WSGI_APPLICATION=None
ASGI_APPLICATION=None
