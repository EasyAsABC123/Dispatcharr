"""Exact original generator XML at a fixed fixture clock; no replacement renderer."""
from datetime import datetime,timezone
from unittest.mock import patch
from django.test import RequestFactory
from apps.accounts.models import User
from apps.output import epg

def capture(users):
    now=datetime.now(timezone.utc).replace(microsecond=0)
    cases=[]
    for role in [None,'viewer','standard','admin','limited','inactive']:
        for mode in ['channel_number','tvg_id','gracenote']:
            for cached in [True,False]:
                user=User.objects.get(pk=users[role]['id']) if role else None
                pairs=[('days','2'),('prev_days','1'),('tvg_id_source',mode),('cachedlogos',str(cached).lower())]
                request=RequestFactory().get('/output/epg',data=dict(pairs),HTTP_HOST='xmltv.fixture.invalid:8088')
                row={'user_id':user.pk if user else None,'profile':None,'origin':'http://xmltv.fixture.invalid:8088','tvg_id_source':mode,'cached_logos':cached,'forward_days':2,'prev_days':1,'now':now.isoformat()}
                with patch.object(epg,'stream_cached_response',side_effect=lambda key,build,**kwargs:build()),patch.object(epg.django_timezone,'now',return_value=now),patch.object(epg,'_epg_export_teardown',return_value=None):
                    row['xml']=''.join(epg.generate_epg(request,user=user))
                cases.append(row)
    return {'scope':'Actual original complete XMLTV generator at injected clock, cache wrapper bypassed only','cases':cases,'native_verified':False}
