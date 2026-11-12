"""Owned copied SQL fixture; exact eleven-model rows and sequences, no import work."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from fixture_guards import database
MODELS=None


def setup(default=False):
    global MODELS
    if os.environ.get('NOTIFICATION_MAIN_FIXTURE')!='notification-fixture-only':raise ValueError('OwnedNotificationFixture')
    name=os.environ.get('POSTGRES_DB','')
    if default:
        if name!='dispatcharr':raise ValueError('ReadOnlyDefaultAudit')
    else:database(name)
    sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
    import django;django.setup()
    from apps.accounts.models import User
    from apps.m3u.models import M3UAccount,M3UAccountProfile
    from apps.vod.models import Movie,M3UMovieRelation,Series,Episode,M3UEpisodeRelation
    from core.models import CoreSettings,SystemNotification,NotificationDismissal
    MODELS=[User,M3UAccount,M3UAccountProfile,Movie,M3UMovieRelation,Series,Episode,M3UEpisodeRelation,CoreSettings,SystemNotification,NotificationDismissal]


def snapshot():
    from django.db import connection,connections
    try:
        with connection.cursor()as c:
            c.execute('SELECT current_database()')
            if c.fetchone()[0]!=os.environ['POSTGRES_DB']:raise ValueError('ActualDatabaseBinding')
        result={'tables':{},'sequences':{}}
        for model in MODELS:
            key=model._meta.db_table;rows=list(model.objects.order_by(model._meta.pk.name).values()[:10001])
            if len(rows)>10000:raise ValueError('SQLRowCapacity')
            result['tables'][key]=json.loads(json.dumps(rows,default=str))
            with connection.cursor()as c:
                c.execute('SELECT pg_get_serial_sequence(%s,%s)',[key,model._meta.pk.column]);seq=c.fetchone()[0]
                if not seq or not all(x.isalnum()or x in '._'for x in seq):raise ValueError('FixedElevenSequenceBinding')
                c.execute('SELECT last_value,is_called FROM '+seq);last,called=c.fetchone();result['sequences'][key]={'last_value':last,'is_called':called}
        if len(json.dumps(result).encode())>8388608:raise ValueError('SQLAuditByteCapacity')
        return result
    finally:connections.close_all()


def main():
    action=sys.argv[1];setup(default=action=='default-audit')
    from django.db import connections
    try:
        if action=='migrate':
            from django.core.management import call_command
            call_command('migrate',interactive=False,verbosity=0);result={'migrated':True}
        elif action=='seed':
            from apps.accounts.models import User
            from core.models import SystemNotification
            from rest_framework_simplejwt.tokens import AccessToken
            nonce=os.environ['NOTIFICATION_MAIN_NONCE']
            import re
            if not re.fullmatch('[0-9a-f]{32}',nonce):raise ValueError('OwnedNonce')
            result={'owned_notification_key':'owned.Main.'+nonce}
            for role,level in [('admin',10),('standard',1)]:
                u=User.objects.create(username='owned-notification-'+role+'-'+nonce,password='!',is_active=True,user_level=level,custom_properties={},is_staff=role=='admin',is_superuser=role=='admin')
                token=AccessToken.for_user(u);token.set_exp(lifetime=datetime.timedelta(hours=1));result[role+'_user_id']=u.pk;result[role+'_token']=str(token)
            SystemNotification.objects.create(notification_key='owned.preexisting.'+nonce,title='Preexisting',message='Exact retention',source='system')
            path=Path('/evidence/notification-seed-private.json');path.write_text(json.dumps(result)+'\n');path.chmod(0o600);result={'seeded':True,'snapshot':snapshot()}
        elif action in ('snapshot','default-audit'):result=snapshot()
        else:raise ValueError('NoOtherDatabaseAction')
        print(json.dumps(result,default=str))
    finally:connections.close_all()


if __name__=='__main__':main()
