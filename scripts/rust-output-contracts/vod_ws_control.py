"""Disposable original Django controls for actual packaged media WebSockets.
Original Dispatcharr authors/contributors retain full application credit.
JWT control replies must remain in memory, never command logs/evidence files.
"""
import os,sys,json,subprocess,signal
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
action=sys.argv[1]
if action=='token':
    from apps.accounts.models import User
    from rest_framework_simplejwt.tokens import AccessToken
    user,_=User.objects.get_or_create(username='rust-packaged-media-observer',defaults={'password':'!','user_level':10,'is_active':True})
    if user.user_level!=10 or not user.is_active:raise RuntimeError('Owned observer account has unexpected policy')
    print(json.dumps({'access':str(AccessToken.for_user(user)),'user_id':user.pk}))
elif action=='start-daphne':
    path=Path('/evidence/media-source-daphne.pid')
    if path.exists():raise RuntimeError('Owned Daphne already recorded')
    process=subprocess.Popen(['/dispatcharrpy/bin/daphne','-b','0.0.0.0','-p','9191','dispatcharr.asgi:application'],cwd='/app',stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    path.write_text(str(process.pid));print(json.dumps({'started':True,'pid':process.pid,'logs_discarded_to_prevent_credential_urls':True}))
elif action=='stop-daphne':
    path=Path('/evidence/media-source-daphne.pid')
    if path.exists():
        pid=int(path.read_text());cmdline=Path(f'/proc/{pid}/cmdline')
        if cmdline.exists():
            value=cmdline.read_bytes()
            if b'daphne' not in value or b'dispatcharr.asgi:application' not in value:raise RuntimeError('Refusing non-owned process')
            os.kill(pid,signal.SIGTERM)
        path.unlink()
    print(json.dumps({'stopped':True}))
elif action=='rapid-audit-before':
    from core.models import SystemEvent
    print(json.dumps({'after':SystemEvent.objects.order_by('-id').values_list('id',flat=True).first() or 0}))
elif action=='rapid-audit-after':
    from core.models import SystemEvent
    from core.utils import RedisClient
    after,sid=sys.argv[2].split(':',1)
    if not after.isascii() or not after.isdecimal() or not sid.startswith('fixture-packaged-vod-') or len(sid)>128:raise RuntimeError('Invalid owned rapid audit marker')
    seed=json.loads(Path('/evidence/vod-seed.json').read_text())
    redis=RedisClient.get_client()
    print(json.dumps({'audit':list(SystemEvent.objects.filter(id__gt=int(after),event_type__in=['vod_start','vod_stop']).order_by('id').values('event_type','details')),
                      'remaining_metadata':bool(redis.exists('vod_persistent_connection:'+sid)),
                      'provider_connections':int(redis.get('profile_connections:'+str(seed['provider_profile_id'])) or 0)}))
elif action=='inspect':
    from core.utils import RedisClient
    sid=sys.argv[2]
    if not sid.startswith('fixture-packaged-vod-') or len(sid)>128:raise RuntimeError('Invalid owned fixture session')
    row=RedisClient.get_client().hgetall('vod_persistent_connection:'+sid)
    print(json.dumps({'exists':bool(row),'active_streams':int(row.get('active_streams',0)),'bytes_sent':int(row.get('bytes_sent',0))}))
else:raise RuntimeError('Unknown fixture operation')
