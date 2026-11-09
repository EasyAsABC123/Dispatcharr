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
elif action=='inspect':
    from core.utils import RedisClient
    sid=sys.argv[2]
    if not sid.startswith('fixture-packaged-vod-') or len(sid)>128:raise RuntimeError('Invalid owned fixture session')
    row=RedisClient.get_client().hgetall('vod_persistent_connection:'+sid)
    print(json.dumps({'exists':bool(row),'active_streams':int(row.get('active_streams',0)),'bytes_sent':int(row.get('bytes_sent',0))}))
else:raise RuntimeError('Unknown fixture operation')
