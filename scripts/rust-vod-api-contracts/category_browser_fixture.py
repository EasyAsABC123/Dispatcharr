"""Owned reversible category browser preparation; no production API claim.
Original Dispatcharr authors retain all application credit; AGPL retained.
Credentials arrive only on stdin; plaintext is neither printed nor persisted.
"""
import os,sys,json
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from apps.accounts.models import User
from apps.vod.models import Movie,Series,VODCategory,M3UVODCategoryRelation
from apps.m3u.models import M3UAccount
prefix='rust-probe-vod-rest-';config=json.load(sys.stdin);state=Path('/tmp/dispatcharr-vod-category-browser-state.json')
assert Movie.objects.filter(name__startswith=prefix).count()==27 and Series.objects.filter(name__startswith=prefix).count()==4
names=[prefix+'series_off',prefix+'movies_off']
if config['op']=='prepare':
    if state.exists():raise RuntimeError('Browser fixture already prepared')
    password=config['password'];assert isinstance(password,str)and len(password)>=24
    users=list(User.objects.filter(username__in=names).order_by('id').values());assert len(users)==2 and all(row['user_level']==1 and row['is_active']for row in users)
    category=VODCategory.objects.get(name=prefix+'Drama',category_type='series');account=M3UAccount.objects.get(name=prefix+'active')
    links=list(M3UVODCategoryRelation.objects.filter(category=category,m3u_account=account).values('id','enabled'));assert len(links)==1
    saved={'users':users,'movies':list(Movie.objects.filter(name__startswith=prefix).values('id','logo_id')),'series':list(Series.objects.filter(name__startswith=prefix).values('id','logo_id')),'links':links}
    with state.open('x')as stream:os.chmod(state,0o600);json.dump(saved,stream,cls=DjangoJSONEncoder)
    for user in User.objects.filter(username__in=names):user.set_password(password);user.save(update_fields=['password'])
    Movie.objects.filter(name__startswith=prefix).update(logo=None);Series.objects.filter(name__startswith=prefix).update(logo=None)
    M3UVODCategoryRelation.objects.filter(id=links[0]['id']).update(enabled=True)
    print(json.dumps({'prepared':True,'roles':[{'username':prefix+'series_off','kind':'movies','label':prefix+'Drama (movie)','category':prefix+'Drama|movie'},{'username':prefix+'movies_off','kind':'series','label':prefix+'Drama (series)','category':prefix+'Drama|series'}],'scope':'Actual unchanged React category selection on seeded movies/series, single-type Standard roles; posters removed only for this separate UI phase; provider/artwork/unified flows not claimed','posters_deliberately_excluded':True}))
elif config['op']in ['reset_login_history','login_history']:
    if not state.exists():raise RuntimeError('Prepared owned browser fixture required')
    import redis
    from django.core.cache import cache
    assert os.environ.get('REDIS_PASSWORD')=='playback-fixture-only'
    client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only')
    keys=list(client.scan_iter(match=':1:throttle_login_*',count=64));assert len(keys)<=16
    lengths=[];contents=[]
    for key in keys:
        logical=key.decode('utf-8').removeprefix(':1:');history=cache.get(logical,[]);assert isinstance(history,list);lengths.append(len(history));contents.append({'key':key.decode('utf-8'),'timestamps':history})
    if keys and config['op']=='reset_login_history':client.delete(*keys)
    print(json.dumps({'cleared_owned_login_buckets':len(keys)if config['op']=='reset_login_history'else 0,'observed_owned_login_buckets':len(keys),'history_entry_counts':lengths,'history_before_clear':contents,'scope':'Independent original/native browser phase isolation only; unchanged3/min login policy, no other caches or production state cleared'}))
elif config['op']=='restore':
    saved=json.loads(state.read_text())
    for row in saved['users']:
        user=User.objects.get(pk=row['id'],username=row['username']);user.password=row['password'];user.save(update_fields=['password'])
        User.objects.filter(pk=row['id'],username=row['username']).update(**{key:value for key,value in row.items()if key!='id'})
    for model,key in [(Movie,'movies'),(Series,'series')]:
        for row in saved[key]:model.objects.filter(pk=row['id'],name__startswith=prefix).update(logo_id=row['logo_id'])
    for row in saved['links']:M3UVODCategoryRelation.objects.filter(pk=row['id']).update(enabled=row['enabled'])
    current=list(User.objects.filter(username__in=names).order_by('id').values());assert json.loads(json.dumps(current,cls=DjangoJSONEncoder))==saved['users']
    assert list(Movie.objects.filter(name__startswith=prefix).values('id','logo_id'))==saved['movies'];assert list(Series.objects.filter(name__startswith=prefix).values('id','logo_id'))==saved['series']
    assert list(M3UVODCategoryRelation.objects.filter(pk__in=[row['id']for row in saved['links']]).values('id','enabled'))==saved['links']
    state.unlink();print(json.dumps({'restored':True,'users_passwords_and_rows_exact':True,'poster_relations_exact':True,'category_enabled_flags_exact':True}))
else:raise RuntimeError('Unknown browser fixture operation')
