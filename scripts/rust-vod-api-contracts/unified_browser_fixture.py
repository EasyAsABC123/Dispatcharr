"""Owned reversible All-browser fixture; no API/runtime/policy modification.
Original Dispatcharr authors retain credit. Password only through stdin.
"""
import os,sys,json,datetime
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from apps.accounts.models import User
from apps.vod.models import Movie,Series,VODCategory,M3UVODCategoryRelation
from apps.m3u.models import M3UAccount
prefix='rust-probe-vod-rest-';config=json.load(sys.stdin);state=Path('/tmp/dispatcharr-vod-All-browser-state.json')
assert Movie.objects.filter(name__startswith=prefix).count()==27 and Series.objects.filter(name__startswith=prefix).count()==4
name=prefix+'standard'
def exact(rows):return [{key:value.isoformat()if isinstance(value,(datetime.datetime,datetime.date))else value for key,value in row.items()}for row in rows]
if config['op']=='prepare':
 if state.exists():raise RuntimeError('Owned All browser already prepared')
 password=config['password'];assert isinstance(password,str)and len(password)>=24
 user=User.objects.get(username=name);assert user.user_level==1 and user.is_active and user.custom_properties=={}
 category=VODCategory.objects.get(name=prefix+'Drama',category_type='series');account=M3UAccount.objects.get(name=prefix+'active')
 links=list(M3UVODCategoryRelation.objects.filter(category=category,m3u_account=account).values('id','enabled'));assert len(links)==1
 saved={'users':exact(User.objects.filter(pk=user.pk).values()),'movies':list(Movie.objects.filter(name__startswith=prefix).values('id','logo_id')),'series':list(Series.objects.filter(name__startswith=prefix).values('id','logo_id')),'links':links}
 with state.open('x')as stream:os.chmod(state,0o600);json.dump(saved,stream,cls=DjangoJSONEncoder)
 user.set_password(password);user.save(update_fields=['password'])
 Movie.objects.filter(name__startswith=prefix).update(logo=None);Series.objects.filter(name__startswith=prefix).update(logo=None);M3UVODCategoryRelation.objects.filter(pk=links[0]['id']).update(enabled=True)
 print(json.dumps({'prepared':True,'username':name,'label':prefix+'Drama (series)','category':prefix+'Drama|series','count':29,'posters_deliberately_excluded':True,'user_datetime_restore':'Fullisoformat microseconds; no DjangoJSONEncoder millisecond shortening','scope':'Both-flags Standard All browser fixture; exactly two separate phase logins below unchanged3/min allowance, no login-bucket reset'}))
elif config['op']=='restore':
 saved=json.loads(state.read_text())
 for row in saved['users']:User.objects.filter(pk=row['id'],username=row['username']).update(**{key:value for key,value in row.items()if key!='id'})
 for model,key in [(Movie,'movies'),(Series,'series')]:
  for row in saved[key]:model.objects.filter(pk=row['id'],name__startswith=prefix).update(logo_id=row['logo_id'])
 for row in saved['links']:M3UVODCategoryRelation.objects.filter(pk=row['id']).update(enabled=row['enabled'])
 assert exact(User.objects.filter(username=name).values())==saved['users'];assert list(Movie.objects.filter(name__startswith=prefix).values('id','logo_id'))==saved['movies'];assert list(Series.objects.filter(name__startswith=prefix).values('id','logo_id'))==saved['series'];assert list(M3UVODCategoryRelation.objects.filter(pk__in=[row['id']for row in saved['links']]).values('id','enabled'))==saved['links']
 state.unlink();print(json.dumps({'restored':True,'users_passwords_full_microsecond_rows_exact':True,'poster_relationships_exact':True,'category_enabled_flags_exact':True}))
else:raise RuntimeError('Unknown owned fixture operation')
