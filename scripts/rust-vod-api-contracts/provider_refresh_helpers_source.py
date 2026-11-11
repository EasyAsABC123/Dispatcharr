"""Original pure provider refresh conversion corpus, explicitly not TCP/SQL.
Original Dispatcharr authors retain all algorithm credit.
"""
import datetime,json,os,sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.vod import tasks
from apps.vod.models import Episode
import _strptime
values=[None,False,True,0,1,-1,172800,172801,1800.0,1.5,1e-8,1e20,'',' ','\x1c x \x1f','  8,5  ','8.0','nan','NaN','inf','-Infinity','1_000','١٢','²','１１２','1:30','1:02:03','1:99','-1:61',' 1 : 02 ','+2','1.0',[],[None],[None,''],[None,' x '],[False,12],[[1,2]],{}, {'a':1}]
records=[]
def record(name,args):
 try:
  value=(args[0]==args[1])if name=='provider_equal'else(Episode._meta.get_field('air_date').get_prep_value(tasks.extract_date_from_data(args[0]))if name=='episode_date_field'else getattr(tasks,name)(*args))
  actual={'value':value.isoformat()if isinstance(value,(datetime.date,datetime.datetime))else value}
 except Exception as error:actual={'error_type':type(error).__name__,'message':str(error)}
 records.append({'function':name,'args':args,'expected':actual})
for value in values:
 for function in ['normalize_duration_secs','normalize_rating','extract_string_from_array_or_string']:record(function,[value])
 record('extract_duration_from_data',[{'duration_secs':value,'duration':'2:03'}]);record('extract_duration_from_data',[{'duration':value}]);record('clean_custom_properties',[value])
for existing in [None,'',' text ',[],[None],False,{},'populated']:
 for new in values:record('should_update_field',[existing,new])
for data in [{'youtube_trailer':None,'actors':[],'director':' ','cast':[None,' A '],'backdrop_path':[None,' /image '],'x':False,'y':{},'z':[None,None]}, {'x':0,'a':[],'b':'','c':None,'d':[False]}, {'actors':[1,False,{'x':1}],'backdrop_path':' /one '}]:record('clean_custom_properties',[data])
for value in values:
 record('extract_year_from_data',[{'year':value,'name':'Fallback (2024)'}]);record('extract_year_from_data',[{'releaseDate':value,'name':'Fallback (2024)'}]);record('_coerce_provider_info',[value]);record('episode_date_field',[{'air_date':value,'releasedate':'2020-1-2'}])
for title in ['Title (2024)','Title (1800) later (2024)','Title - 2023','Title 2022','Title (２０２４)','Title (2031) - 2020','Title (2024) - 2021','Title २०२४','Title (2024)\n','Title (2024) space',None,False,3,{},['x']]:record('extract_year_from_data',[{'name':title}])
for value in ['2024-2-3','20240203','2024-W05-6','2024W056','2024-01-01T00:30:00+02:00','0001-01-01T00:00:00+23:00','9999-12-31T23:00:00-23:00','2024-02-30','2024-02-03\n',' 2024-02-03','２０２４-２-３','２０２４-2-3','2024-2-٣','2024-02-0٣','2024-02- 3','2024-02-03T12:00:00.1234567Z','2024-02-03x12:00:00','2024-02-03+12:00','2024-02-03T+02:00','2024-02-03T12:30:00,5+00:00.5']:record('episode_date_field',[{'air_date':value}])
for left in values:
 for right in values:record('provider_equal',[left,right])
print(json.dumps({'passed':True,'scope':__doc__,'records':records,'count':len(records),'strptime_patterns':{key:_strptime.TimeRE()[key]for key in ['Y','m','d']},'datetime':datetime.datetime.now(datetime.timezone.utc).isoformat()}))
