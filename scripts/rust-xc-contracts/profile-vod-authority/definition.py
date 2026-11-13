"""Fixed Source-only case expansion; no Source/native process or application imports."""
ACTIONS=['get_vod_categories','get_vod_streams','get_series_categories','get_series','get_vod_info','get_series_info']
ROLES=['admin','standard','basic','hide_adult','disabled']
def cases():
 out=[{'name':role+':'+action,'role':role,'action':action,'query':{}}for role in ROLES for action in ACTIONS]
 for name,action,query in [('movie-filter-present','get_vod_streams',{'category_id':'$movie_category'}),('movie-filter-absent','get_vod_streams',{'category_id':'999999999'}),('series-filter-present','get_series',{'category_id':'$series_category'}),('series-filter-absent','get_series',{'category_id':'999999999'}),('movie-info-missing-id','get_vod_info',{'vod_id':None}),('series-info-missing-id','get_series_info',{'series_id':None})]:out.append({'name':name,'role':'standard','action':action,'query':query})
 out += [{'name':'invalid-password','role':'admin','action':'get_vod_streams','query':{},'invalid_password':True},{'name':'missing-password','role':'standard','action':'get_series','query':{},'missing_password':True}]
 return out

def query(case,seed):
 v={'action':case['action'],'username':seed['roles'][case['role']]['username'],'password':seed['roles'][case['role']]['xc_password']}
 if case['action']=='get_vod_info':v['vod_id']=str(seed['movie_id'])
 if case['action']=='get_series_info':v['series_id']=str(seed['series_id'])
 for k,x in case['query'].items():
  if x is None:v.pop(k,None)
  else:v[k]=str(seed[x[1:]])if isinstance(x,str)and x.startswith('$')else x
 if case.get('invalid_password'):v['password']='predeclared-wrong-owned-credential'
 if case.get('missing_password'):v.pop('password',None)
 return v
