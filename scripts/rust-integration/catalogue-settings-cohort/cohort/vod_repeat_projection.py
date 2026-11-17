"""Bounded semantic projection, preserving actual Source/Native observations.
Original Dispatcharr and dependency authors retain all application credit.
Only generated IDs, timestamps, channel names and observed duration are abstracted.
Stability and monotonicity of stored identity/time fields are checked by the workflow.
"""
import json,re
from pathlib import Path

PHASES=['unchanged','update-reassign-deduplicate-category-replace','prune-stale-preserve-other-account','empty-categories-preserve-selected-catalogue','empty-Movies-clean-only-owned-stale-orphans']
def ordered(rows):return sorted(rows,key=lambda x:json.dumps(x,sort_keys=True))
def sql_projection(snapshot):
 own=snapshot['account']['id'];movies={r['id']:[r['name'],r['year']]for r in snapshot['Movie']};cats={r['id']:[r['name'],r['category_type']]for r in snapshot['VODCategory']}
 def cp(value):
  if not isinstance(value,dict):return value
  result={k:cp(v)for k,v in value.items()}
  if '_category_id'in result:result['_category_id']=cats.get(result['_category_id'])
  return result
 skip={'id','uuid','created_at','updated_at','last_seen','last_advanced_refresh'}
 result={'Movie':ordered([{k:cp(v)for k,v in r.items()if k not in skip}for r in snapshot['Movie']]),'categories':ordered([{'name':r['name'],'category_type':r['category_type']}for r in snapshot['VODCategory']])}
 for name in ['M3UMovieRelation','M3UVODCategoryRelation']:
  rows=[]
  for r in snapshot[name]:
   row={k:cp(v)for k,v in r.items()if k not in skip|{'m3u_account_id','category_id','movie_id'}};row['account']='owned'if r['m3u_account_id']==own else'other'
   row['category']=cats.get(r.get('category_id'))
   if 'movie_id'in r:row['movie']=movies[r['movie_id']]
   rows.append(row)
  result[name]=ordered(rows)
 result['empty_models']={name:len(snapshot[name])for name in ['Series','Episode','VODLogo','M3USeriesRelation','M3UEpisodeRelation']}
 return result

def project(private):
 private=Path(private);load=lambda p:json.loads(p.read_text());phases=[]
 for label in PHASES:
  p=private/label;before=load(p/'SQL-before.json');after=load(p/'SQL-after.json');backend=load(p/'backend.json');api=load(p/'API.json');ws=load(p/'WS.json');trace=load(p/'Origin.json')
  updates=[]
  for row in ws['events']:
   value=row.get('data')
   if isinstance(value,dict)and value.get('account')==after['account']['id']:
    value=dict(value,account='<owned-account>');value['message']=re.sub(r' in [0-9]+\.[0-9]{2} seconds$', ' in <duration> seconds',value['message']);updates.append(value)
  requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r]
  actions=[None,'get_vod_categories']if label=='empty-categories-preserve-selected-catalogue'else[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series']
  assert len(requests)==len(actions)
  provider=[{'action':action,**{k:r[k]for k in ['method','path','status','actual_TLS','actual_user_agent']}}for r,action in zip(requests,actions)]
  phases.append({'name':label,'HTTP':{'status':api['status'],'body':api['body']},'before':sql_projection(before),'after':sql_projection(after),'backend':{'status':backend['status'],'result':re.sub(r' in [0-9]+\.[0-9]{2} seconds$', ' in <duration> seconds',backend['result']),'children':backend['children'],'traceback':backend['traceback']},'provider':provider,'updates':updates})
 final=load(private/'final-catalogue.json');return {'phases':phases,'final_catalogue':{'status':final['status'],'body':final['body']}}
