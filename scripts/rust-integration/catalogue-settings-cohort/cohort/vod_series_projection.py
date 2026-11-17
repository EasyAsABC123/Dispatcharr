"""Semantic observations for the bounded Source Series catalogue workflow.
Original Dispatcharr and dependency authors retain application credit.
Generated IDs/timestamps are abstracted only with separate identity/time checks.
"""
import json,re
from pathlib import Path
PHASES=['fresh-nonempty-Series-no-Movies','unchanged-Series-repeat','Series-reassign-globaldedup-category-replace','empty-Series-prunes-only-stale-Series']
def ordered(rows):return sorted(rows,key=lambda r:json.dumps(r,sort_keys=True))
def sql_projection(s):
 cats={r['id']:[r['name'],r['category_type']]for r in s['VODCategory']};series={r['id']:[r['name'],r['year']]for r in s['Series']}
 def cp(v):
  if not isinstance(v,dict):return v
  result={k:cp(x)for k,x in v.items()}
  if '_category_id'in result:result['_category_id']=cats.get(result['_category_id'])
  return result
 skip={'id','uuid','created_at','updated_at','last_seen','last_episode_refresh'}
 result={'Series':ordered([{k:cp(v)for k,v in r.items()if k not in skip}for r in s['Series']]),'categories':ordered([{'name':r['name'],'type':r['category_type']}for r in s['VODCategory']]),'M3USeriesRelation':[],'category_links':[],'empty_models':{name:len(s[name])for name in ['Movie','Episode','VODLogo','M3UMovieRelation','M3UEpisodeRelation']}}
 for r in s['M3USeriesRelation']:
  row={k:cp(v)for k,v in r.items()if k not in skip|{'m3u_account_id','category_id','series_id'}};row.update(series=series[r['series_id']],category=cats.get(r['category_id']),owned_account=r['m3u_account_id']==s['account']['id'],last_episode_refresh_provided=r['last_episode_refresh']is not None);result['M3USeriesRelation'].append(row)
 result['M3USeriesRelation']=ordered(result['M3USeriesRelation'])
 for r in s['M3UVODCategoryRelation']:result['category_links'].append({'category':cats[r['category_id']],'owned_account':r['m3u_account_id']==s['account']['id'],'enabled':r['enabled'],'custom_properties':r['custom_properties']})
 result['category_links']=ordered(result['category_links']);return result

def project(private):
 private=Path(private);load=lambda p:json.loads(p.read_text());phases=[]
 for name in PHASES:
  p=private/name;before=load(p/'SQL-before.json');after=load(p/'SQL-after.json');backend=load(p/'backend.json');api=load(p/'HTTP.json');ws=load(p/'WS.json');trace=load(p/'Origin.json');catalogue=load(p/'catalogue.json');updates=[]
  for event in ws['events']:
   value=event.get('data')
   if isinstance(value,dict)and value.get('account')==after['account']['id']:
    value=dict(value,account='<owned-account>');value['message']=re.sub(r' in [0-9]+\.[0-9]{2} seconds$',' in <duration> seconds',value['message']);updates.append(value)
  requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r];actions=[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series'];assert len(requests)==5
  phases.append({'name':name,'HTTP':{'status':api['status'],'body':api['body']},'before':sql_projection(before),'after':sql_projection(after),'backend':{'status':backend['status'],'result':re.sub(r' in [0-9]+\.[0-9]{2} seconds$',' in <duration> seconds',backend['result']),'children':backend['children'],'traceback':backend['traceback']},'provider':[{'action':action,**{k:r[k]for k in ['method','path','status','actual_TLS','actual_user_agent']}}for r,action in zip(requests,actions)],'updates':updates,'catalogue':{'status':catalogue['status'],'count':catalogue['body']['count'],'names':sorted(r['name']for r in catalogue['body']['results'])}})
 return {'scope':'basic yearNULL Series/category-metadata list import/repeat/reassignment/dedup/cleanup; no episodes/advanced metadata/general API serialization','phases':phases}
