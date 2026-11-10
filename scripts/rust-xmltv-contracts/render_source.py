"""Actual original XML renderer block and standard-dummy helper vectors.

Full application/algorithm credit belongs to original Dispatcharr contributors.
Loaded inside the disposable pinned original container after Django setup.
"""
import ast,hashlib,html,json
from datetime import datetime,timedelta,timezone
from pathlib import Path
from apps.epg.utils import sd_poster_proxy_path
from apps.output.dummy_epg import generate_standard_dummy_programs,_STANDARD_DUMMY_TIME_DESCRIPTIONS

def capture(rich):
    path=Path('/app/apps/output/epg.py');raw=path.read_bytes();module=ast.parse(raw)
    loop=next(node for node in ast.walk(module) if isinstance(node,ast.For) and isinstance(node.target,ast.Name) and node.target.id=='prog')
    def sets(node,name):return isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id==name for target in node.targets)
    start=next(at for at,node in enumerate(loop.body) if sets(node,'st'));end=next(at for at,node in enumerate(loop.body) if sets(node,'xml_text'))
    block=ast.Module(body=loop.body[start:end+1],type_ignores=[]);compiled=compile(block,str(path),'exec')
    base={'id':17,'epg_id':4,'start_time':datetime(2026,1,2,3,4,5,tzinfo=timezone.utc),'end_time':datetime(2026,1,2,4,5,6,tzinfo=timezone.utc),'title':'Title <&> \' " café 😀','sub_title':None,'description':None,'custom_properties':{}}
    cases=[]
    choices=[{},rich,None,[],['not-a-map'],{'ignored':'kept out'}]
    for key in ['categories','keywords','onscreen_episode','episode','season','dd_progid','thetvdb.com_id','themoviedb.org_id','imdb.com_id','language','original_language','length','video','audio','subtitles','rating','rating_system','star_ratings','reviews','images','credits','date','country','icon','sd_icon','previously_shown','previously_shown_details','premiere','premiere_text','last_chance','last_chance_text','new','live']:
        for value in [None,False,True,0,2,1.5,'','Text <&> \' " 😀',[],['One','Two &'],{}]:choices.append({key:value})
    choices.extend([{'season':a,'episode':b} for a,b in [(1,2),(0,0),('02','03'),('٢','٣'),('²','³'),(-1,-2),('1.0','2.0'),('bad','bad'),(True,False)]])
    for props in choices:
        prog={**base,'custom_properties':props};scope={'prog':prog,'escaped_primary_cid':html.escape('channel<&\'"'),'_poster_site_origin':'https://origin.invalid:8443','html':html,'sd_poster_proxy_path':sd_poster_proxy_path}
        row={'programme':{**prog,'start_time':prog['start_time'].isoformat(),'end_time':prog['end_time'].isoformat()},'channel_id':'channel<&\'"','origin':'https://origin.invalid:8443'}
        try:exec(compiled,scope)
        except Exception as error:row['error']=type(error).__name__
        else:row['xml']=scope['xml_text']
        cases.append(row)
    dummy=[]
    for hour in range(24):
        for days in [0,1,3]:
            now=datetime(2026,1,2,hour,tzinfo=timezone.utc)
            for bounds in [False,True]:
                lookback=now+timedelta(hours=4) if bounds else None;cutoff=now+timedelta(hours=12) if bounds else None
                result=generate_standard_dummy_programs('fixture','Channel <&> 東京',now,num_days=days,program_length_hours=4,export_lookback=lookback,export_cutoff=cutoff)
                dummy.append({'hour':hour,'days':days,'lookback':lookback.isoformat() if lookback else None,'cutoff':cutoff.isoformat() if cutoff else None,'programmes':[{**p,'start_time':p['start_time'].isoformat(),'end_time':p['end_time'].isoformat()} for p in result]})
    return {'scope':'Actual original inline real-programme XML statements and standard dummy helper','file_sha256':hashlib.sha256(raw).hexdigest(),'block_ast_sha256':hashlib.sha256(ast.dump(block,include_attributes=False).encode()).hexdigest(),'real_programme_cases':cases,'standard_dummy_cases':dummy,'standard_dummy_descriptions':[{'start':start,'end':end,'templates':templates}for(start,end),templates in _STANDARD_DUMMY_TIME_DESCRIPTIONS.items()],'source_digit_codepoints':[code for code in range(0x110000) if chr(code).isdigit()],'native_verified':False}
