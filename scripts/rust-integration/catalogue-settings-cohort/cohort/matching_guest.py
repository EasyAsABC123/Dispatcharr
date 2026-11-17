"""Owned normal-graph matching setup/readback and pure Source extraction.
Original Dispatcharr and dependencies retain all application/algorithm credit.
No task body, matching helper or application is imported on module import.
"""
import ast,base64,hashlib,importlib.util,importlib.metadata,json,logging,os,re,uuid,sys,unicodedata
from pathlib import Path

def pure_vectors():
    """Execute only named pure Source definitions, never Django/task bodies.
    Fake model scores below are decision vectors, NOT model inference evidence.
    """
    import heapq
    from rapidfuzz import fuzz
    path=Path('/app/apps/channels/epg_matching.py');raw=path.read_bytes();tree=ast.parse(raw)
    names={'normalize_name','_compute_fuzzy_score','_fuzzy_scan_core','_get_epg_match_thresholds','try_epg_name_match'}
    selected=[n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name in names]
    assert {n.name for n in selected}==names
    constants={}
    for n in tree.body:
        if isinstance(n,ast.Assign)and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)and n.targets[0].id in {'COMMON_EXTRANEOUS_WORDS','ML_CANDIDATE_LIMIT'}:
            constants[n.targets[0].id]=ast.literal_eval(n.value)
    class Settings:
        value={}
        @classmethod
        def get_epg_settings(cls):return cls.value
    model_calls=[]
    ns={**constants,'re':re,'heapq':heapq,'fuzz':fuzz,'logger':logging.getLogger('owned-source-pure-vectors'),'CoreSettings':Settings,'_normalize_settings_cache':None}
    exec(compile(ast.Module(body=selected,type_ignores=[]),str(path),'exec'),ns)
    advanced={'epg_match_mode':'advanced','epg_match_ignore_prefixes':['HD:','SD:'],'epg_match_ignore_suffixes':[' 4K'],'epg_match_ignore_custom':['Plus']}
    cases=[]
    malformed=[None,False,0,7,"HD:",{},["HD:"],{"epg_match_mode":"advanced","epg_match_ignore_prefixes":"HD:","epg_match_ignore_suffixes":None,"epg_match_ignore_custom":{"Plus":True}},
      {"epg_match_mode":"advanced","epg_match_ignore_prefixes":[None,False,0,7,[],{},"","HD:"],"epg_match_ignore_suffixes":[False,[]," 4K"],"epg_match_ignore_custom":[True,None,{},"Plus"]},
      {"epg_match_mode":[],"epg_match_ignore_prefixes":["HD:"]}]
    vectors=[('HD:ABC 7 (WXYZ) - Springfield',advanced),('HD:SD:Channel 5',advanced),('HBO Plus East 4K',advanced),('a\u0301\u200d_b',{}),('İΣ ²_À\u001cX',{}),('A [one\ntwo] B (ABCD)',{}),('TV 24/7 Film Movies NBC',{}),
      ('a\u0345\u05b0_b',{}),('ΟΣ ΟΣΑ ΟΣ\nΣ',{}),('A (AB\nCD) [EF\nGH] B',{}),
      ('HD:SD:Owned Plus East 4K',{'epg_match_mode':'advanced','epg_match_ignore_prefixes':['HD:','HD:SD:'],'epg_match_ignore_suffixes':[' 4K',' East 4K'],'epg_match_ignore_custom':['Plus']}),
      ('HD:SD:Owned Plus East 4K',{'epg_match_mode':'advanced','epg_match_ignore_prefixes':['HD:SD:','HD:'],'epg_match_ignore_suffixes':[' East 4K',' 4K'],'epg_match_ignore_custom':['Plus']}),
      *[('HD:Owned Plus East 4K',settings)for settings in malformed],
      *[(name,settings)for name in [None,False,0,[],{},17,['Owned'],{'name':'Owned'}]for settings in [{},advanced]]]
    for ordinal,(name,settings)in enumerate(vectors):
        Settings.value=settings;ns['_normalize_settings_cache']=None
        value={'ordinal':ordinal,'input':name,'settings_snapshot':settings,'snapshot_interface':'CoreSettings.get_epg_settings return value supplied directly; malformed top-level snapshots are pure failure probes, not claimed ordinary DB outputs'}
        try:value.update(outcome='value',actual=ns['normalize_name'](name))
        except Exception as error:value.update(outcome='error',error_class=type(error).__name__)
        cases.append(value)
    # Original caches one snapshot per task invocation, then cleanup resets it.
    # Demonstrate actual cache behavior independently of changing DB snapshots.
    ns['_normalize_settings_cache']=None;Settings.value=advanced
    first=ns['normalize_name']('HD:Owned');Settings.value={}
    retained=ns['normalize_name']('HD:Owned');ns['_normalize_settings_cache']=None
    refreshed=ns['normalize_name']('HD:Owned')
    cache_vector={'first':first,'changed_snapshot_without_cleanup':retained,'after_declared_cache_reset':refreshed}
    ratios=[{'left':a,'right':b,'actual':fuzz.ratio(a,b)}for a,b in [('ab','ac'),('',''),('é','é'),('İ','i\u0307'),('aaaaab','baaaaa')]]
    row={'id':1,'tvg_id':'owned.us','name':'Owned','norm_name':'owned','epg_source_priority':10}
    regions=[{'region':r,'actual':ns['_compute_fuzzy_score']('owned',row,r)}for r in [None,'us','gb']]
    high={**row,'id':2,'epg_source_priority':20};ties=ns['_fuzzy_scan_core']('owned',[row,high],None)
    assert ties[1]['id']==2
    many=[{**row,'id':n,'epg_source_priority':n}for n in range(31)]
    top=ns['_fuzzy_scan_core']('owned',many,None);assert len(top[2])==20 and top[1]['id']==30
    boundaries=[]
    for bulk in [False,True]:
        threshold=ns['_get_epg_match_thresholds'](bulk)
        for score in ([19,20,39,40,74,75,84,85]if not bulk else[49,50,69,70,79,80,89,90]):
            for supplied in [None,0.49,0.50,0.65,0.75]:
                current=[]
                def model():
                    current.append('availability');return (None,None)if supplied is None else(object(),object())
                def scores(*args):
                    current.append('actual-Source-decision-with-test-score');return [supplied]
                ns['get_sentence_transformer']=model;ns['_ml_cosine_similarities']=scores
                got=ns['try_epg_name_match']({'id':1,'name':'Owned','norm_chan':'owned'},score,row,[(score,row)],bulk,use_ml=True,ml_state={})
                boundaries.append({'bulk':bulk,'fuzzy':score,'supplied_model_score':supplied,'matched_id':None if got is None else got['id'],'calls':current})
    return {'source_sha256':hashlib.sha256(raw).hexdigest(),'actual_Python_version':sys.version,'actual_Python_implementation':sys.implementation.name,'actual_Unicode_version':unicodedata.unidata_version,'extraction_names':sorted(names),'normalization':cases,'normalization_cache':cache_vector,'RapidFuzz_distribution':importlib.metadata.version('rapidfuzz'),'ratios':ratios,'regions':regions,'priority_tie_actual_best_id':ties[1]['id'],'top20_actual_ids':[r['id']for _,r in top[2]],'threshold_decisions':boundaries,'actual_model_inference':False,'application_task_body_imported':False}

def identity():
    return {'resuid':list(os.getresuid()),'resgid':list(os.getresgid()),'groups':os.getgroups()}

def uid_status(pid):
    rows=dict(line.split(':',1)for line in(Path('/proc')/str(pid)/'status').read_text().splitlines()if ':'in line)
    return {key:rows[key].split()for key in ('Uid','Gid','Groups')}

def observe_availability(worker_pid,expected_generation):
    before=identity();clear={'attempted':True}
    try:os.setgroups([]);clear['completed']=True
    except PermissionError as error:clear.update(completed=False,error_class=type(error).__name__)
    os.setgid(65532);os.setuid(65532)
    after=identity();assert after['resuid']==[65532]*3 and after['resgid']==[65532]*3
    if clear['completed']:assert after['groups']==[]
    observed={'observer_identity_before':before,'observer_identity_after':after,'supplementary_group_clear':clear}
    try:
        observed['actual_target_status_before']=uid_status(worker_pid)
        assert observed['actual_target_status_before']['Uid']==['65532']*4 and observed['actual_target_status_before']['Gid']==['65532']*4
        observed.update(availability(worker_pid,expected_generation));observed['actual_target_status_after']=uid_status(worker_pid)
        assert observed['actual_target_status_before']==observed['actual_target_status_after']
        observed['status']='passed'
    except Exception as error:observed.update(status='failed',error_class=type(error).__name__)
    return observed

def availability(worker_pid,expected_generation):
    import importlib.metadata
    assert os.geteuid()==65532 and os.getegid()==65532
    process=Path('/proc')/str(worker_pid)
    before=(process/'stat').read_text().rsplit(')',1)[1].split()[19]
    environ=(process/'environ').read_bytes().split(b'\0')
    env={}
    for row in environ:
        key,sep,value=row.partition(b'=')
        if key in [b'DISABLE_ML_DOWNLOADS',b'HF_HUB_OFFLINE',b'TRANSFORMERS_OFFLINE']:
            env[key.decode()]=value.decode(errors='replace')
    after=(process/'stat').read_text().rsplit(')',1)[1].split()[19]
    assert before==after==expected_generation
    cache=Path('/data/models/models--sentence-transformers--all-MiniLM-L6-v2')
    return {'actual_observer_euid':os.geteuid(),'actual_observer_egid':os.getegid(),'actual_worker_pid':worker_pid,'actual_worker_generation':after,'generation_fenced':True,'actual_worker_environment':env,'sentence_transformers_import_spec_present':importlib.util.find_spec('sentence_transformers')is not None,'torch_import_spec_present':importlib.util.find_spec('torch')is not None,'model_cache_path_present':cache.exists(),'model_cache_snapshots':[p.name for p in sorted((cache/'snapshots').glob('*'))][:32],'model_loaded':False,'downloads_attempted':False,'environment_changed':False}

def seed(spec):
    from django.db import connection
    from apps.channels.models import Channel,ChannelGroup
    from apps.epg.models import EPGSource,EPGData
    from core.models import CoreSettings,EPG_SETTINGS_KEY
    from django.core.cache import cache
    assert connection.settings_dict['NAME'].startswith('normal_dvr_')
    # Explicit fixture reset, after prior owned task/children have settled.
    # Avoid model signals/setup publications; all tested effects follow HTTP.
    with connection.cursor()as c:
        c.execute('TRUNCATE dispatcharr_channels_channel RESTART IDENTITY CASCADE')
        c.execute('TRUNCATE epg_epgsource RESTART IDENTITY CASCADE')
    sources={}
    for item in spec['sources']:
        obj=EPGSource(name=item['key'],source_type=item.get('type','dummy'),is_active=item.get('active',True),priority=item.get('priority',0),username='',password='',url=None)
        EPGSource.objects.bulk_create([obj]);sources[item['key']]=obj
    epgs={}
    for item in spec['epgs']:
        obj=EPGData(name=item['name'],tvg_id=item['tvg'],epg_source=sources[item['source']]);EPGData.objects.bulk_create([obj]);epgs[item['key']]=obj
    group=ChannelGroup.objects.filter(name='Owned matching group').first()
    if group is None:
        group=ChannelGroup(name='Owned matching group');ChannelGroup.objects.bulk_create([group])
    # No existing custom relation: single save's real post_save must create it.
    from apps.channels.models import ChannelGroupM3UAccount
    ChannelGroupM3UAccount.objects.filter(channel_group=group).delete()
    channels={}
    for n,item in enumerate(spec['channels']):
        obj=Channel(name=item['name'],tvg_id=item.get('tvg',''),tvc_guide_stationid=item.get('gracenote',''),channel_number=8000+n,epg_data=epgs.get(item.get('assigned')),channel_group=group,auto_created=False)
        Channel.objects.bulk_create([obj]);channels[item['key']]=obj
    CoreSettings.objects.update_or_create(key=EPG_SETTINGS_KEY,defaults={'name':'EPG Settings','value':spec.get('settings',{})})
    CoreSettings.objects.update_or_create(key='preferred-region',defaults={'name':'Preferred region','value':spec.get('region','')})
    cache.clear()
    return {'channels':{k:v.pk for k,v in channels.items()},'epgs':{k:v.pk for k,v in epgs.items()},'sources':{k:v.pk for k,v in sources.items()},'group_id':group.pk,'declared':spec}

def receiver_begin(receiver):
    import redis
    from channels_redis.core import RedisChannelLayer
    from asgiref.sync import async_to_sync
    layer=RedisChannelLayer(hosts=['redis://:playback-fixture-only@127.0.0.1:6379/0'])
    async_to_sync(layer.group_add)('updates',receiver)
    return {'actual_group_add_completed':True,'receiver':receiver}

def snapshot(seed,receiver):
    import redis
    from channels_redis.core import RedisChannelLayer
    from apps.channels.models import Channel,ChannelGroupM3UAccount
    from apps.m3u.models import M3UAccount
    from apps.output.streaming_chunk_cache import EPG_CACHE_GENERATION_KEY
    client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
    layer=RedisChannelLayer(hosts=['redis://:playback-fixture-only@127.0.0.1:6379/0'])
    key=layer.prefix+receiver.split('!')[0]+'!';raw=client.zrange(key,0,1024)
    assert len(raw)<=1024 and sum(map(len,raw))<=4*1024*1024
    decoded=[layer.deserialize(v)for v in raw]
    custom=M3UAccount.get_custom_account()
    value={'channels':list(Channel.objects.filter(pk__in=list(seed['channels'].values())).order_by('id').values()),'events':decoded,'raw_events_base64':[base64.b64encode(x).decode()for x in raw],'output_cache_generation':client.get(EPG_CACHE_GENERATION_KEY),'group_custom':ChannelGroupM3UAccount.objects.filter(channel_group_id=seed['group_id'],m3u_account=custom).exists()}
    if value['output_cache_generation']is not None:value['output_cache_generation']=int(value['output_cache_generation'])
    client.close();return value

def trigger(enabled):
    from django.db import connection
    with connection.cursor()as c:
        c.execute('DROP TRIGGER IF EXISTS owned_matching_failure ON dispatcharr_channels_channel')
        if enabled:
            c.execute("CREATE OR REPLACE FUNCTION owned_matching_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='owned-matching-failure'; END $$")
            c.execute('CREATE TRIGGER owned_matching_failure BEFORE UPDATE ON dispatcharr_channels_channel FOR EACH ROW EXECUTE FUNCTION owned_matching_failure()')
    return {'enabled':enabled}

def backend(ident):
    from celery import current_app
    value=current_app.backend.get_task_meta(ident)
    return value if value.get('status')in ('SUCCESS','FAILURE','REVOKED')else None


def begin_case(spec, receiver):
    """Run unchanged inert setup/readback functions in their original order.
    Only the Native fixture uses this call-fusion. No matching/task body or
    production function is invoked, patched or bypassed.
    """
    actual_seed = seed(spec)
    actual_receiver = receiver_begin(receiver)
    before = snapshot(actual_seed, receiver)
    return {'seed': actual_seed, 'receiver': actual_receiver, 'before': before}
