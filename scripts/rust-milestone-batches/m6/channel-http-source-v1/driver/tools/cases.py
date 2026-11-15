"""Genuinely new HTTP-only cells. Task bodies are intentionally never consumed."""
ROUTES={'names':('set-names-from-epg/','apps.channels.tasks.set_channels_names_from_epg'),'logos':('set-logos-from-epg/','apps.channels.tasks.set_channels_logos_from_epg'),'tvg':('set-tvg-ids-from-epg/','apps.channels.tasks.set_channels_tvg_ids_from_epg'),'bulk':('from-stream/bulk/','apps.channels.tasks.bulk_create_channels_from_streams')}
def plan(channel,stream,source):
 cases=[]
 def add(name,route,body,role='admin',status=200,frames=0,method='POST',raw=None,control=None):cases.append({'name':name,'route':route,'body':body,'credential_role':role,'expected_status':status,'expected_new_family_frames':frames,'method':method,'raw_body':raw,'control':control})
 for route in ROUTES:
  body={'stream_ids':[stream]}if route=='bulk'else{'channel_ids':[channel]};success=202 if route=='bulk'else 200
  add(route+'-admin',route,body,status=success,frames=1)
  for role,status in [('anonymous',401),('standard',403),('inactive',401),('bad_bearer',401)]:add(route+'-'+role,route,body,role,status)
  for role in ['admin_x_api_key','admin_authorization_api_key']:add(route+'-'+role,route,body,role,success,1)
  add(route+'-bad-JWT-before-valid-key',route,body,'bad_bearer_with_valid_api_key',401)
  add(route+'-GET-admin',route,None,method='GET',status=405)
  add(route+'-GET-standard',route,None,'standard',403,method='GET')
  add(route+'-OPTIONS-admin',route,None,method='OPTIONS',status=200)
  add(route+'-required-empty',route,{},status=400)
  add(route+'-body-list',route,[],status=500)
  add(route+'-malformed-JSON',route,None,status=400,raw='{"')
  add(route+'-permission-before-JSON',route,None,'standard',403,raw='{"')
  wrong={'stream_ids':['not-an-integer']}if route=='bulk'else{'channel_ids':['not-an-integer']}
  add(route+'-noninteger-list-child-publishes',route,wrong,status=success,frames=1)
  duplicate={'stream_ids':[stream,stream]}if route=='bulk'else{'channel_ids':[channel,channel]}
  add(route+'-duplicates-preserved',route,duplicate,status=success,frames=1)
  add(route+'-global-UI-denied',route,body,status=403,control='global_deny')
  add(route+'-user-UI-denied',route,body,status=403,control='user_deny')
  add(route+'-untrusted-proxy-cannot-bypass',route,body,'admin_spoof',403,control='global_deny')
 add('logos-source-id','logos',{'epg_source_id':source},status=200,frames=1)
 add('logos-both-selectors','logos',{'epg_source_id':source,'channel_ids':[channel]},status=400)
 add('logos-empty-explicit-beats-source','logos',{'epg_source_id':source,'channel_ids':[]},status=400)
 add('logos-truthy-nonlist-before-source-type','logos',{'epg_source_id':source,'channel_ids':'wrong'},status=400)
 add('logos-invalid-source-publishes-before-HTTP500','logos',{'epg_source_id':'not-an-integer'},status=500,frames=1)
 for profiles in [[],[0],7]:add('bulk-profile-'+repr(profiles),'bulk',{'stream_ids':[stream],'channel_profile_ids':profiles},status=202,frames=1)
 add('bulk-start-minus-one','bulk',{'stream_ids':[stream],'starting_channel_number':-1},status=202,frames=1)
 add('bulk-start-fractional-publishes','bulk',{'stream_ids':[stream],'starting_channel_number':1.5},status=202,frames=1)
 add('bulk-4097-selected-ids-publishes','bulk',{'stream_ids':[stream]*4097},status=202,frames=1)
 add('names-4097-selected-ids-publishes','names',{'channel_ids':[channel]*4097},status=200,frames=1)
 for route in ['names','tvg','bulk']:
  key='stream_ids'if route=='bulk'else'channel_ids'
  add(route+'-truthy-nonlist',route,{key:'wrong'},status=400)
  add(route+'-falsy-zero-before-list-type',route,{key:0},status=400)
 return cases
