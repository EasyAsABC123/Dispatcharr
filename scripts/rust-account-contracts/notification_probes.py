"""Original notification read/dismissal source oracles with independent resets.

Only owned disposable fixture users/rows/cache entries are mutated. Responses
and action values are never recorded; evidence contains only status/digests.
"""
import hashlib
import json

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()
def run(http,baseline,candidate,accounts,record,fixture):
    root='/api/core/notifications/';prefix='rust-probe-notification-';ids=fixture({'action':'notification_prepare'})['ids']
    def invoke(base,path,body=None,method=None,role='viewer'):return http(base+path,body=body,method=method,token=accounts[role]['access'] if role else None)
    def reset(role='viewer',network=False,cached=None):
        user=accounts[role or 'viewer'];return fixture({'action':'notification_reset','user_id':user['id'],'username':user['username'],'network_allow':network,'cached_condition':cached})
    def state(role='viewer'):
        user=accounts[role];return fixture({'action':'notification_state','user_id':user['id'],'username':user['username']})
    def compare(name,path,expected=200,role='viewer',body=None,method=None,before=None,after=None):
        results=[]
        for label,base in [('python',baseline),('rust',candidate)]:
            try:
                (before or (lambda:reset(role)))()
                value=invoke(base,path,body,method,role);results.append(value)
                if after:after(label,base,value)
            except Exception as error:record('notifications:'+name+':'+label,False,{'error_type':type(error).__name__});results.append(None)
        if None in results:return
        a,b=results
        record('notifications:'+name,a[0]==b[0]==expected and digest(a[1])==digest(b[1]),{'python_status':a[0],'rust_status':b[0],'python_body_sha256':digest(a[1]),'rust_body_sha256':digest(b[1])})
    try:
        for role,expected in [('viewer',200),('standard',200),('admin',200),('inactive',401),(None,401)]:
            compare('list-role-'+str(role),root,expected,role);compare('count-role-'+str(role),root+'count/',expected,role)
        for name,params in [('include-dismissed','?include_dismissed=true'),('case-insensitive','?include_dismissed=TRUE'),('no-trim','?include_dismissed=%20true%20'),('query-ignored','?page=invalid&search=absent&limit=1')]:compare(name,root+params)
        for name,status in [('public',200),('admin',404),('inactive',404),('expired',404),('future',200),('developer-known',404),('developer-unknown',200)]:compare('detail-'+name,root+str(ids[name])+'/',status)
        compare('admin-only-owner',root+str(ids['admin'])+'/',role='admin')
        compare('missing-detail',root+'2147483647/',404);compare('malformed-detail',root+'not-an-integer/',404)
        compare('missing-dismiss-before-body',root+'2147483647/dismiss/',404,body=[],method='POST')
        compare('admin-only-dismiss-denied',root+str(ids['admin'])+'/dismiss/',404,body={},method='POST')
        for network,cached,expected in [(False,None,False),(True,None,True),(False,True,True),(True,False,False),('malformed',None,False)]:
            name='condition-network-'+str(network)+'-cached-'+str(cached)
            def witness(label,base,result,expected=expected):
                keys=[row['notification_key'] for row in result[1].get('notifications',[])]
                actual=prefix+'developer-known' in keys
                stored=state()['condition_cache_bool']
                record('notifications:'+name+':witness:'+label,actual==expected and stored is expected,{'shown':actual,'cache_bool':stored})
            compare(name,root,before=lambda network=network,cached=cached:reset(network=network,cached=cached),after=witness)
        path=root+str(ids['public'])+'/dismiss/'
        for name,body in [('no-body',None),('empty',{}),('accepted',{'action_taken':'accepted'}),('boolean',{'action_taken':True}),('zero',{'action_taken':0}),('null',{'action_taken':None})]:
            snapshots=[]
            def verify(label,base,result):
                current=state();snapshots.append((current['dismissal_count'],current['actions_sha256']))
                record('notifications:dismiss-'+name+':stored:'+label,current['dismissal_count']==1,{'count':current['dismissal_count']})
                own=invoke(base,root+str(ids['public'])+'/')
                record('notifications:dismiss-'+name+':detail:'+label,own[0]==200 and own[1].get('is_dismissed') is True,{'status':own[0]})
            compare('dismiss-'+name,path,body=body,method='POST',after=verify)
            record('notifications:dismiss-'+name+':stored-actions',len(snapshots)==2 and snapshots[0]==snapshots[1],{'snapshot_count':len(snapshots)})
        def already_dismissed():
            reset();result=invoke(baseline,path,{'action_taken':'accepted'},'POST')
            if result[0]!=200:raise ValueError('Source fixture dismissal failed')
        for name,body in [('repeat-null',{'action_taken':None}),('repeat-empty',{'action_taken':''}),('repeat-false',{'action_taken':False}),('repeat-truthy',{'action_taken':'changed'})]:
            snapshots=[]
            def before():already_dismissed();snapshots.append(state())
            def after(label,base,result):
                latest=state();previous=snapshots[-1]
                record('notifications:'+name+':timestamp:'+label,latest['timestamps_sha256']==previous['timestamps_sha256'] and latest['dismissal_count']==1,{'count':latest['dismissal_count']})
                snapshots[-1]=latest['actions_sha256']
            compare(name,path,body=body,method='POST',before=before,after=after)
            record('notifications:'+name+':stored-actions',len(snapshots)==2 and snapshots[0]==snapshots[1],{'snapshot_count':len(snapshots)})
        for include in ['', '?include_dismissed=true']:
            compare('personal-list-after-dismiss'+include,root+include,before=already_dismissed)
        compare('personal-count-after-dismiss',root+'count/',before=already_dismissed)
        compare('dismiss-all',root+'dismiss-all/',method='POST')
        def all_dismissed():reset();result=invoke(baseline,root+'dismiss-all/',method='POST');assert result[0]==200
        compare('dismiss-all-noop',root+'dismiss-all/',method='POST',before=all_dismissed)
        compare('dismiss-all-list',root,before=all_dismissed)
        # A dismissal belongs only to its authenticated user.
        def viewer_dismissed():reset('standard');already_dismissed()
        compare('different-user-unaffected',root+str(ids['public'])+'/',role='standard',before=viewer_dismissed)
    finally:
        fixture({'action':'notification_cleanup'})
