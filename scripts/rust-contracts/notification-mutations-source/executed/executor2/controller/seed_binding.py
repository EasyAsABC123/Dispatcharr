"""Pure exact reviewed-plan seed/actor/target binding; no application imports."""
import datetime,json
from fixture_guards import database,dataset,require

def exact(a,b):return json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))
def instant(v):return datetime.datetime.fromisoformat(v.replace('Z','+00:00')).astimezone(datetime.timezone.utc)
def positive(v):return type(v)is int and v>0

def validate_seed(actual,plan):
    db=database(actual['database']);suffix=db.removeprefix('rust_probe_notifmut_');prefix='owned.notifmut.'+suffix
    require(actual['seed_prefix']==prefix,'ExactOwnedSeedPrefix')
    aliases=actual['seed_aliases'];require(set(aliases)==set(plan['seed']['notifications']),'ExactSeedAliasSet')
    require(all(positive(x)for x in aliases.values())and len(set(aliases.values()))==len(aliases),'DistinctPositiveAliasIDs')
    rows=actual['seed_rows']['notifications'];require(len(rows)==len(aliases)and [x['id']for x in rows]==sorted(aliases.values()),'ExactOrderedNotificationIDs')
    lookup={x['id']:x for x in rows};anchor=instant(actual['seed_anchor_utc']);started=instant(actual['seed_started_utc']);completed=instant(actual['seed_completed_utc'])
    require(started<=anchor<=completed,'SeedClockBounds')
    for alias,attrs in plan['seed']['notifications'].items():
        row=lookup[aliases[alias]];expected=dict(attrs);expected['id']=aliases[alias]
        expected['notification_key']=expected['notification_key'].replace('@prefix',prefix)
        expected['expires_at']=(anchor+datetime.timedelta(days=-1 if attrs['expires_at']=='@seed.past'else 1)).isoformat()
        require(set(row)==set(expected)|{'created_at','updated_at'},'ExactSeedColumnSet:'+alias)
        require(all(instant(row[k])==instant(v)if k=='expires_at'else exact(row[k],v)for k,v in expected.items()),'ExactReviewedSeedFields:'+alias)
        require(all(started<=instant(row[k])<=completed for k in ['created_at','updated_at']),'SeedAutoClock:'+alias)
    declared={k:v for k,v in plan['seed']['users'].items()if v is not None};users=actual['seed_users']
    require(len(users)==len(declared)and all(positive(u['id'])for u in users)and len({u['id']for u in users})==len(users),'ExactDistinctSeedActors')
    actor_ids={}
    for role,attrs in declared.items():
        expected_name='notifmut_'+role+'_'+suffix;found=[u for u in users if u['username']==expected_name]
        require(len(found)==1,'ExactSeedUsernameRole:'+role);u=found[0]
        require(type(u['user_level'])is int and u['user_level']==attrs['user_level']and u['is_active']is attrs['is_active'],'ExactSeedRoleLevelActive:'+role)
        require(exact(u['custom_properties'],{'allowed_networks':{'UI':attrs['UI']}}),'ExactSeedRoleNetworks:'+role)
        actor_ids[role]=u['id']
    require([u['id']for u in users]==sorted(actor_ids.values()),'OrderedSeedActorRows')
    dismiss=actual['seed_rows']['dismissals'];declared_pairs={(actor_ids[role],aliases[alias]):action for role,alias,action in plan['seed']['dismissals']}
    require(len(dismiss)==len(plan['seed']['dismissals'])and len(declared_pairs)==len(dismiss),'ExactDismissalCount')
    require(all(positive(d['id'])for d in dismiss)and len({d['id']for d in dismiss})==len(dismiss)and [d['id']for d in dismiss]==sorted(d['id']for d in dismiss),'DistinctOrderedDismissalIDs')
    require({(d['user_id'],d['notification_id'])for d in dismiss}==set(declared_pairs),'ExactReviewedDismissalPairs')
    for d in dismiss:
        require(set(d)=={'id','user_id','notification_id','dismissed_at','action_taken'},'ExactDismissalColumns')
        require(exact(d['action_taken'],declared_pairs[d['user_id'],d['notification_id']]),'ExactDismissalAction')
        require(started<=instant(d['dismissed_at'])<=completed,'SeedDismissalClock')
    audit=actual['seed_audit'];dataset(audit,db)
    require(audit['tables']['core_systemnotification']['rows']==len(rows)and audit['tables']['core_notificationdismissal']['rows']==len(dismiss),'SeedAuditCountsBound')
    return {'actors':actor_ids,'aliases':aliases,'seed':{a:lookup[i]for a,i in aliases.items()}}

def expected_target(case,aliases):
    if case['target']=='malformed':return 'not-an-integer'
    if case['target']=='missing':return max(aliases.values())+100000
    return aliases[case['target']]
