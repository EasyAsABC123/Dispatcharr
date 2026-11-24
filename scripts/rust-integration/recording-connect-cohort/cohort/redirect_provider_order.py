"""Paired saved-order Redirect fallback using a real in-flight peer grant.

Runs within the existing signed redirect-common context. Original Dispatcharr
authors and contributors retain all application and selection algorithm credit.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import uuid

from redirect_cases import save
from redirect_provider_framing import LOCAL_ORIGIN, need, request, scope


SEED = """
from django.db import transaction
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile
from core.models import CoreSettings, StreamProfile
with transaction.atomic():
    settings = CoreSettings.objects.select_for_update().get(key='proxy_settings')
    prior = settings.value
    assert isinstance(prior, dict)
    settings.value = {**prior, 'validate_redirect_urls': True}
    settings.save(update_fields=['value'])
    redirect = StreamProfile.objects.get(name='Redirect', locked=True)
    accounts = M3UAccount.objects.bulk_create([
        M3UAccount(name=marker+'-'+label, account_type='STD', is_active=True)
        for label in ('first','middle','last')
    ])
    assert all(account.server_group_id is None for account in accounts)
    profiles = M3UAccountProfile.objects.bulk_create([
        M3UAccountProfile(m3u_account=account, name=marker, is_default=True,
                          is_active=True, search_pattern='', replace_pattern='', max_streams=1)
        for account in accounts
    ])
    streams = Stream.objects.bulk_create([
        Stream(name=marker, m3u_account=account, url=url)
        for account, url in zip([*accounts,accounts[0]], urls)
    ])
    target, peer = Channel.objects.bulk_create([
        Channel(name=marker, stream_profile=redirect),
        Channel(name=marker, stream_profile=redirect)
    ])
    ChannelStream.objects.bulk_create([
        ChannelStream(channel=target, stream=stream, order=position)
        for position, stream in enumerate(streams[:3])
    ] + [ChannelStream(channel=peer, stream=streams[3], order=0)])
    assert list(ChannelStream.objects.filter(channel=target).order_by('order')
                .values_list('stream_id','order')) == [(s.pk,i) for i,s in enumerate(streams[:3])]
    assert Stream.objects.get(pk=streams[3].pk).m3u_account_id == streams[0].m3u_account_id
print(json.dumps({'target':[target.pk,str(target.uuid)],'peer':[peer.pk,str(peer.uuid)],
                  'profiles':[p.pk for p in profiles],'streams':[s.pk for s in streams],
                  'accounts':[a.pk for a in accounts],'prior':prior}))
"""
CLEANUP = """
from django.db import transaction
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile
from core.models import CoreSettings
with transaction.atomic():
    channels=[seed['target'][0],seed['peer'][0]]
    assert ChannelStream.objects.filter(channel_id__in=channels,stream_id__in=seed['streams'])._raw_delete('default') == 4
    assert Channel.objects.filter(pk__in=channels,name=marker)._raw_delete('default') == 2
    assert Stream.objects.filter(pk__in=seed['streams'],name=marker)._raw_delete('default') == 4
    assert M3UAccountProfile.objects.filter(pk__in=seed['profiles'],name=marker)._raw_delete('default') == 3
    assert M3UAccount.objects.filter(pk__in=seed['accounts'],name__startswith=marker+'-')._raw_delete('default') == 3
    row = CoreSettings.objects.select_for_update().get(key='proxy_settings')
    row.value = seed['prior']
    row.save(update_fields=['value'])
    assert CoreSettings.objects.get(pk=row.pk).value == seed['prior']
print(json.dumps({'fixture_rows_deleted':16,'original_settings_restored':True}))
"""
OBSERVE = """
local result={}
local function append(key)
    table.insert(result,redis.call('GET',key)or'')
end
for i=1,2 do append('channel_stream:'..ARGV[i]) end
for i=3,6 do append('stream_profile:'..ARGV[i]) end
for i=7,9 do table.insert(result,redis.call('GET','profile_connections:'..ARGV[i])or'0') end
for i=10,11 do
    local live='live:channel:'..ARGV[i]
    table.insert(result,redis.call('EXISTS',live..':owner'))
    table.insert(result,redis.call('SCARD',live..':clients'))
end
return result
"""
EMPTY = ["", "", "", "", "", "", "0", "0", "0", 0, 0, 0, 0]


def run(f):
    endpoint = scope(f)
    out = f.output / "saved-three-stream-redirect-order.json"
    need(not out.exists(), "OneOrderedRedirectCasePerOwner")
    nonce = uuid.uuid4().hex
    marker = "owned-redirect-order-" + nonce
    prefix = "/redirect-order/" + nonce
    paths = {label: f"{prefix}/{label}.ts" for label in ("first", "middle", "last", "peer")}
    urls = {label: LOCAL_ORIGIN + path for label, path in paths.items()}
    state = Path(f._redirect_state)
    report = {"status": "running", "backend": f.backend, "checks": {}}
    save(out, report)
    setup = "marker=" + repr(marker) + "\nurls=" + repr(list(urls.values())) + "\n"
    seed = f.sql(setup + SEED, secret=True)

    def observe():
        return f.redis(0, OBSERVE, seed["target"][0], seed["peer"][0], *seed["streams"],
                       *seed["profiles"], seed["target"][1], seed["peer"][1])

    def marker_read(role, event):
        path = str(state / (nonce + f"-{role}-order-{event}.json"))
        script = "from pathlib import Path;p=Path(" + repr(path) + ");print(p.read_text() if p.exists() else 'null')"
        return json.loads(f.python(script, seconds=3))

    def release(role):
        path = str(state / (nonce + f".{role}-order-release"))
        f.python("from pathlib import Path;Path(" + repr(path) + ").write_text('release')",
                 seconds=3)

    def held(role, future):
        entered = f.wait(lambda: marker_read(role, "entered"), 6, f"actual-{role}-HEAD-held")
        need(entered == {"method": "HEAD", "role": role} and not future.done(),
             "ActualOwnedHeadIsStillHeld")

    def confirmed(future, role):
        status, location, body = future.result(timeout=13)
        need((status, location, body) == (302, urls[role], 0), "ExactRedirectLocationAndEmptyBody")

    work = report["checks"]
    three = tuple(map(str, seed["profiles"]))
    _, middle, _, peer = map(str, seed["streams"])
    try:
        need(observe() == EMPTY, "ExactlyIdleOwnedOrderedRedirect")
        token = f.actual_admin_token()
        with ThreadPoolExecutor(max_workers=2) as pool:
            peer_request = None
            target_request = None
            peer_released = False
            middle_released = False
            try:
                peer_request = pool.submit(request, endpoint.port, seed["peer"][1], token)
                held("peer", peer_request)
                need(observe() == ["", peer, "", "", "", three[0], "1", "0", "0", 0, 0, 0, 0],
                     "ActualPeerOwnsFirstProfileSoleSlot")
                target_request = pool.submit(request, endpoint.port, seed["target"][1], token)
                held("middle", target_request)
                need(observe() == [middle, peer, "", three[1], "", three[0], "1", "1", "0", 0, 0, 0, 0],
                     "ActualMiddleSelectedWhilePeerStillHoldsFirst")
                release("peer")
                peer_released = True
                confirmed(peer_request, "peer")
                need(observe() == [middle, "", "", three[1], "", "", "0", "1", "0", 0, 0, 0, 0]
                     and not target_request.done(), "PeerSlotRetiredWhileMiddleHeadStillHeld")
                work.update(peer_holds_first=True, selected_middle_before_failure=True,
                            actual_first_slot_freed_before_fallback=True)
                save(out, report)
                release("middle")
                middle_released = True
                confirmed(target_request, "last")
            finally:
                try:
                    if peer_request is not None and not peer_released:
                        release("peer")
                finally:
                    if target_request is not None and not middle_released:
                        release("middle")
        need(all(marker_read(role, "finished") == {"released_by_fixture": True}
                 for role in ("peer", "middle")), "BothPhysicalHeadFencesReleasedByOwner")
        ledger = str(state / "requests.json")
        events = json.loads(f.python("from pathlib import Path;print(Path(" +
                                     repr(ledger) + ").read_text())", seconds=3))
        actual = [[row["method"], row["path"]] for row in events if row["path"].startswith(prefix + "/")]
        need(actual == [["HEAD", paths["peer"]], ["HEAD", paths["middle"]],
                        ["GET", paths["middle"]], ["HEAD", paths["last"]]],
             "ActualNextSavedStreamValidatedBeforeFirstProfileWrap")
        work.update(first_never_probed=True, selected_last_after_middle_failure=True,
                    exact_four_origin_requests=True)
        f.wait(lambda: observe() == EMPTY, 8, "actual-ordered-Redirect-alias-slot-retirement")
        work["exact_redis_retired"] = True
    except Exception as error:
        report.update(failure_type=type(error).__name__,
                      failed_checkpoint=str(error) if isinstance(error, AssertionError) else type(error).__name__)
        raise
    finally:
        try:
            if observe() == EMPTY:
                retirement = f.sql("seed=" + repr(seed) + "\nmarker=" + repr(marker) +
                                   "\n" + CLEANUP, secret=True)
                need(retirement == {"fixture_rows_deleted": 16, "original_settings_restored": True},
                     "ExactOrderedRedirectOwnedSqlRollback")
                work["sql_retirement"] = retirement
        finally:
            save(out, report)
    report["status"] = "passed"
    save(out, report)
    return report
