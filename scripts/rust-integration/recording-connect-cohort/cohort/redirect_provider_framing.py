"""Grouped signed Redirect provider framing and credential-pool case.

Uses the established redirect-common Fixture, owned SQL/Redis and redirect
origin. Original Dispatcharr authors retain full credit.
"""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import re
from pathlib import Path
from urllib.parse import urlsplit
import uuid

from redirect_cases import save


QUERY = "?Policy=owned%2Fsample~&Signature=abc+def%2Bgh%3D"
LOCAL_ORIGIN = "http://127.0.0.1:18099"
SEED = """
from django.db import transaction
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, ServerGroup
from core.models import CoreSettings, StreamProfile
with transaction.atomic():
    redirect = StreamProfile.objects.get(name='Redirect', locked=True)
    settings = CoreSettings.objects.select_for_update().get(key='proxy_settings')
    old_settings = settings.value
    assert isinstance(old_settings, dict)
    settings.value = {**old_settings, 'validate_redirect_urls': True}
    settings.save(update_fields=['value'])
    group = ServerGroup.objects.create(name=marker)
    primary, alternate = M3UAccount.objects.bulk_create([
        M3UAccount(name=marker+'-primary', account_type='STD', server_group=group,
                   username=account_name, password=account_suffix, is_active=True),
        M3UAccount(name=marker+'-alternate', account_type='STD', is_active=True),
    ])
    primary_profile, alternate_profile = M3UAccountProfile.objects.bulk_create([
        M3UAccountProfile(m3u_account=account, name=marker, is_default=True, is_active=True,
                          search_pattern='', replace_pattern='', max_streams=1)
        for account in (primary, alternate)
    ])
    primary_stream, alternate_stream = Stream.objects.bulk_create([
        Stream(name=account.name, m3u_account=account, url=url)
        for account, url in ((primary, primary_url), (alternate, alternate_url))
    ])
    channel = Channel.objects.bulk_create([Channel(name=marker, stream_profile=redirect)])[0]
    ChannelStream.objects.bulk_create([
        ChannelStream(channel=channel, stream=primary_stream, order=0),
        ChannelStream(channel=channel, stream=alternate_stream, order=1),
    ])
    assert list(ChannelStream.objects.filter(channel=channel).values_list('stream_id','order')) == [
        (primary_stream.pk, 0), (alternate_stream.pk, 1)]
    assert list(Stream.objects.filter(m3u_account=primary).values_list('url',flat=True)) == [primary_url]
print(json.dumps({'channel':channel.pk, 'channel_uuid':str(channel.uuid), 'group':group.pk,
                  'profiles':[primary_profile.pk,alternate_profile.pk],
                  'streams':[primary_stream.pk,alternate_stream.pk],
                  'accounts':[primary.pk,alternate.pk], 'old_settings':old_settings}))
"""
CLEANUP = """
from django.db import transaction
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, ServerGroup
from core.models import CoreSettings
with transaction.atomic():
    assert ChannelStream.objects.filter(channel_id=seed['channel'],stream_id__in=seed['streams'])._raw_delete('default') == 2
    assert Channel.objects.filter(pk=seed['channel'],name=marker)._raw_delete('default') == 1
    assert Stream.objects.filter(pk__in=seed['streams'],name__in=[marker+'-primary',marker+'-alternate'])._raw_delete('default') == 2
    assert M3UAccountProfile.objects.filter(pk__in=seed['profiles'],name=marker)._raw_delete('default') == 2
    assert M3UAccount.objects.filter(pk__in=seed['accounts'],name__in=[marker+'-primary',marker+'-alternate'])._raw_delete('default') == 2
    assert ServerGroup.objects.filter(pk=seed['group'],name=marker)._raw_delete('default') == 1
    row = CoreSettings.objects.select_for_update().get(key='proxy_settings')
    row.value = seed['old_settings']
    row.save(update_fields=['value'])
    assert CoreSettings.objects.get(pk=row.pk).value == seed['old_settings']
print(json.dumps({'fixture_rows_deleted':10, 'original_settings_restored':True}))
"""
OBSERVE = """
local public='live:channel:'..ARGV[1]
return {redis.call('GET','channel_stream:'..ARGV[2])or'',
 redis.call('GET','stream_profile:'..ARGV[3])or'',
 redis.call('GET','stream_profile:'..ARGV[4])or'',
 redis.call('GET','profile_connections:'..ARGV[5])or'0',
 redis.call('GET','profile_connections:'..ARGV[6])or'0',
 redis.call('GET',ARGV[7])or'0',
 redis.call('SCARD',public..':clients'),redis.call('EXISTS',public..':owner')}
"""
EMPTY = ["", "", "", "0", "0", "0", 0, 0]


def need(predicate, label):
    if not predicate:
        raise AssertionError(label)


def scope(f):
    parsed = urlsplit(f.base_url)
    need(parsed.scheme == "http" and parsed.hostname == "127.0.0.1"
         and parsed.port and parsed.path in ("", "/") and not parsed.username
         and not parsed.password and not parsed.query and not parsed.fragment,
         "OwnedLoopbackMainOnly")
    need(f.case == "redirect-common" and f.backend in ("Original", "Native")
         and "redirect-origin" in f.roles
         and re.fullmatch(r"/tmp/owned-redirect-[a-f0-9]{32}", getattr(f, "_redirect_state", "")),
         "ExistingOwnedRedirectCommonScopeOnly")
    return parsed


def request(port, channel, bearer):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=12)
    try:
        connection.request("GET", "/proxy/ts/stream/" + channel,
                           headers={"Authorization": "Bearer " + bearer, "Accept": "application/json"})
        response = connection.getresponse()
        body = response.read(65537)
        need(len(body) <= 65536, "RedirectBody64KiBBound")
        return response.status, response.getheader("Location"), len(body)
    finally:
        connection.close()


def run(f):
    endpoint = scope(f)
    out = f.output / "pooled-redirect-frame-fallback.json"
    need(not out.exists(), "OnePooledRedirectCasePerOwner")
    nonce = uuid.uuid4().hex
    marker = "owned-redirect-frame-" + nonce
    prefix = "/pooled-frame/" + nonce
    primary_path = prefix + "/primary.ts" + QUERY
    alternate_path = prefix + "/fallback.ts"
    account_name = "fixture-" + nonce[:8]
    account_suffix = "only-" + nonce[8:16]
    args = dict(marker=marker, account_name=account_name, account_suffix=account_suffix,
                primary_url=LOCAL_ORIGIN + primary_path, alternate_url=LOCAL_ORIGIN + alternate_path)
    setup = "\n".join(f"{key}={value!r}" for key, value in args.items()) + "\n" + SEED
    report = {"status": "running", "backend": f.backend,
              "scope": "signed owned STD ServerGroup Redirect, declared-length GET fallback", "checks": {}}
    save(out, report)
    seed = f.sql(setup, secret=True)
    digest = hashlib.sha256((account_name + "\0" + account_suffix).encode()).hexdigest()[:16]
    key = f"server_group_connections:{seed['group']}:{digest}"
    state = Path(f._redirect_state)

    def observation():
        return f.redis(0, OBSERVE, seed["channel_uuid"], seed["channel"], *seed["streams"], *seed["profiles"], key)

    def marker_read(name):
        path = str(state / (nonce + name + ".json"))
        return json.loads(f.python("from pathlib import Path;p=Path(" + repr(path)
                                   + ");print(p.read_text() if p.exists() else 'null')", seconds=3))

    def release():
        path = str(state / (nonce + ".pooled-release"))
        f.python("from pathlib import Path;Path(" + repr(path) + ").write_text('release')", seconds=3)

    idle = False
    work = report["checks"]
    try:
        bearer = f.actual_admin_token()
        need(observation() == EMPTY, "InitialIdleOwnedProviderAndChannel")
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(request, endpoint.port, seed["channel_uuid"], bearer)
            try:
                before = f.wait(lambda: marker_read("-pooled-entered"), 6, "actual-fixed-GET-prefix")
                need(before == {"method": "GET", "initial_bytes": 32, "declared_bytes": 1880},
                     "ActualFixedLengthShortPrefix")
                held = observation()
                need(held == [str(seed["streams"][0]), str(seed["profiles"][0]), "", "1", "0", "1", 0, 0],
                     "ActualPooledPrimaryHeldExactlyOnceWithoutViewer")
                need(not pending.done(), "DownstreamRedirectMustWaitForDeclaredSample")
                work.update(held_primary_profile=1, held_pool=1, held_alternate=0,
                            no_early_redirect=True, actual_fixed_get_prefix=before)
                save(out, report)
            finally:
                release()
            status, location, body_bytes = pending.result(timeout=13)
        close = marker_read("-pooled-closed")
        need(close == {"released_by_fixture": True}, "ActualShortReplyClosedAtFixtureFence")
        need(status == 302 and location == LOCAL_ORIGIN + alternate_path and body_bytes == 0,
             "ActualRedirectToValidatedAlternate")
        ledger = json.loads(f.python("from pathlib import Path;print(Path("
                                      + repr(str(state / "requests.json")) + ").read_text())", seconds=3))
        events = [[row["method"], row["path"]] for row in ledger if row["path"].startswith(prefix + "/")]
        need(events == [["HEAD", primary_path], ["GET", primary_path], ["HEAD", alternate_path]],
             "ExactOpaqueQueryAndOrdinaryProviderOrder")
        work.update(head_then_get_then_alternate_head=True, raw_encoded_query_preserved=True,
                    selected_fallback_status=status, selected_fallback_empty_body=True)
        idle = f.wait(lambda: observation() == EMPTY, 8, "actual-pooled-Redirect-provider-retirement")
        work["exact_redis_retired"] = idle
    except BaseException as error:
        report["failure_type"] = type(error).__name__
        report["failed_checkpoint"] = str(error) if isinstance(error, AssertionError) else type(error).__name__
        raise
    finally:
        try:
            if idle or observation() == EMPTY:
                retirement = f.sql("seed=" + repr(seed) + "\nmarker=" + repr(marker) + "\n" + CLEANUP, secret=True)
                need(retirement == {"fixture_rows_deleted": 10, "original_settings_restored": True},
                     "ExactOwnedSqlRowsAndCoreSettingsRetired")
                work["sql_retirement"] = retirement
        finally:
            save(out, report)
    report["status"] = "passed"
    save(out, report)
    return report


SAME_SLOT_SEED = """
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
    account = M3UAccount.objects.bulk_create([
        M3UAccount(name=marker, account_type='STD', is_active=True)
    ])[0]
    assert account.server_group_id is None
    profile = M3UAccountProfile.objects.bulk_create([
        M3UAccountProfile(m3u_account=account, name=marker, is_default=True,
                          is_active=True, search_pattern='', replace_pattern='', max_streams=1)
    ])[0]
    primary, backup = Stream.objects.bulk_create([
        Stream(name=marker, m3u_account=account, url=url) for url in (primary_url, backup_url)
    ])
    channel = Channel.objects.bulk_create([Channel(name=marker, stream_profile=redirect)])[0]
    ChannelStream.objects.bulk_create([
        ChannelStream(channel=channel, stream=primary, order=0),
        ChannelStream(channel=channel, stream=backup, order=1),
    ])
    assert list(ChannelStream.objects.filter(channel=channel).values_list('stream_id','order')) == [
        (primary.pk, 0), (backup.pk, 1)]
print(json.dumps({'channel':channel.pk,'channel_uuid':str(channel.uuid),'profile':profile.pk,
                  'account':account.pk,'streams':[primary.pk,backup.pk],'prior':prior}))
"""
SAME_SLOT_CLEANUP = """
from django.db import transaction
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile
from core.models import CoreSettings
with transaction.atomic():
    assert ChannelStream.objects.filter(channel_id=seed['channel'],stream_id__in=seed['streams'])._raw_delete('default') == 2
    assert Channel.objects.filter(pk=seed['channel'],name=marker)._raw_delete('default') == 1
    assert Stream.objects.filter(pk__in=seed['streams'],name=marker)._raw_delete('default') == 2
    assert M3UAccountProfile.objects.filter(pk=seed['profile'],name=marker)._raw_delete('default') == 1
    assert M3UAccount.objects.filter(pk=seed['account'],name=marker)._raw_delete('default') == 1
    row = CoreSettings.objects.select_for_update().get(key='proxy_settings')
    row.value = seed['prior']
    row.save(update_fields=['value'])
    assert CoreSettings.objects.get(pk=row.pk).value == seed['prior']
print(json.dumps({'fixture_rows_deleted':7,'original_settings_restored':True}))
"""
SAME_SLOT_OBSERVE = """
local live='live:channel:'..ARGV[1]
return {redis.call('GET','channel_stream:'..ARGV[2])or'',
 redis.call('GET','stream_profile:'..ARGV[3])or'',
 redis.call('GET','stream_profile:'..ARGV[4])or'',
 redis.call('GET','profile_connections:'..ARGV[5])or'0',
 redis.call('SCARD',live..':clients'),redis.call('EXISTS',live..':owner'),
 redis.call('EXISTS','profile_credential_release:'..ARGV[5])}
"""
SAME_SLOT_EMPTY = ["", "", "", "0", 0, 0, 0]


def run_same_slot(f):
    endpoint = scope(f)
    out = f.output / "same-profile-one-slot-redirect-fallback.json"
    need(not out.exists(), "OneSameProfileRedirectCasePerOwner")
    nonce = uuid.uuid4().hex
    marker = "owned-redirect-same-slot-" + nonce
    prefix = "/same-slot/" + nonce
    primary_path, backup_path = prefix + "/primary.ts", prefix + "/fallback.ts"
    state = Path(f._redirect_state)
    variables = dict(marker=marker, primary_url=LOCAL_ORIGIN + primary_path,
                     backup_url=LOCAL_ORIGIN + backup_path)
    setup = "\n".join(f"{key}={value!r}" for key, value in variables.items())
    report = {"status": "running", "backend": f.backend, "checks": {}}
    save(out, report)
    seed = f.sql(setup + "\n" + SAME_SLOT_SEED, secret=True)

    def observe():
        return f.redis(0, SAME_SLOT_OBSERVE, seed["channel_uuid"], seed["channel"],
                       *seed["streams"], seed["profile"])

    def marker_read(suffix):
        path = str(state / (nonce + suffix + ".json"))
        return json.loads(f.python("from pathlib import Path;p=Path(" + repr(path)
                                   + ");print(p.read_text() if p.exists() else 'null')", seconds=3))

    work = report["checks"]
    try:
        need(observe() == SAME_SLOT_EMPTY, "ExactInitiallyIdleSingleProfileAndChannel")
        bearer = f.actual_admin_token()
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(request, endpoint.port, seed["channel_uuid"], bearer)
            try:
                entered = f.wait(lambda: marker_read("-same-slot-entered"), 6,
                                 "actual-on-wire-eligible-same-profile-backup")
                need(entered == {"method": "HEAD", "backup": True} and not pending.done(),
                     "ActualBackupHeadHeldBeforeReply")
                need(observe() == [str(seed["streams"][0]), str(seed["profile"]),
                                   "", "1", 0, 0, 0] and not pending.done(),
                     "OwnOriginalAliasesWithExactlyOneSlotAndNoLiveOwnerOrCredentialPool")
                work.update(held_backup_head=True, exact_one_owned_slot=True,
                            no_second_assignment_or_live_owner=True)
                save(out, report)
            finally:
                release = str(state / (nonce + ".same-slot-release"))
                f.python("from pathlib import Path;Path(" + repr(release) +
                         ").write_text('release')", seconds=3)
            status, location, body_bytes = pending.result(timeout=13)
        need(marker_read("-same-slot-finished") == {"released_by_fixture": True},
             "OwnedBackupGateCompleted")
        need(status == 302 and location == LOCAL_ORIGIN + backup_path and body_bytes == 0,
             "OrdinaryRedirectHandsOffToSameProfileBackup")
        ledger_path = str(state / "requests.json")
        ledger = json.loads(f.python("from pathlib import Path;print(Path(" +
                                     repr(ledger_path) + ").read_text())", seconds=3))
        events = [[row["method"], row["path"]] for row in ledger if row["path"].startswith(prefix + "/")]
        need(events == [["HEAD", primary_path], ["GET", primary_path], ["HEAD", backup_path]],
             "ExactSameProviderPrimaryFailureAndAlternateValidationOrder")
        work.update(fallback_status=status, exact_probe_order=True)
        f.wait(lambda: observe() == SAME_SLOT_EMPTY, 8, "actual-one-slot-fallback-retirement")
        work["exact_redis_retired"] = True
    except Exception as error:
        report.update(failure_type=type(error).__name__,
                      failed_checkpoint=str(error) if isinstance(error, AssertionError) else type(error).__name__)
        raise
    finally:
        try:
            if observe() == SAME_SLOT_EMPTY:
                retirement = f.sql("seed=" + repr(seed) + "\nmarker=" + repr(marker) +
                                   "\n" + SAME_SLOT_CLEANUP, secret=True)
                need(retirement == {"fixture_rows_deleted": 7, "original_settings_restored": True},
                     "ExactSingleSlotOwnedSqlRollback")
                work["sql_retirement"] = retirement
        finally:
            save(out, report)
    report["status"] = "passed"
    save(out, report)
    return report
