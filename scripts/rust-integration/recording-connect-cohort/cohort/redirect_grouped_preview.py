"""Default and scoped grouped-STD previews in the existing Redirect owner."""
import hashlib
import json
from pathlib import Path
import uuid

from output_profile_reader import Reader
from redirect_cases import save
from redirect_provider_framing import LOCAL_ORIGIN, need, request, scope


SEED = """
from datetime import timedelta
from django.db import transaction
from django.utils import timezone
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, ServerGroup
from core.models import StreamProfile
with transaction.atomic():
    proxy = StreamProfile.objects.get(name='Proxy', locked=True)
    redirect = StreamProfile.objects.get(name='Redirect', locked=True)
    group = ServerGroup.objects.create(name=marker)
    viewer, peer = M3UAccount.objects.bulk_create([
        M3UAccount(name=marker+'-'+label, account_type='STD', server_group=group,
                   username=marker+'-saved-'+label, password='owned-stored', is_active=True)
        for label in ('viewer','peer')
    ])
    profiles = M3UAccountProfile.objects.bulk_create([
        M3UAccountProfile(m3u_account=account, name=marker, is_default=True, is_active=True,
                          search_pattern='', replace_pattern='', max_streams=1)
        for account in (viewer,peer)
    ])
    view, alternate, sample, peer_sample = Stream.objects.bulk_create([
        Stream(name=marker, m3u_account=viewer, stream_hash=worker, url=media_url, stream_profile=proxy),
        Stream(name=marker, m3u_account=peer, url=media_url),
        Stream(name=marker, m3u_account=viewer, url=sample_url),
        Stream(name=marker, m3u_account=peer, url=sample_url),
    ])
    Stream.objects.filter(pk__in=(sample.pk,peer_sample.pk)).update(updated_at=timezone.now()+timedelta(seconds=30))
    for account in (viewer,peer):
        assert Stream.objects.filter(m3u_account=account).exclude(url='').values_list('url',flat=True).first() == sample_url
    channel = Channel.objects.bulk_create([Channel(name=marker, stream_profile=redirect)])[0]
    ChannelStream.objects.bulk_create([ChannelStream(channel=channel, stream=alternate, order=0)])
print(json.dumps({'profile':[p.pk for p in profiles],'streams':[s.pk for s in (view,alternate,sample,peer_sample)],
                  'channel':[channel.pk,str(channel.uuid)],'accounts':[viewer.pk,peer.pk],'group':group.pk}))
"""
CLEANUP = """
from django.db import transaction
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, ServerGroup
with transaction.atomic():
    assert ChannelStream.objects.filter(channel_id=seed['channel'][0],stream_id=seed['streams'][1])._raw_delete('default') == 1
    assert Channel.objects.filter(pk=seed['channel'][0],name=marker)._raw_delete('default') == 1
    assert Stream.objects.filter(pk__in=seed['streams'],name=marker)._raw_delete('default') == 4
    assert M3UAccountProfile.objects.filter(pk__in=seed['profile'],name=marker)._raw_delete('default') == 2
    assert M3UAccount.objects.filter(pk__in=seed['accounts'],name__in=[marker+'-viewer',marker+'-peer'])._raw_delete('default') == 2
    assert ServerGroup.objects.filter(pk=seed['group'],name=marker)._raw_delete('default') == 1
print(json.dumps({'fixture_rows_deleted':11}))
"""
OBSERVE = """
local plain='live:channel:'..ARGV[1]
local scoped='live:channel:'..ARGV[2]
local peer='live:channel:'..ARGV[3]
return {
 redis.call('GET','stream_profile:'..ARGV[4]..':preview')or'',
 redis.call('GET','stream_profile:'..ARGV[4]..':p'..ARGV[5])or'',
 redis.call('GET','profile_connections:'..ARGV[5])or'0',
 redis.call('GET','profile_connections:'..ARGV[6])or'0',
 redis.call('GET',ARGV[7])or'0',
 redis.call('EXISTS','profile_credential_release:'..ARGV[5]),
 redis.call('EXISTS','profile_credential_release:'..ARGV[6]),
 redis.call('SCARD',plain..':clients'),redis.call('SCARD',scoped..':clients'),
 redis.call('EXISTS',plain..':owner'),redis.call('EXISTS',scoped..':owner'),
 redis.call('EXISTS',peer..':owner'),redis.call('SCARD',peer..':clients'),
 redis.call('EXISTS','stream_profile:'..ARGV[4],'channel_stream:'..ARGV[8],
            'stream_profile:'..ARGV[9])}
"""
EMPTY = ["", "", "0", "0", "0", 0, 0, 0, 0, 0, 0, 0, 0, 0]


def active_expected(profile, scoped):
    return ["" if scoped else profile, profile if scoped else "", "1", "0", "1", 1, 0,
            0 if scoped else 1, 1 if scoped else 0, 0 if scoped else 1, 1 if scoped else 0,
            0, 0, 0]


def strict_decode(f, path):
    location = "/evidence/" + str(path.relative_to(f.output))
    progress = f.command(f.compose + ["exec", "-T", "baseline", "ffmpeg",
        "-hide_banner", "-loglevel", "error", "-xerror", "-threads", "1",
        "-filter_threads", "1", "-filter_complex_threads", "1", "-progress", "pipe:1",
        "-nostats", "-i", location, "-t", "3", "-map", "0:v:0", "-map", "0:a:0",
        "-threads:v", "1", "-threads:a", "1", "-f", "null", "-"], 30, allow=True)
    actual = f.cli[-1]
    frames = max((int(line.split("=")[1]) for line in progress.splitlines()
                  if line.startswith("frame=")), default=0)
    need(actual["actual_exit"] == 0 and actual["stdout_EOF"] and actual["stderr_EOF"]
         and actual["process_group_settled"] and not Path(actual["stderr"]).stat().st_size
         and frames >= 70 and "progress=end" in progress, "ActualGroupedPreviewStrictAV")


def run(f):
    endpoint = scope(f)
    out = f.output / "grouped-std-preview"
    out.mkdir(mode=0o700, exist_ok=False)
    nonce = uuid.uuid4().hex
    marker = "owned-std-preview-" + nonce
    worker = "owned-grouped-preview-" + nonce
    user, key = "viewer-" + nonce[:12], "owned-" + nonce[12:24]
    sample_path = f"/live/{user}/{key}/839.ts"
    values = dict(marker=marker, worker=worker, media_url=f._redirect_seed["media_url"],
                  sample_url=LOCAL_ORIGIN + sample_path)
    report = {"status": "running", "backend": f.backend, "cells": []}
    seed = f.sql("\n".join(f"{name}={value!r}" for name, value in values.items()) + "\n" + SEED,
                 secret=True)
    digest = hashlib.sha256((user + "\0" + key).encode()).hexdigest()[:16]
    counter = f"server_group_connections:{seed['group']}:{digest}"
    profile = str(seed["profile"][0])
    scoped_worker = worker + ".p" + profile

    def observe():
        return f.redis(0, OBSERVE, worker, scoped_worker, seed["channel"][1], seed["streams"][0],
                       *seed["profile"], counter, seed["channel"][0], seed["streams"][1])

    def origin():
        return json.loads(f.get(f.origin + "/stats")[1])

    def retired():
        return observe() == EMPTY and origin()["active"] == 0

    def play(scoped):
        scope_name = "scoped" if scoped else "default"
        owned_worker = scoped_worker if scoped else worker
        url = f.base_url + "/proxy/ts/stream/" + worker + ("?skip_redirect=TRUE" if scoped else "")
        path = out / (scope_name + ".ts")
        token = f.actual_admin_token()
        f.wait(retired, 30, "previous-actual-preview-retirement")
        before = origin()
        reader = Reader(url, path, token)
        try:
            f.wait(lambda: reader.snapshot()["media_bytes"] >= 1_000_000 and not reader.snapshot()["errors"],
                   30, "actual-grouped-preview-media")
            expected = active_expected(profile, scoped)
            need(observe() == expected, "OneProfilePoolSlotOnlyForItsExactPreviewAlias")
            active = origin()
            need(active["connections"] == before["connections"] + 1 and active["active"] == 1,
                 "OneRealGroupedPreviewUpstream")
            status, location, _ = request(endpoint.port, seed["channel"][1], token)
            need(status == 503 and location is None and observe() == expected,
                 "OtherAccountInSamePoolBlockedWithoutSecondReservation")
            shared = reader.snapshot()["media_bytes"]
            f.wait(lambda: reader.snapshot()["media_bytes"] >= shared + 188_000, 12,
                   "actual-preview-survives-other-account-denial")
            need(origin()["connections"] == active["connections"] and not reader.snapshot()["errors"],
                 "PoolDenialLeavesExistingLiveBodyAndNoAdditionalOrigin")
        finally:
            reader.close()
            stopped = f.api("POST", "/proxy/ts/stop/" + owned_worker, {}, "admin")
            need(stopped["status"] in (200, 404), "ActualOwnedGroupedPreviewStop")
            f.wait(retired, 35, "actual-preview-owner-profile-pool-and-origin-retirement")
        strict_decode(f, path)
        return {"scope": scope_name, "resolved_exact_preview_alias": True,
                "sample_pool_held_once": True, "other_account_status": status,
                "same_reader_continued": True, "fresh_strict_av": True, "exact_retirement": True}

    try:
        need(retired(), "InitialPreviewGroupIsIdle")
        for scoped in (False, True):
            report["cells"].append(play(scoped))
            save(out / "report.json", report)
        state = Path(f._redirect_state) / "requests.json"
        entries = json.loads(f.python("from pathlib import Path;print(Path(" + repr(str(state)) +
                                      ").read_text())", seconds=3))
        need(all(row["path"] != sample_path for row in entries), "AccountSampleIsNeverOpened")
        report["only_assigned_media_was_opened"] = True
    except Exception as error:
        report.update(failure_type=type(error).__name__,
                      failed_checkpoint=str(error) if isinstance(error, AssertionError) else type(error).__name__)
        raise
    finally:
        try:
            if retired():
                result = f.sql("seed=" + repr(seed) + "\nmarker=" + repr(marker) + "\n" + CLEANUP,
                               secret=True)
                need(result == {"fixture_rows_deleted": 11}, "ExactGroupedPreviewSQLRetirement")
                report["sql_retirement"] = result
        finally:
            save(out / "report.json", report)
    report["status"] = "passed"
    save(out / "report.json", report)
    return report
