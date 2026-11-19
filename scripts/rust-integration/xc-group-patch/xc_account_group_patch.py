"""Owned XC group-only PATCH through the normal Main API. Original authors retain credit."""
import json
import urllib.error
import urllib.request

from budget import bounded_read

CASE = "xc-account-partial-group-patch"
REPLY_LIMIT = 1_048_576
ACCOUNT_TIME = "2031-01-02T03:04:05.123456Z"
GROUP_TIME = "2031-01-03T03:04:05.123456Z"

PREFERENCES = {
    "auto_enable_new_groups_live": False,
    "auto_enable_new_groups_vod": False,
    "auto_enable_new_groups_series": True,
}
INITIAL_PREFERENCES = {key: not value for key, value in PREFERENCES.items()}
ACCOUNT_PROPERTIES = {"enable_vod": False, "fixture_marker": "account", "hash_key": "name,url"}
ORPHAN_MODES = ("preserve_customized", "never", "always")
SEED = """
from datetime import datetime
from django.db import transaction
from apps.channels.models import ChannelGroup, ChannelGroupM3UAccount
from apps.m3u.models import M3UAccount, M3UAccountProfile

with transaction.atomic():
    target = M3UAccount.objects.create(name="Owned PATCH target", account_type="XC", is_active=False, refresh_interval=0)
    other = M3UAccount.objects.create(name="Owned PATCH foreign", account_type="XC", is_active=False, refresh_interval=0)
    M3UAccount.objects.filter(pk__in=[target.pk, other.pk]).update(
        created_at=datetime.fromisoformat("2031-01-02T03:04:05.123456+00:00")
    )
    M3UAccount.objects.filter(pk=target.pk).update(custom_properties={properties!r})
    groups = {
        name: ChannelGroup.objects.create(name="Owned PATCH " + name)
        for name in ("target", "sibling", "unrelated")
    }
    for name, group in groups.items():
        ChannelGroupM3UAccount.objects.create(
            channel_group=group,
            m3u_account=other if name == "unrelated" else target,
            enabled=True,
            auto_channel_sync=name == "target",
            auto_sync_channel_start=12.5 if name == "target" else None,
            auto_sync_channel_end=20.5 if name == "target" else None,
            custom_properties={"fixture_marker": name},
            last_seen=datetime.fromisoformat("2031-01-03T03:04:05.123456+00:00"),
        )
    profile = M3UAccountProfile.objects.create(
        m3u_account=target, name="Owned PATCH nondefault", is_default=False,
        max_streams=3, search_pattern="(.*)", replace_pattern="$1",
        custom_properties={"fixture_marker": "profile"},
    )
print(json.dumps({
    "account": target.pk, "foreign_account": other.pk, "target": groups["target"].pk,
    "sibling": groups["sibling"].pk, "unrelated": groups["unrelated"].pk, "profile": profile.pk,
}))
""".replace("{properties!r}", repr({**ACCOUNT_PROPERTIES, **INITIAL_PREFERENCES}))


def snapshot(fixture, ids):
    script = """
from apps.channels.models import ChannelGroupM3UAccount
from apps.m3u.models import M3UAccountProfile
roles = {ids[name]: name for name in ("target", "sibling", "unrelated")}
rows = ChannelGroupM3UAccount.objects.filter(m3u_account_id__in=[ids["account"], ids["foreign_account"]])
groups = {
    ("owned" if row.m3u_account_id == ids["account"] else "foreign") + "_" + roles.get(row.channel_group_id, "unexpected"):
    {
        "enabled": row.enabled, "auto_channel_sync": row.auto_channel_sync,
        "start": row.auto_sync_channel_start, "end": row.auto_sync_channel_end,
        "marker_kept": (row.custom_properties or {}).get("fixture_marker") == roles.get(row.channel_group_id),
        "last_seen": row.last_seen.isoformat().replace("+00:00", "Z"),
    }
    for row in rows.order_by("id")
}
profile = M3UAccountProfile.objects.filter(pk=ids["profile"], m3u_account_id=ids["account"]).first()
print(json.dumps({
    "count": rows.count(),
    "groups": groups,
    "profile_unchanged": profile is not None and not profile.is_default and profile.is_active
        and profile.max_streams == 3 and profile.search_pattern == "(.*)" and profile.replace_pattern == "$1"
        and profile.exp_date is None and (profile.custom_properties or {}).get("fixture_marker") == "profile",
}))
"""
    return fixture.sql(f"ids = {ids!r}\n" + script, secret=True)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def request(fixture, method, path, value=None):
    # Do not persist request headers, raw HTTP bytes or rejected JSON.
    portable = {"Accept": "application/json", "Authorization": "Bearer " + fixture.actual_admin_token()}
    payload = None
    if value is not None:
        portable["Content-Type"] = "application/json"
        payload = json.dumps(value).encode()
    opener = urllib.request.build_opener(NoRedirect())
    req = urllib.request.Request(fixture.base_url + path, data=payload, headers=portable, method=method)
    try:
        response = opener.open(req, timeout=fixture.remaining(15))
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.status
        raw = bounded_read(response, REPLY_LIMIT, fixture.remaining)
        if status not in (200, 400):
            return status, None
        return status, json.loads(raw)


def observe(payload, ids, *, target_enabled=False, sibling_enabled=True):
    if not isinstance(payload, dict):
        return {"account_object": False}
    groups = payload.get("channel_groups")
    profiles = payload.get("profiles")
    if not isinstance(groups, list) or not all(isinstance(group, dict) for group in groups):
        return {"nested_group_shape": False}
    if not isinstance(profiles, list) or not all(isinstance(profile, dict) for profile in profiles):
        return {"nested_profile_shape": False}
    present = {group.get("channel_group"): group for group in groups}
    retained = [profile for profile in profiles if profile.get("id") == ids["profile"]]
    existing = present.get(ids["target"], {})
    sibling = present.get(ids["sibling"], {})
    profile = retained[0] if len(retained) == 1 else {}
    return {
        "account_identity_unchanged": payload.get("id") == ids["account"]
        and payload.get("created_at") == ACCOUNT_TIME,
        "target_enabled_matches": existing.get("enabled") is target_enabled,
        "target_metadata_unchanged": existing.get("auto_channel_sync") is True
        and existing.get("auto_sync_channel_start") == 12.5 and existing.get("auto_sync_channel_end") == 20.5
        and existing.get("custom_properties") == {"fixture_marker": "target"}
        and existing.get("last_seen") == GROUP_TIME,
        "sibling_matches": sibling.get("enabled") is sibling_enabled
        and sibling.get("auto_channel_sync") is False and sibling.get("auto_sync_channel_start") is None
        and sibling.get("auto_sync_channel_end") is None
        and sibling.get("custom_properties") == {"fixture_marker": "sibling"}
        and sibling.get("last_seen") == GROUP_TIME,
        "unrelated_group_absent": len(groups) == 2 and set(present) == {ids["target"], ids["sibling"]},
        "nondefault_profile_unchanged": profile.get("is_default") is False
        and profile.get("is_active") is True and profile.get("max_streams") == 3
        and profile.get("search_pattern") == "(.*)" and profile.get("replace_pattern") == "$1"
        and profile.get("custom_properties") == {"fixture_marker": "profile"}
        and profile.get("exp_date") is None,
    }


def verify_missing_groups(fixture, path, ids, before):
    missing = [2_147_483_546, 2_147_483_548]
    exists = fixture.sql(
        "from apps.channels.models import ChannelGroup;print(json.dumps(ChannelGroup.objects.filter(pk__in="
        + repr(missing) + ").exists()))", secret=True,
    )
    if exists is not False:
        raise ValueError("missing group fixture IDs already exist")
    submitted = [ids["target"], missing[0], ids["sibling"], missing[1]]
    status, payload = request(fixture, "PATCH", path, {"channel_groups": [
        {"channel_group": group, "enabled": i == 0} for i, group in enumerate(submitted)
    ]})
    expected = {"channel_groups": {
        str(index): {"channel_group": [f'Invalid pk "{group}" - object does not exist.']}
        for index, group in zip((1, 3), missing, strict=True)
    }}
    get_status, follow = request(fixture, "GET", path)
    observed = observe(follow, ids) if get_status == 200 else {}
    return {
        "status":status, "exact_sparse_error":payload==expected,
        "followup_status":get_status, "followup":observed,
        "sql_unchanged":snapshot(fixture, ids)==before,
    }


def verify_duplicate_groups(fixture, path, ids, before):
    submitted = [(ids["target"], True), (ids["unrelated"], False), (ids["sibling"], False),
                 (ids["target"], False), (ids["sibling"], True)]
    status, payload = request(fixture, "PATCH", path, {"channel_groups": [
        {"channel_group":group,"enabled":enabled} for group,enabled in submitted
    ]})
    get_status, follow = request(fixture, "GET", path)
    groups = before["groups"]
    expected = {**before, "groups": {
        **groups, "owned_target": {**groups["owned_target"], "enabled": True},
        "owned_sibling": {**groups["owned_sibling"], "enabled": False},
    }}
    return {
        "status":status, "first_values_match":observe(payload, ids, target_enabled=True, sibling_enabled=False) if status==200 else {},
        "followup_status":get_status, "followup":observe(follow, ids, target_enabled=True, sibling_enabled=False) if get_status==200 else {},
        "sql_first_values_match":snapshot(fixture, ids)==expected,
    }


def preferences_snapshot(fixture, account):
    script = """
from apps.m3u.models import M3UAccount
record = M3UAccount.objects.filter(pk=account).values(
    "name", "account_type", "is_active", "status", "max_streams", "refresh_interval",
    "refresh_task_id", "locked", "server_group_id", "user_agent_id", "custom_properties"
).get()
print(json.dumps(record))
"""
    return fixture.sql(f"account = {account!r}\n" + script, secret=True)


def observe_preferences(payload, ids):
    state = observe(payload, ids, target_enabled=True, sibling_enabled=False)
    if isinstance(payload, dict):
        state.update(
            requested_preferences=all(payload.get(key) is value for key, value in PREFERENCES.items()),
            inactive_xc_preserved=payload.get("account_type") == "XC" and payload.get("is_active") is False,
            vod_disabled=payload.get("enable_vod") is False,
            properties_match=payload.get("custom_properties") == {**ACCOUNT_PROPERTIES, **PREFERENCES},
        )
    return state


def verify_preferences(fixture, path, ids):
    before = preferences_snapshot(fixture, ids["account"])
    relations = snapshot(fixture, ids)
    ready = (
        before["is_active"] is False
        and before["account_type"] == "XC"
        and before["custom_properties"] == {**ACCOUNT_PROPERTIES, **INITIAL_PREFERENCES}
    )
    if not ready:
        raise ValueError("account preference seed did not reach expected state")
    status, payload = request(fixture, "PATCH", path, PREFERENCES)
    follow_status, follow = request(fixture, "GET", path)
    expected = {**before, "custom_properties": {**ACCOUNT_PROPERTIES, **PREFERENCES}}
    return {
        "status": status,
        "checks": observe_preferences(payload, ids) if status == 200 else {},
        "followup_status": follow_status,
        "followup": observe_preferences(follow, ids) if follow_status == 200 else {},
        "sql_only_preferences_changed": preferences_snapshot(fixture, ids["account"]) == expected,
        "sql_relationships_unchanged": snapshot(fixture, ids) == relations,
    }


def observe_orphan(payload, ids, properties):
    state = observe(payload, ids, target_enabled=True, sibling_enabled=False)
    if isinstance(payload, dict):
        state.update(
            properties_match=payload.get("custom_properties") == properties,
            flags_preserved=all(payload.get(key) is value for key, value in PREFERENCES.items()),
            inactive_xc_preserved=payload.get("account_type") == "XC" and payload.get("is_active") is False,
            vod_disabled=payload.get("enable_vod") is False,
        )
    return state


def verify_orphan_cleanup(fixture, path, ids):
    before = preferences_snapshot(fixture, ids["account"])
    relations = snapshot(fixture, ids)
    properties = {**ACCOUNT_PROPERTIES, **PREFERENCES}
    if before["custom_properties"] != properties:
        raise ValueError("account preference prerequisites did not persist")
    results = []
    for mode in ORPHAN_MODES:
        # This is the existing React snapshot payload, echoed with one override.
        properties = {**properties, "orphan_channel_cleanup": mode}
        status, body = request(fixture, "PATCH", path, {"custom_properties": properties})
        follow_status, follow = request(fixture, "GET", path)
        expected = {**before, "custom_properties": properties}
        results.append({
            "mode": mode,
            "status": status,
            "checks": observe_orphan(body, ids, properties) if status == 200 else {},
            "followup_status": follow_status,
            "followup": observe_orphan(follow, ids, properties) if follow_status == 200 else {},
            "sql_only_orphan_mode_changed": preferences_snapshot(fixture, ids["account"]) == expected,
            "sql_relationships_unchanged": snapshot(fixture, ids) == relations,
        })
    return results


ACTIVITY_CRON = "30 5 * * *"

SEED_ACTIVITY = """
from core.models import SystemNotification
from apps.m3u.models import M3UAccount, M3UAccountProfile

account = M3UAccount.objects.get(pk=ids["account"])
account.refresh_interval = 12
account._cron_expression = activity_cron
account.save(update_fields=["refresh_interval"])
M3UAccount.objects.filter(pk=account.pk).update(status="disabled")
foreign_profile = M3UAccountProfile.objects.get(m3u_account_id=ids["foreign_account"], is_default=True)
keys = {
    "owned_warning": f"xc-exp-warning-{ids['profile']}",
    "owned_expiration": f"xc-exp-expired-{ids['profile']}",
    "foreign_warning": f"xc-exp-warning-{foreign_profile.pk}",
}
for key in keys.values():
    SystemNotification.objects.create(notification_key=key, title="Owned expiration", message="Fixture")
print(json.dumps(keys))
"""


def setup_activity(fixture, ids):
    return fixture.sql(f"ids = {ids!r}\nactivity_cron = {ACTIVITY_CRON!r}\n" + SEED_ACTIVITY, secret=True)


def schedule_and_notifications(fixture, account, keys):
    script = """
from apps.m3u.models import M3UAccount
from core.models import SystemNotification

task = M3UAccount.objects.select_related("refresh_task__crontab").get(pk=account).refresh_task
assert task is not None and task.crontab is not None
ct = task.crontab
present = set(SystemNotification.objects.filter(notification_key__in=keys.values())
              .values_list("notification_key", flat=True))
print(json.dumps({
    "enabled": task.enabled,
    "task_id": task.pk,
    "cron": f"{ct.minute} {ct.hour} {ct.day_of_month} {ct.month_of_year} {ct.day_of_week}",
    "timezone": str(ct.timezone),
    "provider_task": task.task == "apps.m3u.tasks.refresh_single_m3u_account",
    "kwargs_match": json.loads(task.kwargs) == {"account_id": account},
    "notifications": {name: key in present for name, key in keys.items()},
}))
"""
    return fixture.sql(f"account = {account!r}\nkeys = {keys!r}\n" + script, secret=True)


def observe_activity(payload, ids, account):
    state = observe(payload, ids, target_enabled=True, sibling_enabled=False)
    if isinstance(payload, dict):
        state.update(
            state_matches=payload.get("is_active") is account["is_active"] and payload.get("status") == account["status"],
            cron_preserved=payload.get("cron_expression") == ACTIVITY_CRON and payload.get("refresh_interval") == 12,
            account_type_preserved=payload.get("account_type") == "XC",
            properties_preserved=payload.get("custom_properties") == account["custom_properties"],
        )
    return state


def verify_activity(fixture, path, ids):
    keys = setup_activity(fixture, ids)
    account_id = ids["account"]
    before = preferences_snapshot(fixture, account_id)
    relations = snapshot(fixture, ids)
    effects = schedule_and_notifications(fixture, account_id, keys)
    if not (
        before["is_active"] is False and before["status"] == "disabled"
        and before["refresh_interval"] == 12 and effects["enabled"] is False
        and effects["task_id"] == before["refresh_task_id"] and effects["cron"] == ACTIVITY_CRON
        and effects["provider_task"] and effects["kwargs_match"] and all(effects["notifications"].values())
        and set(effects["notifications"]) == {"owned_warning", "owned_expiration", "foreign_warning"}
    ):
        raise ValueError("activity schedule or notification seed did not reach the expected state")
    results = []
    for active, expected_status in ((True, "idle"), (False, "disabled")):
        expected_account = {**before, "is_active": active, "status": expected_status}
        expected_effects = {**effects, "enabled": active, "notifications": {
            "owned_warning": active, "owned_expiration": active, "foreign_warning": True,
        }}
        status, payload = request(fixture, "PATCH", path, {"is_active": active})
        follow_status, follow = request(fixture, "GET", path)
        actual_effects = schedule_and_notifications(fixture, account_id, keys)
        results.append({
            "is_active": active,
            "status": status,
            "checks": observe_activity(payload, ids, expected_account) if status == 200 else {},
            "followup_status": follow_status,
            "followup": observe_activity(follow, ids, expected_account) if follow_status == 200 else {},
            "sql_account_preserved": preferences_snapshot(fixture, account_id) == expected_account,
            "sql_schedule_matches": {k: v for k, v in actual_effects.items() if k != "notifications"}
            == {k: v for k, v in expected_effects.items() if k != "notifications"},
            "sql_notifications_match": actual_effects["notifications"] == expected_effects["notifications"],
            "sql_relationships_unchanged": snapshot(fixture, ids) == relations,
        })
    return results


RENAME = "Owned PATCH updated label"
RENAME_USERNAME = "fixture-user"
RENAME_URL = "http://owned-provider:8080/api"
RENAME_PROPERTIES = {**ACCOUNT_PROPERTIES, "enable_vod": True}
RENAME_SEED = """
from hashlib import sha256
from django.db import transaction
from apps.m3u.models import M3UAccount, ServerGroup
from core.models import UserAgent

with transaction.atomic():
    account = M3UAccount.objects.get(pk=account_id)
    agent = UserAgent.objects.create(name="Owned rename agent", user_agent="OwnedRename/1")
    group = ServerGroup.objects.create(name="Owned rename server group")
    account.user_agent = agent
    account.server_group = group
    account.server_url = expected_url
    account.username = expected_username
    account.password = sha256(b"disposable XC rename preservation fixture").hexdigest()
    account.custom_properties = properties
    account.status = "disabled"
    account.stale_stream_days = 9
    account.priority = 4
    account._cron_expression = cron
    account.save()
print(json.dumps({"user_agent": agent.pk, "server_group": group.pk}))
"""


def rename_sql(fixture, account_id):
    script = """
from hashlib import sha256
from apps.m3u.models import M3UAccount, M3UAccountProfile

account = M3UAccount.objects.get(pk=account_id)
profiles = list(M3UAccountProfile.objects.filter(m3u_account_id=account_id, is_default=True)
                .values("id", "name", "is_active", "max_streams", "search_pattern", "replace_pattern"))
print(json.dumps({
    "name": account.name,
    "server_url_matches": account.server_url == expected_url,
    "username_matches": account.username == expected_username,
    "password_matches": account.password == sha256(b"disposable XC rename preservation fixture").hexdigest(),
    "stale_stream_days": account.stale_stream_days,
    "priority": account.priority,
    "default_profiles": profiles,
}))
"""
    return fixture.sql(
        f"account_id = {account_id!r}\nexpected_url = {RENAME_URL!r}\n"
        f"expected_username = {RENAME_USERNAME!r}\n" + script,
        secret=True,
    )


def observe_rename(payload, ids, related, default, *, previous=False):
    state = observe(payload, ids)
    if isinstance(payload, dict):
        defaults = [p for p in (payload.get("profiles") or []) if isinstance(p, dict) and p.get("is_default")]
        state.update(
            name_matches=payload.get("name") == ("Owned PATCH target" if previous else RENAME),
            default_profile_unchanged=len(defaults) == 1 and all(
                defaults[0].get(key) == value for key, value in default.items()
            ),
            provider_matches=payload.get("server_url") == RENAME_URL
            and payload.get("username") == RENAME_USERNAME
            and payload.get("user_agent") == related["user_agent"]
            and payload.get("server_group") == related["server_group"],
            schedule_matches=payload.get("cron_expression") == ACTIVITY_CRON
            and payload.get("refresh_interval") == 0,
            options_match=payload.get("account_type") == "XC" and payload.get("is_active") is False
            and payload.get("status") == "disabled" and payload.get("max_streams") == 0
            and payload.get("stale_stream_days") == 9 and payload.get("priority") == 4,
            properties_match=payload.get("enable_vod") is True and payload.get("hash_key") == "name,url"
            and payload.get("custom_properties") == RENAME_PROPERTIES,
        )
    return state


def verify_rename(fixture, path, ids):
    account_id = ids["account"]
    related = fixture.sql(
        f"account_id = {account_id!r}\nexpected_url = {RENAME_URL!r}\nexpected_username = {RENAME_USERNAME!r}\n"
        f"properties = {RENAME_PROPERTIES!r}\ncron = {ACTIVITY_CRON!r}\n" + RENAME_SEED,
        secret=True,
    )
    initial = rename_sql(fixture, account_id)
    account = preferences_snapshot(fixture, account_id)
    relations = snapshot(fixture, ids)
    schedule = schedule_and_notifications(fixture, account_id, {})
    assert initial["name"] == "Owned PATCH target"
    assert len(initial["default_profiles"]) == 1
    default = initial["default_profiles"][0]
    assert default["name"] == initial["name"] + " Default"
    assert all(initial[key] for key in ("server_url_matches", "username_matches", "password_matches"))
    assert schedule["task_id"] == account["refresh_task_id"] and schedule["cron"] == ACTIVITY_CRON
    assert schedule["enabled"] is False and schedule["provider_task"] and schedule["kwargs_match"]
    get_status, before = request(fixture, "GET", path)
    assert get_status == 200 and all(observe_rename(before, ids, related, default, previous=True).values())
    # M3U.jsx supplies the form fields and prepareSubmitValues omits password and exp_date for this XC draft.
    status, patched = request(fixture, "PATCH", path, {
        "name": RENAME,
        "server_url": RENAME_URL,
        "user_agent": str(related["user_agent"]),
        "server_group": str(related["server_group"]),
        "is_active": False,
        "max_streams": 0,
        "refresh_interval": 0,
        "cron_expression": ACTIVITY_CRON,
        "account_type": "XC",
        "username": RENAME_USERNAME,
        "stale_stream_days": 9,
        "priority": 4,
        "enable_vod": True,
        "hash_key": "name,url",
    })
    follow_status, follow = request(fixture, "GET", path)
    final = rename_sql(fixture, account_id)
    return {
        "status": status,
        "checks": observe_rename(patched, ids, related, default) if status == 200 else {},
        "followup_status": follow_status,
        "followup": observe_rename(follow, ids, related, default) if follow_status == 200 else {},
        "sql_account_preserved": final == {**initial, "name": RENAME}
        and preferences_snapshot(fixture, account_id) == {**account, "name": RENAME},
        "sql_schedule_preserved": schedule_and_notifications(fixture, account_id, {}) == schedule,
        "sql_relationships_unchanged": snapshot(fixture, ids) == relations,
        "sql_credentials_preserved": all(final[key] for key in (
            "server_url_matches", "username_matches", "password_matches",
        )),
    }


INTERVAL_START = 6
INTERVAL_EXCLUSIVE = 137
INTERVAL_REUSABLE = 173


def setup_interval_case(fixture, ids, active):
    script = """
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from apps.m3u.models import M3UAccount

for account_id in (ids["account"], ids["foreign_account"]):
    account = M3UAccount.objects.get(pk=account_id)
    if account_id == ids["account"]:
        account.is_active = active
    account.refresh_interval = start
    account._cron_expression = ""
    account.save(update_fields=["is_active", "refresh_interval"])
M3UAccount.objects.filter(pk=ids["account"]).update(status="success" if active else "disabled")
first = PeriodicTask.objects.get(name="m3u_account-refresh-" + str(ids["account"]))
sibling = PeriodicTask.objects.get(name="m3u_account-refresh-" + str(ids["foreign_account"]))
assert first.interval_id is not None and first.interval_id == sibling.interval_id
assert not IntervalSchedule.objects.filter(every=exclusive, period=IntervalSchedule.HOURS).exists()
existing, _ = IntervalSchedule.objects.get_or_create(every=reusable, period=IntervalSchedule.HOURS)
print(json.dumps({"original": first.interval_id, "sibling_task": sibling.pk, "reusable": existing.pk}))
"""
    return fixture.sql(
        f"ids = {ids!r}\nactive = {active!r}\nstart = {INTERVAL_START}\n"
        f"exclusive = {INTERVAL_EXCLUSIVE}\nreusable = {INTERVAL_REUSABLE}\n" + script,
        secret=True,
    )


def interval_snapshot(fixture, account_id, schedules):
    script = """
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from apps.m3u.models import M3UAccount

account = M3UAccount.objects.select_related("refresh_task__interval").get(pk=account_id)
task = account.refresh_task
assert task is not None and task.interval is not None
sibling = PeriodicTask.objects.get(pk=schedules["sibling_task"])
print(json.dumps({
    "task_id": task.pk,
    "interval_id": task.interval_id,
    "hours": task.interval.every,
    "period": task.interval.period,
    "no_cron": task.crontab_id is None,
    "enabled": task.enabled,
    "provider_task": task.task == "apps.m3u.tasks.refresh_single_m3u_account",
    "kwargs_match": json.loads(task.kwargs) == {"account_id": account_id},
    "sibling_unchanged": sibling.interval_id == schedules["original"],
    "exclusive_exists": IntervalSchedule.objects.filter(every=exclusive, period=IntervalSchedule.HOURS).exists(),
}))
"""
    return fixture.sql(
        f"account_id = {account_id!r}\nschedules = {schedules!r}\nexclusive = {INTERVAL_EXCLUSIVE}\n" + script,
        secret=True,
    )


def observe_interval(payload, ids, related, default, active, hours, cron=""):
    state = observe(payload, ids)
    if isinstance(payload, dict):
        defaults = [profile for profile in (payload.get("profiles") or [])
                    if isinstance(profile, dict) and profile.get("is_default")]
        state.update(
            account_unchanged=payload.get("name") == "Owned PATCH target"
            and payload.get("is_active") is active
            and payload.get("status") == ("success" if active else "disabled")
            and payload.get("account_type") == "XC",
            provider_unchanged=payload.get("user_agent") == related["user_agent"]
            and payload.get("server_group") == related["server_group"]
            and payload.get("server_url") == RENAME_URL and payload.get("username") == RENAME_USERNAME,
            default_profile_unchanged=len(defaults) == 1 and all(
                defaults[0].get(key) == value for key, value in default.items()
            ),
            properties_unchanged=payload.get("enable_vod") is True and payload.get("hash_key") == "name,url"
            and payload.get("custom_properties") == RENAME_PROPERTIES,
            schedule_fields_match=payload.get("refresh_interval") == hours and payload.get("cron_expression") == cron,
        )
    return state


def stock_schedule_draft(account, related, hours, cron):
    return {
        "name": account["name"],
        "server_url": RENAME_URL,
        "user_agent": str(related["user_agent"]),
        "server_group": str(related["server_group"]),
        "is_active": account["is_active"],
        "max_streams": 0,
        "refresh_interval": hours,
        "cron_expression": cron,
        "account_type": "XC",
        "username": RENAME_USERNAME,
        "stale_stream_days": 9,
        "priority": 4,
        "enable_vod": True,
        "hash_key": "name,url",
    }


def verify_interval(fixture, path, ids, record):
    account_id = ids["account"]
    related = fixture.sql(
        f"account_id = {account_id!r}\nexpected_url = {RENAME_URL!r}\nexpected_username = {RENAME_USERNAME!r}\n"
        f"properties = {RENAME_PROPERTIES!r}\ncron = ''\n" + RENAME_SEED,
        secret=True,
    )
    last = rename_sql(fixture, account_id)
    assert len(last["default_profiles"]) == 1
    default = last["default_profiles"][0]
    assert default["name"] == last["name"] + " Default"
    assert all(last[key] for key in ("server_url_matches", "username_matches", "password_matches"))
    relations = snapshot(fixture, ids)
    for active in (True, False):
        schedules = setup_interval_case(fixture, ids, active)
        account = preferences_snapshot(fixture, account_id)
        assert account["refresh_interval"] == INTERVAL_START and account["is_active"] is active
        initial = interval_snapshot(fixture, account_id, schedules)
        assert initial["interval_id"] == schedules["original"] and initial["enabled"] is active
        assert initial["task_id"] == account["refresh_task_id"]
        for hours in (INTERVAL_EXCLUSIVE, 0, INTERVAL_REUSABLE):
            status, payload = request(fixture, "PATCH", path, stock_schedule_draft(account, related, hours, ""))
            follow_status, follow = request(fixture, "GET", path)
            actual = interval_snapshot(fixture, account_id, schedules)
            record.append({
                "is_active": active,
                "hours": hours,
                "status": status,
                "checks": observe_interval(payload, ids, related, default, active, hours) if status == 200 else {},
                "followup_status": follow_status,
                "followup": observe_interval(follow, ids, related, default, active, hours) if follow_status == 200 else {},
                "sql_only_interval_changed": preferences_snapshot(fixture, account_id)
                == {**account, "refresh_interval": hours},
                "sql_task_matches": actual["task_id"] == initial["task_id"]
                and actual["enabled"] is (active and hours > 0)
                and actual["hours"] == max(hours, 1) and actual["period"] == "hours"
                and actual["no_cron"] and actual["provider_task"] and actual["kwargs_match"],
                "sql_shared_preserved": actual["sibling_unchanged"],
                "sql_orphan_removed": actual["exclusive_exists"] is (hours == INTERVAL_EXCLUSIVE),
                "sql_interval_reused": hours != INTERVAL_REUSABLE or actual["interval_id"] == schedules["reusable"],
                "sql_credentials_and_profiles_preserved": rename_sql(fixture, account_id) == last
                and snapshot(fixture, ids) == relations,
            })


DAILY_CRON = "37 4 * * *"
CRON_TIMEZONE = "America/New_York"


def setup_daily_cron_case(fixture, ids, active):
    script = """
from django_celery_beat.models import CrontabSchedule, IntervalSchedule, PeriodicTask
from apps.m3u.models import M3UAccount
from core.models import CoreSettings
from core.scheduling import parse_cron_expression

CoreSettings.set_system_time_zone(timezone)
assert CoreSettings.get_system_time_zone() == timezone
for account_id in (ids["account"], ids["foreign_account"]):
    account = M3UAccount.objects.get(pk=account_id)
    if account_id == ids["account"]:
        account.is_active = active
        account.refresh_interval = start
        account._cron_expression = ""
    else:
        account.refresh_interval = 6 if active else 0
        account._cron_expression = "" if active else cron
    account.save(update_fields=["is_active", "refresh_interval"])
M3UAccount.objects.filter(pk=ids["account"]).update(status="success" if active else "disabled")
first = PeriodicTask.objects.get(name="m3u_account-refresh-" + str(ids["account"]))
sibling = PeriodicTask.objects.get(name="m3u_account-refresh-" + str(ids["foreign_account"]))
crontab, _ = CrontabSchedule.objects.get_or_create(timezone=timezone, **parse_cron_expression(cron))
interval, _ = IntervalSchedule.objects.get_or_create(every=6, period=IntervalSchedule.HOURS)
assert first.interval_id is not None and first.interval_id != interval.pk
assert (sibling.interval_id, sibling.crontab_id) == (
    (interval.pk, None) if active else (None, crontab.pk)
)
print(json.dumps({
    "task": first.pk, "original": first.interval_id, "sibling_task": sibling.pk,
    "sibling_interval": sibling.interval_id, "sibling_cron": sibling.crontab_id,
    "reuse_interval": interval.pk, "reuse_cron": crontab.pk,
}))
"""
    return fixture.sql(
        f"ids = {ids!r}\nactive = {active!r}\nstart = {INTERVAL_EXCLUSIVE}\n"
        f"cron = {DAILY_CRON!r}\ntimezone = {CRON_TIMEZONE!r}\n" + script,
        secret=True,
    )


def cron_transition_snapshot(fixture, account_id, schedules):
    script = """
from django_celery_beat.models import CrontabSchedule, IntervalSchedule, PeriodicTask
from apps.m3u.models import M3UAccount

account = M3UAccount.objects.select_related("refresh_task__interval", "refresh_task__crontab").get(pk=account_id)
task = account.refresh_task
sibling = PeriodicTask.objects.get(pk=schedules["sibling_task"])
cron = task.crontab
interval = task.interval
print(json.dumps({
    "task_id": task.pk,
    "interval_id": task.interval_id,
    "hours": interval.every if interval else None,
    "period": interval.period if interval else None,
    "crontab_id": task.crontab_id,
    "cron": " ".join((cron.minute, cron.hour, cron.day_of_month, cron.month_of_year, cron.day_of_week)) if cron else "",
    "timezone": str(cron.timezone) if cron else None,
    "enabled": task.enabled,
    "provider_task": task.task == "apps.m3u.tasks.refresh_single_m3u_account",
    "kwargs_match": json.loads(task.kwargs) == {"account_id": account_id},
    "sibling_unchanged": (sibling.interval_id, sibling.crontab_id)
        == (schedules["sibling_interval"], schedules["sibling_cron"]),
    "old_interval_exists": IntervalSchedule.objects.filter(pk=schedules["original"]).exists(),
    "cron_exists": CrontabSchedule.objects.filter(pk=schedules["reuse_cron"]).exists(),
    "reuse_interval_exists": IntervalSchedule.objects.filter(pk=schedules["reuse_interval"]).exists(),
}))
"""
    return fixture.sql(f"account_id = {account_id!r}\nschedules = {schedules!r}\n" + script, secret=True)


def verify_daily_cron(fixture, path, ids, record):
    account_id = ids["account"]
    related = fixture.sql(
        f"account_id = {account_id!r}\nexpected_url = {RENAME_URL!r}\nexpected_username = {RENAME_USERNAME!r}\n"
        f"properties = {RENAME_PROPERTIES!r}\ncron = ''\n" + RENAME_SEED,
        secret=True,
    )
    identity = rename_sql(fixture, account_id)
    assert len(identity["default_profiles"]) == 1
    default = identity["default_profiles"][0]
    assert default["name"] == identity["name"] + " Default"
    assert all(identity[key] for key in ("server_url_matches", "username_matches", "password_matches"))
    relations = snapshot(fixture, ids)
    for active in (True, False):
        schedules = setup_daily_cron_case(fixture, ids, active)
        account = preferences_snapshot(fixture, account_id)
        assert account["refresh_interval"] == INTERVAL_EXCLUSIVE and account["is_active"] is active
        assert account["refresh_task_id"] == schedules["task"]
        before = cron_transition_snapshot(fixture, account_id, schedules)
        assert before["interval_id"] == schedules["original"] and before["crontab_id"] is None
        assert before["cron_exists"] and before["reuse_interval_exists"] and before["enabled"] is active
        for hours, cron in ((0, DAILY_CRON), (6, "")):
            status, payload = request(fixture, "PATCH", path, stock_schedule_draft(account, related, hours, cron))
            follow_status, follow = request(fixture, "GET", path)
            actual = cron_transition_snapshot(fixture, account_id, schedules)
            use_cron = bool(cron)
            record.append({
                "is_active": active,
                "uses_cron": use_cron,
                "status": status,
                "checks": observe_interval(payload, ids, related, default, active, hours, cron) if status == 200 else {},
                "followup_status": follow_status,
                "followup": observe_interval(follow, ids, related, default, active, hours, cron) if follow_status == 200 else {},
                "sql_only_schedule_fields_changed": preferences_snapshot(fixture, account_id)
                == {**account, "refresh_interval": hours},
                "sql_task_preserved": actual["task_id"] == schedules["task"] and actual["enabled"] is active
                and actual["provider_task"] and actual["kwargs_match"],
                "sql_schedule_and_timezone_match": actual["cron"] == cron
                and actual["timezone"] == (CRON_TIMEZONE if use_cron else None)
                and actual["crontab_id"] == (schedules["reuse_cron"] if use_cron else None)
                and actual["interval_id"] == (None if use_cron else schedules["reuse_interval"])
                and actual["hours"] == (None if use_cron else hours)
                and actual["period"] == (None if use_cron else "hours"),
                "sql_shared_schedules_untouched": actual["sibling_unchanged"] and actual["reuse_interval_exists"],
                "sql_orphans_removed": not actual["old_interval_exists"]
                and actual["cron_exists"] is (use_cron or schedules["sibling_cron"] is not None),
                "sql_credentials_and_profiles_preserved": rename_sql(fixture, account_id) == identity
                and snapshot(fixture, ids) == relations,
            })


def run(fixture, record):
    assert fixture.case == CASE and fixture.backend in {"Original", "Native"}
    evidence = {"case": CASE, "backend": fixture.backend, "passed": False, "steps": []}
    stage = "seed"
    try:
        ids = fixture.sql(SEED, secret=True)
        assert all(type(value) is int and value > 0 for value in ids.values())
        stage = "before"
        before = snapshot(fixture, ids)
        groups = before["groups"]
        expected_keys = {"owned_target", "owned_sibling", "foreign_unrelated"}
        ready = before["count"] == 3 and set(groups) == expected_keys and before["profile_unchanged"] and all(
            group["enabled"] and group["marker_kept"] and group["last_seen"] == GROUP_TIME
            for group in groups.values()
        )
        evidence["valid_precondition"] = ready
        if ready:
            path = f"/api/m3u/accounts/{ids['account']}/"
            value = {"channel_groups": [
                {"channel_group": ids["target"], "enabled": False},
                {"channel_group": ids["unrelated"], "enabled": False},
            ]}
            for method, body in [("PATCH", value), ("GET", None)]:
                stage = method
                status, payload = request(fixture, method, path, body)
                evidence["steps"].append({
                    "method": method, "status": status,
                    "checks": observe(payload, ids) if status == 200 else {},
                })
            stage = "after"
            after = snapshot(fixture, ids)
            expected = {**groups, "owned_target": {**groups["owned_target"], "enabled": False}}
            evidence["sql_matches_only_enabled_transition"] = (
                after["count"] == 3 and after["groups"] == expected and after["profile_unchanged"]
            )
            evidence["passed"] = evidence["sql_matches_only_enabled_transition"] and all(
                step["status"] == 200 and all(step["checks"].values()) for step in evidence["steps"]
            )
        if evidence["passed"]:
            evidence["passed"] = False
            stage = "missing group references"
            missing = verify_missing_groups(fixture, path, ids, after)
            evidence["missing_group_references"] = missing
            assert missing["status"] == 400 and missing["exact_sparse_error"] and missing["sql_unchanged"]
            assert missing["followup_status"] == 200 and missing["followup"] and all(missing["followup"].values())
            if fixture.binding.get("xc_group_patch_daily_cron_edit"):
                stage = "playlist editor daily cron"
                evidence["daily_cron_edit"] = schedules = []
                verify_daily_cron(fixture, path, ids, schedules)
                assert len(schedules) == 4
                for schedule in schedules:
                    assert schedule["status"] == schedule["followup_status"] == 200
                    assert schedule["checks"] and all(schedule["checks"].values())
                    assert schedule["followup"] and all(schedule["followup"].values())
                    assert all(schedule[key] for key in (
                        "sql_only_schedule_fields_changed", "sql_task_preserved", "sql_schedule_and_timezone_match",
                        "sql_shared_schedules_untouched", "sql_orphans_removed", "sql_credentials_and_profiles_preserved",
                    ))
            if fixture.binding.get("xc_group_patch_interval_edit"):
                stage = "playlist editor interval"
                evidence["interval_edit"] = intervals = []
                verify_interval(fixture, path, ids, intervals)
                assert len(intervals) == 6
                for interval in intervals:
                    assert interval["status"] == interval["followup_status"] == 200
                    assert interval["checks"] and all(interval["checks"].values())
                    assert interval["followup"] and all(interval["followup"].values())
                    assert all(interval[key] for key in (
                        "sql_only_interval_changed", "sql_task_matches", "sql_shared_preserved",
                        "sql_orphan_removed", "sql_interval_reused", "sql_credentials_and_profiles_preserved",
                    ))
            if fixture.binding.get("xc_group_patch_label_edit"):
                stage = "playlist editor rename"
                rename = verify_rename(fixture, path, ids)
                evidence["label_edit"] = rename
                assert rename["status"] == rename["followup_status"] == 200
                assert rename["checks"] and all(rename["checks"].values())
                assert rename["followup"] and all(rename["followup"].values())
                assert all(rename[key] for key in (
                    "sql_account_preserved", "sql_schedule_preserved",
                    "sql_relationships_unchanged", "sql_credentials_preserved",
                ))
            if fixture.binding.get("xc_group_patch_duplicates"):
                stage = "duplicate group selections"
                duplicate = verify_duplicate_groups(fixture, path, ids, after)
                evidence["duplicate_group_selections"] = duplicate
                assert duplicate["status"] == duplicate["followup_status"] == 200
                assert duplicate["sql_first_values_match"] and all(duplicate["first_values_match"].values())
                assert duplicate["followup"] and all(duplicate["followup"].values())
                stage = "group filter preferences"
                preferences = verify_preferences(fixture, path, ids)
                evidence["group_filter_preferences"] = preferences
                assert preferences["status"] == preferences["followup_status"] == 200
                assert preferences["checks"] and all(preferences["checks"].values())
                assert preferences["followup"] and all(preferences["followup"].values())
                assert preferences["sql_only_preferences_changed"] and preferences["sql_relationships_unchanged"]
            if fixture.binding.get("xc_group_patch_orphan_modes"):
                stage = "live group filter orphan cleanup"
                modes = verify_orphan_cleanup(fixture, path, ids)
                evidence["orphan_cleanup_modes"] = modes
                for mode in modes:
                    assert mode["status"] == mode["followup_status"] == 200
                    assert mode["checks"] and all(mode["checks"].values())
                    assert mode["followup"] and all(mode["followup"].values())
                    assert mode["sql_only_orphan_mode_changed"] and mode["sql_relationships_unchanged"]
            if fixture.binding.get("xc_group_patch_active_toggle"):
                stage = "playlist table activity"
                transitions = verify_activity(fixture, path, ids)
                evidence["active_toggle"] = transitions
                for transition in transitions:
                    assert transition["status"] == transition["followup_status"] == 200
                    assert transition["checks"] and all(transition["checks"].values())
                    assert transition["followup"] and all(transition["followup"].values())
                    assert all(transition[key] for key in (
                        "sql_account_preserved", "sql_schedule_matches", "sql_notifications_match",
                        "sql_relationships_unchanged",
                    ))
            evidence["passed"] = True
    except Exception as error:
        evidence["failure"] = {"stage": stage, "type": type(error).__name__}
    record(evidence)
    return evidence
