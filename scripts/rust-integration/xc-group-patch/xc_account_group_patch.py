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
