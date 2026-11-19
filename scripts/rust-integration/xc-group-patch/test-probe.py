"""Offline checks of the exact staged Main group fixture."""
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

import xc_account_group_patch as probe
import workflow

IDS = {"account": 11, "target": 21, "sibling": 22, "unrelated": 23, "profile": 31}

def relations(target, sibling):
    return {"groups":{
        "owned_target":{"enabled":target,"marker_kept":True,"last_seen":probe.GROUP_TIME},
        "owned_sibling":{"enabled":sibling,"marker_kept":True,"last_seen":probe.GROUP_TIME},
        "foreign_unrelated":{"enabled":True,"marker_kept":True,"last_seen":probe.GROUP_TIME},
    },"count":3,"profile_unchanged":True}

def body(target, sibling):
    return {
        "id":IDS["account"],"created_at":probe.ACCOUNT_TIME,
        "channel_groups":[
            {"channel_group":IDS["target"],"enabled":target,"auto_channel_sync":True,
             "auto_sync_channel_start":12.5,"auto_sync_channel_end":20.5,
             "custom_properties":{"fixture_marker":"target"},"last_seen":probe.GROUP_TIME},
            {"channel_group":IDS["sibling"],"enabled":sibling,"auto_channel_sync":False,
             "custom_properties":{"fixture_marker":"sibling"},"last_seen":probe.GROUP_TIME},
        ],
        "profiles":[{"id":IDS["profile"],"is_default":False,"is_active":True,"max_streams":3,
             "search_pattern":"(.*)","replace_pattern":"$1","custom_properties":{"fixture_marker":"profile"}}],
    }

class Fixture:
    case=probe.CASE
    backend="Original"
    def __init__(self, duplicates, orphans, activity):
        self.binding = {
            "xc_group_patch_duplicates": duplicates,
            "xc_group_patch_orphan_modes": orphans,
            "xc_group_patch_active_toggle": activity,
        }
    def sql(self,source,*,secret):
        assert secret
        return False if "ChannelGroup.objects.filter" in source else IDS

class Tests(unittest.TestCase):
    def execute_case(self, duplicate, wrong_error=False, account_side_effect=False,
                     orphans=False, orphan_side_effect=False, activity=False, retain_notice=False):
        initial,selected,opposite=relations(True,True),relations(False,True),relations(True,False)
        live = deepcopy(initial)
        account = {
            "account_type": "XC", "is_active": False, "status": "idle",
            "custom_properties": {**probe.ACCOUNT_PROPERTIES, **probe.INITIAL_PREFERENCES},
        }
        effects = {
            "enabled": False, "task_id": 91, "cron": probe.ACTIVITY_CRON, "timezone": "UTC",
            "provider_task": True, "kwargs_match": True,
            "notifications": {"owned_warning": True, "owned_expiration": True, "foreign_warning": True},
        }

        def setup_activity(_f, ids):
            assert ids == IDS
            account.update(status="disabled", refresh_interval=12, refresh_task_id=91, cron_expression=probe.ACTIVITY_CRON)
            return {key: str(i) for i, key in enumerate(effects["notifications"])}

        seen = []
        def response_body(target, sibling):
            return {
                **body(target, sibling), **account,
                **account["custom_properties"],
            }
        def request(_f,method,path,value=None):
            assert path=="/api/m3u/accounts/11/"
            if method=="GET":
                return 200,response_body(live["groups"]["owned_target"]["enabled"],live["groups"]["owned_sibling"]["enabled"])
            if value == probe.PREFERENCES:
                seen.append(value)
                account["custom_properties"].update(value)
                if account_side_effect:
                    account["status"] = "success"
                return 200, response_body(True, False)
            if "custom_properties" in value:
                assert set(value) == {"custom_properties"}
                previous = account["custom_properties"]
                changed = value["custom_properties"]
                assert changed == {**previous, "orphan_channel_cleanup": changed["orphan_channel_cleanup"]}
                seen.append(value)
                account["custom_properties"] = dict(changed)
                if orphan_side_effect:
                    account["status"] = "success"
                return 200, response_body(True, False)
            if "is_active" in value:
                assert set(value) == {"is_active"} and type(value["is_active"]) is bool
                current = value["is_active"]
                seen.append(value)
                account.update(is_active=current, status="idle" if current else "disabled")
                effects["enabled"] = current
                if not current:
                    effects["notifications"]["owned_warning"] = retain_notice
                    effects["notifications"]["owned_expiration"] = False
                return 200, response_body(True, False)
            rows=value["channel_groups"]
            seen.append(rows)
            if len(rows)==2:
                live.update(deepcopy(selected))
                return 200,response_body(False,True)
            if len(rows)==4:
                invalid={str(i):{"channel_group":[f'Invalid pk "{r["channel_group"]}" - object does not exist.']}
                         for i,r in enumerate(rows) if r["channel_group"] not in IDS.values()}
                return (400,{"channel_groups":invalid}) if not wrong_error else (501,None)
            assert len(rows)==5 and [r["enabled"] for r in rows]==[True,False,False,False,True]
            live.update(deepcopy(opposite))
            return 200,response_body(True,False)
        with patch.multiple(
            probe, request=request, snapshot=lambda *_: deepcopy(live),
            preferences_snapshot=lambda *_: deepcopy(account),
            setup_activity=setup_activity,
            schedule_and_notifications=lambda *_: deepcopy(effects),
        ):
            out=[]
            result=probe.run(Fixture(duplicate, orphans, activity),out.append)
        assert result==out[0]
        return result,seen

    def test_reference_only(self):
        result,requests=self.execute_case(False)
        self.assertTrue(result["passed"])
        self.assertEqual(len(requests),2)
        self.assertTrue(result["missing_group_references"]["exact_sparse_error"])

    def test_both_cases(self):
        result,requests=self.execute_case(True)
        self.assertTrue(result["passed"])
        self.assertEqual(len(requests),4)
        self.assertTrue(result["duplicate_group_selections"]["sql_first_values_match"])
        self.assertTrue(result["group_filter_preferences"]["sql_only_preferences_changed"])
        self.assertTrue(result["group_filter_preferences"]["sql_relationships_unchanged"])

    def test_preference_followup_rejects_sql_side_effect(self):
        result, _ = self.execute_case(True, account_side_effect=True)
        self.assertEqual(result["group_filter_preferences"]["status"], 200)
        self.assertFalse(result["group_filter_preferences"]["sql_only_preferences_changed"])
        self.assertFalse(result["passed"])

    def test_orphan_cleanup_modes_persist(self):
        result, requests = self.execute_case(True, orphans=True)
        self.assertTrue(result["passed"])
        self.assertEqual(len(requests), 7)
        self.assertEqual([mode["mode"] for mode in result["orphan_cleanup_modes"]], list(probe.ORPHAN_MODES))
        self.assertTrue(all(mode["sql_only_orphan_mode_changed"] for mode in result["orphan_cleanup_modes"]))

    def test_orphan_cleanup_sql_side_effect_fails(self):
        result, _ = self.execute_case(True, orphans=True, orphan_side_effect=True)
        self.assertFalse(result["passed"])
        self.assertTrue(all(mode["status"] == 200 for mode in result["orphan_cleanup_modes"]))
        self.assertFalse(any(mode["sql_only_orphan_mode_changed"] for mode in result["orphan_cleanup_modes"]))

    def test_activity_preserves_schedule_and_removes_only_owned_notices(self):
        result, requests = self.execute_case(True, orphans=True, activity=True)
        self.assertTrue(result["passed"])
        self.assertEqual(requests[-2:], [{"is_active": True}, {"is_active": False}])
        self.assertTrue(all(step["sql_schedule_matches"] for step in result["active_toggle"]))
        self.assertTrue(all(step["sql_notifications_match"] for step in result["active_toggle"]))

    def test_activity_leftover_notice_rejects_a_200_response(self):
        result, _ = self.execute_case(True, orphans=True, activity=True, retain_notice=True)
        self.assertFalse(result["passed"])
        self.assertEqual(result["active_toggle"][-1]["status"], 200)
        self.assertFalse(result["active_toggle"][-1]["sql_notifications_match"])

    def test_wrong_error_fails_before_duplicate(self):
        result,requests=self.execute_case(True,True)
        self.assertFalse(result["passed"])
        self.assertEqual(len(requests),2)

    def execute_rename(self, *, damaged_password=False, relabeled_profile=False, changed_schedule=False):
        related = {"user_agent": 41, "server_group": 51}
        profile = {
            "id": 30, "name": "Owned PATCH target Default", "is_active": True, "max_streams": 0,
            "search_pattern": "^(.*)$", "replace_pattern": "$1",
        }
        account = {
            "name": "Owned PATCH target", "server_url": probe.RENAME_URL,
            "username": probe.RENAME_USERNAME, **related, "cron_expression": probe.ACTIVITY_CRON,
            "refresh_task_id": 91, "refresh_interval": 0, "status": "disabled", "is_active": False,
            "account_type": "XC", "max_streams": 0, "stale_stream_days": 9, "priority": 4,
            "enable_vod": True, "hash_key": "name,url", "custom_properties": dict(probe.RENAME_PROPERTIES),
        }

        class RenameFixture:
            def sql(self, source, *, secret):
                assert secret and probe.RENAME_SEED in source
                return related

        patched = False

        def rename_sql(*_):
            return {
                "name": account["name"], "username_matches": True, "server_url_matches": True,
                "password_matches": not (patched and damaged_password),
                "default_profiles": [deepcopy(profile)],
            }

        def schedule(*_):
            return {
                "enabled": False, "task_id": 91, "provider_task": True, "kwargs_match": True,
                "cron": "31 5 * * *" if patched and changed_schedule else probe.ACTIVITY_CRON,
            }

        def request(_fixture, method, path, value=None):
            nonlocal patched
            assert path == "/api/m3u/accounts/11/"
            if method == "PATCH":
                assert value == {
                    "name": probe.RENAME, "server_url": probe.RENAME_URL, "user_agent": "41",
                    "server_group": "51", "is_active": False, "max_streams": 0, "refresh_interval": 0,
                    "cron_expression": probe.ACTIVITY_CRON, "account_type": "XC",
                    "username": probe.RENAME_USERNAME, "stale_stream_days": 9, "priority": 4,
                    "enable_vod": True, "hash_key": "name,url",
                }
                patched = True
                account["name"] = probe.RENAME
                if relabeled_profile:
                    profile["name"] = probe.RENAME + " Default"
            response = {**body(False, True), **account}
            response["profiles"].append({**profile, "is_default": True})
            return 200, response

        with patch.multiple(
            probe, request=request, rename_sql=rename_sql, schedule_and_notifications=schedule,
            preferences_snapshot=lambda *_: deepcopy(account), snapshot=lambda *_: relations(False, True),
        ):
            result = probe.verify_rename(RenameFixture(), "/api/m3u/accounts/11/", IDS)
        assert patched and result["status"] == result["followup_status"] == 200
        return result

    def test_playlist_rename_preserves_default_profile_schedule_and_credentials(self):
        proof = self.execute_rename()
        self.assertTrue(all(proof["checks"].values()) and all(proof["followup"].values()))
        self.assertTrue(proof["sql_account_preserved"] and proof["sql_relationships_unchanged"])
        self.assertTrue(proof["sql_schedule_preserved"] and proof["sql_credentials_preserved"])

    def test_playlist_rename_rejects_side_effects_despite_200(self):
        with self.subTest(field="credential"):
            proof = self.execute_rename(damaged_password=True)
            self.assertFalse(proof["sql_credentials_preserved"])
        with self.subTest(field="default profile label"):
            proof = self.execute_rename(relabeled_profile=True)
            self.assertFalse(proof["checks"]["default_profile_unchanged"])
            self.assertFalse(proof["sql_account_preserved"])
        with self.subTest(field="schedule"):
            proof = self.execute_rename(changed_schedule=True)
            self.assertFalse(proof["sql_schedule_preserved"])


    def execute_interval_edits(self, *, leave_orphan=False):
        related = {"user_agent": 41, "server_group": 51}
        profile = {"id": 30, "name": "Owned PATCH target Default"}
        account = {"name": "Owned PATCH target", "refresh_task_id": 91, "account_type": "XC"}
        current = []
        schedule_rows = {"original": 42, "sibling_task": 43, "reusable": 44}

        class IntervalFixture:
            def sql(self, source, *, secret):
                assert secret and probe.RENAME_SEED in source
                return related

        def setup(_fixture, ids, active):
            assert ids == IDS
            current.clear()
            account.update(is_active=active, refresh_interval=probe.INTERVAL_START,
                           status="success" if active else "disabled")
            return schedule_rows

        def task(_fixture, account_id, schedules):
            assert account_id == IDS["account"] and schedules == schedule_rows
            hours = account["refresh_interval"]
            interval_ids = {probe.INTERVAL_START: 42, probe.INTERVAL_EXCLUSIVE: 45, 0: 46, probe.INTERVAL_REUSABLE: 44}
            return {
                "task_id": 91, "interval_id": interval_ids[hours], "hours": max(hours, 1), "period": "hours",
                "enabled": account["is_active"] and hours > 0, "provider_task": True, "kwargs_match": True,
                "no_cron": True, "sibling_unchanged": True,
                "exclusive_exists": hours == probe.INTERVAL_EXCLUSIVE or (leave_orphan and bool(current)),
            }

        requests = []
        def send(_fixture, method, path, value=None):
            assert path == "/api/m3u/accounts/11/"
            if method == "PATCH":
                assert set(value) == {
                    "name", "server_url", "username", "user_agent", "server_group", "cron_expression",
                    "refresh_interval", "max_streams", "is_active", "account_type", "stale_stream_days",
                    "priority", "enable_vod", "hash_key",
                }
                assert value["is_active"] == account["is_active"] and value["cron_expression"] == ""
                requests.append((value["is_active"], value["refresh_interval"]))
                current.append(value["refresh_interval"])
                account["refresh_interval"] = value["refresh_interval"]
            payload = {
                **body(False, True), **account, **related, "server_url": probe.RENAME_URL,
                "username": probe.RENAME_USERNAME, "enable_vod": True, "hash_key": "name,url",
                "custom_properties": probe.RENAME_PROPERTIES, "cron_expression": "",
            }
            payload["profiles"].append({**profile, "is_default": True})
            return 200, payload

        def safe_account(*_):
            return {
                "name": account["name"], "server_url_matches": True, "username_matches": True,
                "password_matches": True, "default_profiles": [profile],
            }

        with patch.multiple(
            probe, request=send, setup_interval_case=setup, interval_snapshot=task,
            rename_sql=safe_account, preferences_snapshot=lambda *_: deepcopy(account),
            snapshot=lambda *_: relations(False, True),
        ):
            result = []
            probe.verify_interval(IntervalFixture(), "/api/m3u/accounts/11/", IDS, result)
        self.assertEqual(requests, [
            (active, hours) for active in (True, False)
            for hours in (probe.INTERVAL_EXCLUSIVE, 0, probe.INTERVAL_REUSABLE)
        ])
        return result

    def test_active_and_inactive_interval_changes_preserve_task_identity_and_sharing(self):
        result = self.execute_interval_edits()
        self.assertEqual(len(result), 6)
        for attempt in result:
            self.assertEqual(attempt["status"], 200)
            self.assertTrue(all(attempt["checks"].values()) and all(attempt["followup"].values()))
            self.assertTrue(all(passed for key, passed in attempt.items() if key.startswith("sql_")))

    def test_interval_orphan_surviving_a_200_is_detected(self):
        result = self.execute_interval_edits(leave_orphan=True)
        failures = [entry for entry in result if not entry["sql_orphan_removed"]]
        self.assertEqual([entry["hours"] for entry in failures], [0, probe.INTERVAL_REUSABLE] * 2)
        self.assertTrue(all(entry["status"] == 200 for entry in failures))


    def execute_daily_cron(self, *, wrong_timezone=False, leave_orphan=False):
        account = {"name": "Owned PATCH target", "refresh_task_id": 91, "account_type": "XC"}
        profile = {"id": 30, "name": "Owned PATCH target Default"}
        related = {"user_agent": 41, "server_group": 51}

        class CronFixture:
            def sql(self, source, *, secret):
                assert secret and probe.RENAME_SEED in source
                return related

        schedules = {"task": 91, "original": 43, "reuse_interval": 42, "reuse_cron": 44}
        observed = []

        def setup(_fixture, ids, active):
            assert ids == IDS
            account.update(refresh_interval=probe.INTERVAL_EXCLUSIVE, is_active=active,
                           status="success" if active else "disabled")
            return {**schedules, "sibling_cron": None if active else 44}

        def task(_fixture, account_id, setup_values):
            assert account_id == IDS["account"]
            hours = account["refresh_interval"]
            initial = hours == probe.INTERVAL_EXCLUSIVE
            uses_cron = hours == 0
            return {
                "task_id": 91, "interval_id": 43 if initial else None if uses_cron else 42,
                "crontab_id": 44 if uses_cron else None, "hours": None if uses_cron else hours,
                "cron": probe.DAILY_CRON if uses_cron else "",
                "timezone": ("UTC" if wrong_timezone else probe.CRON_TIMEZONE) if uses_cron else None,
                "period": None if uses_cron else "hours",
                "enabled": account["is_active"], "provider_task": True, "kwargs_match": True,
                "sibling_unchanged": True, "old_interval_exists": initial,
                "reuse_interval_exists": True,
                "cron_exists": initial or uses_cron or setup_values["sibling_cron"] is not None or leave_orphan,
            }

        def safe_account(*_):
            return {
                "name": account["name"], "server_url_matches": True, "username_matches": True,
                "password_matches": True, "default_profiles": [profile],
            }

        def send(_fixture, method, path, value=None):
            assert path == "/api/m3u/accounts/11/"
            if method == "PATCH":
                assert "password" not in value and "exp_date" not in value
                observed.append((value["is_active"], value["refresh_interval"], value["cron_expression"]))
                account["refresh_interval"] = value["refresh_interval"]
            payload = {
                **body(False, True), **account, **related, "server_url": probe.RENAME_URL,
                "username": probe.RENAME_USERNAME, "enable_vod": True, "hash_key": "name,url",
                "custom_properties": probe.RENAME_PROPERTIES, "cron_expression": observed[-1][2],
            }
            payload["profiles"].append({**profile, "is_default": True})
            return 200, payload

        with patch.multiple(
            probe, request=send, setup_daily_cron_case=setup, cron_transition_snapshot=task,
            rename_sql=safe_account, preferences_snapshot=lambda *_: deepcopy(account),
            snapshot=lambda *_: relations(False, True),
        ):
            result = []
            probe.verify_daily_cron(CronFixture(), "/api/m3u/accounts/11/", IDS, result)
        self.assertEqual(observed, [
            (active, hours, cron) for active in (True, False)
            for hours, cron in ((0, probe.DAILY_CRON), (6, ""))
        ])
        return result

    def test_daily_cron_round_trips_keep_the_task_and_configured_timezone(self):
        for attempt in self.execute_daily_cron():
            self.assertEqual(attempt["status"], attempt["followup_status"])
            self.assertEqual(attempt["status"], 200)
            self.assertTrue(all(attempt["checks"].values()) and all(attempt["followup"].values()))
            self.assertTrue(all(passed for key, passed in attempt.items() if key.startswith("sql_")))

    def test_daily_cron_detects_wrong_zone_and_leftover_orphan_despite_200(self):
        result = self.execute_daily_cron(wrong_timezone=True, leave_orphan=True)
        self.assertTrue(all(attempt["status"] == 200 for attempt in result))
        self.assertEqual([attempt["sql_schedule_and_timezone_match"] for attempt in result],
                         [False, True, False, True])
        self.assertEqual([attempt["sql_orphans_removed"] for attempt in result],
                         [True, False, True, True])


class WorkflowBindingTests(unittest.TestCase):
    def test_selected_context_dispatches_the_same_probe_and_quiets_original_beat(self):
        for backend in ("Original", "Native"):
            with self.subTest(backend=backend):
                actions = []

                class BoundFixture:
                    binding = {"execution_contexts": {backend: [probe.CASE]}}
                    functional_budget_expired = staticmethod(lambda: False)
                    actual_isolation_valid = staticmethod(lambda: True)
                    retained_native_case = retained_source_case = staticmethod(lambda name: None)

                    @contextmanager
                    def independent_context(self, name, *, start_consumers):
                        self.case = name
                        actions.append(("context", name, start_consumers))
                        yield self

                    def role(self, name, args, *, stop):
                        actions.append(("role", name, stop))

                fixture = BoundFixture()
                fixture.backend = backend

                def run_selected(f, write):
                    actions.append(("probe", f.backend))
                    result = {"passed": True}
                    write(result)
                    return result

                with TemporaryDirectory() as tmp, patch.object(probe, "run", run_selected):
                    output = Path(tmp) / backend
                    verdict = workflow.Workflow(fixture, output).run()
                    self.assertTrue(verdict["executed_slice_passed"])
                    self.assertEqual(verdict["cases"], [{"case": probe.CASE, "status": "passed"}])
                    self.assertTrue((output / (probe.CASE + ".json")).is_file())
                expected = [("context", probe.CASE, True)]
                if backend == "Original":
                    expected.append(("role", "beat", True))
                self.assertEqual(actions, [*expected, ("probe", backend)])


if __name__=="__main__":
    assert hasattr(workflow.Workflow,"xc_account_group_patch")
    unittest.main()
